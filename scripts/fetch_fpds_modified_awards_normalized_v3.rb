#!/usr/bin/env ruby

require 'date'
require 'open-uri'
require 'nokogiri'
require 'json'
require 'stringio'

require 'nori'
require 'sequel'
require 'logger'
require 'cgi'
require 'timeout'
require 'dotenv'
require 'amazing_print'
require 'digest'
require "addressable"
require 'openssl'

require_relative '../lib/parsers'
Dotenv.load(File.expand_path('../.env', __dir__))




# --- Configuration ---
JOB_NAME               = "fpds_daily_ingestion".freeze # Identifier for the job tracker
DEFAULT_DAYS_BACK      = 2 # Used only if no previous state is found
FETCH_TIMEOUT_SECONDS  = 4 * 60 * 60 # 4 hours, adjust as needed
MODEL_PROCESSING_ORDER = [:vendors, :agencies, :pscs, :naics, :offices].freeze
BUSINESS_KEYS          = { vendors: :uei_sam, agencies: :agency_code, offices: :office_code, pscs: :psc_code, naics: :naics_code }.freeze

LOG       = Logger.new(STDOUT)
LOG.level = Logger::INFO

require_relative '../lib/database'

# --- Database Connection ---
begin
  DB = Database.connect(logger: LOG)
rescue StandardError
  exit 1
end

# --- Parsing & Sanitization Helpers ---


include Parsers

# --- Database Setup (with Job Tracker table) ---
def setup_database(db, logger)
	logger.info "Setting up database tables..."
	Sequel.extension :migration

	# Rename existing FPDS-related tables to use fpds_ prefix if needed
	prefix_renames = {
		vendors: :fpds_vendors,
		agencies: :fpds_agencies,
		government_offices: :fpds_government_offices,
		product_or_service_codes: :fpds_product_or_service_codes,
		naics_codes: :fpds_naics_codes,
		contract_actions: :fpds_contract_actions
	}

	prefix_renames.each do |old_name, new_name|
		begin
			if db.table_exists?(old_name) && !db.table_exists?(new_name)
				logger.warn "Renaming table #{old_name} -> #{new_name} to add source prefix."
				db.rename_table(old_name, new_name)
			end
		rescue => e
			logger.error "Failed to rename table #{old_name} to #{new_name}: #{e.message}"
		end
	end

	unless db.table_exists?(:fpds_vendors)
		db.create_table(:fpds_vendors) do
			primary_key :id
			String :uei_sam, unique: true, null: true, size: 12 # UEI can be null if vendor is not identifiable by it
			String :vendor_name, null: false, text: true
			DateTime :created_at, default: Sequel::CURRENT_TIMESTAMP
			DateTime :updated_at, default: Sequel::CURRENT_TIMESTAMP
			index :uei_sam
		end
		logger.info "Created table: fpds_vendors"
	end

	unless db.table_exists?(:fpds_agencies)
		db.create_table(:fpds_agencies) do
			primary_key :id
			String :agency_code, unique: true, null: false
			String :agency_name, text: true
			DateTime :created_at, default: Sequel::CURRENT_TIMESTAMP
			DateTime :updated_at, default: Sequel::CURRENT_TIMESTAMP
			index :agency_code
		end
		logger.info "Created table: fpds_agencies"
	end

	unless db.table_exists?(:fpds_government_offices)
		db.create_table(:fpds_government_offices) do
			primary_key :id
			String :office_code, unique: true, null: false
			String :office_name, text: true
			foreign_key :agency_id, :fpds_agencies, null: true # Office might be known before its agency is clear, or agency is not in our DB
			DateTime :created_at, default: Sequel::CURRENT_TIMESTAMP
			DateTime :updated_at, default: Sequel::CURRENT_TIMESTAMP
			index :office_code
		end
		logger.info "Created table: fpds_government_offices"
	end

	unless db.table_exists?(:fpds_product_or_service_codes)
		db.create_table(:fpds_product_or_service_codes) do
			primary_key :id
			String :psc_code, unique: true, null: false
			Text :psc_description
			DateTime :created_at, default: Sequel::CURRENT_TIMESTAMP
			DateTime :updated_at, default: Sequel::CURRENT_TIMESTAMP
			index :psc_code
		end
		logger.info "Created table: fpds_product_or_service_codes"
	end

	unless db.table_exists?(:fpds_naics_codes)
		db.create_table(:fpds_naics_codes) do
			primary_key :id
			String :naics_code, unique: true, null: false
			Text :naics_description
			DateTime :created_at, default: Sequel::CURRENT_TIMESTAMP
			DateTime :updated_at, default: Sequel::CURRENT_TIMESTAMP
			index :naics_code
		end
		logger.info "Created table: fpds_naics_codes"
	end

	unless db.table_exists?(:fpds_contract_actions)
		db.create_table(:fpds_contract_actions) do
			primary_key :id
			String :atom_entry_id, null: false, type: :varchar, size: 64, unique: true
			String :atom_title, text: true
			DateTime :atom_feed_modified_date # From <entry><modified>
			String :piid, null: false # Can be non-unique if modNumber differs
			String :modification_number, default: '0'

			foreign_key :vendor_id, :fpds_vendors, null: true # Allow null if vendor cannot be identified/created
			foreign_key :agency_id, :fpds_agencies # Contracting Office Agency ID

			DateTime :effective_date
			Date :last_date_to_order
			Date :completion_date
			Float :obligated_amount # Already present in user's original code, added here for completeness
			Float :base_and_all_options_value
			Float :total_estimated_order_value

			foreign_key :contracting_office_id, :fpds_government_offices, null: true
			foreign_key :funding_agency_id, :fpds_agencies, null: true
			foreign_key :funding_office_id, :fpds_government_offices, null: true
			foreign_key :product_or_service_code_id, :fpds_product_or_service_codes, null: true
			foreign_key :naics_code_id, :fpds_naics_codes, null: true

			Text :description_of_requirement
			String :action_type_code
			String :action_type_description
			String :pricing_type_code
			String :pricing_type_description
			DateTime :fpds_last_modified_date # From <transactionInformation><lastModifiedDate>

			String :raw_xml_content_sha256, size: 64
			String :reason_for_modification, text: true
			column :atom_content, :jsonb

			DateTime :fetched_at, default: Sequel::CURRENT_TIMESTAMP
			DateTime :db_updated_at, default: Sequel::CURRENT_TIMESTAMP

			index [:piid, :modification_number] # Common query pattern
			index :raw_xml_content_sha256
			index :fpds_last_modified_date
		end
		logger.info "Created table: fpds_contract_actions"
	end

	unless db.table_exists?(:job_tracker)
		db.create_table(:job_tracker) do
			primary_key :id
			String :job_name, unique: true, null: false
			String :status, default: 'idle'
			DateTime :last_successful_run_start_time
			DateTime :last_attempted_run_start_time
			Text :next_page_url
			Text :notes
			DateTime :updated_at
		end
		logger.info "Created table: job_tracker"
	end
	logger.info "Database schema check/creation complete."
end

setup_database(DB, LOG)

# --- Sequel Models ---
class Vendor < Sequel::Model(:fpds_vendors); end

class Agency < Sequel::Model(:fpds_agencies); end

class GovernmentOffice < Sequel::Model(:fpds_government_offices); end

class ProductOrServiceCode < Sequel::Model(:fpds_product_or_service_codes); end

class NaicsCode < Sequel::Model(:fpds_naics_codes); end

class ContractAction < Sequel::Model(:fpds_contract_actions); end

class JobTracker < Sequel::Model(:job_tracker); end

# --- Data Saving Logic ---
def process_page_batch(entries_xml_strings, logger)
	page_lookups = {
	vendors:      {}, agencies: {}, offices: {}, pscs: {}, naics: {},
	entry_hashes: {} # Maps entry_hash_id to its parsed data { doc:, title:, modified: }
	}

	processed_entry_count_stage1 = 0
	# === Stage 1: Parse all entries and collect unique identifiers for lookup tables ===
	entries_xml_strings.each_with_index do |entry_xml, idx|
		begin
			doc = Nokogiri::XML(entry_xml)
			doc.remove_namespaces!
			# nori_hash = Nori.new({
			#                      strip_namespaces:              true,
			#                      delete_namespace_attributes:   false,
			#                      convert_tags_to:               'underscore',
			#                      convert_attributes_to:         nil,
			#                      empty_tag_value:               nil,
			#                      advanced_typecasting:          true,
			#                      convert_dashes_to_underscores: true,
			#                      scrub_xml:                     true,
			#                      parser:                        :nokogiri
			#                      }).parse entry_xml
			# puts nori_hash
		rescue Nokogiri::XML::SyntaxError => e
			logger.error "Malformed XML for entry #{idx + 1} on page. Error: #{e.message}. Skipping this entry."
			next
		end

		title           = doc.at_xpath('//title')&.text&.strip
		modified_dt_str = doc.at_xpath('//modified')&.text&.strip
		modified_dt     = parse_datetime(modified_dt_str)

		unless title && !title.empty? && modified_dt
			logger.warn "Skipping entry #{idx + 1} due to missing title ('#{title}') or modified_dt ('#{modified_dt_str}')."
			next
		end

		key_str       = "#{title}-#{modified_dt.iso8601(3)}" # Ensure modified_dt is a DateTime object
		entry_hash_id = Digest::SHA256.hexdigest(key_str)

		content_node = doc.at_xpath('//content/*')
		unless content_node
			logger.warn "Skipping entry with hash_id #{entry_hash_id} (title: #{title}) due to missing content/award or content/IDV node."
			next
		end
		processed_entry_count_stage1               += 1
		page_lookups[:entry_hashes][entry_hash_id] = { doc: doc, title: title, modified: modified_dt, content_node: content_node }

		# Collect IDs for lookup tables
		uei                         = content_node.at_xpath('//vendorSiteDetails/entityIdentifiers/vendorUEIInformation/UEI')&.text&.strip
		page_lookups[:vendors][uei] = { name: content_node.at_xpath('//vendorHeader/vendorName')&.text&.strip || 'N/A' } if uei && !uei.empty?

		ca_code                          = content_node.at_xpath('//purchaserInformation/contractingOfficeAgencyID')&.text&.strip
		page_lookups[:agencies][ca_code] = { name: content_node.at_xpath('//purchaserInformation/contractingOfficeAgencyID/@name')&.text&.strip } if ca_code && !ca_code.empty?

		fa_code                          = content_node.at_xpath('//purchaserInformation/fundingRequestingAgencyID')&.text&.strip
		page_lookups[:agencies][fa_code] = { name: content_node.at_xpath('//purchaserInformation/fundingRequestingAgencyID/@name')&.text&.strip } if fa_code && !fa_code.empty? && !page_lookups[:agencies].key?(fa_code)

		co_code = content_node.at_xpath('//purchaserInformation/contractingOfficeID')&.text&.strip
		if co_code && !co_code.empty?
			page_lookups[:offices][co_code] = {
			name:        content_node.at_xpath('//purchaserInformation/contractingOfficeID/@name')&.text&.strip,
			agency_code: content_node.at_xpath('//purchaserInformation/contractingOfficeAgencyID')&.text&.strip
			}
		end

		fo_code = content_node.at_xpath('//purchaserInformation/fundingRequestingOfficeID')&.text&.strip
		if fo_code && !fo_code.empty? && !page_lookups[:offices].key?(fo_code) # Avoid overwriting if co_code was same
			page_lookups[:offices][fo_code] = {
			name:        content_node.at_xpath('//purchaserInformation/fundingRequestingOfficeID/@name')&.text&.strip,
			agency_code: content_node.at_xpath('//purchaserInformation/fundingRequestingAgencyID')&.text&.strip
			}
		end

		psc_node = content_node.at_xpath('//productOrServiceInformation/productOrServiceCode')
		if psc_node
			psc_code                      = psc_node.text&.strip
			page_lookups[:pscs][psc_code] = { description: psc_node['description']&.strip } if psc_code && !psc_code.empty?
		end

		naics_node = content_node.at_xpath('//productOrServiceInformation/principalNAICSCode')
		if naics_node
			naics_code                       = naics_node.text&.strip
			page_lookups[:naics][naics_code] = { description: naics_node['description']&.strip } if naics_code && !naics_code.empty?
		end
	end
	logger.info "Stage 1: Parsed #{processed_entry_count_stage1} entries from page. Found #{page_lookups[:entry_hashes].size} unique entry_hashes initially."
	return 0 if page_lookups[:entry_hashes].empty?

	# === Stage 2: Bulk check which items already exist in the DB (by atom_entry_id) ===
	existing_entry_hashes = ContractAction.where(atom_entry_id: page_lookups[:entry_hashes].keys).select_map(:atom_entry_id)
	page_lookups[:entry_hashes].reject! { |k, _v| existing_entry_hashes.include?(k) }
	logger.info "Stage 2: After duplicate check, #{page_lookups[:entry_hashes].size} new entries to process."
	return 0 if page_lookups[:entry_hashes].empty?

	# Pre-fetch existing lookup table items to minimize DB queries later
	id_caches = {
	vendors:  Vendor.where(uei_sam: page_lookups[:vendors].keys.compact).all.to_h { |v| [v.uei_sam, v.id] },
	agencies: Agency.where(agency_code: page_lookups[:agencies].keys.compact).all.to_h { |a| [a.agency_code, a.id] },
	offices:  GovernmentOffice.where(office_code: page_lookups[:offices].keys.compact).all.to_h { |o| [o.office_code, o.id] },
	pscs:     ProductOrServiceCode.where(psc_code: page_lookups[:pscs].keys.compact).all.to_h { |p| [p.psc_code, p.id] },
	naics:    NaicsCode.where(naics_code: page_lookups[:naics].keys.compact).all.to_h { |n| [n.naics_code, n.id] }
	}

	new_lookups_to_create = { vendors: [], agencies: [], offices: [], pscs: [], naics: [] }
	actions_to_create     = []

	DB.transaction do
		# === Stage 3: Collect details for NEW lookup items from the NEW entries ===
		page_lookups[:entry_hashes].each do |entry_hash_id, entry_data|
			content_node = entry_data[:content_node] # Use pre-parsed content_node

			# Vendor
			uei = content_node.at_xpath('//vendorSiteDetails/entityIdentifiers/vendorUEIInformation/UEI')&.text&.strip
			if uei && !uei.empty? && !id_caches[:vendors].key?(uei)
				vendor_name = content_node.at_xpath('//vendorHeader/vendorName')&.text&.strip || 'N/A'
				new_lookups_to_create[:vendors] << { uei_sam: uei, vendor_name: vendor_name }
				id_caches[:vendors][uei] = :placeholder # Mark for creation
			end

			# Agencies
			[
			{ code_node: content_node.at_xpath('//purchaserInformation/contractingOfficeAgencyID') },
			{ code_node: content_node.at_xpath('//purchaserInformation/fundingRequestingAgencyID') }
			].compact.each do |agency_spec|
				node = agency_spec[:code_node]
				next unless node
				code = node.text&.strip
				if code && !code.empty? && !id_caches[:agencies].key?(code)
					name = node['name']&.strip
					new_lookups_to_create[:agencies] << { agency_code: code, agency_name: name }
					id_caches[:agencies][code] = :placeholder
				end
			end

			# Offices
			[
			{ office_node:     content_node.at_xpath('//purchaserInformation/contractingOfficeID'),
			  agency_code_val: content_node.at_xpath('//purchaserInformation/contractingOfficeAgencyID')&.text&.strip },
			{ office_node:     content_node.at_xpath('//purchaserInformation/fundingRequestingOfficeID'),
			  agency_code_val: content_node.at_xpath('//purchaserInformation/fundingRequestingAgencyID')&.text&.strip }
			].each do |office_spec|
				office_node            = office_spec[:office_node]
				agency_code_for_office = office_spec[:agency_code_val]
				next unless office_node && agency_code_for_office && !agency_code_for_office.empty?

				office_code = office_node.text&.strip
				if office_code && !office_code.empty? && !id_caches[:offices].key?(office_code)
					office_name = office_node['name']&.strip
					new_lookups_to_create[:offices] << { office_code: office_code, office_name: office_name, agency_code: agency_code_for_office }
					id_caches[:offices][office_code] = :placeholder
				end
			end

			# PSCs
			psc_node = content_node.at_xpath('//productOrServiceInformation/productOrServiceCode')
			if psc_node
				psc_code = psc_node.text&.strip
				if psc_code && !psc_code.empty? && !id_caches[:pscs].key?(psc_code)
					psc_desc = psc_node['description']&.strip
					new_lookups_to_create[:pscs] << { psc_code: psc_code, psc_description: psc_desc }
					id_caches[:pscs][psc_code] = :placeholder
				end
			end

			# NAICS
			naics_node = content_node.at_xpath('//productOrServiceInformation/principalNAICSCode')
			if naics_node
				naics_code = naics_node.text&.strip
				if naics_code && !naics_code.empty? && !id_caches[:naics].key?(naics_code)
					naics_desc = naics_node['description']&.strip
					new_lookups_to_create[:naics] << { naics_code: naics_code, naics_description: naics_desc }
					id_caches[:naics][naics_code] = :placeholder
				end
			end
		end
		logger.info "Stage 3: Collected details for new lookup items."

		# === Stage 4: Bulk insert new lookup items and update caches ===

		MODEL_PROCESSING_ORDER.each do |model_key|
			records_to_process = new_lookups_to_create[model_key]
			next if records_to_process.nil? || records_to_process.empty?

			class_name_str      = model_key.to_s.chomp('s').split('_').map(&:capitalize).join
			# Fix for special cases where the singular form doesn't match the class name
			class_name_str      = "Agency" if class_name_str == "Agencie"
			class_name_str      = "ProductOrServiceCode" if class_name_str == "Psc"
			class_name_str      = "NaicsCode" if class_name_str == "Naic"
			class_name_str      = "GovernmentOffice" if class_name_str == "Office"
			model_class         = Object.const_get(class_name_str)
			unique_business_key = BUSINESS_KEYS[model_key]

			# Deduplicate records based on the business unique key before attempting to insert
			# Ensure all records have the unique_key and it's not empty
			unique_payloads = records_to_process.filter { |r| r.key?(unique_business_key) && r[unique_business_key] && !r[unique_business_key].to_s.strip.empty? }
			                                    .uniq { |r| r[unique_business_key] }

			if model_key == :offices
				unique_payloads = unique_payloads.map do |r|
					agency_id          = id_caches[:agencies][r[:agency_code]] # agency_code should exist from collection
					resolved_agency_id = agency_id.is_a?(Integer) ? agency_id : nil
					if (agency_id == :placeholder || agency_id.nil?) && r[:agency_code]
						logger.warn "Could not resolve agency_id for office #{r[:office_code]} with agency_code #{r[:agency_code]}. Setting agency_id to NULL."
					end
					r.except(:agency_code).merge(agency_id: resolved_agency_id)
				end
			end

			next if unique_payloads.empty?

			begin
				model_class.multi_insert(unique_payloads, commit_every: 100, return: :primary_key)
				logger.info "Batch inserted #{unique_payloads.size} new #{model_key}."

				# Refresh cache for newly inserted items
				# Query back the inserted records to get their DB IDs and update id_caches
				inserted_unique_values = unique_payloads.map { |r| r[unique_business_key] }.compact.uniq
				unless inserted_unique_values.empty?
					model_class.where(unique_business_key => inserted_unique_values).all.each do |rec|
						id_caches[model_key][rec[unique_business_key]] = rec[model_class.primary_key]
					end
				end
			rescue Sequel::UniqueConstraintViolation => e
				logger.error "Unique constraint violation during multi_insert for #{model_key}: #{e.message}. Some records might already exist or there are duplicates in batch not caught by pre-filter."
				# Attempt to refresh cache even on error, as some might have been inserted before error or by another process
				inserted_unique_values = unique_payloads.map { |r| r[unique_business_key] }.compact.uniq
				unless inserted_unique_values.empty?
					model_class.where(unique_business_key => inserted_unique_values).all.each do |rec|
						id_caches[model_key][rec[unique_business_key]] = rec[model_class.primary_key]
					end
				end
			rescue StandardError => e
				logger.error "Error during multi_insert for #{model_key}: #{e.message}"
				# Potentially re-raise or handle more gracefully
				raise e # Re-raise to ensure transaction rollback if critical
			end
		end
		logger.info "Stage 4: Bulk inserted new lookup items and updated caches."

		# === Stage 5: Build final contract_actions batch with all FKs resolved ===
		page_lookups[:entry_hashes].each do |entry_hash_id, entry_data|
			# Loop over NEW entries AGAIN
			content_node = entry_data[:content_node]

			uei       = content_node.at_xpath('//vendorSiteDetails/entityIdentifiers/vendorUEIInformation/UEI')&.text&.strip
			vendor_id = (uei && !uei.empty?) ? id_caches[:vendors][uei] : nil

			logger.debug "Attempting to build contract action for entry_hash_id: #{entry_hash_id}, UEI: #{uei}, Resolved vendor_id: #{vendor_id.inspect}"

			unless vendor_id.is_a?(Integer)
				logger.warn "Skipping contract action for entry_hash_id #{entry_hash_id} (title: #{entry_data[:title]}) due to unresolved or missing vendor_id (UEI: #{uei})."
				next
			end

			# Contracting Office Agency (primary agency for the contract)
			ca_code   = content_node.at_xpath('//purchaserInformation/contractingOfficeAgencyID')&.text&.strip
			agency_id = (ca_code && !ca_code.empty?) ? id_caches[:agencies][ca_code] : nil
			unless agency_id.is_a?(Integer)
				logger.warn "Contract action for entry_hash_id #{entry_hash_id} will have NULL agency_id (Contracting Office Agency Code: #{ca_code})."
			end

			# Resolve other FKs
			co_code               = content_node.at_xpath('//purchaserInformation/contractingOfficeID')&.text&.strip
			contracting_office_id = (co_code && !co_code.empty?) ? id_caches[:offices][co_code] : nil

			fa_code_val       = content_node.at_xpath('//purchaserInformation/fundingRequestingAgencyID')&.text&.strip
			funding_agency_id = (fa_code_val && !fa_code_val.empty?) ? id_caches[:agencies][fa_code_val] : nil

			fo_code_val       = content_node.at_xpath('//purchaserInformation/fundingRequestingOfficeID')&.text&.strip
			funding_office_id = (fo_code_val && !fo_code_val.empty?) ? id_caches[:offices][fo_code_val] : nil

			psc_code_val               = content_node.at_xpath('//productOrServiceInformation/productOrServiceCode')&.text&.strip
			product_or_service_code_id = (psc_code_val && !psc_code_val.empty?) ? id_caches[:pscs][psc_code_val] : nil

			naics_code_val = content_node.at_xpath('//productOrServiceInformation/principalNAICSCode')&.text&.strip
			naics_code_id  = (naics_code_val && !naics_code_val.empty?) ? id_caches[:naics][naics_code_val] : nil

			content_xml_string = content_node.to_s
			raw_xml_sha256     = Digest::SHA256.hexdigest(content_xml_string)

			# Parse atom content with Nori for reasonForModification and jsonb storage
			begin
				nori_hash = Nori.new({
					strip_namespaces:              true,
					delete_namespace_attributes:   false,
					convert_tags_to:               'underscore',
					convert_attributes_to:         nil,
					empty_tag_value:               nil,
					advanced_typecasting:          true,
					convert_dashes_to_underscores: true,
					scrub_xml:                     true,
					parser:                        :nokogiri
				}).parse(content_xml_string)

				# Extract reasonForModification from the parsed hash
				reason_for_modification = nori_hash.dig('award', 'contractData', 'reasonForModification') ||
				                         nori_hash.dig('award', 'contract_data', 'reason_for_modification') ||
				                         nori_hash.dig('idv', 'contractData', 'reasonForModification') ||
				                         nori_hash.dig('idv', 'contract_data', 'reason_for_modification') ||
				                         nori_hash.dig('contractData', 'reasonForModification') ||
				                         nori_hash.dig('contract_data', 'reason_for_modification')

				atom_content_json = nori_hash.to_json
			rescue => e
				logger.warn "Error parsing XML with Nori for entry #{entry_hash_id}: #{e.message}. Using nil values."
				reason_for_modification = nil
				atom_content_json = nil
			end

			actions_to_create << {
			atom_entry_id:           entry_hash_id,
			atom_title:              entry_data[:title],
			atom_feed_modified_date: entry_data[:modified],
			piid:                    content_node.at_xpath('//awardID/awardContractID/PIID | //contractID/IDVID/PIID')&.text&.strip || 'UNKNOWN_PIID',
			modification_number:     content_node.at_xpath('//awardID/awardContractID/modNumber | //contractID/IDVID/modNumber')&.text&.strip || '0',
			vendor_id:               vendor_id, # Integer or will fail FK constraint if table expects int
			agency_id: agency_id.is_a?(Integer) ? agency_id : nil, # Integer or nil

			obligated_amount: parse_float(content_node.at_xpath('//dollarValues/obligatedAmount')&.text),
			# signed_date field removed as it doesn't exist in the database schema
			effective_date:              parse_datetime(content_node.at_xpath('//relevantContractDates/effectiveDate')&.text),
			last_date_to_order:          parse_date(content_node.at_xpath('//relevantContractDates/lastDateToOrder')&.text),
			completion_date:             parse_date(content_node.at_xpath('//relevantContractDates/completionDate')&.text),
			base_and_all_options_value:  parse_float(content_node.at_xpath('//dollarValues/baseAndAllOptionsValue')&.text),
			total_estimated_order_value: parse_float(content_node.at_xpath('//dollarValues/totalEstimatedOrderValue')&.text),

			contracting_office_id:       contracting_office_id.is_a?(Integer) ? contracting_office_id : nil,
			funding_agency_id:           funding_agency_id.is_a?(Integer) ? funding_agency_id : nil,
			funding_office_id:           funding_office_id.is_a?(Integer) ? funding_office_id : nil,
			product_or_service_code_id:  product_or_service_code_id.is_a?(Integer) ? product_or_service_code_id : nil,
			naics_code_id:               naics_code_id.is_a?(Integer) ? naics_code_id : nil,

			description_of_requirement:  content_node.at_xpath('//contractData/descriptionOfContractRequirement')&.text&.strip,
			action_type_code:            content_node.at_xpath('//contractData/contractActionType')&.text&.strip,
			action_type_description:     content_node.at_xpath('//contractData/contractActionType/@description')&.text&.strip,
			pricing_type_code:           content_node.at_xpath('//contractData/typeOfContractPricing')&.text&.strip,
			pricing_type_description:    content_node.at_xpath('//contractData/typeOfContractPricing/@description')&.text&.strip,
			fpds_last_modified_date:     parse_datetime(content_node.at_xpath('//transactionInformation/lastModifiedDate')&.text),
			raw_xml_content_sha256:      raw_xml_sha256,
			reason_for_modification:     reason_for_modification,
			atom_content:                atom_content_json,
			# fetched_at and db_updated_at have defaults in DB schema
			}
		end
		logger.info "Stage 5: Built final contract_actions batch. Size: #{actions_to_create.size}."

		unless actions_to_create.empty?
			begin
				ContractAction.multi_insert(actions_to_create, commit_every: 100) # commit_every for large batches
				logger.info "Batch inserted #{actions_to_create.size} new contract actions."
			rescue Sequel::UniqueConstraintViolation => e
				logger.error "Unique constraint violation during ContractAction multi_insert: #{e.message}. This might happen if atom_entry_id was already processed in a rare edge case."
			rescue StandardError => e
				logger.error "Error during ContractAction multi_insert: #{e.message}"
				raise e # Re-raise to ensure transaction rollback
			end
		end
	end # End of DB.transaction

	actions_to_create.size
end

# --- Data Fetching Logic ---
def process_fpds_feed(start_url, logger)
	total_successful_saves = 0
	job_tracker            = JobTracker.find_or_create(job_name: JOB_NAME)
	current_url            = start_url
	max_retries            = 5 # Maximum number of retry attempts

	loop do
		logger.info "Fetching page: #{current_url}"
		xml_data_full_page = nil
		retry_count = 0
		success = false

		while !success && retry_count <= max_retries
			begin
				if retry_count > 0
					backoff_time = (2 ** retry_count) # Exponential backoff: 2, 4, 8, 16, 32 seconds
					logger.info "Retry attempt #{retry_count}/#{max_retries} for #{current_url}. Waiting #{backoff_time} seconds..."
					sleep(backoff_time)
				end

				job_tracker.update(next_page_url: current_url, status: 'running', updated_at: Time.now, last_attempted_run_start_time: job_tracker.last_attempted_run_start_time || Time.now) # Keep last_attempted consistent

				addr = Addressable::URI.parse(current_url)
                                response = URI.open(
                                        addr.normalize.to_s,
                                        "User-Agent" => "FPDS.me Client/2.0",
                                        open_timeout: 360,
                                        read_timeout: 360
                                )
				xml_data_full_page = response.read
				success = true
			rescue => e
				retry_count += 1
				logger.warn "Error fetching page (#{current_url}): #{e.message}. Attempt #{retry_count}/#{max_retries}"

				if retry_count > max_retries
					logger.error "CRITICAL ERROR: Maximum retry attempts reached for #{current_url}: #{e.message}"
					job_tracker.update(status: 'failed', notes: "Failed on URL: #{current_url} after #{max_retries} retries. Error: #{e.message}")
					raise
				end
			end
		end

		page_doc = Nokogiri::XML(xml_data_full_page)
		page_doc.remove_namespaces!
		entries_nodes = page_doc.xpath('//entry')
		logger.info "Found #{entries_nodes.size} entries on page."
		break if entries_nodes.empty?

		# Ensure entries_nodes are converted to XML strings
		saved_count            = process_page_batch(entries_nodes.map(&:to_xml), logger) # Use to_xml for cleaner entry XML
		total_successful_saves += saved_count

		next_link_node = page_doc.at_xpath('//link[@rel="next"]/@href')
		if next_link_node
			# Ensure the next URL is absolute and properly encoded
			begin
				base_uri_str = response.base_uri.to_s
				next_href = next_link_node.value.to_s

				# Use Addressable for safer URL handling
				base_uri = Addressable::URI.parse(base_uri_str)
				next_uri = Addressable::URI.parse(next_href)

				# If next_uri is relative, join with base_uri
				if next_uri.relative?
					current_url = (base_uri + next_uri).to_s
				else
					current_url = next_uri.to_s
				end

				logger.info "Next page URL: #{current_url}"
			rescue => e
				logger.error "Error parsing next URL: #{e.message}. Base: #{base_uri_str}, Next: #{next_href}"
				current_url = nil
			end
		else
			current_url = nil # No more pages
			logger.info "No next page link found. Reached end of feed."
		end
		break if current_url.nil?
	end

	logger.info "Finished fetching all available pages from starting point."
	total_successful_saves
end

# --- Main Execution ---
if __FILE__ == $0
	lock_file_path = File.join(__dir__, "#{File.basename(__FILE__)}.lock")
	lock_file      = File.open(lock_file_path, 'w')
	unless lock_file.flock(File::LOCK_EX | File::LOCK_NB)
		LOG.warn "Script is already running. Another instance holds the lock. Exiting."
		exit
	end

	LOG.info "Script starting up with exclusive lock."
	current_run_start_time = Time.now

	job_tracker = JobTracker.find_or_create(job_name: JOB_NAME)
	job_tracker.update(last_attempted_run_start_time: current_run_start_time, status: 'initializing') # Initial status

	start_url = nil
	begin
		if job_tracker.status == 'failed' && job_tracker.next_page_url
			LOG.warn "Previous run failed. Resuming from last known page: #{job_tracker.next_page_url}"
			start_url = job_tracker.next_page_url
			saved_count = process_fpds_feed(start_url, LOG)
		else
			# Check for missed days since last successful run
			saved_count = 0

			if job_tracker.last_successful_run_start_time
				last_successful_date = job_tracker.last_successful_run_start_time.to_date
				LOG.info "Last successful run was on: #{last_successful_date}"

				# Calculate missed days (days between last successful run and yesterday)
				missed_days = []
				current_date = last_successful_date + 1
				yesterday = Date.today - 1

				while current_date <= yesterday
					missed_days << current_date
					current_date += 1
				end

				if missed_days.empty?
					LOG.info "No missed days detected. Last run was recent."
				else
					LOG.info "Detected #{missed_days.length} missed days: #{missed_days.first} to #{missed_days.last}"

					# Process each missed day individually
					missed_days.each do |missed_date|
						LOG.info "Processing missed day: #{missed_date}"

						# Create URL for specific day (closed date range)
						start_str = missed_date.strftime('%Y/%m/%d')
						end_str = missed_date.strftime('%Y/%m/%d')

						uri = Addressable::URI.parse("https://www.fpds.gov/ezsearch/FEEDS/ATOM")
						uri.query_values = {
							"FEEDNAME" => "PUBLIC",
							"VERSION" => "1.5.3",
							"q" => "LAST_MOD_DATE:[#{start_str},#{end_str}]",
							"start" => "0"
						}
						day_url = uri.to_s

						# Process this specific day
						day_saved_count = process_fpds_feed(day_url, LOG)
						saved_count += day_saved_count
						LOG.info "Processed missed day #{missed_date}: saved #{day_saved_count} records"

						# Update job tracker after each day to track progress
						job_tracker.update(
							status: 'running',
							notes: "Processing missed day #{missed_date}. Saved #{day_saved_count} records for this day.",
							updated_at: Time.now
						)
					end

					LOG.info "Finished processing #{missed_days.length} missed days. Total saved: #{saved_count}"
				end
			else
				LOG.info "No previous successful run found. This appears to be the first run."
			end

			# Now process from yesterday onwards (current day logic)
			# Start with today's date and go back day by day until entries are found
			max_days_to_check = 30 # Safety limit to prevent infinite loop
			current_date = Date.today - 1 # Start from yesterday
			days_checked = 0
			entries_found = false

			while !entries_found && days_checked < max_days_to_check
				start_str = current_date.strftime('%Y/%m/%d')
				# FPDS ATOM feed requires a date range like [YYYY/MM/DD,YYYY/MM/DD] or [YYYY/MM/DD,] for open-ended
				# Using open-ended: LAST_MOD_DATE:[start_date,]

				# Use Addressable for safer URL handling
				uri = Addressable::URI.parse("https://www.fpds.gov/ezsearch/FEEDS/ATOM")
				uri.query_values = {
				  "FEEDNAME" => "PUBLIC",
				  "VERSION" => "1.5.3",
				  "q" => "LAST_MOD_DATE:[#{start_str},]",
				  "start" => "0"
				}
				temp_url = uri.to_s
				LOG.info "Checking for entries modified on #{current_date} (URL: #{temp_url})."

				retry_count = 0
				max_retries = 3 # Maximum retry attempts for initial check
				success = false

				while !success && retry_count <= max_retries
					begin
						if retry_count > 0
							backoff_time = (2 ** retry_count) # Exponential backoff
							LOG.info "Retry attempt #{retry_count}/#{max_retries} for initial check. Waiting #{backoff_time} seconds..."
							sleep(backoff_time)
						end

                                                response = URI.open(
                                                        temp_url,
                                                        "User-Agent" => "Ruby FPDS Client/2.0 (Resilient/Fast)",
                                                        open_timeout: 60,
                                                        read_timeout: 180
                                                )
						xml_data = response.read
						page_doc = Nokogiri::XML(xml_data)
						page_doc.remove_namespaces!
						entries_nodes = page_doc.xpath('//entry')
						success = true

						if entries_nodes.empty?
							LOG.info "No entries found for #{current_date}, trying previous day."
							current_date = current_date - 1
							days_checked += 1
						else
							LOG.info "Found #{entries_nodes.size} entries for #{current_date}."
							entries_found = true
							start_url = temp_url
						end
					rescue => e
						retry_count += 1
						LOG.warn "Error checking entries for #{current_date}: #{e.message}. Attempt #{retry_count}/#{max_retries}"

						if retry_count > max_retries
							LOG.error "Maximum retry attempts reached for initial check on #{current_date}. Moving to previous day."
							current_date = current_date - 1
							days_checked += 1
							break
						end
					end
				end
			end

			if !entries_found
				# If no entries found after checking multiple days, fall back to DEFAULT_DAYS_BACK
				LOG.warn "No entries found after checking #{days_checked} days. Falling back to DEFAULT_DAYS_BACK."
				start_date = Date.today - DEFAULT_DAYS_BACK
				start_str = start_date.strftime('%Y/%m/%d')

				# Use Addressable for safer URL handling
				uri = Addressable::URI.parse("https://www.fpds.gov/ezsearch/FEEDS/ATOM")
				uri.query_values = {
				  "FEEDNAME" => "PUBLIC",
				  "VERSION" => "1.5.3",
				  "q" => "LAST_MOD_DATE:[#{start_str},]",
				  "start" => "0"
				}
				start_url = uri.to_s
			end

			LOG.info "Starting current run for records modified since #{start_str} (URL: #{start_url})."

			# Process current data (from yesterday onwards)
			current_saved_count = process_fpds_feed(start_url, LOG)
			saved_count += current_saved_count
			LOG.info "Current run saved #{current_saved_count} records"
		end


		job_tracker.update(
		status:        'idle',
		notes:         "Run completed successfully at #{Time.now}. Saved #{saved_count} new records in this run.",
		next_page_url: nil, # Clear next_page_url on successful completion of the whole feed
		last_successful_run_start_time: current_run_start_time, # Mark this run's start as successful
		updated_at: Time.now
		)
		LOG.info "Run completed. Successfully saved #{saved_count} new unique awards in this run."

	rescue Timeout::Error
		LOG.error "Script timed out after #{FETCH_TIMEOUT_SECONDS / 3600} hours."
		job_tracker.update(status: 'failed', notes: "Run timed out at #{Time.now}. Last URL: #{job_tracker.next_page_url}")
	rescue StandardError => e
		LOG.fatal "A critical unhandled error terminated the run: #{e.message}"
		LOG.fatal e.backtrace.join("\n") if LOG.level <= Logger::DEBUG # Ensure backtrace is logged for debug
		# notes field in job_tracker already updated by process_fpds_feed on fetch error
		# If error is outside process_fpds_feed, update status here
		if job_tracker.status != 'failed' # If not already marked as failed by sub-method
			error_msg = e.message.length > 200 ? "#{e.message[0...197]}..." : e.message
			job_tracker.update(status: 'failed', notes: "Run failed with error: #{error_msg} at #{Time.now}. Last URL: #{job_tracker.next_page_url}")
		end
	ensure
		lock_file.flock(File::LOCK_UN)
		lock_file.close
		LOG.info "Script finished and lock released."
	end
end
