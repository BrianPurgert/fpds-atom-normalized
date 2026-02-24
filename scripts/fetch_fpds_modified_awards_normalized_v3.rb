#!/usr/bin/env ruby

require 'date'
require 'open-uri'
require 'nokogiri'
require 'json'
require 'stringio'
require 'optparse'

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
require 'concurrent'

require_relative '../lib/parsers'
require_relative '../lib/normalizer'
Dotenv.load(File.expand_path('../.env', __dir__))




# --- Configuration ---
JOB_NAME               = "fpds_daily_ingestion".freeze # Identifier for the job tracker
BACKFILL_JOB_NAME      = "fpds_backfill".freeze # Identifier for the backfill job tracker
DEFAULT_DAYS_BACK      = 2 # Used only if no previous state is found
FETCH_TIMEOUT_SECONDS  = 4 * 60 * 60 # 4 hours, adjust as needed
BACKFILL_EARLIEST_DATE = Date.new(1998, 1, 1) # FY2001 start — earliest FPDS data
DEFAULT_BACKFILL_THREADS = 4 # Number of concurrent day-fetching threads
MODEL_PROCESSING_ORDER = [:vendors, :agencies, :pscs, :naics, :offices].freeze
BUSINESS_KEYS          = { vendors: :uei_sam, agencies: :agency_code, offices: :office_code, pscs: :psc_code, naics: :naics_code }.freeze

LOG       = Logger.new(STDOUT)
LOG.level = Logger::INFO

require_relative '../lib/database'

# --- Database Connection ---
# Size the connection pool based on whether we're running backfill with threads.
# Peek at ARGV early so the pool is sized before Sequel models are defined.
_backfill_mode = ARGV.include?('--backfill') || ARGV.include?('--resume')
_thread_arg_idx = ARGV.index('--threads')
_thread_count = _thread_arg_idx ? (ARGV[_thread_arg_idx + 1]&.to_i || DEFAULT_BACKFILL_THREADS) : DEFAULT_BACKFILL_THREADS
_pool_size = _backfill_mode ? [_thread_count + 2, 6].max : 4
begin
  DB = Database.connect(logger: LOG, max_connections: _pool_size)
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
			DateTime :atom_feed_modified_date
			String :piid, null: false
			String :modification_number, default: '0'
			String :record_type, text: true

			foreign_key :vendor_id, :fpds_vendors, null: true
			foreign_key :agency_id, :fpds_agencies, null: true

			DateTime :effective_date
			Date :last_date_to_order
			Date :completion_date
			Float :obligated_amount
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
			DateTime :fpds_last_modified_date

			String :raw_xml_content_sha256, size: 64
			String :reason_for_modification, text: true
			column :atom_content, :jsonb

			# Award/Contract ID
			String :referenced_idv_piid, text: true
			String :referenced_idv_mod_number, text: true
			String :referenced_idv_agency_id, text: true
			String :transaction_number, text: true

			# Competition
			String :extent_competed, text: true
			String :solicitation_procedures, text: true
			String :type_of_set_aside, text: true
			String :type_of_set_aside_source, text: true
			String :evaluated_preference, text: true
			Integer :number_of_offers_received
			String :number_of_offers_source, text: true
			String :commercial_item_acquisition_procedures, text: true
			String :commercial_item_test_program, text: true
			String :a76_action, text: true
			String :fed_biz_opps, text: true
			String :local_area_set_aside, text: true
			String :fair_opportunity_limited_sources, text: true
			String :reason_not_competed, text: true
			String :competitive_procedures, text: true
			String :research, text: true
			String :small_business_competitiveness_demo, text: true
			String :idv_type_of_set_aside, text: true
			Integer :idv_number_of_offers_received

			# Contract Data
			String :cost_or_pricing_data, text: true
			String :contract_financing, text: true
			String :gfe_gfp, text: true
			String :sea_transportation, text: true
			String :undefinitized_action, text: true
			String :consolidated_contract, text: true
			String :performance_based_service_contract, text: true
			String :multi_year_contract, text: true
			String :contingency_humanitarian_peacekeeping_operation, text: true
			String :purchase_card_as_payment_method, text: true
			String :number_of_actions, text: true
			String :referenced_idv_type, text: true
			String :referenced_idv_multiple_or_single, text: true
			String :major_program_code, text: true
			String :national_interest_action_code, text: true
			String :cost_accounting_standards_clause, text: true
			String :inherently_governmental_function, text: true
			String :solicitation_id, text: true
			String :type_of_idc, text: true
			String :multiple_or_single_award_idc, text: true

			# Dollar Values
			Float :base_and_exercised_options_value
			Float :total_obligated_amount
			Float :total_base_and_all_options_value
			Float :total_base_and_exercised_options_value

			# Legislative Mandates
			String :clinger_cohen_act, text: true
			String :construction_wage_rate_requirements, text: true
			String :labor_standards, text: true
			String :materials_supplies_articles_equipment, text: true
			String :interagency_contracting_authority, text: true
			String :other_statutory_authority, text: true

			# Place of Performance
			String :pop_street_address, text: true
			String :pop_city, text: true
			String :pop_state_code, text: true
			String :pop_zip_code, text: true
			String :pop_country_code, text: true
			String :pop_congressional_district, text: true

			# Relevant Contract Dates
			Date :signed_date
			Date :current_completion_date
			Date :ultimate_completion_date

			# Transaction Information
			String :created_by, text: true
			DateTime :created_date
			String :last_modified_by, text: true
			String :transaction_status, text: true
			String :approved_by, text: true
			DateTime :approved_date
			String :closed_status, text: true
			String :closed_by, text: true
			DateTime :closed_date

			# Full Text Search vector
			column :fts_vector, :tsvector

			# Contract Marketing Data
			String :fee_paid_for_use_of_service, text: true
			String :who_can_use, text: true
			String :ordering_procedure, text: true
			String :individual_order_limit, text: true
			String :type_of_fee_for_use_of_service, text: true
			String :contract_marketing_email, text: true

			# Product/Service Info
			String :claimant_program_code, text: true
			String :contract_bundling, text: true
			String :country_of_origin, text: true
			String :information_technology_commercial_item_category, text: true
			String :manufacturing_organization_type, text: true
			String :place_of_manufacture, text: true
			String :recovered_material_clauses, text: true
			String :system_equipment_code, text: true
			String :use_of_epa_designated_products, text: true

			# Purchaser / Vendor / Preference
			String :foreign_funding, text: true
			String :contracting_officer_business_size_determination, text: true
			String :subcontract_plan, text: true

			# Generic Tags
			column :generic_strings, :jsonb
			column :generic_booleans, :jsonb

			DateTime :fetched_at, default: Sequel::CURRENT_TIMESTAMP
			DateTime :db_updated_at, default: Sequel::CURRENT_TIMESTAMP

			index [:piid, :modification_number]
			index :raw_xml_content_sha256
			index :fpds_last_modified_date
			index :record_type
		end
		logger.info "Created table: fpds_contract_actions"
	end

	unless db.table_exists?(:fpds_contract_vendor_details)
		db.create_table(:fpds_contract_vendor_details) do
			primary_key :id
			foreign_key :contract_action_id, :fpds_contract_actions, null: false, unique: true, on_delete: :cascade
			String :vendor_name, text: true
			String :vendor_alternate_name, text: true
			String :vendor_legal_organization_name, text: true
			String :vendor_doing_business_as_name, text: true
			TrueClass :vendor_enabled
			String :uei, text: true
			String :ultimate_parent_uei, text: true
			String :uei_legal_business_name, text: true
			String :ultimate_parent_uei_name, text: true
			String :cage_code, text: true
			String :street_address, text: true
			String :city, text: true
			String :state, text: true
			String :zip_code, text: true
			String :country_code, text: true
			String :phone_no, text: true
			String :fax_no, text: true
			String :congressional_district, text: true
			TrueClass :vendor_location_disabled_flag
			String :entity_data_source, text: true
			Date :registration_date
			Date :renewal_date
			String :vendor_alternate_site_code, text: true
			TrueClass :is_alaskan_native_owned_corporation_or_firm
			TrueClass :is_american_indian_owned
			TrueClass :is_indian_tribe
			TrueClass :is_native_hawaiian_owned_organization_or_firm
			TrueClass :is_tribally_owned_firm
			TrueClass :is_veteran_owned
			TrueClass :is_service_related_disabled_veteran_owned_business
			TrueClass :is_women_owned
			TrueClass :is_women_owned_small_business
			TrueClass :is_economically_disadvantaged_women_owned_small_business
			TrueClass :is_joint_venture_women_owned_small_business
			TrueClass :is_joint_venture_economically_disadvantaged_women_owned_small_business
			TrueClass :is_small_business
			TrueClass :is_very_small_business
			TrueClass :is_minority_owned
			TrueClass :is_subcontinent_asian_american_owned_business
			TrueClass :is_asian_pacific_american_owned_business
			TrueClass :is_black_american_owned_business
			TrueClass :is_hispanic_american_owned_business
			TrueClass :is_native_american_owned_business
			TrueClass :is_other_minority_owned
			TrueClass :is_community_developed_corporation_owned_firm
			TrueClass :is_labor_surplus_area_firm
			TrueClass :is_federal_government
			TrueClass :is_federally_funded_research_and_development_corp
			TrueClass :is_federal_government_agency
			TrueClass :is_state_government
			TrueClass :is_local_government
			TrueClass :is_city_local_government
			TrueClass :is_county_local_government
			TrueClass :is_inter_municipal_local_government
			TrueClass :is_local_government_owned
			TrueClass :is_municipality_local_government
			TrueClass :is_school_district_local_government
			TrueClass :is_township_local_government
			TrueClass :is_tribal_government
			TrueClass :is_foreign_government
			TrueClass :is_corporate_entity_not_tax_exempt
			TrueClass :is_corporate_entity_tax_exempt
			TrueClass :is_partnership_or_limited_liability_partnership
			TrueClass :is_sole_proprietorship
			TrueClass :is_small_agricultural_cooperative
			TrueClass :is_international_organization
			TrueClass :is_us_government_entity
			TrueClass :is_dot_certified_disadvantaged_business_enterprise
			TrueClass :is_self_certified_small_disadvantaged_business
			TrueClass :is_sba_certified_small_disadvantaged_business
			TrueClass :is_sba_certified_8a_program_participant
			TrueClass :is_self_certified_hubzone_joint_venture
			TrueClass :is_sba_certified_hubzone
			TrueClass :is_sba_certified_8a_joint_venture
			String :organizational_type, text: true
			TrueClass :is_sheltered_workshop
			TrueClass :is_limited_liability_corporation
			TrueClass :is_subchapter_s_corporation
			TrueClass :is_foreign_owned_and_located
			String :country_of_incorporation, text: true
			String :state_of_incorporation, text: true
			TrueClass :is_for_profit_organization
			TrueClass :is_nonprofit_organization
			TrueClass :is_other_not_for_profit_organization
			TrueClass :is_1862_land_grant_college
			TrueClass :is_1890_land_grant_college
			TrueClass :is_1994_land_grant_college
			TrueClass :is_historically_black_college_or_university
			TrueClass :is_minority_institution
			TrueClass :is_private_university_or_college
			TrueClass :is_school_of_forestry
			TrueClass :is_state_controlled_institution_of_higher_learning
			TrueClass :is_tribal_college
			TrueClass :is_veterinary_college
			TrueClass :is_alaskan_native_servicing_institution
			TrueClass :is_native_hawaiian_servicing_institution
			TrueClass :is_airport_authority
			TrueClass :is_council_of_governments
			TrueClass :is_housing_authorities_public_or_tribal
			TrueClass :is_interstate_entity
			TrueClass :is_planning_commission
			TrueClass :is_port_authority
			TrueClass :is_transit_authority
			TrueClass :receives_contracts
			TrueClass :receives_grants
			TrueClass :receives_contracts_and_grants
			DateTime :created_at, default: Sequel::CURRENT_TIMESTAMP
			DateTime :updated_at, default: Sequel::CURRENT_TIMESTAMP
			index :uei
			index :contract_action_id
		end
		logger.info "Created table: fpds_contract_vendor_details"
	end

	unless db.table_exists?(:fpds_treasury_accounts)
		db.create_table(:fpds_treasury_accounts) do
			primary_key :id
			foreign_key :contract_action_id, :fpds_contract_actions, null: false, on_delete: :cascade
			String :agency_identifier, text: true
			String :main_account_code, text: true
			String :sub_account_code, text: true
			String :sub_level_prefix_code, text: true
			String :allocation_transfer_agency_identifier, text: true
			String :beginning_period_of_availability, text: true
			String :ending_period_of_availability, text: true
			String :availability_type_code, text: true
			String :initiative, text: true
			Float :obligated_amount
			DateTime :created_at, default: Sequel::CURRENT_TIMESTAMP
			index :contract_action_id
		end
		logger.info "Created table: fpds_treasury_accounts"
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

class ContractVendorDetail < Sequel::Model(:fpds_contract_vendor_details); end

class TreasuryAccount < Sequel::Model(:fpds_treasury_accounts); end

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
		vendor_details_queue  = [] # Array of { entry_hash_id:, vendor_details: }
		treasury_accounts_queue = [] # Array of { entry_hash_id:, accounts: [...] }

		page_lookups[:entry_hashes].each do |entry_hash_id, entry_data|
			content_node = entry_data[:content_node]

			uei       = content_node.at_xpath('.//vendorSiteDetails/entityIdentifiers/vendorUEIInformation/UEI')&.text&.strip
			vendor_id = (uei && !uei.empty?) ? id_caches[:vendors][uei] : nil
			vendor_id = vendor_id.is_a?(Integer) ? vendor_id : nil

			# Contracting Office Agency
			ca_code   = content_node.at_xpath('.//purchaserInformation/contractingOfficeAgencyID')&.text&.strip
			agency_id = (ca_code && !ca_code.empty?) ? id_caches[:agencies][ca_code] : nil

			# Resolve other FKs
			co_code               = content_node.at_xpath('.//purchaserInformation/contractingOfficeID')&.text&.strip
			contracting_office_id = (co_code && !co_code.empty?) ? id_caches[:offices][co_code] : nil

			fa_code_val       = content_node.at_xpath('.//purchaserInformation/fundingRequestingAgencyID')&.text&.strip
			funding_agency_id = (fa_code_val && !fa_code_val.empty?) ? id_caches[:agencies][fa_code_val] : nil

			fo_code_val       = content_node.at_xpath('.//purchaserInformation/fundingRequestingOfficeID')&.text&.strip
			funding_office_id = (fo_code_val && !fo_code_val.empty?) ? id_caches[:offices][fo_code_val] : nil

			psc_code_val               = content_node.at_xpath('.//productOrServiceInformation/productOrServiceCode')&.text&.strip
			product_or_service_code_id = (psc_code_val && !psc_code_val.empty?) ? id_caches[:pscs][psc_code_val] : nil

			naics_code_val = content_node.at_xpath('.//productOrServiceInformation/principalNAICSCode')&.text&.strip
			naics_code_id  = (naics_code_val && !naics_code_val.empty?) ? id_caches[:naics][naics_code_val] : nil

			content_xml_string = content_node.to_s
			raw_xml_sha256     = Digest::SHA256.hexdigest(content_xml_string)

			# Parse atom content with Nori for jsonb storage
			begin
				nori_hash = Nori.new({
					strip_namespaces:              true,
					delete_namespace_attributes:   false,
					convert_attributes_to:         nil,
					empty_tag_value:               nil,
					advanced_typecasting:          true,
					convert_dashes_to_underscores: false,
					scrub_xml:                     true,
					parser:                        :nokogiri
				}).parse(content_xml_string)

				reason_for_modification = nori_hash.dig('award', 'contractData', 'reasonForModification') ||
				                         nori_hash.dig('IDV', 'contractData', 'reasonForModification') ||
				                         nori_hash.dig('OtherTransactionAward', 'contractDetail', 'reasonForModification') ||
				                         nori_hash.dig('OtherTransactionIDV', 'contractDetail', 'reasonForModification')

				atom_content_json = nori_hash.to_json
			rescue => e
				logger.warn "Error parsing XML with Nori for entry #{entry_hash_id}: #{e.message}. Using nil values."
				reason_for_modification = nil
				atom_content_json = nil
			end

			# Extract generic tags
			generic_strings_hash  = nil
			generic_booleans_hash = nil
			gs_node = content_node.at_xpath('.//genericTags/genericStrings')
			if gs_node
				h = {}
				gs_node.element_children.each { |c| h[c.name] = c.text&.strip }
				generic_strings_hash = h.to_json unless h.empty?
			end
			gb_node = content_node.at_xpath('.//genericTags/genericBooleans')
			if gb_node
				h = {}
				gb_node.element_children.each { |c| h[c.name] = Normalizer.to_bool(c.text&.strip) }
				generic_booleans_hash = h.to_json unless h.empty?
			end

			# Extract all normalized fields via Normalizer
			normalized = Normalizer.extract_all_action_fields(content_node, Parsers)

			# Also handle PIID from OtherTransaction types
			piid = content_node.at_xpath('.//awardID/awardContractID/PIID')&.text&.strip ||
			       content_node.at_xpath('.//contractID/IDVID/PIID')&.text&.strip ||
			       content_node.at_xpath('.//OtherTransactionAwardID/OtherTransactionAwardContractID/PIID')&.text&.strip ||
			       content_node.at_xpath('.//OtherTransactionIDVID/OtherTransactionIDVContractID/PIID')&.text&.strip ||
			       'UNKNOWN_PIID'

			mod_number = content_node.at_xpath('.//awardID/awardContractID/modNumber')&.text&.strip ||
			             content_node.at_xpath('.//contractID/IDVID/modNumber')&.text&.strip ||
			             content_node.at_xpath('.//OtherTransactionAwardID/OtherTransactionAwardContractID/modNumber')&.text&.strip ||
			             content_node.at_xpath('.//OtherTransactionIDVID/OtherTransactionIDVContractID/modNumber')&.text&.strip ||
			             '0'

			action_record = {
			atom_entry_id:           entry_hash_id,
			atom_title:              entry_data[:title],
			atom_feed_modified_date: entry_data[:modified],
			piid:                    piid,
			modification_number:     mod_number,
			vendor_id:               vendor_id,
			agency_id:               agency_id.is_a?(Integer) ? agency_id : nil,

			obligated_amount:            parse_float(content_node.at_xpath('.//dollarValues/obligatedAmount')&.text),
			effective_date:              parse_datetime(content_node.at_xpath('.//relevantContractDates/effectiveDate')&.text),
			last_date_to_order:          parse_date(content_node.at_xpath('.//relevantContractDates/lastDateToOrder')&.text),
			completion_date:             parse_date(content_node.at_xpath('.//relevantContractDates/completionDate')&.text),
			base_and_all_options_value:  parse_float(content_node.at_xpath('.//dollarValues/baseAndAllOptionsValue')&.text),
			total_estimated_order_value: parse_float(content_node.at_xpath('.//dollarValues/totalEstimatedOrderValue')&.text),

			contracting_office_id:       contracting_office_id.is_a?(Integer) ? contracting_office_id : nil,
			funding_agency_id:           funding_agency_id.is_a?(Integer) ? funding_agency_id : nil,
			funding_office_id:           funding_office_id.is_a?(Integer) ? funding_office_id : nil,
			product_or_service_code_id:  product_or_service_code_id.is_a?(Integer) ? product_or_service_code_id : nil,
			naics_code_id:               naics_code_id.is_a?(Integer) ? naics_code_id : nil,

			description_of_requirement:  content_node.at_xpath('.//contractData/descriptionOfContractRequirement')&.text&.strip,
			action_type_code:            content_node.at_xpath('.//contractData/contractActionType')&.text&.strip,
			action_type_description:     content_node.at_xpath('.//contractData/contractActionType/@description')&.text&.strip,
			pricing_type_code:           content_node.at_xpath('.//contractData/typeOfContractPricing')&.text&.strip,
			pricing_type_description:    content_node.at_xpath('.//contractData/typeOfContractPricing/@description')&.text&.strip,
			fpds_last_modified_date:     parse_datetime(content_node.at_xpath('.//transactionInformation/lastModifiedDate')&.text),
			raw_xml_content_sha256:      raw_xml_sha256,
			reason_for_modification:     reason_for_modification,
			atom_content:                atom_content_json,
			generic_strings:             generic_strings_hash,
			generic_booleans:            generic_booleans_hash,
			}.merge(normalized)

			actions_to_create << action_record

			# Queue vendor details for post-insert
			vd = Normalizer.extract_vendor_details(content_node, Parsers)
			vendor_details_queue << { entry_hash_id: entry_hash_id, vendor_details: vd } if vd && !vd.empty?

			# Queue treasury accounts for post-insert
			ta = Normalizer.extract_treasury_accounts(content_node)
			treasury_accounts_queue << { entry_hash_id: entry_hash_id, accounts: ta } unless ta.empty?
		end
		logger.info "Stage 5: Built final contract_actions batch. Size: #{actions_to_create.size}."

		unless actions_to_create.empty?
			begin
				ContractAction.multi_insert(actions_to_create, commit_every: 100)
				logger.info "Batch inserted #{actions_to_create.size} new contract actions."
			rescue Sequel::UniqueConstraintViolation => e
				logger.error "Unique constraint violation during ContractAction multi_insert: #{e.message}."
			rescue StandardError => e
				logger.error "Error during ContractAction multi_insert: #{e.message}"
				raise e
			end

			# === Stage 6: Insert vendor details and treasury accounts ===
			unless vendor_details_queue.empty? && treasury_accounts_queue.empty?
				# Build a mapping from atom_entry_id -> contract_action.id
				entry_ids = actions_to_create.map { |a| a[:atom_entry_id] }
				id_map = ContractAction.where(atom_entry_id: entry_ids).select_map([:atom_entry_id, :id]).to_h

				# Insert vendor details
				unless vendor_details_queue.empty?
					vd_records = vendor_details_queue.filter_map do |item|
						ca_id = id_map[item[:entry_hash_id]]
						next unless ca_id
						item[:vendor_details].merge(contract_action_id: ca_id)
					end
					unless vd_records.empty?
						begin
							ContractVendorDetail.multi_insert(vd_records, commit_every: 100)
							logger.info "Batch inserted #{vd_records.size} vendor detail records."
						rescue StandardError => e
							logger.error "Error inserting vendor details: #{e.message}"
						end
					end
				end

				# Insert treasury accounts
				unless treasury_accounts_queue.empty?
					ta_records = treasury_accounts_queue.flat_map do |item|
						ca_id = id_map[item[:entry_hash_id]]
						next [] unless ca_id
						item[:accounts].map { |acc| acc.merge(contract_action_id: ca_id) }
					end
					unless ta_records.empty?
						begin
							TreasuryAccount.multi_insert(ta_records, commit_every: 100)
							logger.info "Batch inserted #{ta_records.size} treasury account records."
						rescue StandardError => e
							logger.error "Error inserting treasury accounts: #{e.message}"
						end
					end
				end
			end
			logger.info "Stage 6: Inserted vendor details and treasury accounts."
		end
	end # End of DB.transaction

	actions_to_create.size
end

# --- Backfill Logic ---
# Analyzes backfill coverage by counting records per day using group_and_count.
# Returns a Hash of { Date => start_offset } where start_offset is the number of
# records already in the DB for that day (rounded down to nearest 10 for FPDS page
# alignment). Missing days have offset 0; partial days resume from their offset.
def find_missing_backfill_dates(start_date, end_date, logger)
	logger.info "Analyzing backfill coverage between #{start_date} and #{end_date}..."

	# 1. Get count of records per day using group_and_count
	daily_counts = DB[:fpds_contract_actions]
		.where(fpds_last_modified_date: (start_date.to_time..(end_date + 1).to_time))
		.group_and_count(Sequel.cast(:fpds_last_modified_date, :date).as(:modified_date))
		.all
		.to_h { |row| [row[:modified_date], row[:count]] }

	logger.info "Found data for #{daily_counts.size} distinct days in the DB."

	# 2. Build result: { date => start_offset } for every date in range
	#    The FPDS ATOM feed returns 10 entries per page; the 'start' query parameter
	#    is a 0-based offset. Round down to the nearest 10 so we land on a valid page
	#    boundary and let the existing content-hash dedup handle any overlap.
	date_offsets  = {}
	missing_count = 0
	partial_count = 0

	(start_date..end_date).each do |d|
		count = daily_counts[d] || 0
		if count == 0
			date_offsets[d] = 0
			missing_count += 1
		else
			# Round down to nearest 10 for FPDS page alignment
			start_offset    = (count / 10) * 10
			date_offsets[d] = start_offset
			partial_count  += 1
		end
	end

	logger.info "Coverage: #{missing_count} missing days, #{partial_count} days with existing data (will resume from offset)."
	date_offsets
end

# Downloads all historical FPDS data by iterating day-by-day through a date range.
# Uses a thread pool to process multiple days concurrently for maximum throughput.
# Progress is tracked in the job_tracker table so the backfill can resume if interrupted.
def backfill_fpds_feed(start_date, end_date, logger, thread_count: DEFAULT_BACKFILL_THREADS, specific_dates: nil)
	total_saved = Concurrent::AtomicFixnum.new(0)
	completed_days = Concurrent::AtomicFixnum.new(0)
	failed_dates = Concurrent::Array.new
	tracker_mutex = Mutex.new
	job_tracker = JobTracker.find_or_create(job_name: BACKFILL_JOB_NAME)
	last_completed_date = Concurrent::AtomicReference.new(nil)

	# Build the list of dates to process and their start offsets.
	# specific_dates may be:
	#   nil   -> normal backfill, all offsets default to 0
	#   Hash  -> gap-fill: { Date => start_offset } from find_missing_backfill_dates
	#   Array -> legacy: plain array of Date objects, all offsets 0
	date_offsets = {}
	dates_to_process = nil

	if specific_dates.is_a?(Hash)
		date_offsets     = specific_dates
		dates_to_process = specific_dates.keys.sort
	elsif specific_dates.is_a?(Array)
		dates_to_process = specific_dates
	end

	if dates_to_process.nil?
		# Standard resume logic only if not doing a gap-fill
		max_modified = DB[:fpds_contract_actions].max(:fpds_last_modified_date)
		if max_modified
			last_completed = max_modified.to_date
			if last_completed >= start_date && last_completed < end_date
				logger.info "Resuming backfill from #{last_completed + 1} (last ingested fpds_last_modified_date: #{last_completed})"
				start_date = last_completed + 1
				last_completed_date.set(last_completed)
			end
		end
		dates_to_process = (start_date..end_date).to_a
	end

	total_days = dates_to_process.size
	logger.info "Backfill: #{total_days} days to process with #{thread_count} threads"

	# Create a fixed thread pool
	pool = Concurrent::FixedThreadPool.new(thread_count)

	dates_to_process.each do |current_date|
		pool.post do
			date_str     = current_date.strftime('%Y/%m/%d')
			start_offset = date_offsets[current_date] || 0

			uri = Addressable::URI.parse("https://www.fpds.gov/ezsearch/FEEDS/ATOM")
			uri.query_values = {
				"FEEDNAME" => "PUBLIC",
				"VERSION" => "1.5.3",
				"q" => "LAST_MOD_DATE:[#{date_str},#{date_str}]",
				"start" => start_offset.to_s
			}
			day_url = uri.to_s

			remaining = total_days - completed_days.value
			offset_note = start_offset > 0 ? " (resuming from offset #{start_offset})" : ""
			logger.info "Backfill [#{current_date}]: Fetching records#{offset_note} (#{remaining} days remaining, #{failed_dates.size} failed)"

			begin
				day_saved = process_fpds_feed(day_url, logger)
				total_saved.increment(day_saved)
				completed_days.increment

				logger.info "Backfill [#{current_date}]: Saved #{day_saved} records (total so far: #{total_saved.value})"

				# Update job tracker with the latest sequentially-completed date
				tracker_mutex.synchronize do
					prev = last_completed_date.get
					if prev.nil? || current_date > prev
						last_completed_date.set(current_date)
					end
 				job_tracker.update(
 					status: 'running',
 					notes: "Completed through #{last_completed_date.get}. #{completed_days.value}/#{total_days} days done. Total saved: #{total_saved.value}. Failed: #{failed_dates.size}",
 					updated_at: Time.now
 				)
				end
			rescue => e
				failed_dates << current_date
				completed_days.increment
				logger.error "Backfill [#{current_date}]: Error: #{e.message}. Will retry on next run."

 			tracker_mutex.synchronize do
 				lcd_note = last_completed_date.get ? "Completed through #{last_completed_date.get}. " : ""
 				job_tracker.update(
 					status: 'running',
 					notes: "#{lcd_note}#{completed_days.value}/#{total_days} days done. Total saved: #{total_saved.value}. Failed dates: #{failed_dates.join(', ')}",
 					updated_at: Time.now
 				)
 			end
			end
		end
	end

	# Wait for all threads to finish
	pool.shutdown
	pool.wait_for_termination

	if failed_dates.empty?
		job_tracker.update(
			status: 'idle',
			notes: "Completed through #{end_date}. Backfill complete. Range: #{start_date} to #{end_date}. Total saved: #{total_saved.value}. All #{total_days} days succeeded.",
			last_successful_run_start_time: Time.now,
			updated_at: Time.now
		)
		logger.info "Backfill complete! Processed #{start_date} through #{end_date}. Total saved: #{total_saved.value}"
	else
		job_tracker.update(
			status: 'partial',
			notes: "Completed through #{end_date}. Backfill partial. Total saved: #{total_saved.value}. #{failed_dates.size} days failed: #{failed_dates.sort.join(', ')}",
			updated_at: Time.now
		)
		logger.warn "Backfill finished with #{failed_dates.size} failed days: #{failed_dates.sort.join(', ')}. Run with --resume to retry."
	end
	total_saved.value
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

# --- CLI Argument Parsing ---
def parse_cli_options
	options = { mode: :daily }

	OptionParser.new do |opts|
		opts.banner = "Usage: #{$0} [options]"

		opts.on('--backfill', 'Download ALL historical FPDS data (day-by-day)') do
			options[:mode] = :backfill
		end

		opts.on('--start-date DATE', 'Backfill start date (YYYY-MM-DD). Default: 2000-10-01') do |d|
			options[:start_date] = Date.parse(d)
		end

		opts.on('--end-date DATE', 'Backfill end date (YYYY-MM-DD). Default: yesterday') do |d|
			options[:end_date] = Date.parse(d)
		end

		opts.on('--resume', 'Resume a previously interrupted backfill from where it left off') do
			options[:mode] = :backfill
			options[:resume] = true
		end

		opts.on('--gap-fill', 'Find and fill gaps in the historical data instead of resuming from max date') do
			options[:mode] = :backfill
			options[:gap_fill] = true
		end

		opts.on('--threads N', Integer, "Number of concurrent threads for backfill (default: #{DEFAULT_BACKFILL_THREADS})") do |n|
			options[:threads] = [1, n].max
		end

		opts.on('-h', '--help', 'Show this help message') do
			puts opts
			exit
		end
	end.parse!

	options
end

# --- Main Execution ---
if __FILE__ == $0
	options = parse_cli_options
	thread_count = options[:threads] || DEFAULT_BACKFILL_THREADS
	lock_file_path = File.join(__dir__, "#{File.basename(__FILE__)}.lock")
	lock_file      = File.open(lock_file_path, 'w')
	unless lock_file.flock(File::LOCK_EX | File::LOCK_NB)
		LOG.warn "Script is already running. Another instance holds the lock. Exiting."
		exit
	end

	LOG.info "Script starting up with exclusive lock."
	current_run_start_time = Time.now

	if options[:mode] == :backfill
		# --- Backfill Mode ---
		backfill_start = options[:start_date] || BACKFILL_EARLIEST_DATE
		backfill_end   = options[:end_date] || (Date.today - 1)

		LOG.info "=== BACKFILL MODE ==="
		LOG.info "Downloading all FPDS records from #{backfill_start} to #{backfill_end}"
		LOG.info "Using #{thread_count} concurrent threads (DB pool: #{_pool_size})"
		LOG.info "This will iterate day-by-day through #{(backfill_end - backfill_start).to_i + 1} days."

		setup_database(DB, LOG)

		begin
			specific_dates = nil
			if options[:gap_fill]
				specific_dates = find_missing_backfill_dates(backfill_start, backfill_end, LOG)
			end

			saved_count = backfill_fpds_feed(backfill_start, backfill_end, LOG, thread_count: thread_count, specific_dates: specific_dates)
			LOG.info "Backfill completed successfully. Total records saved: #{saved_count}"
		rescue => e
			LOG.fatal "Backfill failed: #{e.message}"
			LOG.fatal e.backtrace.join("\n")
			LOG.info "Run with --resume to continue from where it left off."
		ensure
			lock_file.flock(File::LOCK_UN)
			lock_file.close
			LOG.info "Script finished and lock released."
		end
	else
		# --- Daily Mode (original behavior) ---
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
end
