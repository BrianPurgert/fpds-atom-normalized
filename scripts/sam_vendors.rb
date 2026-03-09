#!/usr/bin/env ruby
# sam_vendor.rb
#
# 1️⃣  Looks for the newest ZIP in ./sam_extracts/  ➜  uses it if found.
# 2️⃣  Otherwise calls the SAM API once, saves the ZIP to ./sam_extracts/.

# 4️⃣  Streams the .dat, builds a CSV,


require 'date'
require 'logger'
require 'net/http'
require 'uri'
require 'csv'
require 'tmpdir'
require 'zip'          # gem install rubyzip

require 'roo'          # gem install roo
require 'sequel'       # gem install sequel pg
require 'dotenv'       # gem install dotenv

# URL validation and cleanup helper
def clean_and_validate_url(url_string)
  return nil if url_string.nil? || url_string.strip.empty?

  url = url_string.strip

  # Remove common invalid characters and whitespace
  url = url.gsub(/[\s\r\n\t]/, '')

  # Return nil if URL is empty after cleaning
  return nil if url.empty?

  # If URL doesn't start with http:// or https://, add https://
  unless url.match?(/\A(https?:\/\/)/i)
    # Skip if it looks like an email or other non-URL format
    return nil if url.include?('@') || url.start_with?('mailto:', 'ftp:', 'file:')

    # Basic validation: should contain at least one dot for domain
    return nil unless url.include?('.')

    url = "https://#{url}"
  end

  # Basic URL validation using URI
  begin
    uri = URI.parse(url)
    # Ensure it has a valid scheme and host
    return nil unless uri.scheme && uri.host && !uri.host.empty?
    # Ensure scheme is http or https
    return nil unless ['http', 'https'].include?(uri.scheme.downcase)
    # Ensure host contains at least one dot (basic domain validation)
    return nil unless uri.host.include?('.')
    # Return the cleaned URL
    return uri.to_s
  rescue URI::InvalidURIError
    return nil
  end
end


Dotenv.load('.env', File.expand_path('../.env', __FILE__))

# Redacts sensitive tokens (like API keys) from any text before logging
def redact_sensitive(text, api_key)
	"#{text} #{api_key}"
end

# ───────────────  CONFIG  ─────────────────────────────────────────────
API_KEY      = ENV.fetch('SAM_API_KEY') { abort '🔥  SAM_API_KEY missing' }
BASE_URL     = 'https://api.sam.gov/data-services/v1/extracts'

EXTRACT_DIR  = File.expand_path('../downloads/sam_extracts', __dir__)

TABLE_NAME   = :sam_vendors
Dir.mkdir(EXTRACT_DIR) unless Dir.exist?(EXTRACT_DIR)

LOG = Logger.new($stdout)
LOG.level = Logger::INFO

require_relative '../lib/database'

# ───────────────  DB CONNECT  ─────────────────────────────────────────
begin
  DB = Database.connect(logger: LOG)
  Sequel.extension :pg_json, :pg_json_ops
  LOG.info "Connected to PostgreSQL database"
rescue StandardError => e
  LOG.fatal "DB connect failed: #{e.message}"
  exit 1
end


  # DB.create_table? TABLE_NAME do
  #   String :uei_sam, primary_key: true, size: 12
  #   String :blank_column # Add blank column after uei_sam
  #   String :entity_eft_indicator, size: 4
	# 	String :cage_code,                        size:        5
	# 	String :dodaac,                           size:        9
	# 	String :sam_extract_code,                 size:        1
	# 	String :purpose_of_registration,          size:        2
	# 	String :initial_registration_date,        size:        10
	# 	String :registration_expiration_date,     size:        10
	# 	String :last_update_date,                 size:        10
	# 	String :activation_date,                  size:        10
	# 	String :vendor_name,                      size:        120
	# 	String :dba_name,                         size:        120
	# 	String :entity_division_name,             size:        60
	# 	String :entity_division_number,           size:        10
	# 	String :physical_address_line_1,          size:        150
	# 	String :physical_address_line_2,          size:        150
	# 	String :physical_address_city,            size:        40
	# 	String :physical_address_state,           size:        55
	# 	String :physical_address_postal_code,     size:        50
	# 	String :physical_address_plus4,           size:        4
	# 	String :physical_address_country_code,    size:        3
	# 	String :physical_congressional_district,  size:        10
	# 	String :dnb_open_data_flag,               size:        1
	# 	String :entity_start_date,                size:        10
	# 	String :fiscal_year_end_close_date,       size:        4
	# 	String :entity_url,                       size:        200
	# 	String :entity_structure,                 size:        2
	# 	String :state_of_incorporation,           size:        2
	# 	String :country_of_incorporation,         size:        3
	# 	String :business_type_counter,            size:        10
	# 	Text   :business_type_string
	# 	String :primary_naics,                    size:        6
	# 	String :naics_code_counter,               size:        10
	# 	Text   :naics_code_string
	# 	String :psc_code_counter,                 size:        10
	# 	Text   :psc_code_string
	# 	String :credit_card_usage,                size:        1
	# 	String :correspondence_flag,              size:        1
	# 	String :mailing_address_line_1,           size:        150
	# 	String :mailing_address_line_2,           size:        150
	# 	String :mailing_address_city,             size:        40
	# 	String :mailing_address_postal_code,      size:        50
	# 	String :mailing_address_plus4,            size:        4
	# 	String :mailing_address_country_code,     size:        3
	# 	String :mailing_address_state,            size:        55
	# 	String :govt_bus_poc_first_name,          size:        65
	# 	String :govt_bus_poc_middle_initial,      size:        3
	# 	String :govt_bus_poc_last_name,           size:        65
	# 	String :govt_bus_poc_title,               size:        50
	# 	String :govt_bus_poc_st_add1,             size:        150
	# 	String :govt_bus_poc_st_add2,             size:        150
	# 	String :govt_bus_poc_city,                size:        40
	# 	String :govt_bus_poc_postal_code,         size:        50
	# 	String :govt_bus_poc_plus4,               size:        4
	# 	String :govt_bus_poc_country_code,        size:        3
	# 	String :govt_bus_poc_state,               size:        55
	# 	String :alt_govt_bus_poc_first_name,      size:        65
	# 	String :alt_govt_bus_poc_middle_initial,  size:        3
	# 	String :alt_govt_bus_poc_last_name,       size:        65
	# 	String :alt_govt_bus_poc_title,           size:        50
	# 	String :alt_govt_bus_poc_st_add1,         size:        150
	# 	String :alt_govt_bus_poc_st_add2,         size:        150
	# 	String :alt_govt_bus_poc_city,            size:        40
	# 	String :alt_govt_bus_poc_postal_code,     size:        50
	# 	String :alt_govt_bus_poc_plus4,           size:        4
	# 	String :alt_govt_bus_poc_country_code,    size:        3
	# 	String :alt_govt_bus_poc_state,           size:        55
	# 	String :past_perf_poc_first_name,         size:        65
	# 	String :past_perf_poc_middle_initial,     size:        3
	# 	String :past_perf_poc_last_name,          size:        65
	# 	String :past_perf_poc_title,              size:        50
	# 	String :past_perf_poc_st_add1,            size:        150
	# 	String :past_perf_poc_st_add2,            size:        150
	# 	String :past_perf_poc_city,               size:        40
	# 	String :past_perf_poc_postal_code,        size:        50
	# 	String :past_perf_poc_plus4,              size:        4
	# 	String :past_perf_poc_country_code,       size:        3
	# 	String :past_perf_poc_state,              size:        55
	# 	String :alt_past_perf_poc_first_name,     size:        65
	# 	String :alt_past_perf_poc_middle_initial, size:        3
	# 	String :alt_past_perf_poc_last_name,      size:        65
	# 	String :alt_past_perf_poc_title,          size:        50
	# 	String :alt_past_perf_poc_st_add1,        size:        150
	# 	String :alt_past_perf_poc_st_add2,        size:        150
	# 	String :alt_past_perf_poc_city,           size:        40
	# 	String :alt_past_perf_poc_postal_code,    size:        50
	# 	String :alt_past_perf_poc_plus4,          size:        4
	# 	String :alt_past_perf_poc_country_code,   size:        3
	# 	String :alt_past_perf_poc_state,          size:        55
	# 	String :elec_bus_poc_first_name,          size:        65
	# 	String :elec_bus_poc_middle_initial,      size:        3
	# 	String :elec_bus_poc_last_name,           size:        65
	# 	String :elec_bus_poc_title,               size:        50
	# 	String :elec_bus_poc_st_add1,             size:        150
	# 	String :elec_bus_poc_st_add2,             size:        150
	# 	String :elec_bus_poc_city,                size:        40
	# 	String :elec_bus_poc_postal_code,         size:        50
	# 	String :elec_bus_poc_plus4,               size:        4
	# 	String :elec_bus_poc_country_code,        size:        3
	# 	String :elec_bus_poc_state,               size:        55
	# 	String :alt_elec_poc_first_name,          size:        65
	# 	String :alt_elec_poc_middle_initial,      size:        3
	# 	String :alt_elec_poc_last_name,           size:        65
	# 	String :alt_elec_poc_title,               size:        50
	# 	String :alt_elec_poc_st_add1,             size:        150
	# 	String :alt_elec_poc_st_add2,             size:        150
	# 	String :alt_elec_poc_city,                size:        40
	# 	String :alt_elec_poc_postal_code,         size:        50
	# 	String :alt_elec_poc_plus4,               size:        4
	# 	String :alt_elec_poc_country_code,        size:        3
	# 	String :alt_elec_poc_state,               size:        55
	# 	String :naics_exception_counter,          size:        10
	# 	Text   :naics_exception_string
	# 	String :debt_subject_to_offset_flag,      size:        1
	# 	String :exclusion_status_flag,            size:        1
	# 	String :sba_business_types_counter,       size:        10
	# 	Text   :sba_business_types_string
	# 	String :no_public_display_flag,           size:        4
	# 	String :disaster_response_counter,        size:        10
	# 	Text   :disaster_response_string
	# 	String :entity_evs_source,                size:        10
	# 	Text   :flex_field_1
	# 	Text   :flex_field_2
	# 	Text   :flex_field_3
	# 	Text   :flex_field_4
	# 	Text   :flex_field_5
	# 	Text   :flex_field_6
	# 	Text   :flex_field_7
	# 	Text   :flex_field_8
	# 	Text   :flex_field_9
	# 	Text   :flex_field_10
	# 	Text   :flex_field_11
	# 	Text   :flex_field_12
	# 	Text   :flex_field_13
	# 	Text   :flex_field_14
	# 	Text   :flex_field_15
	# 	Text   :flex_field_16
	# 	Text   :flex_field_17
	# 	Text   :flex_field_18
	# 	Text   :flex_field_19
	# 	String :end_of_record_indicator,          size:        12
	# end
	# LOG.info "Created table #{TABLE_NAME}"

# ───────────────  STEP 1: USE CACHED ZIP IF PRESENT  ─────────────────
def latest_cached_zip
	Dir.glob(File.join(EXTRACT_DIR, 'SAM_PUBLIC_MONTHLY_V2_*.ZIP')).max_by do |p|
		p[/V2_(\d{8})/, 1] || '00000000'
	end
end

def expected_current_filename
	today = Date.today
	# Get current month's first Sunday
	current_month = today
	fs = Date.new(current_month.year, current_month.month, 1)
	fs += 1 until fs.sunday?
	"SAM_PUBLIC_MONTHLY_V2_#{fs.strftime('%Y%m%d')}.ZIP"
end

def should_use_cached_zip?(cached_zip_path)
	return false unless cached_zip_path && File.exist?(cached_zip_path)

	# Extract date from cached filename
	cached_filename = File.basename(cached_zip_path)
	cached_date_str = cached_filename[/V2_(\d{8})/, 1]
	return false unless cached_date_str

	cached_date = Date.strptime(cached_date_str, '%Y%m%d')
	expected_filename = expected_current_filename
	expected_date_str = expected_filename[/V2_(\d{8})/, 1]
	expected_date = Date.strptime(expected_date_str, '%Y%m%d')

	# Use cached version if it's from the current expected month or newer
	cached_date >= expected_date
end

local_zip = latest_cached_zip

# ───────────────  STEP 2: IF NO CACHE, HIT THE API  ──────────────────

# Define API functions outside the conditional blocks
def raw_get(api_key, **q)
	uri        = URI(BASE_URL)
	# Compose full query including API key for the actual request
	full_query = q.merge(api_key: api_key)
	uri.query  = URI.encode_www_form(full_query)

	# Log a redacted version of the URL (do not leak the API key)
	redacted_uri = URI(BASE_URL)
	redacted_uri.query = URI.encode_www_form(full_query.merge(api_key: '[REDACTED]'))
	LOG.info "GET #{redacted_uri}"

	response = nil
	current_uri = uri
	limit = 5

	limit.times do
		Net::HTTP.start(current_uri.host, current_uri.port, use_ssl: (current_uri.scheme == 'https')) do |http|
			http.open_timeout = 10
			http.read_timeout = 30
			request = Net::HTTP::Get.new(current_uri.request_uri)
			response = http.request(request)
		end

		if response.is_a?(Net::HTTPRedirection)
			location = response['location']
			LOG.info "Redirected to #{location.split('?').first}..."
			current_uri = URI.join(current_uri.to_s, location)
		else
			break
		end
	end

	[response.code.to_i, response.body]
rescue Net::OpenTimeout, Net::ReadTimeout => e
	LOG.warn "HTTP timeout for #{redacted_uri}: #{e.class}"
	[0, nil]
rescue => e
	LOG.warn "HTTP error for #{redacted_uri}: #{e.class}: #{e.message}"
	[0, nil]
end

def api_latest_public(api_key)
	today = Date.today
	0.upto(12) do |i|
		m          = today << i
		fs         = Date.new(m.year, m.month, 1)
		fs        += 1 until fs.sunday?
		fn         = "SAM_PUBLIC_MONTHLY_V2_#{fs.strftime('%Y%m%d')}.ZIP"
		code, body = raw_get(api_key, fileName: fn)
		if code == 200
			return [fn, body]
		elsif code == 429
			LOG.fatal "API returned 429 Too Many Requests for #{fn}; aborting."
			exit 1
		elsif code == 401 || code == 403
			LOG.fatal "API returned #{code} for #{fn} — SAM API key may be invalid/disabled. Failing the run."
			LOG.error redact_sensitive(body, API_KEY)
			exit 1
		else
			LOG.error "API returned #{code} for #{fn}"
			LOG.error redact_sensitive(body, API_KEY)
		end
	end
	nil
end

# Check if we should use cached file or download new one
if local_zip && should_use_cached_zip?(local_zip)
	LOG.info "Using cached extract: #{File.basename(local_zip)}"
else
	# Either no cached file or cached file is outdated - download a new one
	if local_zip
		LOG.info "Cached extract #{File.basename(local_zip)} is outdated, downloading newer version"
	else
		LOG.info "No cached file found, downloading from API"
	end

	fn, bytes = api_latest_public(API_KEY)

	if fn.nil? || bytes.nil?
		if local_zip && File.exist?(local_zip)
			LOG.warn "No newer extract available from API; falling back to existing cached file #{File.basename(local_zip)}"
		else
			LOG.error "No extract available from API in the last 13 months and no cached file present. Nothing to do."
			# Exit gracefully (success) to avoid failing the workflow when data isn't published yet
			exit 0
		end
	else
		local_zip = File.join(EXTRACT_DIR, fn)
		File.binwrite(local_zip, bytes)
		LOG.info "Downloaded and cached #{fn} (#{bytes.bytesize / 1_048_576} MB)"
	end
end

# ───────────────  STEP 3: UNZIP, PARSE, UPSERT  ──────────────────────

Dir.mktmpdir do |dir|
	csv_path = nil
	Zip::File.open(local_zip) do |zip|
		entry = zip.find { |e| e.name.downcase.end_with?('.dat') }
		csv_path = File.join(dir, entry.name)
		entry.extract(csv_path)
	end
	LOG.info "Extracted #{File.basename(csv_path)}"
	extract_date_str = File.basename(csv_path).split('_').last.split('.').first
	extract_date = Date.strptime(extract_date_str, '%Y%m%d') rescue nil

def parse_sam_date(str)
  return nil if str.nil? || str.strip.empty? || str.strip == '00000000'
  Date.strptime(str.strip, '%Y%m%d') rescue nil
end

def parse_tilde_array(str)
  return nil if str.nil? || str.strip.empty?
  Sequel.pg_jsonb(str.strip.split('~'))
end

DAT_COLUMNS = [
  :uei_sam, :blank_column, :entity_eft_indicator, :cage_code, :dodaac, :sam_extract_code,
  :purpose_of_registration, :initial_registration_date, :registration_expiration_date,
  :last_update_date, :activation_date, :vendor_name, :dba_name,
  :entity_division_name, :entity_division_number, :physical_address_line_1,
  :physical_address_line_2, :physical_address_city, :physical_address_state,
  :physical_address_postal_code, :physical_address_plus4, :physical_address_country_code,
  :physical_congressional_district, :dnb_open_data_flag, :entity_start_date,
  :fiscal_year_end_close_date, :entity_url, :entity_structure, :state_of_incorporation,
  :country_of_incorporation, :business_type_counter, :business_type_string,
  :primary_naics, :naics_code_counter, :naics_code_string, :psc_code_counter,
  :psc_code_string, :credit_card_usage, :correspondence_flag, :mailing_address_line_1,
  :mailing_address_line_2, :mailing_address_city, :mailing_address_postal_code,
  :mailing_address_plus4, :mailing_address_country_code, :mailing_address_state,
  :govt_bus_poc_first_name, :govt_bus_poc_middle_initial, :govt_bus_poc_last_name,
  :govt_bus_poc_title, :govt_bus_poc_st_add1, :govt_bus_poc_st_add2, :govt_bus_poc_city,
  :govt_bus_poc_postal_code, :govt_bus_poc_plus4, :govt_bus_poc_country_code,
  :govt_bus_poc_state, :alt_govt_bus_poc_first_name, :alt_govt_bus_poc_middle_initial,
  :alt_govt_bus_poc_last_name, :alt_govt_bus_poc_title, :alt_govt_bus_poc_st_add1,
  :alt_govt_bus_poc_st_add2, :alt_govt_bus_poc_city, :alt_govt_bus_poc_postal_code,
  :alt_govt_bus_poc_plus4, :alt_govt_bus_poc_country_code, :alt_govt_bus_poc_state,
  :past_perf_poc_first_name, :past_perf_poc_middle_initial, :past_perf_poc_last_name,
  :past_perf_poc_title, :past_perf_poc_st_add1, :past_perf_poc_st_add2,
  :past_perf_poc_city, :past_perf_poc_postal_code, :past_perf_poc_plus4,
  :past_perf_poc_country_code, :past_perf_poc_state, :alt_past_perf_poc_first_name,
  :alt_past_perf_poc_middle_initial, :alt_past_perf_poc_last_name, :alt_past_perf_poc_title,
  :alt_past_perf_poc_st_add1, :alt_past_perf_poc_st_add2, :alt_past_perf_poc_city,
  :alt_past_perf_poc_postal_code, :alt_past_perf_poc_plus4, :alt_past_perf_poc_country_code,
  :alt_past_perf_poc_state, :elec_bus_poc_first_name, :elec_bus_poc_middle_initial,
  :elec_bus_poc_last_name, :elec_bus_poc_title, :elec_bus_poc_st_add1,
  :elec_bus_poc_st_add2, :elec_bus_poc_city, :elec_bus_poc_postal_code,
  :elec_bus_poc_plus4, :elec_bus_poc_country_code, :elec_bus_poc_state,
  :alt_elec_poc_first_name, :alt_elec_poc_middle_initial, :alt_elec_poc_last_name,
  :alt_elec_poc_title, :alt_elec_poc_st_add1, :alt_elec_poc_st_add2, :alt_elec_poc_city,
  :alt_elec_poc_postal_code, :alt_elec_poc_plus4, :alt_elec_poc_country_code,
  :alt_elec_poc_state, :naics_exception_counter, :naics_exception_string,
  :debt_subject_to_offset_flag, :exclusion_status_flag, :sba_business_types_counter,
  :sba_business_types_string, :no_public_display_flag, :disaster_response_counter,
  :disaster_response_string, :entity_evs_source, :flex_field_1, :flex_field_2,
  :flex_field_3, :flex_field_4, :flex_field_5, :flex_field_6, :flex_field_7,
  :flex_field_8, :flex_field_9, :flex_field_10, :flex_field_11, :flex_field_12,
  :flex_field_13, :flex_field_14, :flex_field_15, :flex_field_16, :flex_field_17,
  :flex_field_18, :flex_field_19, :extract_date
]

DB.drop_table?(TABLE_NAME)
DB.create_table TABLE_NAME do
  String :uei_sam,                          size: 12
  String :entity_eft_indicator,             size:        4
  String :cage_code,                        size:        5
  String :dodaac,                           size:        9
  String :sam_extract_code,                 size:        1
  String :purpose_of_registration,          size:        2
  Date   :initial_registration_date
  Date   :registration_expiration_date
  Date   :last_update_date
  Date   :activation_date
  String :vendor_name,                      size:        120
  String :dba_name,                         size:        120
  String :entity_division_name,             size:        60
  String :entity_division_number,           size:        10
  String :physical_address_line_1,          size:        150
  String :physical_address_line_2,          size:        150
  String :physical_address_city,            size:        40
  String :physical_address_state,           size:        55
  String :physical_address_postal_code,     size:        50
  String :physical_address_plus4,           size:        4
  String :physical_address_country_code,    size:        3
  String :physical_congressional_district,  size:        10
  String :dnb_open_data_flag,               size:        1
  Date   :entity_start_date
  String :fiscal_year_end_close_date,       size:        4
  String :entity_url,                       size:        200
  String :entity_structure,                 size:        2
  String :state_of_incorporation,           size:        2
  String :country_of_incorporation,         size:        3
  column :business_type_string, :jsonb
  String :primary_naics,                    size:        6
  column :naics_code_string, :jsonb
  column :psc_code_string, :jsonb
  String :credit_card_usage,                size:        1
  String :correspondence_flag,              size:        1
  String :mailing_address_line_1,           size:        150
  String :mailing_address_line_2,           size:        150
  String :mailing_address_city,             size:        40
  String :mailing_address_postal_code,      size:        50
  String :mailing_address_plus4,            size:        4
  String :mailing_address_country_code,     size:        3
  String :mailing_address_state,            size:        55
  String :govt_bus_poc_first_name,          size:        65
  String :govt_bus_poc_middle_initial,      size:        3
  String :govt_bus_poc_last_name,           size:        65
  String :govt_bus_poc_title,               size:        50
  String :govt_bus_poc_st_add1,             size:        150
  String :govt_bus_poc_st_add2,             size:        150
  String :govt_bus_poc_city,                size:        40
  String :govt_bus_poc_postal_code,         size:        50
  String :govt_bus_poc_plus4,               size:        4
  String :govt_bus_poc_country_code,        size:        3
  String :govt_bus_poc_state,               size:        55
  String :alt_govt_bus_poc_first_name,      size:        65
  String :alt_govt_bus_poc_middle_initial,  size:        3
  String :alt_govt_bus_poc_last_name,       size:        65
  String :alt_govt_bus_poc_title,           size:        50
  String :alt_govt_bus_poc_st_add1,         size:        150
  String :alt_govt_bus_poc_st_add2,         size:        150
  String :alt_govt_bus_poc_city,            size:        40
  String :alt_govt_bus_poc_postal_code,     size:        50
  String :alt_govt_bus_poc_plus4,           size:        4
  String :alt_govt_bus_poc_country_code,    size:        3
  String :alt_govt_bus_poc_state,           size:        55
  String :past_perf_poc_first_name,         size:        65
  String :past_perf_poc_middle_initial,     size:        3
  String :past_perf_poc_last_name,          size:        65
  String :past_perf_poc_title,              size:        50
  String :past_perf_poc_st_add1,            size:        150
  String :past_perf_poc_st_add2,            size:        150
  String :past_perf_poc_city,               size:        40
  String :past_perf_poc_postal_code,        size:        50
  String :past_perf_poc_plus4,              size:        4
  String :past_perf_poc_country_code,       size:        3
  String :past_perf_poc_state,              size:        55
  String :alt_past_perf_poc_first_name,     size:        65
  String :alt_past_perf_poc_middle_initial, size:        3
  String :alt_past_perf_poc_last_name,      size:        65
  String :alt_past_perf_poc_title,          size:        50
  String :alt_past_perf_poc_st_add1,        size:        150
  String :alt_past_perf_poc_st_add2,        size:        150
  String :alt_past_perf_poc_city,           size:        40
  String :alt_past_perf_poc_postal_code,    size:        50
  String :alt_past_perf_poc_plus4,          size:        4
  String :alt_past_perf_poc_country_code,   size:        3
  String :alt_past_perf_poc_state,          size:        55
  String :elec_bus_poc_first_name,          size:        65
  String :elec_bus_poc_middle_initial,      size:        3
  String :elec_bus_poc_last_name,           size:        65
  String :elec_bus_poc_title,               size:        50
  String :elec_bus_poc_st_add1,             size:        150
  String :elec_bus_poc_st_add2,             size:        150
  String :elec_bus_poc_city,                size:        40
  String :elec_bus_poc_postal_code,         size:        50
  String :elec_bus_poc_plus4,               size:        4
  String :elec_bus_poc_country_code,        size:        3
  String :elec_bus_poc_state,               size:        55
  String :alt_elec_poc_first_name,          size:        65
  String :alt_elec_poc_middle_initial,      size:        3
  String :alt_elec_poc_last_name,           size:        65
  String :alt_elec_poc_title,               size:        50
  String :alt_elec_poc_st_add1,             size:        150
  String :alt_elec_poc_st_add2,             size:        150
  String :alt_elec_poc_city,                size:        40
  String :alt_elec_poc_postal_code,         size:        50
  String :alt_elec_poc_plus4,               size:        4
  String :alt_elec_poc_country_code,        size:        3
  String :alt_elec_poc_state,               size:        55
  column :naics_exception_string, :jsonb
  String :debt_subject_to_offset_flag,      size:        1
  String :exclusion_status_flag,            size:        1
  column :sba_business_types_string, :jsonb
  String :no_public_display_flag,           size:        4
  column :disaster_response_string, :jsonb
  String :entity_evs_source,                size:        10
  Date   :extract_date
end

vendors = DB[TABLE_NAME]
LOG.info "Recreated table #{TABLE_NAME}"

sam_vendor_columns = vendors.columns
LOG.info "Table has #{sam_vendor_columns.size} columns"

column_info = DB.schema(TABLE_NAME)
column_sizes = {}
column_info.each do |col|
  name, info = col
  if info[:type] == :string && info[:db_type].include?('(')
    size = info[:db_type].match(/\((\d+)\)/)[1].to_i
    column_sizes[name] = size
  end
end

batch = []
batch_size = 1000
total_processed = 0

begin
  unless File.exist?(csv_path) && File.readable?(csv_path)
    LOG.error "Cannot read DAT file: #{csv_path}"
    exit 1
  end

  LOG.info "Starting to read DAT file: #{csv_path}"
  
  File.foreach(csv_path) do |line|
    begin
      stripped_line = line.strip

      next if stripped_line.start_with?('BOF')
      next if stripped_line.start_with?('EOF')
      next if stripped_line.empty?

      parsed_columns = stripped_line.split('|')

      if parsed_columns.length < 142
        parsed_columns.fill(nil, parsed_columns.length, 142 - parsed_columns.length)
      end

      row_hash = {}
      DAT_COLUMNS.each_with_index do |col_name, idx|
        next unless sam_vendor_columns.include?(col_name)

        if col_name == :extract_date
          row_hash[col_name] = extract_date
          next
        end

        val = parsed_columns[idx]

        if [:initial_registration_date, :registration_expiration_date, :last_update_date, :activation_date, :entity_start_date].include?(col_name)
          row_hash[col_name] = parse_sam_date(val)
          next
        end

        if [:business_type_string, :naics_code_string, :psc_code_string, :naics_exception_string, :sba_business_types_string, :disaster_response_string].include?(col_name)
          row_hash[col_name] = parse_tilde_array(val)
          next
        end

        if val && col_name == :entity_url
          val = clean_and_validate_url(val)
        end

        if column_sizes[col_name] && val.is_a?(String) && val.length > column_sizes[col_name]
          val = val[0...column_sizes[col_name]]
        end

        row_hash[col_name] = val
      end

      batch << row_hash

      if batch.size >= batch_size
        vendors.multi_insert(batch)
        total_processed += batch.size
        LOG.info "Processed #{total_processed} rows..."
        batch.clear
      end
    rescue => e
      LOG.error "Error processing line: #{line[0..100]}..."
      LOG.error e.message
    end
  end

  if batch.any?
    vendors.multi_insert(batch)
    total_processed += batch.size
    LOG.info "Processed #{total_processed} rows..."
  end

  LOG.info "Finished reading file. Total rows inserted: #{total_processed}"
rescue => e
  LOG.error "Failed to process DAT file: #{e.message}"
  LOG.error e.backtrace.join("\n")
  exit 1
end
end # End of Dir.mktmpdir block


# --- Exclusions Extract Download (appendix) ---
# Attempts to download the latest Exclusions public monthly extract using the same Extracts API.
# Naming is inferred; we try multiple variants across recent months until we get HTTP 200.

def first_sunday_of_month(date)
  d = Date.new(date.year, date.month, 1)
  d += 1 until d.sunday?
  d
end

EXCLUSIONS_NAME_PATTERNS = [
  'EXCLUSIONS_PUBLIC_MONTHLY_V2_%Y%m%d.ZIP',
  'EXCLUSIONS_PUBLIC_MONTHLY_%Y%m%d.ZIP',
  'SAM_EXCLUSIONS_PUBLIC_MONTHLY_V2_%Y%m%d.ZIP',
  'SAM_EXCLUSIONS_PUBLIC_MONTHLY_%Y%m%d.ZIP',
  'EXCLUSIONS_PUBLIC_V2_%Y%m%d.ZIP',
  'EXCLUSIONS_PUBLIC_%Y%m%d.ZIP'
]

# Try recent months back to 12 months

def api_latest_exclusions(api_key)
  today = Date.today
  0.upto(12) do |i|
    m  = today << i
    fs = first_sunday_of_month(m)
    EXCLUSIONS_NAME_PATTERNS.each do |fmt|
      fn = fs.strftime(fmt)
      code, body = raw_get(api_key, fileName: fn)

      case code
      when 200
        if body && body.bytesize > 0
          return [fn, body]
        else
          LOG.warn "Received 200 but empty body for #{fn}"
        end
      when 429
        LOG.warn "Rate limited (429) for #{fn}; backing off briefly before continuing"
        sleep 2
      when 401, 403
        LOG.warn "Authorization error (#{code}) for exclusions request #{fn}. Skipping exclusions download."
        return nil
      when 0
        # Timeout or transport-level error already logged in raw_get
        # Continue to next pattern/month
      else
        LOG.debug "Non-success #{code} for #{fn}"
      end

      sleep 0.25 # gentle pacing to avoid hammering API
    end
  end
  nil
end

begin
  LOG.info 'Attempting to download the Exclusions public monthly extract...'
  ex = api_latest_exclusions(API_KEY)
  if ex
    fn, bytes = ex
    out_path = File.join(EXTRACT_DIR, fn)
    File.binwrite(out_path, bytes)
    LOG.info "Saved Exclusions extract to #{out_path} (#{bytes.bytesize / 1_048_576} MB)"
  else
    LOG.warn 'Could not locate a recent Exclusions extract via API.'
  end
rescue => e
  LOG.warn "Exclusions download failed: #{e.message}"
end
