#!/usr/bin/env ruby

require 'date'
require 'json'
require 'logger'
require 'net/http'
require 'sequel'
require 'uri'

require 'dotenv'

Dotenv.load(File.expand_path('../.env', __dir__))

require_relative '../lib/database'

CONTACT_TABLE = :sam_vendor_small_business_contacts
PROFILE_BASE_URL = 'https://search.certifications.sba.gov/_api/v2/profile'.freeze
TRANSIENT_HTTP_STATUSES = [408, 429, 500, 502, 503, 504].freeze

LOG = Logger.new($stdout)
LOG.formatter = proc { |severity, _datetime, _progname, msg| "#{severity} #{msg}\n" }

log_level = (ENV['LOG_LEVEL'] || 'INFO').upcase
LOG.level =
  case log_level
  when 'DEBUG' then Logger::DEBUG
  when 'WARN' then Logger::WARN
  when 'ERROR' then Logger::ERROR
  else Logger::INFO
  end

DRY_RUN = ENV['DRY_RUN'] == '1'
LIMIT = [ENV['SBA_PROFILE_LIMIT']&.to_i || 0, 0].max
PROGRESS_EVERY = [(ENV['PROGRESS_EVERY']&.to_i || 250), 1].max
REQUEST_DELAY_SECONDS = [ENV['SBA_PROFILE_REQUEST_DELAY']&.to_f || 0.25, 0.0].max
HTTP_TIMEOUT_SECONDS = [(ENV['SBA_PROFILE_HTTP_TIMEOUT']&.to_i || 30), 5].max
MAX_HTTP_RETRIES = [(ENV['SBA_PROFILE_MAX_HTTP_RETRIES']&.to_i || 4), 1].max
RETRY_FAILURE_AFTER_HOURS = [(ENV['SBA_PROFILE_RETRY_FAILURE_AFTER_HOURS']&.to_i || 6), 1].max
RETRY_NOT_FOUND_AFTER_DAYS = [(ENV['SBA_PROFILE_RETRY_NOT_FOUND_AFTER_DAYS']&.to_i || 30), 1].max

LOG.info(
  "Startup: DRY_RUN=#{DRY_RUN ? 'ON' : 'OFF'} LOG_LEVEL=#{log_level} " \
  "LIMIT=#{LIMIT.zero? ? 'none' : LIMIT} PROGRESS_EVERY=#{PROGRESS_EVERY}"
)

DB = Database.connect(logger: LOG)

def clean_string(value)
  return nil if value.nil?

  text = value.to_s.strip
  text.empty? ? nil : text
end

def clean_and_validate_url(url_string)
  return nil if url_string.nil? || url_string.to_s.strip.empty?

  url = url_string.to_s.strip.gsub(/[\s\r\n\t]/, '')
  return nil if url.empty?

  unless url.match?(/\Ahttps?:\/\//i)
    return nil if url.include?('@') || url.start_with?('mailto:', 'ftp:', 'file:')
    return nil unless url.include?('.')

    url = "https://#{url}"
  end

  uri = URI.parse(url)
  return nil unless uri.scheme && uri.host
  return nil unless %w[http https].include?(uri.scheme.downcase)

  uri.scheme = 'https' if uri.scheme.downcase == 'http'
  uri.to_s
rescue URI::InvalidURIError
  nil
end

def normalize_email(value)
  email = clean_string(value)
  return nil if email.nil?

  email = email.sub(/^mailto:/i, '')
  email = email.split(/[;,\s]+/).find { |part| part.include?('@') } || email
  clean_string(email)&.downcase
end

def jsonb_or_nil(value)
  return nil if value.nil?
  Sequel.pg_jsonb(value)
end

def to_time_from_epoch(value)
  return nil if value.nil?

  integer = value.is_a?(Numeric) ? value.to_i : clean_string(value)&.to_i
  return nil if integer.nil? || integer <= 0

  Time.at(integer).utc
rescue StandardError
  nil
end

def ensure_source_table!(db)
  return if db.table_exists?(:sam_vendors)

  raise Sequel::Error, 'Required source table sam_vendors does not exist'
end

def setup_database(db, logger)
  ensure_source_table!(db)

  return if db.table_exists?(CONTACT_TABLE)

  db.create_table(CONTACT_TABLE) do
    String :uei_sam, primary_key: true, size: 12
    String :cage_code, size: 5, null: false
    Integer :request_status
    Integer :attempt_count, default: 0, null: false
    String :request_error, text: true
    DateTime :source_last_update_at
    String :legal_business_name, text: true
    String :dba_name, text: true
    String :contact_person, text: true
    String :phone, text: true
    String :fax, text: true
    String :email, text: true
    String :website, text: true
    String :additional_website, text: true
    String :address_1, text: true
    String :address_2, text: true
    String :city, text: true
    String :state, text: true
    String :zipcode, text: true
    String :county, text: true
    String :congressional_district, text: true
    TrueClass :display_phone
    TrueClass :display_email
    TrueClass :display_fax
    TrueClass :display_address
    TrueClass :public_display
    TrueClass :public_display_limited
    column :certifications, :jsonb
    column :naics, :jsonb
    column :performance_history, :jsonb
    column :raw_profile, :jsonb
    DateTime :last_attempted_at
    DateTime :last_successful_at
    DateTime :created_at, default: Sequel::CURRENT_TIMESTAMP
    DateTime :db_updated_at, default: Sequel::CURRENT_TIMESTAMP

    index :cage_code
    index :request_status
    index :last_attempted_at
    index :source_last_update_at
  end

  logger.info("Created table: #{CONTACT_TABLE}")
end

setup_database(DB, LOG)
CONTACT_DS = DB[CONTACT_TABLE]

class SBASmallBusinessContactImporter
  def run
    candidates = candidate_scope
    total_candidates = candidates.count
    LOG.info("Starting SBA contact import: pending=#{total_candidates}")

    processed = 0
    success = 0
    not_found = 0
    failures = 0

    candidates.each do |vendor|
      status = process_vendor(vendor)
      processed += 1

      case status
      when 200 then success += 1
      when 404 then not_found += 1
      else failures += 1
      end

      if (processed % PROGRESS_EVERY).zero?
        LOG.info(
          "Progress: processed=#{processed}/#{total_candidates} " \
          "success=#{success} not_found=#{not_found} failures=#{failures}"
        )
      end

      sleep(REQUEST_DELAY_SECONDS) if REQUEST_DELAY_SECONDS.positive?
    end

    LOG.info(
      "Completed SBA contact import: processed=#{processed} " \
      "success=#{success} not_found=#{not_found} failures=#{failures}"
    )

    processed
  end

  private

  def candidate_scope
    source = Sequel[:sam_vendors]
    target = Sequel[CONTACT_TABLE]
    failure_cutoff = Time.now.utc - (RETRY_FAILURE_AFTER_HOURS * 3600)
    not_found_cutoff = Time.now.utc - (RETRY_NOT_FOUND_AFTER_DAYS * 86_400)

    missing_contact = { target[:uei_sam] => nil }
    retry_not_found = Sequel.&(
      { target[:request_status] => 404 },
      Sequel.|(
        { target[:last_attempted_at] => nil },
        Sequel.lit("#{CONTACT_TABLE}.last_attempted_at <= ?", not_found_cutoff)
      )
    )
    retry_failure = Sequel.&(
      Sequel.|(
        { target[:request_status] => nil },
        Sequel.~(target[:request_status] => [200, 404])
      ),
      Sequel.|(
        { target[:last_attempted_at] => nil },
        Sequel.lit("#{CONTACT_TABLE}.last_attempted_at <= ?", failure_cutoff)
      )
    )

    ds = DB[:sam_vendors]
      .left_join(CONTACT_TABLE, uei_sam: source[:uei_sam])
      .select(
        source[:uei_sam],
        source[:cage_code],
        source[:vendor_name]
      )
      .exclude(source[:sba_business_types_string] => nil)
      .exclude(source[:uei_sam] => nil)
      .exclude(source[:cage_code] => nil)
      .where(Sequel.lit("trim(coalesce(sam_vendors.uei_sam, '')) <> ''"))
      .where(Sequel.lit("trim(coalesce(sam_vendors.cage_code, '')) <> ''"))
      .where(Sequel.|(missing_contact, retry_not_found, retry_failure))
      .order(source[:uei_sam])

    LIMIT.positive? ? ds.limit(LIMIT) : ds
  end

  def process_vendor(vendor)
    uei_sam = vendor[:uei_sam].to_s.strip
    cage_code = vendor[:cage_code].to_s.strip
    attempted_at = Time.now.utc

    result = http_get_profile(uei_sam, cage_code)
    status = result[:status]

    if DRY_RUN
      LOG.info("DRY_RUN #{uei_sam}/#{cage_code} -> HTTP #{status}")
      return status
    end

    if status == 200
      payload = success_row(vendor, result[:payload], attempted_at, status)
      CONTACT_DS
        .insert_conflict(target: :uei_sam, update: success_update_map)
        .insert(payload)
    else
      payload = error_row(vendor, attempted_at, status, result[:error_message])
      CONTACT_DS
        .insert_conflict(target: :uei_sam, update: error_update_map)
        .insert(payload)

      LOG.warn("HTTP #{status} for #{uei_sam}/#{cage_code}: #{result[:error_message]}")
    end

    status
  rescue Sequel::DatabaseError => e
    LOG.warn("Database write failed for #{uei_sam}/#{cage_code}: #{e.message}")
    0
  end

  def http_get_profile(uei_sam, cage_code)
    uri = URI("#{PROFILE_BASE_URL}/#{URI.encode_www_form_component(uei_sam)}/#{URI.encode_www_form_component(cage_code)}")
    attempts = 0

    loop do
      attempts += 1

      begin
        response = perform_request(uri)
        status = response.code.to_i
        payload = parse_json(response.body)
        error_message = extract_error_message(status, payload)

        if TRANSIENT_HTTP_STATUSES.include?(status) && attempts < MAX_HTTP_RETRIES
          backoff_seconds = 2**(attempts - 1)
          LOG.warn("Retrying #{uei_sam}/#{cage_code} after HTTP #{status} in #{backoff_seconds}s")
          sleep(backoff_seconds)
          next
        end

        return { status: status, payload: payload, error_message: error_message }
      rescue Net::OpenTimeout, Net::ReadTimeout, EOFError, Errno::ECONNRESET, SocketError => e
        if attempts < MAX_HTTP_RETRIES
          backoff_seconds = 2**(attempts - 1)
          LOG.warn("Retrying #{uei_sam}/#{cage_code} after #{e.class} in #{backoff_seconds}s")
          sleep(backoff_seconds)
          next
        end

        return { status: 0, payload: nil, error_message: "#{e.class}: #{e.message}" }
      end
    end
  end

  def perform_request(uri, max_redirects: 3)
    redirects = 0
    current = uri

    loop do
      http = Net::HTTP.new(current.host, current.port)
      http.use_ssl = current.scheme == 'https'
      http.open_timeout = HTTP_TIMEOUT_SECONDS
      http.read_timeout = HTTP_TIMEOUT_SECONDS

      request = Net::HTTP::Get.new(current.request_uri)
      request['Accept'] = 'application/json'
      request['User-Agent'] = 'fpds-atom/1.0'

      response = http.request(request)
      return response unless response.is_a?(Net::HTTPRedirection)

      location = response['location']
      return response if location.to_s.strip.empty? || redirects >= max_redirects

      current = URI.join(current, location)
      redirects += 1
    end
  end

  def parse_json(body)
    return nil if body.nil? || body.to_s.strip.empty?

    JSON.parse(body)
  rescue JSON::ParserError
    nil
  end

  def extract_error_message(status, payload)
    return nil if status == 200
    return payload['error'] if payload.is_a?(Hash) && payload['error']
    return payload['message'] if payload.is_a?(Hash) && payload['message']

    "HTTP #{status}"
  end

  def success_row(vendor, payload, attempted_at, status)
    entity = payload.is_a?(Hash) ? payload['entity'] || {} : {}

    {
      uei_sam: vendor[:uei_sam],
      cage_code: vendor[:cage_code],
      request_status: status,
      attempt_count: 1,
      request_error: nil,
      source_last_update_at: to_time_from_epoch(entity['last_update_date']),
      legal_business_name: clean_string(entity['legal_business_name']),
      dba_name: clean_string(entity['dba_name']),
      contact_person: clean_string(entity['contact_person']),
      phone: clean_string(entity['phone']),
      fax: clean_string(entity['fax']),
      email: normalize_email(entity['email']),
      website: clean_and_validate_url(entity['website']) || clean_string(entity['website']),
      additional_website: clean_and_validate_url(entity['additional_website']) || clean_string(entity['additional_website']),
      address_1: clean_string(entity['address_1']),
      address_2: clean_string(entity['address_2']),
      city: clean_string(entity['city']),
      state: clean_string(entity['state']),
      zipcode: clean_string(entity['zipcode']),
      county: clean_string(entity['county']),
      congressional_district: clean_string(entity['congressional_district']),
      display_phone: entity['display_phone'],
      display_email: entity['display_email'],
      display_fax: entity['display_fax'],
      display_address: entity['display_address'],
      public_display: entity['public_display'],
      public_display_limited: entity['public_display_limited'],
      certifications: jsonb_or_nil(entity['certs']),
      naics: jsonb_or_nil(
        {
          naics_primary: entity['naics_primary'],
          naics_all_codes: entity['naics_all_codes'],
          naics_small_codes: entity['naics_small_codes'],
          naics_exception_codes: entity['naics_exception_codes'],
          records: payload['naics']
        }
      ),
      performance_history: jsonb_or_nil(payload['performanceHistory']),
      raw_profile: jsonb_or_nil(payload),
      last_attempted_at: attempted_at,
      last_successful_at: attempted_at,
      db_updated_at: Sequel::CURRENT_TIMESTAMP
    }
  end

  def error_row(vendor, attempted_at, status, error_message)
    {
      uei_sam: vendor[:uei_sam],
      cage_code: vendor[:cage_code],
      request_status: status,
      attempt_count: 1,
      request_error: error_message,
      last_attempted_at: attempted_at,
      db_updated_at: Sequel::CURRENT_TIMESTAMP
    }
  end

  def success_update_map
    {
      cage_code: Sequel[:excluded][:cage_code],
      request_status: Sequel[:excluded][:request_status],
      attempt_count: Sequel[CONTACT_TABLE][:attempt_count] + 1,
      request_error: nil,
      source_last_update_at: Sequel[:excluded][:source_last_update_at],
      legal_business_name: Sequel[:excluded][:legal_business_name],
      dba_name: Sequel[:excluded][:dba_name],
      contact_person: Sequel[:excluded][:contact_person],
      phone: Sequel[:excluded][:phone],
      fax: Sequel[:excluded][:fax],
      email: Sequel[:excluded][:email],
      website: Sequel[:excluded][:website],
      additional_website: Sequel[:excluded][:additional_website],
      address_1: Sequel[:excluded][:address_1],
      address_2: Sequel[:excluded][:address_2],
      city: Sequel[:excluded][:city],
      state: Sequel[:excluded][:state],
      zipcode: Sequel[:excluded][:zipcode],
      county: Sequel[:excluded][:county],
      congressional_district: Sequel[:excluded][:congressional_district],
      display_phone: Sequel[:excluded][:display_phone],
      display_email: Sequel[:excluded][:display_email],
      display_fax: Sequel[:excluded][:display_fax],
      display_address: Sequel[:excluded][:display_address],
      public_display: Sequel[:excluded][:public_display],
      public_display_limited: Sequel[:excluded][:public_display_limited],
      certifications: Sequel[:excluded][:certifications],
      naics: Sequel[:excluded][:naics],
      performance_history: Sequel[:excluded][:performance_history],
      raw_profile: Sequel[:excluded][:raw_profile],
      last_attempted_at: Sequel[:excluded][:last_attempted_at],
      last_successful_at: Sequel[:excluded][:last_successful_at],
      db_updated_at: Sequel::CURRENT_TIMESTAMP
    }
  end

  def error_update_map
    {
      cage_code: Sequel[:excluded][:cage_code],
      request_status: Sequel[:excluded][:request_status],
      attempt_count: Sequel[CONTACT_TABLE][:attempt_count] + 1,
      request_error: Sequel[:excluded][:request_error],
      last_attempted_at: Sequel[:excluded][:last_attempted_at],
      db_updated_at: Sequel::CURRENT_TIMESTAMP
    }
  end
end

if __FILE__ == $PROGRAM_NAME
  lock_file_path = File.join(__dir__, "#{File.basename(__FILE__)}.lock")
  lock_file = File.open(lock_file_path, 'w')

  unless lock_file.flock(File::LOCK_EX | File::LOCK_NB)
    LOG.warn('Script is already running. Another instance holds the lock. Exiting.')
    exit
  end

  begin
    importer = SBASmallBusinessContactImporter.new
    processed = importer.run
    puts "Processed #{processed} SBA profile request(s) into #{CONTACT_TABLE}"
  ensure
    lock_file.flock(File::LOCK_UN)
    lock_file.close
    LOG.info('Script finished and lock released.')
  end
end
