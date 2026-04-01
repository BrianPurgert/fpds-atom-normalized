#!/usr/bin/env ruby

require 'cgi'
require 'date'
require 'digest'
require 'logger'
require 'net/http'
require 'nokogiri'
require 'roo'
require 'sequel'
require 'tempfile'
require 'uri'

require 'dotenv'

Dotenv.load(File.expand_path('../.env', __dir__))

require_relative '../lib/database'

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
PROGRESS_EVERY = [(ENV['PROGRESS_EVERY']&.to_i || 500), 1].max
LOG.info("Startup: DRY_RUN=#{DRY_RUN ? 'ON' : 'OFF'} LOG_LEVEL=#{log_level} PROGRESS_EVERY=#{PROGRESS_EVERY}")

DB = Database.connect(logger: LOG)

ELIB_TABLE = :elibrary

def ensure_generated_columns(db, logger)
  db.run(<<~SQL)
    ALTER TABLE #{ELIB_TABLE}
    ADD COLUMN IF NOT EXISTS contract_start_date date
    GENERATED ALWAYS AS (((ultimate_contract_end_date - interval '20 years')::date)) STORED
  SQL

  db.run(<<~SQL)
    CREATE INDEX IF NOT EXISTS idx_elibrary_contract_start_date
      ON #{ELIB_TABLE} (contract_start_date DESC)
      WHERE contract_start_date IS NOT NULL
  SQL

  logger.info("Ensured generated columns/indexes for #{ELIB_TABLE}")
end

def setup_database(db, logger)
  unless db.table_exists?(ELIB_TABLE)
    db.create_table(ELIB_TABLE) do
      primary_key :id
      String :record_key, size: 64, null: false, unique: true
      String :large_category, text: true
      String :sub_category, text: true
      String :source, text: true
      String :category, text: true
      String :vendor_name, text: true
      String :contract_number, text: true
      TrueClass :is_closed_for_new_award, default: false
      String :street_address, text: true
      String :street_address_2, text: true
      String :city, text: true
      String :state_code, text: true
      String :postal_code, text: true
      String :country_code, text: true
      String :phone_number, text: true
      String :contact_email, text: true
      String :email_domain, text: true
      String :website_domain, text: true
      Date :current_option_period_end_date
      Date :ultimate_contract_end_date
      String :uei, size: 12
      TrueClass :is_small_business, default: false
      TrueClass :is_other_than_small_business, default: false
      TrueClass :is_woman_owned, default: false
      TrueClass :is_women_owned_small_business, default: false
      TrueClass :is_economically_disadvantaged_women_owned_small_business, default: false
      TrueClass :is_veteran_owned, default: false
      TrueClass :is_service_disabled_veteran_owned, default: false
      TrueClass :is_small_disadvantaged_business, default: false
      TrueClass :is_eight_a, default: false
      TrueClass :is_eight_a_sole_source_pool, default: false
      Date :eight_a_sole_source_exit_date
      TrueClass :is_hubzone, default: false
      TrueClass :is_tribally_owned_firm, default: false
      TrueClass :is_american_indian_owned, default: false
      TrueClass :is_alaskan_native_corporation_owned_firm, default: false
      TrueClass :is_native_hawaiian_organization_owned_firm, default: false
      TrueClass :is_eight_a_joint_venture_eligible, default: false
      TrueClass :is_women_owned_joint_venture_eligible, default: false
      TrueClass :is_service_disabled_veteran_owned_joint_venture_eligible, default: false
      TrueClass :is_hubzone_joint_venture_eligible, default: false
      TrueClass :is_cooperative_purchase, default: false
      TrueClass :is_disaster_recovery, default: false
      String :terms_url, text: true
      String :price_list_url, text: true
      String :view_catalog_url, text: true
      DateTime :fetched_at, default: Sequel::CURRENT_TIMESTAMP
      DateTime :db_updated_at, default: Sequel::CURRENT_TIMESTAMP

      index :contract_number
      index :vendor_name
      index :uei
      index :email_domain
      index :website_domain
      index %i[contract_number uei]
    end

    logger.info("Created table: #{ELIB_TABLE}")
  end

  ensure_generated_columns(db, logger)
end

setup_database(DB, LOG)
ELIB_DS = DB[ELIB_TABLE]

class GSAElibraryScheduleImporter
  BASE_ROOT = 'https://www.gsaelibrary.gsa.gov/ElibMain/'
  SCHEDULE_LIST_URL = BASE_ROOT + 'scheduleList.do'
  START_ROW_BASE = 4
  BATCH_SIZE = [(ENV['SCHEDULES_BATCH_SIZE']&.to_i || 1000), 1].max
  PREFERRED_SHEET_NAME = 'Contracts'
  FIXED_COLUMN_MAPPING = [
    :large_category,
    :sub_category,
    :source,
    :category,
    :vendor_name,
    :contract_number,
    :is_closed_for_new_award,
    :street_address,
    :street_address_2,
    :city,
    :state_code,
    :postal_code,
    :country_code,
    :phone_number,
    :contact_email,
    :website_domain,
    :current_option_period_end_date,
    :ultimate_contract_end_date,
    :uei,
    :is_small_business,
    :is_other_than_small_business,
    :is_woman_owned,
    :is_women_owned_small_business,
    :is_economically_disadvantaged_women_owned_small_business,
    :is_veteran_owned,
    :is_service_disabled_veteran_owned,
    :is_small_disadvantaged_business,
    :is_eight_a,
    :is_eight_a_sole_source_pool,
    :eight_a_sole_source_exit_date,
    :is_hubzone,
    :is_tribally_owned_firm,
    :is_american_indian_owned,
    :is_alaskan_native_corporation_owned_firm,
    :is_native_hawaiian_organization_owned_firm,
    :is_eight_a_joint_venture_eligible,
    :is_women_owned_joint_venture_eligible,
    :is_service_disabled_veteran_owned_joint_venture_eligible,
    :is_hubzone_joint_venture_eligible,
    :is_cooperative_purchase,
    :is_disaster_recovery,
    :terms_url,
    :price_list_url,
    :view_catalog_url
  ].freeze
  FIXED_COLUMN_COUNT = FIXED_COLUMN_MAPPING.size
  BOOL_COLUMNS = [
    :is_closed_for_new_award,
    :is_small_business,
    :is_other_than_small_business,
    :is_woman_owned,
    :is_women_owned_small_business,
    :is_economically_disadvantaged_women_owned_small_business,
    :is_veteran_owned,
    :is_service_disabled_veteran_owned,
    :is_small_disadvantaged_business,
    :is_eight_a,
    :is_eight_a_sole_source_pool,
    :is_hubzone,
    :is_tribally_owned_firm,
    :is_american_indian_owned,
    :is_alaskan_native_corporation_owned_firm,
    :is_native_hawaiian_organization_owned_firm,
    :is_eight_a_joint_venture_eligible,
    :is_women_owned_joint_venture_eligible,
    :is_service_disabled_veteran_owned_joint_venture_eligible,
    :is_hubzone_joint_venture_eligible,
    :is_cooperative_purchase,
    :is_disaster_recovery
  ].freeze
  DATE_COLUMNS = [
    :current_option_period_end_date,
    :ultimate_contract_end_date,
    :eight_a_sole_source_exit_date
  ].freeze
  HYPERLINK_COLUMNS = %i[website_domain terms_url price_list_url view_catalog_url].freeze
  REQUIRED_IDENTITY_COLUMNS = %i[contract_number vendor_name].freeze
  UPSERT_COLUMNS = (FIXED_COLUMN_MAPPING + [:email_domain]).freeze

  def initialize(limits: (ENV['SCHEDULES_LIMIT']&.to_i || 0), limit_rows: (ENV['SCHEDULES_LIMIT_ROWS']&.to_i || 0))
    @limits = limits
    @limit_rows = limit_rows
  end

  def http_get(url, max_redirects: 3, timeout: 30)
    redirects = 0
    current = URI.parse(url)

    while redirects <= max_redirects
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      LOG.debug("HTTP GET #{current} (redirects=#{redirects})")
      http = Net::HTTP.new(current.host, current.port)
      http.use_ssl = current.scheme == 'https'
      http.open_timeout = timeout
      http.read_timeout = timeout

      req = Net::HTTP::Get.new(current.request_uri)
      req['User-Agent'] = 'fpds.me bot/1.0'
      res = http.request(req)
      dur = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
      LOG.info("HTTP #{res.code} #{current} in #{format('%.2f', dur)}s")

      case res
      when Net::HTTPRedirection
        location = res['location']
        break unless location
        current = URI.join(current, location)
        redirects += 1
      else
        return res
      end
    end

    nil
  rescue StandardError => e
    LOG.warn("http_get error for #{url}: #{e.message}")
    nil
  end

  def parse_query_values(url)
    uri = URI.parse(url)
    query = uri.query.to_s
    return {} if query.empty?

    URI.decode_www_form(query).each_with_object({}) do |(key, value), out|
      k = key.to_s
      next if k.empty?
      out[k] = value.to_s
    end
  rescue StandardError
    {}
  end

  def fetch_list
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    LOG.info("Fetching schedule list: #{SCHEDULE_LIST_URL}")
    res = http_get(SCHEDULE_LIST_URL)
    unless res&.is_a?(Net::HTTPSuccess)
      LOG.warn("Failed to fetch schedule list (status=#{res&.code})")
      return []
    end

    doc = Nokogiri::HTML(res.body)
    schedules = []
    doc.css('a[href*="scheduleSummary.do"]').each do |a|
      href = a['href']
      next if href.to_s.strip.empty?

      begin
        abs = URI.join(SCHEDULE_LIST_URL, href).to_s
        q = parse_query_values(abs)
        schedule_number = (q['scheduleNumber'] || q['schedulenumber'] || '').strip
        next if schedule_number.empty?
        schedules << { schedule_number: schedule_number, title: a.text.strip, summary_url: abs }
      rescue StandardError
        next
      end
    end

    uniq = schedules.uniq { |s| s[:schedule_number] }
    dur = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
    LOG.info("Found #{uniq.size} schedule(s) in #{format('%.2f', dur)}s")
    uniq
  end

  def find_download_info_url(summary_url)
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    LOG.debug("Fetching summary page for download info: #{summary_url}")
    res = http_get(summary_url)
    unless res&.is_a?(Net::HTTPSuccess)
      LOG.warn("Failed to fetch summary page (status=#{res&.code}) for #{summary_url}")
      return nil
    end

    doc = Nokogiri::HTML(res.body)
    a = doc.css('a[href*="downloadInfo.do"]').first
    dur = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started

    if a
      url = URI.join(summary_url, a['href']).to_s
      LOG.info("Found downloadInfo URL in #{format('%.2f', dur)}s: #{url}")
      url
    else
      LOG.warn("No downloadInfo link found (#{format('%.2f', dur)}s)")
      nil
    end
  end

  def find_xlsx_url(download_info_url)
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    LOG.debug("Parsing download info page for XLSX link: #{download_info_url}")
    res = http_get(download_info_url)
    unless res&.is_a?(Net::HTTPSuccess)
      LOG.warn("Failed to fetch download info page (status=#{res&.code}) for #{download_info_url}")
      return { xlsx: nil, source: nil }
    end

    doc = Nokogiri::HTML(res.body)

    source = nil
    begin
      q = parse_query_values(download_info_url)
      source = (q['source'] || '').strip
      source = nil if source.empty?
    rescue StandardError
      source = nil
    end

    link = doc.css('a[href$=".xlsx"], a[href*="elib_contracts/"]').find do |x|
      x['href'].to_s.downcase.end_with?('.xlsx')
    end
    dur = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started

    if link
      href = link['href']
      abs = URI.join(download_info_url, href).to_s
      LOG.info("Found XLSX URL in #{format('%.2f', dur)}s: #{abs} (source=#{source || 'n/a'})")
      { xlsx: abs, source: source }
    else
      LOG.warn("No XLSX link found in #{format('%.2f', dur)}s (source=#{source || 'n/a'})")
      { xlsx: nil, source: source }
    end
  end

  def to_bool(value)
    return value if value == true || value == false
    return false if value.nil?

    s = value.to_s.strip.downcase
    return false if s.empty?
    return false if %w[false f no n 0 none n/a na].include?(s)
    return true if %w[true t yes y 1 x].include?(s)
    # eLibrary uses non-boolean indicator codes like "s", "w", "wo", "dv", "8a".
    # For these columns, any non-empty value that isn't an explicit negative marker means "present".
    true
  end

  def to_date(value)
    return nil if value.nil?
    return value.to_date if value.respond_to?(:to_date)

    if value.is_a?(Numeric)
      base = Date.new(1899, 12, 30)
      return base + value.to_i
    end

    s = value.to_s.strip
    return nil if s.empty?

    Date.parse(s)
  rescue StandardError
    nil
  end

  def clean_string(value)
    return nil if value.nil?
    text = value.to_s.strip
    text.empty? ? nil : text
  end

  def extract_first_url(text)
    return nil if text.nil?
    match = text.to_s.match(%r{https?://[^\s\)\]\"']+})
    match ? match[0] : nil
  end

  def serialize_hyperlink_cell(cell)
    raw = cell&.value
    if raw.is_a?(Roo::Link)
      href = raw.href.to_s.strip
      return href unless href.empty?
    end

    from_text = extract_first_url(raw)
    return from_text unless from_text.nil? || from_text.empty?

    clean_string(raw)
  end

  def normalize_uei(raw)
    s = clean_string(raw)
    return nil if s.nil?
    s = s.gsub(/\s+/, '')
    s.empty? ? nil : s[0, 12]
  end

  def normalize_phone_e164(raw)
    s = clean_string(raw)
    return nil if s.nil?

    s = s.gsub(/(?:ext\.?|x)\s*\d+/i, '')
    s = s.gsub(/[^\d\+]/, '')

    if s.start_with?('+')
      digits = s[1..].to_s.gsub(/\D/, '')
      return digits.empty? ? nil : "+#{digits}"
    end

    digits = s.gsub(/\D/, '')
    return nil if digits.empty?
    return "+#{digits}" if digits.length == 11 && digits.start_with?('1')
    return "+1#{digits}" if digits.length == 10
    return "+#{digits}" if digits.length >= 12 && digits.length <= 15

    nil
  end

  def normalize_email(raw)
    s = clean_string(raw)
    return nil if s.nil?
    s = s.sub(/^mailto:/i, '')
    candidate = s.split(/[;,\s]+/).find { |token| token.include?('@') } || s
    clean_string(candidate)&.downcase
  end

  def extract_email_domain(email)
    return nil if email.nil?
    _, domain = email.split('@', 2)
    dom = clean_string(domain)&.downcase
    dom = dom.sub(/^www\./, '') if dom
    dom
  end

  def extract_domain(text)
    s = clean_string(text)
    return nil if s.nil?

    host = nil
    begin
      candidate = s =~ %r{^https?://}i ? s : "http://#{s}"
      host = URI.parse(candidate).host
    rescue StandardError
      host = nil
    end

    domain = clean_string(host || s)&.downcase
    return nil if domain.nil?
    domain = domain.sub(/^www\./, '')
    domain = domain.sub(%r{/$}, '')
    domain.empty? ? nil : domain
  end

  def last_row_from_dimension(ref)
    return 0 if ref.nil?
    tail = ref.to_s.split(':').last.to_s
    match = tail.match(/(\d+)\z/)
    match ? match[1].to_i : 0
  end

  def choose_sheet(xlsx)
    candidates = xlsx.sheets.each_with_index.map do |name, idx|
      sheet = xlsx.sheet_for(name)
      { name: name, index: idx, rows: last_row_from_dimension(sheet.dimensions), sheet: sheet }
    end

    chosen = candidates.find { |candidate| candidate[:name].to_s.casecmp(PREFERRED_SHEET_NAME).zero? }
    chosen ||= candidates.max_by { |candidate| candidate[:rows] }
    chosen || { name: nil, index: nil, rows: 0, sheet: nil }
  end

  def row_number(row_cells)
    row_cells.filter_map { |cell| cell&.coordinate&.row }.min.to_i
  end

  def build_row(row_cells, schedule_source)
    out = {}
    cells_by_index = {}

    row_cells.each do |cell|
      next unless cell&.coordinate
      col_idx = cell.coordinate.column.to_i - 1
      next if col_idx.negative? || col_idx >= FIXED_COLUMN_COUNT
      cells_by_index[col_idx] = cell
    end

    FIXED_COLUMN_MAPPING.each_with_index do |column, i|
      cell = cells_by_index[i]
      raw = cell&.value

      out[column] =
        if HYPERLINK_COLUMNS.include?(column)
          serialize_hyperlink_cell(cell)
        elsif DATE_COLUMNS.include?(column)
          to_date(raw)
        elsif BOOL_COLUMNS.include?(column)
          to_bool(raw)
        else
          clean_string(raw)
        end
    end

    out[:source] = schedule_source if out[:source].nil? && schedule_source
    out[:phone_number] = normalize_phone_e164(out[:phone_number])
    out[:contact_email] = normalize_email(out[:contact_email])
    out[:email_domain] = extract_email_domain(out[:contact_email])
    out[:website_domain] = extract_domain(out[:website_domain])
    out[:uei] = normalize_uei(out[:uei])

    return nil unless REQUIRED_IDENTITY_COLUMNS.all? { |key| clean_string(out[key]) }

    out[:record_key] = Digest::SHA256.hexdigest([
      out[:contract_number].to_s.strip.upcase,
      out[:vendor_name].to_s.strip.downcase,
      out[:uei].to_s.strip.upcase,
      out[:source].to_s.strip.downcase
    ].join('|'))

    out
  end

  def conflict_update_map
    updates = {}
    UPSERT_COLUMNS.each { |column| updates[column] = Sequel[:excluded][column] }
    updates[:db_updated_at] = Sequel::CURRENT_TIMESTAMP
    updates
  end

  def flush_rows(schedule_number, rows)
    return if rows.empty?
    if DRY_RUN
      rows.clear
      return
    end

    deduped = {}
    rows.each do |row|
      key = row[:record_key]
      next if key.nil?
      deduped[key] = row
    end
    rows_to_write = deduped.values
    dropped = rows.size - rows_to_write.size
    LOG.warn("Dropped #{dropped} duplicate rows in batch for #{schedule_number}") if dropped.positive?
    return if rows_to_write.empty?

    ELIB_DS.insert_conflict(target: :record_key, update: conflict_update_map).multi_insert(rows_to_write)
  rescue Sequel::DatabaseError => e
    LOG.warn("Batch upsert failed for #{schedule_number} (#{rows_to_write&.size || rows.size} rows): #{e.message}")
    (rows_to_write || rows).each do |row|
      begin
        ELIB_DS.insert_conflict(target: :record_key, update: conflict_update_map).insert(row)
      rescue Sequel::DatabaseError => row_error
        LOG.warn("Row upsert failed for contract_number=#{row[:contract_number]}: #{row_error.message}")
      end
    end
  ensure
    rows.clear
  end

  def import(schedule)
    schedule_number = schedule[:schedule_number]
    schedule_title = schedule[:title]
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    LOG.info("Importing schedule #{schedule_number} - #{schedule_title}")

    download_info_url = find_download_info_url(schedule[:summary_url])
    return 0 unless download_info_url

    info = find_xlsx_url(download_info_url)
    xlsx_url = info[:xlsx]
    schedule_source = info[:source]
    unless xlsx_url
      LOG.warn("No XLSX URL for schedule #{schedule_number}")
      return 0
    end

    res = http_get(xlsx_url, timeout: 60)
    return 0 unless res&.is_a?(Net::HTTPSuccess)

    LOG.info("Got XLSX response for #{schedule_number} (#{res.body.to_s.bytesize} bytes)")

    Tempfile.create(['elib_sched', '.xlsx']) do |tmp|
      tmp.binmode
      tmp.write(res.body)
      tmp.flush

      open_started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      xlsx = Roo::Excelx.new(tmp.path)
      selection = choose_sheet(xlsx)
      sheet = selection[:sheet]
      open_dur = Process.clock_gettime(Process::CLOCK_MONOTONIC) - open_started

      if sheet.nil?
        LOG.warn("No worksheet available in #{schedule_number}")
        return 0
      end

      rows_hint = selection[:rows].to_i
      rows_hint_note = rows_hint.positive? ? " (row hint=#{rows_hint})" : ''
      LOG.info("Prepared spreadsheet; selected sheet '#{selection[:name]}' (index=#{selection[:index]})#{rows_hint_note} in #{format('%.2f', open_dur)}s")
      LOG.info("Skipping first 3 rows and using fixed #{FIXED_COLUMN_COUNT}-column mapping; batch_size=#{BATCH_SIZE}")

      count = 0
      loop_started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      total_iter = @limit_rows.positive? ? @limit_rows : nil
      pending_rows = []
      row_iterator_method = sheet.respond_to?(:each_row_streaming) ? :each_row_streaming : :each_row
      LOG.info("Using row iterator #{row_iterator_method} for schedule #{schedule_number}")

      sheet.public_send(row_iterator_method) do |row_cells|
        current_row = row_number(row_cells)
        next if current_row.positive? && current_row < START_ROW_BASE
        break if @limit_rows.positive? && count >= @limit_rows

        out = build_row(row_cells, schedule_source)
        next if out.nil?

        pending_rows << out
        count += 1
        flush_rows(schedule_number, pending_rows) if pending_rows.size >= BATCH_SIZE

        if (count % PROGRESS_EVERY).zero?
          elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - loop_started
          rate = count / [elapsed, 0.0001].max
          if total_iter
            remaining = [total_iter - count, 0].max
            est_remaining = remaining / [rate, 0.0001].max
            LOG.info("Progress #{schedule_number}: #{count}/#{total_iter} rows (#{format('%.1f', rate)} r/s, est #{format('%.1f', est_remaining)}s left)")
          else
            LOG.info("Progress #{schedule_number}: #{count} rows (#{format('%.1f', rate)} r/s)")
          end
        end
      end

      flush_rows(schedule_number, pending_rows)
      total_dur = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
      LOG.info("Finished schedule #{schedule_number}: #{count} rows in #{format('%.2f', total_dur)}s (avg #{format('%.1f', count / [total_dur, 0.0001].max)} r/s)")
      count
    end
  rescue StandardError => e
    LOG.warn("Failed to import schedule #{schedule_number}: #{e.message}")
    0
  end

  def import_all
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    schedules = fetch_list
    schedules = schedules.first(@limits) if @limits.positive?
    LOG.info("Starting import of #{schedules.size} schedule(s); limit_rows=#{@limit_rows}")

    total_rows = 0
    schedules.each_with_index do |schedule, idx|
      schedule_started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      rows = import(schedule)
      total_rows += rows
      schedule_dur = Process.clock_gettime(Process::CLOCK_MONOTONIC) - schedule_started
      LOG.info("Imported schedule #{schedule[:schedule_number]} (#{rows} rows) in #{format('%.2f', schedule_dur)}s [#{idx + 1}/#{schedules.size}]")
    end

    dur = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
    LOG.info("All schedules complete: total_rows=#{total_rows} in #{format('%.2f', dur)}s")
    total_rows
  end
end

if __FILE__ == $PROGRAM_NAME
  importer = GSAElibraryScheduleImporter.new
  total_rows = importer.import_all
  puts "Imported #{total_rows} rows into #{ELIB_TABLE}"
end
