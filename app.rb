require 'sinatra/base'
require 'sinatra/json'
require 'sequel'
require 'logger'
require 'dotenv'
require 'json'
require 'cgi'

Dotenv.load

require_relative 'lib/database'

class FPDSApp < Sinatra::Base
  helpers Sinatra::JSON

  LOG = Logger.new(STDOUT)
  LOG.level = Logger::INFO

  configure do
    set :views, File.join(File.dirname(__FILE__), 'views')
    set :public_folder, File.join(File.dirname(__FILE__), 'public')
    set :port, ENV.fetch('PORT', 4567).to_i
    set :bind, '0.0.0.0'
    set :show_exceptions, :after_handler

    begin
      db = Database.connect(logger: LOG)
      set :db, db
    rescue StandardError => e
      LOG.fatal "Database connection failed: #{e.message}"
      raise
    end
  end

  helpers do
    def db
      settings.db
    end

    def h(text)
      CGI.escapeHTML(text.to_s)
    end

    def format_currency(amount)
      return 'N/A' if amount.nil?
      negative = amount < 0
      abs = amount.abs
      formatted = abs.round(2).to_s.reverse.gsub(/(\d{3})(?=\d)/, '\\1,').reverse
      formatted = "$#{formatted}"
      formatted = "-#{formatted}" if negative
      # Ensure two decimal places
      parts = formatted.split('.')
      parts[1] = (parts[1] || '').ljust(2, '0')
      parts.join('.')
    end

    def format_date(date)
      return 'N/A' if date.nil?
      date.strftime('%m/%d/%Y')
    end

    def format_datetime(dt)
      return 'N/A' if dt.nil?
      dt.strftime('%m/%d/%Y %H:%M')
    end

    def page_range(current, total, window = 3)
      left = [1, current - window].max
      right = [total, current + window].min
      (left..right).to_a
    end

    def truncate(text, length = 120)
      return '' if text.nil?
      text.length > length ? "#{text[0...length]}..." : text
    end

    def build_search_qs(target_page, q, pp, f)
      params_hash = {
        'q' => q, 'page' => target_page, 'per_page' => pp,
        'agency' => f[:agency], 'naics' => f[:naics], 'psc' => f[:psc],
        'date_from' => f[:date_from], 'date_to' => f[:date_to],
        'amount_min' => f[:amount_min], 'amount_max' => f[:amount_max],
        'set_aside' => f[:set_aside], 'state' => f[:state], 'vendor_name' => f[:vendor_name]
      }.reject { |_k, v| v.nil? || v.to_s.empty? }
      params_hash.map { |k, v| "#{CGI.escape(k.to_s)}=#{CGI.escape(v.to_s)}" }.join('&')
    end
  end

  # --- Home Page ---
  get '/' do
    stats = {}
    begin
      stats[:contracts] = db[:fpds_contract_actions].count
      stats[:vendors] = db[:fpds_vendors].count
      stats[:agencies] = db[:fpds_agencies].count
      stats[:total_obligated] = db[:fpds_contract_actions].sum(:obligated_amount) || 0
      stats[:latest_date] = db[:fpds_contract_actions].max(:fpds_last_modified_date)
      stats[:earliest_date] = db[:fpds_contract_actions].min(:fpds_last_modified_date)
    rescue => e
      LOG.warn "Could not load stats: #{e.message}"
    end
    erb :home, locals: { stats: stats }
  end

  # --- ezSearch ---
  get '/search' do
    query = (params[:q] || '').strip
    page = [1, (params[:page] || 1).to_i].max
    per_page = [[10, (params[:per_page] || 25).to_i].max, 100].min

    # Filters
    agency_code = (params[:agency] || '').strip
    naics_code = (params[:naics] || '').strip
    psc_code = (params[:psc] || '').strip
    date_from = params[:date_from].to_s.strip
    date_to = params[:date_to].to_s.strip
    amount_min = params[:amount_min].to_s.strip
    amount_max = params[:amount_max].to_s.strip
    set_aside = (params[:set_aside] || '').strip
    state = (params[:state] || '').strip
    vendor_name = (params[:vendor_name] || '').strip

    results = []
    total = 0
    total_pages = 0
    search_time = 0

    has_search = !query.empty? || !agency_code.empty? || !naics_code.empty? || !psc_code.empty? ||
                 !date_from.empty? || !date_to.empty? || !amount_min.empty? || !amount_max.empty? ||
                 !set_aside.empty? || !state.empty? || !vendor_name.empty?

    if has_search
      start_time = Time.now

      ds = db[:fpds_contract_actions]
             .left_join(:fpds_vendors, id: :vendor_id)
             .left_join(:fpds_agencies, Sequel[:fpds_agencies][:id] => Sequel[:fpds_contract_actions][:agency_id])
             .left_join(:fpds_government_offices, Sequel[:fpds_government_offices][:id] => Sequel[:fpds_contract_actions][:contracting_office_id])
             .left_join(:fpds_product_or_service_codes, Sequel[:fpds_product_or_service_codes][:id] => Sequel[:fpds_contract_actions][:product_or_service_code_id])
             .left_join(:fpds_naics_codes, Sequel[:fpds_naics_codes][:id] => Sequel[:fpds_contract_actions][:naics_code_id])

      # Keyword search across multiple text fields
      unless query.empty?
        like_pattern = "%#{query}%"
        ds = ds.where(
          Sequel.|(
            Sequel[:fpds_contract_actions][:piid].ilike(like_pattern),
            Sequel[:fpds_contract_actions][:description_of_requirement].ilike(like_pattern),
            Sequel[:fpds_contract_actions][:solicitation_id].ilike(like_pattern),
            Sequel[:fpds_contract_actions][:referenced_idv_piid].ilike(like_pattern),
            Sequel[:fpds_vendors][:vendor_name].ilike(like_pattern),
            Sequel[:fpds_agencies][:agency_name].ilike(like_pattern),
            Sequel[:fpds_government_offices][:office_name].ilike(like_pattern)
          )
        )
      end

      # Agency filter
      unless agency_code.empty?
        ds = ds.where(Sequel[:fpds_agencies][:agency_code] => agency_code)
      end

      # NAICS filter
      unless naics_code.empty?
        ds = ds.where(Sequel[:fpds_naics_codes][:naics_code] => naics_code)
      end

      # PSC filter
      unless psc_code.empty?
        ds = ds.where(Sequel[:fpds_product_or_service_codes][:psc_code] => psc_code)
      end

      # Date range filter (on signed_date)
      unless date_from.empty?
        begin
          ds = ds.where { Sequel[:fpds_contract_actions][:signed_date] >= Date.parse(date_from) }
        rescue ArgumentError; end
      end
      unless date_to.empty?
        begin
          ds = ds.where { Sequel[:fpds_contract_actions][:signed_date] <= Date.parse(date_to) }
        rescue ArgumentError; end
      end

      # Dollar amount filter
      unless amount_min.empty?
        ds = ds.where { Sequel[:fpds_contract_actions][:obligated_amount] >= amount_min.to_f }
      end
      unless amount_max.empty?
        ds = ds.where { Sequel[:fpds_contract_actions][:obligated_amount] <= amount_max.to_f }
      end

      # Set-aside filter
      unless set_aside.empty?
        ds = ds.where(Sequel[:fpds_contract_actions][:type_of_set_aside].ilike("%#{set_aside}%"))
      end

      # State (place of performance)
      unless state.empty?
        ds = ds.where(Sequel[:fpds_contract_actions][:pop_state_code] => state.upcase)
      end

      # Vendor name filter
      unless vendor_name.empty?
        ds = ds.where(Sequel[:fpds_vendors][:vendor_name].ilike("%#{vendor_name}%"))
      end

      # Count and paginate
      count_ds = ds.select { count(Sequel[:fpds_contract_actions][:id]).as(:cnt) }
      total = count_ds.first[:cnt] rescue 0
      total_pages = (total.to_f / per_page).ceil

      results = ds.select(
        Sequel[:fpds_contract_actions][:id].as(:id),
        Sequel[:fpds_contract_actions][:piid],
        Sequel[:fpds_contract_actions][:modification_number],
        Sequel[:fpds_contract_actions][:description_of_requirement],
        Sequel[:fpds_contract_actions][:obligated_amount],
        Sequel[:fpds_contract_actions][:signed_date],
        Sequel[:fpds_contract_actions][:effective_date],
        Sequel[:fpds_contract_actions][:action_type_description],
        Sequel[:fpds_contract_actions][:pricing_type_description],
        Sequel[:fpds_contract_actions][:type_of_set_aside],
        Sequel[:fpds_contract_actions][:pop_state_code],
        Sequel[:fpds_contract_actions][:pop_city],
        Sequel[:fpds_contract_actions][:extent_competed],
        Sequel[:fpds_contract_actions][:referenced_idv_piid],
        Sequel[:fpds_vendors][:vendor_name].as(:vendor_name),
        Sequel[:fpds_vendors][:uei_sam].as(:vendor_uei),
        Sequel[:fpds_agencies][:agency_name].as(:agency_name),
        Sequel[:fpds_agencies][:agency_code].as(:agency_code_val),
        Sequel[:fpds_government_offices][:office_name].as(:office_name),
        Sequel[:fpds_product_or_service_codes][:psc_code].as(:psc_code_val),
        Sequel[:fpds_product_or_service_codes][:psc_description].as(:psc_description),
        Sequel[:fpds_naics_codes][:naics_code].as(:naics_code_val),
        Sequel[:fpds_naics_codes][:naics_description].as(:naics_description)
      )
        .order(Sequel.desc(Sequel[:fpds_contract_actions][:signed_date]))
        .limit(per_page)
        .offset((page - 1) * per_page)
        .all

      search_time = ((Time.now - start_time) * 1000).round(1)
    end

    # Load agencies for filter dropdown
    agencies = db[:fpds_agencies].order(:agency_name).select(:agency_code, :agency_name).all rescue []

    erb :search, locals: {
      query: query, results: results, total: total, total_pages: total_pages,
      page: page, per_page: per_page, has_search: has_search, search_time: search_time,
      agencies: agencies,
      filters: {
        agency: agency_code, naics: naics_code, psc: psc_code,
        date_from: date_from, date_to: date_to,
        amount_min: amount_min, amount_max: amount_max,
        set_aside: set_aside, state: state, vendor_name: vendor_name
      }
    }
  end

  # --- Contract Detail ---
  get '/contract/:id' do
    contract = db[:fpds_contract_actions]
                 .left_join(:fpds_vendors, id: :vendor_id)
                 .left_join(:fpds_agencies, Sequel[:fpds_agencies][:id] => Sequel[:fpds_contract_actions][:agency_id])
                 .left_join(Sequel[:fpds_agencies].as(:funding_agency), Sequel[:funding_agency][:id] => Sequel[:fpds_contract_actions][:funding_agency_id])
                 .left_join(:fpds_government_offices, Sequel[:fpds_government_offices][:id] => Sequel[:fpds_contract_actions][:contracting_office_id])
                 .left_join(Sequel[:fpds_government_offices].as(:funding_office), Sequel[:funding_office][:id] => Sequel[:fpds_contract_actions][:funding_office_id])
                 .left_join(:fpds_product_or_service_codes, Sequel[:fpds_product_or_service_codes][:id] => Sequel[:fpds_contract_actions][:product_or_service_code_id])
                 .left_join(:fpds_naics_codes, Sequel[:fpds_naics_codes][:id] => Sequel[:fpds_contract_actions][:naics_code_id])
                 .select_all(:fpds_contract_actions)
                 .select_append(
                   Sequel[:fpds_vendors][:vendor_name].as(:vendor_name),
                   Sequel[:fpds_vendors][:uei_sam].as(:vendor_uei),
                   Sequel[:fpds_agencies][:agency_name].as(:agency_name),
                   Sequel[:fpds_agencies][:agency_code].as(:agency_code_val),
                   Sequel[:funding_agency][:agency_name].as(:funding_agency_name),
                   Sequel[:funding_agency][:agency_code].as(:funding_agency_code),
                   Sequel[:fpds_government_offices][:office_name].as(:contracting_office_name),
                   Sequel[:fpds_government_offices][:office_code].as(:contracting_office_code),
                   Sequel[:funding_office][:office_name].as(:funding_office_name),
                   Sequel[:funding_office][:office_code].as(:funding_office_code),
                   Sequel[:fpds_product_or_service_codes][:psc_code].as(:psc_code_val),
                   Sequel[:fpds_product_or_service_codes][:psc_description].as(:psc_description),
                   Sequel[:fpds_naics_codes][:naics_code].as(:naics_code_val),
                   Sequel[:fpds_naics_codes][:naics_description].as(:naics_description)
                 )
                 .where(Sequel[:fpds_contract_actions][:id] => params[:id].to_i)
                 .first

    halt 404, erb(:not_found) unless contract

    vendor_detail = db[:fpds_contract_vendor_details].where(contract_action_id: contract[:id]).first rescue nil
    treasury_accounts = db[:fpds_treasury_accounts].where(contract_action_id: contract[:id]).all rescue []

    erb :contract_detail, locals: { contract: contract, vendor_detail: vendor_detail, treasury_accounts: treasury_accounts }
  end

  # --- API: JSON search endpoint ---
  get '/api/search' do
    content_type :json
    query = (params[:q] || '').strip
    page = [1, (params[:page] || 1).to_i].max
    per_page = [[10, (params[:per_page] || 25).to_i].max, 100].min

    ds = db[:fpds_contract_actions]
           .left_join(:fpds_vendors, id: :vendor_id)
           .left_join(:fpds_agencies, Sequel[:fpds_agencies][:id] => Sequel[:fpds_contract_actions][:agency_id])

    unless query.empty?
      like_pattern = "%#{query}%"
      ds = ds.where(
        Sequel.|(
          Sequel[:fpds_contract_actions][:piid].ilike(like_pattern),
          Sequel[:fpds_contract_actions][:description_of_requirement].ilike(like_pattern),
          Sequel[:fpds_vendors][:vendor_name].ilike(like_pattern),
          Sequel[:fpds_agencies][:agency_name].ilike(like_pattern)
        )
      )
    end

    total = ds.count
    results = ds.select(
      Sequel[:fpds_contract_actions][:id],
      Sequel[:fpds_contract_actions][:piid],
      Sequel[:fpds_contract_actions][:description_of_requirement],
      Sequel[:fpds_contract_actions][:obligated_amount],
      Sequel[:fpds_contract_actions][:signed_date],
      Sequel[:fpds_vendors][:vendor_name],
      Sequel[:fpds_agencies][:agency_name]
    )
      .order(Sequel.desc(Sequel[:fpds_contract_actions][:signed_date]))
      .limit(per_page)
      .offset((page - 1) * per_page)
      .all

    json({
      total: total,
      page: page,
      per_page: per_page,
      total_pages: (total.to_f / per_page).ceil,
      results: results
    })
  end

  # --- FAQ ---
  get '/faq' do
    erb :faq
  end

  # --- Help ---
  get '/help' do
    erb :help
  end

  # --- Reports ---
  get '/reports' do
    top_agencies = db[:fpds_contract_actions]
                     .left_join(:fpds_agencies, Sequel[:fpds_agencies][:id] => Sequel[:fpds_contract_actions][:agency_id])
                     .select(
                       Sequel[:fpds_agencies][:agency_name],
                       Sequel[:fpds_agencies][:agency_code],
                       Sequel.function(:count, Sequel[:fpds_contract_actions][:id]).as(:contract_count),
                       Sequel.function(:sum, Sequel[:fpds_contract_actions][:obligated_amount]).as(:total_obligated)
                     )
                     .group(Sequel[:fpds_agencies][:agency_name], Sequel[:fpds_agencies][:agency_code])
                     .order(Sequel.desc(:total_obligated))
                     .limit(25)
                     .all rescue []

    top_vendors = db[:fpds_contract_actions]
                    .left_join(:fpds_vendors, id: :vendor_id)
                    .select(
                      Sequel[:fpds_vendors][:vendor_name],
                      Sequel[:fpds_vendors][:uei_sam],
                      Sequel.function(:count, Sequel[:fpds_contract_actions][:id]).as(:contract_count),
                      Sequel.function(:sum, Sequel[:fpds_contract_actions][:obligated_amount]).as(:total_obligated)
                    )
                    .exclude(Sequel[:fpds_vendors][:vendor_name] => nil)
                    .group(Sequel[:fpds_vendors][:vendor_name], Sequel[:fpds_vendors][:uei_sam])
                    .order(Sequel.desc(:total_obligated))
                    .limit(25)
                    .all rescue []

    top_naics = db[:fpds_contract_actions]
                  .left_join(:fpds_naics_codes, Sequel[:fpds_naics_codes][:id] => Sequel[:fpds_contract_actions][:naics_code_id])
                  .select(
                    Sequel[:fpds_naics_codes][:naics_code],
                    Sequel[:fpds_naics_codes][:naics_description],
                    Sequel.function(:count, Sequel[:fpds_contract_actions][:id]).as(:contract_count),
                    Sequel.function(:sum, Sequel[:fpds_contract_actions][:obligated_amount]).as(:total_obligated)
                  )
                  .exclude(Sequel[:fpds_naics_codes][:naics_code] => nil)
                  .group(Sequel[:fpds_naics_codes][:naics_code], Sequel[:fpds_naics_codes][:naics_description])
                  .order(Sequel.desc(:total_obligated))
                  .limit(25)
                  .all rescue []

    erb :reports, locals: { top_agencies: top_agencies, top_vendors: top_vendors, top_naics: top_naics }
  end

  # --- 404 handler ---
  not_found do
    erb :not_found
  end

  # --- Error handler ---
  error do
    erb :error
  end
end
