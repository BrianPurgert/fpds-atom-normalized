require 'sequel'
require 'logger'
require 'dotenv'

# Ensure Sequel JSON helpers are available globally (pg_json_op, jsonb ops, etc.)
# pg_json defines type-casting; pg_json_ops defines Sequel.pg_json_op helpers
begin
  Sequel.extension :pg_json
  Sequel.extension :pg_json_ops
rescue => _
  # If extensions are unavailable for some reason, proceed without crashing.
end

module Database
  def self.connect(logger: Logger.new($stdout))
    Dotenv.load
    begin
      db = if ENV['POSTGRES_URL']
              Sequel.connect(ENV['POSTGRES_URL'])
            else
              Sequel.connect(
                adapter:  'postgres',
                host:     ENV['POSTGRES_HOST'] ,
                database: ENV['POSTGRES_DATABASE'] || 'fpds_data',
                user:     ENV['POSTGRES_USER'] || 'postgres',
                password: ENV['POSTGRES_PASSWORD'] || 'password',
                sslmode:  ENV['POSTGRES_SSLMODE'] || 'prefer'
              )
            end
      db.test_connection
      logger.info "Connected to PostgreSQL database"
      db
    rescue Sequel::DatabaseConnectionError => e
      logger.fatal "Failed to connect to the database: #{e.message}"
      raise
    end
  end
end
