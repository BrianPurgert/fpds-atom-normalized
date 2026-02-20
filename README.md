# fpds-atom

Standalone tools for ingesting federal contract data from the [FPDS.gov](https://www.fpds.gov) Atom feed into a PostgreSQL database.

This package fetches modified contract award records from the FPDS Atom feed, parses the XML entries, and loads them into a normalized PostgreSQL schema with dimension tables for vendors, agencies, offices, product/service codes, and NAICS codes.

## What It Does

- Fetches modified FPDS contract awards via the public **Atom feed** from fpds.gov
- Parses XML entries with **Nokogiri** and converts content to normalized relational data
- Stores raw Atom content as JSONB for traceability and auditability
- Creates and maintains dimension tables:
  - `fpds_vendors` — Contractor info (UEI, name)
  - `fpds_agencies` — Federal agencies
  - `fpds_government_offices` — Awarding/funding offices
  - `fpds_product_or_service_codes` — PSC classifications
  - `fpds_naics_codes` — Industry codes
- Creates the fact table `fpds_contract_actions` with contract details, obligated amounts, and foreign keys to all dimensions
- Uses content hashing for idempotency (avoids duplicate ingestion)
- Supports resume on failure by querying the latest `fpds_last_modified_date` in `fpds_contract_actions`
- Handles missed days automatically by backfilling since the last successful run

## Requirements

- Ruby (>= 3.0 recommended)
- Bundler
- A PostgreSQL database

## Install

```bash
bundle install
```

## Environment

Copy `.env.example` to `.env` and set your PostgreSQL connection string:

```bash
cp .env.example .env
```

Required variables:
- `POSTGRES_URL` — PostgreSQL connection string (e.g., `postgres://user:password@host:5432/database`)

Alternative individual connection parameters:
- `POSTGRES_HOST`, `POSTGRES_DATABASE`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_SSLMODE`

## Usage

### Daily Mode (default)

```bash
bundle exec ruby scripts/fetch_fpds_modified_awards_normalized_v3.rb
```

The script will:
1. Connect to your PostgreSQL database
2. Create all required tables if they don't exist
3. Determine the date range to fetch (based on last successful run or default)
4. Fetch and process all pages of the Atom feed
5. Insert new records into the normalized schema

### Backfill Mode (download everything)

Since the FPDS Atom feed is being decommissioned, use backfill mode to download **all** historical records:

```bash
# Download everything from FY2001 (2000-10-01) through yesterday
bundle exec ruby scripts/fetch_fpds_modified_awards_normalized_v3.rb --backfill

# Download a specific date range
bundle exec ruby scripts/fetch_fpds_modified_awards_normalized_v3.rb --backfill --start-date 2020-01-01 --end-date 2024-12-31

# Resume an interrupted backfill from where it left off
bundle exec ruby scripts/fetch_fpds_modified_awards_normalized_v3.rb --resume
```

Backfill mode iterates day-by-day through the date range, querying the feed with closed date ranges (`LAST_MOD_DATE:[date,date]`) and paginating through all results for each day. **Multiple days are processed concurrently** using a thread pool (default: 4 threads) for significantly faster throughput.

Progress is tracked by querying the latest `fpds_last_modified_date` in `fpds_contract_actions`, so interrupted runs can be resumed with `--resume`.

Existing records are automatically skipped via content-hash deduplication, so it is safe to re-run overlapping date ranges.

```bash
# Use 8 threads for faster backfill
bundle exec ruby scripts/fetch_fpds_modified_awards_normalized_v3.rb --backfill --threads 8

# Backfill a specific range with 6 threads
bundle exec ruby scripts/fetch_fpds_modified_awards_normalized_v3.rb --backfill --start-date 2015-01-01 --end-date 2020-12-31 --threads 6
```

The database connection pool is automatically sized to match the thread count. Higher thread counts increase throughput but also increase load on FPDS.gov servers; 4-8 threads is recommended.

#### Options

| Flag | Description |
|------|-------------|
| `--backfill` | Enable backfill mode (iterate day-by-day) |
| `--start-date YYYY-MM-DD` | Start date for backfill (default: `2000-10-01`) |
| `--end-date YYYY-MM-DD` | End date for backfill (default: yesterday) |
| `--threads N` | Number of concurrent threads for backfill (default: `4`) |
| `--resume` | Resume a previously interrupted backfill |
| `-h`, `--help` | Show help message |

```bash
bundle exec ruby scripts/fetch_fpds_modified_awards_normalized_v3.rb --backfill --start-date 2009-12-07 --threads 7
```


## Automation (GitHub Actions)

A GitHub Actions workflow is included at `.github/workflows/fetch-fpds-modified-awards-v3.yml` that runs the ingestion daily at 03:00 UTC. Configure the following repository secrets:

- `POSTGRES_URL` (or `PG_URL`) — PostgreSQL connection string

Or use individual connection parameters:
- `POSTGRES_HOST`, `POSTGRES_DATABASE`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_SSLMODE`

## Testing

```bash
bundle exec rspec
```

## Project Structure

```
fpds-atom/
├── scripts/
│   └── fetch_fpds_modified_awards_normalized_v3.rb  # Main ingestion script
├── lib/
│   ├── database.rb          # PostgreSQL connection (Sequel ORM)
│   └── parsers.rb           # Date/datetime/float/boolean parsing utilities
├── spec/
│   ├── spec_helper.rb
│   └── parse_date_spec.rb
├── docs/
│   └── atom-feed-spec.md    # FPDS Atom feed data specification
├── .github/
│   └── workflows/
│       └── fetch-fpds-modified-awards-v3.yml  # Daily ingestion workflow
├── .env.example
├── .gitignore
├── .rspec
├── Gemfile
└── README.md
```

## Documentation

See `docs/atom-feed-spec.md` for the full FPDS Atom feed data specification, including all contract data fields, vendor information, competition details, and transaction metadata.

## Security

Do not commit secrets. Use `.env` locally and `.env.example` for documentation. Avoid committing real keys or database credentials.

## License

No license information specified. Add one if you plan to distribute.
