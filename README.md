# fpds-atom

Standalone tools for ingesting and searching federal contract data from the [FPDS.gov](https://www.fpds.gov) Atom feed, stored in a PostgreSQL database with a modern web interface.

This package fetches modified contract award records from the FPDS Atom feed, parses the XML entries, loads them into a normalized PostgreSQL schema, and provides a web application for searching and browsing the data.

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
- Tracks state via a `job_tracker` table (remembers last fetch date, supports resume on failure)
- Handles missed days automatically by backfilling since the last successful run
- **Web application** with ezSearch, contract detail views, reports, FAQ, and JSON API

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

Optional:
- `PORT` — Web server port (default: `4567`)

## Web Application

### Starting the Server

```bash
bundle exec puma config.ru -p 4567
```

Then open [http://localhost:4567](http://localhost:4567) in your browser.

### Pages

| Route | Description |
|-------|-------------|
| `/` | Home page with stats dashboard and search bar |
| `/search` | ezSearch — keyword search with advanced filters |
| `/contract/:id` | Contract detail view with full record |
| `/reports` | Top agencies, vendors, and NAICS codes by spending |
| `/faq` | Frequently asked questions about FPDS data |
| `/help` | Help guides, glossary, and API documentation |
| `/api/search` | JSON API endpoint for programmatic access |

### ezSearch Features

- **Keyword search** across PIID, vendor name, agency, office, description, solicitation ID, and IDV references
- **Advanced filters**: agency, vendor name, NAICS code, PSC code, date range, dollar amount, state, set-aside type
- **Paginated results** with result count and search timing
- **Result cards** showing PIID, obligated amount, description, vendor, agency, office, NAICS, PSC, and state
- **Contract detail** pages with 10+ sections: award ID, dollar values, contract details, dates, purchaser info, vendor info (with socioeconomic indicators), product/service, competition, place of performance, transaction info, and treasury accounts

### JSON API

```
GET /api/search?q=keyword&page=1&per_page=25
```

Returns JSON with `total`, `page`, `per_page`, `total_pages`, and `results` array containing contract data.

## Data Ingestion

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

Backfill mode iterates day-by-day through the date range, querying the feed with closed date ranges (`LAST_MOD_DATE:[date,date]`) and paginating through all results for each day. Progress is tracked in the `job_tracker` table, so interrupted runs can be resumed with `--resume`.

Existing records are automatically skipped via content-hash deduplication, so it is safe to re-run overlapping date ranges.

#### Options

| Flag | Description |
|------|-------------|
| `--backfill` | Enable backfill mode (iterate day-by-day) |
| `--start-date YYYY-MM-DD` | Start date for backfill (default: `2000-10-01`) |
| `--end-date YYYY-MM-DD` | End date for backfill (default: yesterday) |
| `--resume` | Resume a previously interrupted backfill |
| `-h`, `--help` | Show help message |

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
├── app.rb                    # Sinatra web application
├── config.ru                 # Rack configuration
├── views/
│   ├── layout.erb            # Main layout template
│   ├── home.erb              # Home page with stats & search
│   ├── search.erb            # ezSearch results page
│   ├── contract_detail.erb   # Full contract record view
│   ├── reports.erb           # Top agencies/vendors/NAICS
│   ├── faq.erb               # Frequently asked questions
│   ├── help.erb              # Help guides & API docs
│   ├── not_found.erb         # 404 page
│   └── error.erb             # Error page
├── public/
│   └── css/
│       └── style.css         # Application stylesheet
├── scripts/
│   └── fetch_fpds_modified_awards_normalized_v3.rb  # Data ingestion script
├── lib/
│   ├── database.rb           # PostgreSQL connection (Sequel ORM)
│   ├── normalizer.rb         # Data normalization utilities
│   └── parsers.rb            # Date/datetime/float/boolean parsing
├── spec/
│   ├── spec_helper.rb
│   └── parse_date_spec.rb
├── docs/
│   └── atom-feed-spec.md     # FPDS Atom feed data specification
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
