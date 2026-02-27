# bid-crawler

Commercial construction bid aggregator for DFW-area public institutions (school districts, cities, counties, state agencies, federal projects).

## Features

- **4 data sources**: SAM.gov (federal), Texas ESBD (state), BidNet Direct, OpenGov
- **Smart filtering**: NAICS codes 236–238 + configurable keywords + county geo-filter
- **Incremental loads**: only fetches bids posted since the last run
- **Streamlit dashboard**: filterable table with deadline highlighting, calendar heatmap, stats
- **CLI**: `bid-crawler init / run / list / export`

## Quick Start

```bash
# 1. Install
pip install -e .
playwright install chromium   # for BidNet scraper

# 2. Set API key (for SAM.gov — free registration at sam.gov)
set SAM_GOV_API_KEY=your_key_here   # Windows
export SAM_GOV_API_KEY=your_key_here  # Linux/Mac

# 3. Initialize database
bid-crawler init

# 4. Run crawlers
bid-crawler run --all

# 5. Launch dashboard
streamlit run bid_crawler/app.py
```

## CLI Reference

```
bid-crawler init                     # Create DuckDB + schema
bid-crawler run --all [--skip-fresh] # Run all enabled sources
bid-crawler run sam_gov              # Run single source
bid-crawler run sam_gov --dry-run    # Preview matches, no DB writes
bid-crawler list                     # Show source status & open bid counts
bid-crawler export --format csv      # Export CSV to data/exports/
bid-crawler export --format jsonl    # Export JSONL
bid-crawler export --format parquet  # Export Parquet
```

## Source Configuration

| Source | File | Notes |
|--------|------|-------|
| SAM.gov | `config/sources/sam_gov.yaml` | Set `SAM_GOV_API_KEY` env var |
| Texas ESBD | `config/sources/texas_esbd.yaml` | No auth required |
| BidNet Direct | `config/sources/bidnet.yaml` | Playwright-based |
| OpenGov | `config/sources/opengov.yaml` | Set `OPENGOV_EMAIL` + `OPENGOV_API_KEY` |

Enable/disable sources via the `enabled: true/false` field in each YAML file.

## Match Scoring

Each bid receives a score 0–100:
- **+10** per matched keyword (construction, renovation, HVAC, roofing, etc.)
- **+20** for NAICS code match (236xx, 237xx, 238xx)
- **+15** for DFW county match

Bids must score ≥1 keyword OR have a NAICS match to be stored. The `$50,000` value floor filters
out micro-purchases.

Edit `config/criteria.yaml` to customize keywords, counties, NAICS prefixes, and the value floor.

## Daily Scheduler (Windows Task Scheduler)

1. Open **Task Scheduler** → Create Basic Task
2. Trigger: Daily at 6:00 AM
3. Action: Start a program
   - Program: `bid-crawler`
   - Arguments: `run --all --skip-fresh`
   - Start in: `C:\Users\natha\.claude\bid-crawler`

The `--skip-fresh` flag skips sources that ran within the last 20 hours (configurable in `settings.yaml`).

## Project Structure

```
bid-crawler/
├── config/
│   ├── settings.yaml          # DB path, rate limits, log level
│   ├── criteria.yaml          # Keywords, NAICS codes, counties, value floor
│   └── sources/               # Per-source YAML configs
├── bid_crawler/
│   ├── cli.py                 # Click CLI
│   ├── config.py              # Config dataclasses
│   ├── db.py                  # DuckDB wrapper
│   ├── matcher.py             # Scoring engine
│   ├── app.py                 # Streamlit dashboard
│   ├── sources/               # Source plugins
│   ├── etl/                   # Pipeline, transformer, loader
│   └── export/                # CSV / Sheets exporters
├── sql/schema.sql             # DuckDB schema
└── data/                      # Created at runtime
    ├── bids.duckdb
    └── exports/
```

## Dashboard

```bash
streamlit run bid_crawler/app.py
```

- **Tab 1 — Bid Table**: Sortable, filterable. Rows highlighted red (≤7 days), yellow (≤14 days).
- **Tab 2 — Calendar**: Scatter plot and heatmap of deadlines by county.
- **Tab 3 — Stats**: Open bid counts by county, agency type, and source.

Sidebar filters: County, Agency Type, NAICS prefix, Status, Due Date range, Min Score, Keyword.
