-- Supabase schema for BidCrawler
-- Run this in Supabase SQL Editor: https://app.supabase.com → SQL Editor

-- Sources table (one row per crawler source)
CREATE TABLE IF NOT EXISTS sources (
  id          TEXT PRIMARY KEY,
  source_type TEXT,
  last_run_at TIMESTAMPTZ,
  last_row_count INTEGER DEFAULT 0
);

-- Main bids table
CREATE TABLE IF NOT EXISTS bids (
  id                TEXT PRIMARY KEY,
  source_id         TEXT REFERENCES sources(id),
  external_id       TEXT,
  bid_number        TEXT,
  title             TEXT,
  description       TEXT,
  agency            TEXT,
  agency_type       TEXT,
  posted_date       DATE,
  due_date          DATE,
  estimated_value   FLOAT,
  location_city     TEXT,
  location_county   TEXT,
  location_state    TEXT DEFAULT 'TX',
  location_zip      TEXT,
  naics_code        TEXT,
  naics_description TEXT,
  set_aside         TEXT,
  contact_name      TEXT,
  contact_email     TEXT,
  contact_phone     TEXT,
  bid_url           TEXT,
  documents_url     TEXT,
  status            TEXT DEFAULT 'open',
  matched_keywords  TEXT,
  match_score       INTEGER DEFAULT 0,
  fetched_at        TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(source_id, external_id)
);

-- Indexes for feed queries
CREATE INDEX IF NOT EXISTS idx_bids_status ON bids(status);
CREATE INDEX IF NOT EXISTS idx_bids_score  ON bids(match_score DESC);
CREATE INDEX IF NOT EXISTS idx_bids_due    ON bids(due_date ASC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_bids_county ON bids(location_county);
CREATE INDEX IF NOT EXISTS idx_bids_source ON bids(source_id);

-- Enable Row Level Security (read-only public access — no auth required)
ALTER TABLE bids    ENABLE ROW LEVEL SECURITY;
ALTER TABLE sources ENABLE ROW LEVEL SECURITY;

-- Allow anonymous reads (the Next.js frontend uses the anon key)
CREATE POLICY "Public read bids"    ON bids    FOR SELECT USING (true);
CREATE POLICY "Public read sources" ON sources FOR SELECT USING (true);

-- The Python crawler writes via the service_role key (never exposed in frontend)
