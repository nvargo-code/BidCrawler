CREATE TABLE IF NOT EXISTS sources (
    id TEXT PRIMARY KEY,
    source_type TEXT,
    last_run_at TIMESTAMP,
    last_row_count INTEGER
);

CREATE TABLE IF NOT EXISTS bids (
    id TEXT PRIMARY KEY,
    source_id TEXT REFERENCES sources(id),
    external_id TEXT,
    -- Bid identity
    bid_number TEXT,
    title TEXT,
    description TEXT,
    -- Agency
    agency TEXT,
    agency_type TEXT,       -- federal | state | county | city | school | authority
    -- Dates
    posted_date DATE,
    due_date DATE,
    -- Value
    estimated_value FLOAT,
    -- Location
    location_city TEXT,
    location_county TEXT,
    location_state TEXT DEFAULT 'TX',
    location_zip TEXT,
    -- Classification
    naics_code TEXT,
    naics_description TEXT,
    set_aside TEXT,
    -- Contact
    contact_name TEXT,
    contact_email TEXT,
    contact_phone TEXT,
    -- URLs
    bid_url TEXT,
    documents_url TEXT,
    -- Status
    status TEXT,            -- open | closed | awarded | cancelled
    -- Match scoring
    matched_keywords TEXT,  -- comma-separated matched keywords
    match_score INTEGER,    -- 0-100; higher = more relevant
    -- Audit
    raw_payload TEXT,       -- full JSON
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_id, external_id)
);
