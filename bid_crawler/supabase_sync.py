"""Sync bids from local DuckDB to Supabase.

Uses the service_role key (write access) — never expose this in the frontend.
Env vars required:
    SUPABASE_URL          project URL (e.g. https://xyz.supabase.co)
    SUPABASE_SERVICE_KEY  service_role JWT
"""

from __future__ import annotations
import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

BATCH_SIZE = 500  # rows per upsert request

# Columns to sync — excludes raw_payload (too large, not needed by frontend)
SYNC_COLS = [
    "id", "source_id", "external_id", "bid_number", "title", "description",
    "agency", "agency_type", "posted_date", "due_date", "estimated_value",
    "location_city", "location_county", "location_state", "location_zip",
    "naics_code", "naics_description", "set_aside",
    "bid_url", "status", "matched_keywords", "match_score", "fetched_at",
]


def _get_client():
    from supabase import create_client
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        raise RuntimeError(
            "SUPABASE_URL and SUPABASE_SERVICE_KEY env vars must be set.\n"
            "Run:  setx SUPABASE_URL \"https://your-project.supabase.co\"\n"
            "      setx SUPABASE_SERVICE_KEY \"your-service-role-key\""
        )
    return create_client(url, key)


def sync_to_supabase(db_path: Path) -> dict[str, int]:
    """
    Read all bids + sources from DuckDB and upsert into Supabase.
    Returns {"sources": N, "bids": N} counts.
    """
    from bid_crawler.db import BidDB

    client = _get_client()
    db = BidDB(db_path)
    db.connect(read_only=True)

    try:
        results = {"sources": 0, "bids": 0}

        # --- Sync sources first (FK dependency) ---
        sources_df = db.conn.execute("SELECT id, source_type, last_run_at, last_row_count FROM sources").df()
        if not sources_df.empty:
            source_rows = _df_to_dicts(sources_df)
            _upsert_batched(client, "sources", source_rows, on_conflict="id")
            results["sources"] = len(source_rows)
            logger.info("Synced %d sources", len(source_rows))

        # --- Sync bids ---
        available = db.conn.execute("SELECT column_name FROM information_schema.columns WHERE table_name='bids'").fetchall()
        available_cols = {r[0] for r in available}
        cols = [c for c in SYNC_COLS if c in available_cols]
        cols_sql = ", ".join(cols)

        bids_df = db.conn.execute(f"SELECT {cols_sql} FROM bids WHERE status = 'open'").df()
        if bids_df.empty:
            logger.info("No open bids in local DB to sync")
            return results

        bid_rows = _df_to_dicts(bids_df)
        _upsert_batched(client, "bids", bid_rows, on_conflict="id")
        results["bids"] = len(bid_rows)
        logger.info("Synced %d open bids to Supabase", len(bid_rows))

        return results

    finally:
        db.close()


def _upsert_batched(client, table: str, rows: list[dict], on_conflict: str) -> None:
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        client.table(table).upsert(batch, on_conflict=on_conflict).execute()
        logger.debug("  %s: upserted rows %d–%d", table, i, i + len(batch))


def _df_to_dicts(df) -> list[dict[str, Any]]:
    """Convert DataFrame to list of JSON-safe dicts (handles NaT, NaN, dates)."""
    import math
    import pandas as pd

    records = df.to_dict(orient="records")
    cleaned = []
    for row in records:
        clean = {}
        for k, v in row.items():
            # pandas NaT and numpy NaN → None
            if v is None or (isinstance(v, float) and math.isnan(v)) or v is pd.NaT:
                clean[k] = None
            elif pd.isnull(v) if not isinstance(v, (list, dict)) else False:
                clean[k] = None
            elif hasattr(v, "isoformat"):
                clean[k] = v.isoformat()
            else:
                clean[k] = v
        cleaned.append(clean)
    return cleaned
