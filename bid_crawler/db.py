"""DuckDB wrapper for bid storage."""

from __future__ import annotations
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
import duckdb

logger = logging.getLogger(__name__)


class BidDB:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: Optional[duckdb.DuckDBPyConnection] = None

    def connect(self) -> "BidDB":
        self._conn = duckdb.connect(str(self.db_path))
        return self

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        return self.connect()

    def __exit__(self, *_):
        self.close()

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        if self._conn is None:
            raise RuntimeError("BidDB not connected — use as context manager or call connect()")
        return self._conn

    def apply_schema(self, schema_path: str | Path):
        sql = Path(schema_path).read_text()
        # Strip inline comments before splitting so we don't get empty statements
        import re
        sql_clean = re.sub(r"--[^\n]*", "", sql)
        for stmt in sql_clean.split(";"):
            stmt = stmt.strip()
            if stmt:
                self.conn.execute(stmt)
        logger.info("Schema applied from %s", schema_path)

    # ------------------------------------------------------------------
    # Source management
    # ------------------------------------------------------------------

    def ensure_source(self, source_id: str, source_type: str):
        self.conn.execute(
            """
            INSERT INTO sources (id, source_type)
            VALUES (?, ?)
            ON CONFLICT (id) DO NOTHING
            """,
            [source_id, source_type],
        )

    def get_source_last_run(self, source_id: str) -> Optional[datetime]:
        row = self.conn.execute(
            "SELECT last_run_at FROM sources WHERE id = ?", [source_id]
        ).fetchone()
        if row and row[0]:
            val = row[0]
            if isinstance(val, str):
                return datetime.fromisoformat(val)
            return val
        return None

    def update_source_run(self, source_id: str, row_count: int):
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """
            UPDATE sources
            SET last_run_at = ?, last_row_count = ?
            WHERE id = ?
            """,
            [now, row_count, source_id],
        )

    # ------------------------------------------------------------------
    # Bid upsert
    # ------------------------------------------------------------------

    def upsert_bid(self, bid: dict[str, Any]) -> bool:
        """Insert or update a bid. Returns True if it was a new insert."""
        # Serialize raw_payload
        if "raw_payload" in bid and not isinstance(bid["raw_payload"], str):
            bid = dict(bid)
            bid["raw_payload"] = json.dumps(bid["raw_payload"])

        cols = list(bid.keys())
        placeholders = ", ".join(["?"] * len(cols))
        col_list = ", ".join(cols)

        # Build UPDATE clause for all cols except PKs
        skip = {"id", "source_id", "external_id"}
        update_pairs = ", ".join(
            f"{c} = excluded.{c}" for c in cols if c not in skip
        )
        update_pairs += ", fetched_at = now()"

        sql = f"""
            INSERT INTO bids ({col_list})
            VALUES ({placeholders})
            ON CONFLICT (source_id, external_id) DO UPDATE SET
                {update_pairs}
        """
        values = [bid[c] for c in cols]

        before = self.conn.execute(
            "SELECT COUNT(*) FROM bids WHERE source_id=? AND external_id=?",
            [bid.get("source_id"), bid.get("external_id")],
        ).fetchone()[0]

        self.conn.execute(sql, values)

        after = self.conn.execute(
            "SELECT COUNT(*) FROM bids WHERE source_id=? AND external_id=?",
            [bid.get("source_id"), bid.get("external_id")],
        ).fetchone()[0]

        return before == 0 and after == 1

    def upsert_bids(self, bids: list[dict[str, Any]]) -> tuple[int, int]:
        """Upsert a list of bids. Returns (inserted, updated)."""
        inserted = 0
        updated = 0
        for bid in bids:
            is_new = self.upsert_bid(bid)
            if is_new:
                inserted += 1
            else:
                updated += 1
        return inserted, updated

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def get_open_bid_count(self, source_id: Optional[str] = None) -> int:
        if source_id:
            return self.conn.execute(
                "SELECT COUNT(*) FROM bids WHERE status = 'open' AND source_id = ?",
                [source_id],
            ).fetchone()[0]
        return self.conn.execute(
            "SELECT COUNT(*) FROM bids WHERE status = 'open'"
        ).fetchone()[0]

    def get_source_status(self) -> list[dict]:
        rows = self.conn.execute(
            """
            SELECT s.id, s.source_type, s.last_run_at, s.last_row_count,
                   COUNT(b.id) FILTER (WHERE b.status = 'open') AS open_bids
            FROM sources s
            LEFT JOIN bids b ON b.source_id = s.id
            GROUP BY s.id, s.source_type, s.last_run_at, s.last_row_count
            ORDER BY s.id
            """
        ).fetchall()
        cols = ["id", "source_type", "last_run_at", "last_row_count", "open_bids"]
        return [dict(zip(cols, row)) for row in rows]

    def export_bids_df(self, filters: Optional[dict] = None):
        """Return a pandas DataFrame of bids, optionally filtered."""
        import pandas as pd

        sql = "SELECT * FROM bids WHERE 1=1"
        params = []
        if filters:
            if filters.get("status"):
                sql += " AND status = ?"
                params.append(filters["status"])
            if filters.get("county"):
                sql += " AND location_county = ?"
                params.append(filters["county"])
            if filters.get("min_score") is not None:
                sql += " AND match_score >= ?"
                params.append(filters["min_score"])

        rel = self.conn.execute(sql, params)
        return rel.df()
