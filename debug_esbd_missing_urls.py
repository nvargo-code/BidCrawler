"""Show ESBD bids with no bid_url to diagnose what fields are available."""
import json
from pathlib import Path
import duckdb

db_path = Path(__file__).parent / "data" / "bids.duckdb"
con = duckdb.connect(str(db_path), read_only=True)

rows = con.execute("""
    SELECT id, bid_number, title, bid_url, raw_payload
    FROM bids
    WHERE source_id = 'texas_esbd'
      AND (bid_url IS NULL OR bid_url = '')
    ORDER BY id
""").fetchall()

print(f"{len(rows)} ESBD bids with no bid_url:\n")
for row in rows:
    id_, bid_number, title, bid_url, raw_payload = row
    payload = json.loads(raw_payload) if raw_payload else {}
    print(f"  id={id_}  bid_number={bid_number!r}  url={payload.get('url')!r}  solicitationId={payload.get('solicitationId')!r}")
    print(f"    title: {title[:70]}")

con.close()
