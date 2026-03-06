import duckdb

con = duckdb.connect("data/bids.duckdb")
con.execute("""
    UPDATE bids
    SET bid_url = 'https://www.txsmartbuy.gov/esbd/' || bid_number
    WHERE source_id = 'texas_esbd'
      AND (bid_url IS NULL OR bid_url = '')
      AND bid_number != ''
""")
count = con.execute("SELECT COUNT(*) FROM bids WHERE source_id = 'texas_esbd' AND (bid_url IS NULL OR bid_url = '')").fetchone()[0]
print(f"Done. Remaining bids with no url: {count}")
con.close()
