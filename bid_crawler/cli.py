"""Click CLI for bid-crawler."""

from __future__ import annotations
import logging
import sys
from pathlib import Path

import click

# Project root = parent of this file's parent
PROJECT_ROOT = Path(__file__).parent.parent


def _setup_logging(level: str):
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _load_configs():
    from bid_crawler.config import load_configs
    return load_configs(PROJECT_ROOT)


@click.group()
@click.version_option("0.1.0")
def cli():
    """bid-crawler: DFW commercial construction bid aggregator."""


# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------

@cli.command()
def init():
    """Create DuckDB database and apply schema."""
    crawler_cfg, _, _ = _load_configs()
    _setup_logging(crawler_cfg.log_level)
    logger = logging.getLogger("cli.init")

    from bid_crawler.db import BidDB

    db_path = PROJECT_ROOT / crawler_cfg.db_path
    schema_path = PROJECT_ROOT / "sql" / "schema.sql"

    logger.info("Initializing database at %s", db_path)
    with BidDB(db_path) as db:
        db.apply_schema(schema_path)

    click.echo(f"Database initialized: {db_path}")


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------

@cli.command()
@click.argument("source_ids", nargs=-1)
@click.option("--all", "run_all", is_flag=True, help="Run all enabled sources")
@click.option("--skip-fresh", is_flag=True, help="Skip sources that ran recently")
@click.option("--dry-run", is_flag=True, help="Fetch and score without writing to DB")
def run(source_ids, run_all, skip_fresh, dry_run):
    """Run one or more source crawlers.

    \b
    Examples:
        bid-crawler run sam_gov
        bid-crawler run --all --skip-fresh
        bid-crawler run sam_gov --dry-run
    """
    crawler_cfg, criteria_cfg, all_source_cfgs = _load_configs()
    _setup_logging(crawler_cfg.log_level)
    logger = logging.getLogger("cli.run")

    from bid_crawler.db import BidDB
    from bid_crawler.etl.pipeline import run_source

    # Determine which sources to run
    if run_all:
        sources_to_run = [s for s in all_source_cfgs if s.enabled]
    elif source_ids:
        id_set = set(source_ids)
        sources_to_run = [s for s in all_source_cfgs if s.id in id_set]
        missing = id_set - {s.id for s in sources_to_run}
        if missing:
            click.echo(f"Unknown source IDs: {', '.join(sorted(missing))}", err=True)
            sys.exit(1)
    else:
        click.echo("Specify source IDs or use --all", err=True)
        sys.exit(1)

    if not sources_to_run:
        click.echo("No sources to run.")
        return

    db_path = PROJECT_ROOT / crawler_cfg.db_path

    if dry_run:
        click.echo("[DRY RUN] Fetching and scoring - no writes to DB")

    results = []
    with BidDB(db_path) as db:
        for source_cfg in sources_to_run:
            click.echo(f">> Running source: {source_cfg.id}")
            result = run_source(
                source_cfg=source_cfg,
                crawler_cfg=crawler_cfg,
                criteria_cfg=criteria_cfg,
                db=db,
                dry_run=dry_run,
                skip_fresh=skip_fresh,
            )
            results.append(result)

            if result.skipped_fresh:
                click.echo(f"  Skipped (ran recently)")
            elif dry_run:
                click.echo(f"  Fetched={result.fetched}, Matched={result.matched} (dry run)")
            else:
                click.echo(
                    f"  Fetched={result.fetched}, Matched={result.matched}, "
                    f"Inserted={result.inserted}, Updated={result.updated}"
                )
            if result.errors:
                for err in result.errors:
                    click.echo(f"  [ERROR] {err}", err=True)

    # Summary
    total_new = sum(r.inserted for r in results)
    total_match = sum(r.matched for r in results)
    click.echo(f"\nDone. Total matched={total_match}, new bids inserted={total_new}")


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------

@cli.command("list")
def list_cmd():
    """Show source status, last run time, and open bid counts."""
    crawler_cfg, _, _ = _load_configs()
    _setup_logging(crawler_cfg.log_level)

    from bid_crawler.db import BidDB

    db_path = PROJECT_ROOT / crawler_cfg.db_path
    if not db_path.exists():
        click.echo("Database not found. Run 'bid-crawler init' first.", err=True)
        sys.exit(1)

    with BidDB(db_path) as db:
        rows = db.get_source_status()
        total_open = db.get_open_bid_count()

    if not rows:
        click.echo("No sources in database. Run 'bid-crawler run --all' first.")
        return

    click.echo(f"\n{'Source':<20} {'Type':<18} {'Last Run':<22} {'Rows':<8} {'Open'}")
    click.echo("-" * 80)
    for row in rows:
        last_run = str(row.get("last_run_at") or "never")[:19]
        click.echo(
            f"{row['id']:<20} {row['source_type']:<18} {last_run:<22} "
            f"{row.get('last_row_count') or 0:<8} {row.get('open_bids') or 0}"
        )

    click.echo(f"\nTotal open bids: {total_open}")


# ---------------------------------------------------------------------------
# export
# ---------------------------------------------------------------------------

@cli.command()
@click.option("--format", "fmt", type=click.Choice(["csv", "jsonl", "parquet"]), default="csv")
@click.option("--status", default="open", help="Filter by status (open/closed/all)")
@click.option("--min-score", type=int, default=0, help="Minimum match score")
def export(fmt, status, min_score):
    """Export bids to a file in data/exports/."""
    crawler_cfg, _, _ = _load_configs()
    _setup_logging(crawler_cfg.log_level)

    from bid_crawler.db import BidDB
    from datetime import datetime
    import os

    db_path = PROJECT_ROOT / crawler_cfg.db_path
    export_dir = PROJECT_ROOT / crawler_cfg.export_dir
    export_dir.mkdir(parents=True, exist_ok=True)

    filters: dict = {}
    if status != "all":
        filters["status"] = status
    if min_score > 0:
        filters["min_score"] = min_score

    with BidDB(db_path) as db:
        df = db.export_bids_df(filters)

    if df.empty:
        click.echo("No bids match the filter criteria.")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"bids_{timestamp}.{fmt}"
    output_path = export_dir / filename

    if fmt == "csv":
        df.to_csv(output_path, index=False)
    elif fmt == "jsonl":
        df.to_json(output_path, orient="records", lines=True)
    elif fmt == "parquet":
        df.to_parquet(output_path, index=False)

    click.echo(f"Exported {len(df)} bids to {output_path}")


if __name__ == "__main__":
    cli()
