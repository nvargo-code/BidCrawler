"""Google Sheets export using gspread (optional integration)."""

from __future__ import annotations
import logging
from datetime import date
from typing import Optional

logger = logging.getLogger(__name__)

try:
    import gspread
    from gspread.utils import rowcol_to_a1
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False
    logger.debug("gspread not installed — Google Sheets export unavailable")


EXPORT_COLUMNS = [
    "title", "agency", "agency_type", "location_county", "location_city",
    "due_date", "posted_date", "estimated_value", "match_score",
    "matched_keywords", "naics_code", "status", "bid_url", "contact_email",
]

# Urgency color thresholds (Google Sheets RGB hex)
COLOR_RED = {"red": 1.0, "green": 0.8, "blue": 0.8}
COLOR_YELLOW = {"red": 1.0, "green": 0.98, "blue": 0.8}
COLOR_WHITE = {"red": 1.0, "green": 1.0, "blue": 1.0}


def export_to_sheets(
    df,
    spreadsheet_id: str,
    worksheet_name: str = "Bids",
    credentials_path: str = "credentials.json",
) -> bool:
    """
    Write a bids DataFrame to a Google Sheet.

    Args:
        df: pandas DataFrame of bids
        spreadsheet_id: Google Sheets document ID from the URL
        worksheet_name: Tab name to write to
        credentials_path: Path to service account JSON credentials

    Returns:
        True on success, False on failure.
    """
    if not GSPREAD_AVAILABLE:
        logger.error("gspread not installed. pip install gspread")
        return False

    try:
        gc = gspread.service_account(filename=credentials_path)
    except Exception as exc:
        logger.error("Could not authenticate with Google Sheets: %s", exc)
        return False

    try:
        sh = gc.open_by_key(spreadsheet_id)
    except Exception as exc:
        logger.error("Could not open spreadsheet %s: %s", spreadsheet_id, exc)
        return False

    # Get or create worksheet
    try:
        ws = sh.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=worksheet_name, rows=2000, cols=len(EXPORT_COLUMNS) + 2)

    # Build export data
    cols = [c for c in EXPORT_COLUMNS if c in df.columns]
    export_df = df[cols].copy()

    # Format values
    if "estimated_value" in export_df.columns:
        export_df["estimated_value"] = export_df["estimated_value"].apply(
            lambda v: f"${v:,.0f}" if v == v and v is not None else ""
        )

    rows = [cols] + export_df.fillna("").astype(str).values.tolist()

    # Clear and write
    ws.clear()
    ws.update(rows, value_input_option="USER_ENTERED")

    # Freeze header row
    ws.freeze(rows=1)

    # Bold header
    ws.format("1:1", {"textFormat": {"bold": True}})

    # Apply deadline coloring
    _apply_urgency_colors(ws, export_df, cols)

    logger.info("Exported %d bids to Google Sheets worksheet %r", len(export_df), worksheet_name)
    return True


def _apply_urgency_colors(ws, df, cols):
    """Color rows by due date urgency."""
    if "due_date" not in cols:
        return

    today = date.today()
    due_col_idx = cols.index("due_date")

    requests = []
    for row_idx, (_, row) in enumerate(df.iterrows(), start=2):  # row 1 is header
        due_val = row.get("due_date")
        color = COLOR_WHITE
        if due_val:
            try:
                if isinstance(due_val, str):
                    due_date = date.fromisoformat(due_val)
                else:
                    due_date = due_val
                days_left = (due_date - today).days
                if days_left <= 7:
                    color = COLOR_RED
                elif days_left <= 14:
                    color = COLOR_YELLOW
            except (ValueError, TypeError):
                pass

        if color != COLOR_WHITE:
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": ws._properties["sheetId"],
                        "startRowIndex": row_idx - 1,
                        "endRowIndex": row_idx,
                        "startColumnIndex": 0,
                        "endColumnIndex": len(cols),
                    },
                    "cell": {
                        "userEnteredFormat": {"backgroundColor": color}
                    },
                    "fields": "userEnteredFormat.backgroundColor",
                }
            })

    if requests:
        ws.spreadsheet.batch_update({"requests": requests})
