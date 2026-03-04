"""Streamlit dashboard for bid-crawler.

Run: streamlit run bid_crawler/app.py
"""

from __future__ import annotations
import os
import sys
from pathlib import Path
from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Resolve project root (works whether run from project dir or bid_crawler/)
PROJECT_ROOT = Path(__file__).parent.parent

_SOURCE_LABELS = {
    "texas_esbd":             "Texas ESBD",
    "sam_gov":                "SAM.gov (Federal)",
    "fort_worth_bonfire":     "Fort Worth Bonfire",
    "dallas_bonfire":         "Dallas Bonfire",
    "dallas_isd_bonfire":     "Dallas ISD",
    "richardson_isd_bonfire": "Richardson ISD",
    "rockwall_isd_bonfire":   "Rockwall ISD",
    "bidnet":                 "BidNet Direct",
    "opengov":                "OpenGov",
}

# Add to path so imports work
sys.path.insert(0, str(PROJECT_ROOT))

# ---------------------------------------------------------------------------
# Config & DB
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300, show_spinner="Loading bids…")
def load_bids_df() -> pd.DataFrame:
    from bid_crawler.config import CrawlerConfig
    from bid_crawler.db import BidDB

    cfg_path = PROJECT_ROOT / "config" / "settings.yaml"
    cfg = CrawlerConfig.from_yaml(cfg_path)
    db_path = PROJECT_ROOT / cfg.db_path

    # Open read-only, load data, close immediately so the crawler can write
    db = BidDB(db_path)
    db.connect(read_only=True)
    try:
        df = db.export_bids_df()
    finally:
        db.close()

    if df.empty:
        return df

    # Type coercions
    for col in ("posted_date", "due_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    if "estimated_value" in df.columns:
        df["estimated_value"] = pd.to_numeric(df["estimated_value"], errors="coerce")

    if "match_score" in df.columns:
        df["match_score"] = pd.to_numeric(df["match_score"], errors="coerce").fillna(0).astype(int)

    return df


# ---------------------------------------------------------------------------
# Page setup
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Bid Crawler — DFW Construction",
    page_icon="🏗️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("🏗️ DFW Construction Bid Aggregator")

df_all = load_bids_df()

if df_all.empty:
    st.warning(
        "No bids in database. Run `bid-crawler init` then `bid-crawler run --all` first."
    )
    st.stop()

today = date.today()

# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------

st.sidebar.header("Filters")

# County
all_counties = sorted(df_all["location_county"].dropna().unique().tolist())
all_counties = [c for c in all_counties if c]
selected_counties = st.sidebar.multiselect("County", all_counties, default=[])

# Agency type
all_agency_types = sorted(df_all["agency_type"].dropna().unique().tolist())
all_agency_types = [a for a in all_agency_types if a]
selected_agency_types = st.sidebar.multiselect("Agency Type", all_agency_types, default=[])

# Source
source_ids = sorted(df_all["source_id"].dropna().unique().tolist()) if "source_id" in df_all.columns else []
source_labels = [_SOURCE_LABELS.get(s, s) for s in source_ids]
selected_labels = st.sidebar.multiselect("Source", source_labels)
label_to_id = {v: k for k, v in _SOURCE_LABELS.items()}
selected_sources = [label_to_id.get(l, l) for l in selected_labels]

# NAICS prefix
all_naics = sorted(df_all["naics_code"].dropna().unique().tolist())
naics_prefixes = sorted({n[:3] for n in all_naics if n})
selected_naics = st.sidebar.multiselect("NAICS Prefix", naics_prefixes, default=[])

# Status
status_options = ["open", "closed", "awarded", "cancelled"]
selected_status = st.sidebar.multiselect("Status", status_options, default=["open"])

# Due date range
min_due = st.sidebar.date_input("Due Date — From", value=today)
max_due = st.sidebar.date_input("Due Date — To", value=today + timedelta(days=90))

# Min match score
min_score = st.sidebar.slider("Min Match Score", 0, 100, 10)

# Keyword search
keyword_search = st.sidebar.text_input("Keyword Search (title/description)")

# ---------------------------------------------------------------------------
# Apply filters
# ---------------------------------------------------------------------------

df = df_all.copy()

if selected_counties:
    df = df[df["location_county"].isin(selected_counties)]

if selected_agency_types:
    df = df[df["agency_type"].isin(selected_agency_types)]

if selected_sources:
    df = df[df["source_id"].isin(selected_sources)]

if selected_naics:
    df = df[df["naics_code"].str[:3].isin(selected_naics)]

if selected_status:
    df = df[df["status"].isin(selected_status)]

if "due_date" in df.columns:
    mask = df["due_date"].notna()
    df = df[~mask | (
        (df["due_date"] >= min_due) & (df["due_date"] <= max_due)
    )]

if "match_score" in df.columns:
    df = df[df["match_score"] >= min_score]

if keyword_search:
    kw = keyword_search.lower()
    text_mask = (
        df["title"].str.lower().str.contains(kw, na=False)
        | df["description"].str.lower().str.contains(kw, na=False)
        | df["matched_keywords"].str.lower().str.contains(kw, na=False)
    )
    df = df[text_mask]

# Sort: due_date ASC, then match_score DESC
df = df.sort_values(
    ["due_date", "match_score"],
    ascending=[True, False],
    na_position="last",
)

st.sidebar.markdown(f"---\n**{len(df):,} bids** match filters")

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab1, tab2, tab3 = st.tabs(["📋 Bid Table", "📅 Calendar", "📊 Stats"])

# ===========================================================================
# Tab 1 — Bid Table
# ===========================================================================

with tab1:
    st.subheader(f"Open Bids ({len(df):,})")

    display_cols = [
        "title", "agency", "agency_type", "location_county", "location_city",
        "due_date", "estimated_value", "match_score", "matched_keywords",
        "naics_code", "status", "bid_url",
    ]
    display_cols = [c for c in display_cols if c in df.columns]
    df_display = df[display_cols].copy()

    # Deadline highlighting via Styler
    def highlight_urgency(row):
        due = row.get("due_date")
        if pd.isna(due) or due is None:
            return [""] * len(row)
        if isinstance(due, str):
            try:
                due = date.fromisoformat(due)
            except ValueError:
                return [""] * len(row)
        days_left = (due - today).days
        if days_left <= 7:
            return ["background-color: #ffe0e0"] * len(row)  # red-ish
        elif days_left <= 14:
            return ["background-color: #fff4cc"] * len(row)  # yellow-ish
        return [""] * len(row)

    styled = df_display.style.apply(highlight_urgency, axis=1)

    # Format value column
    if "estimated_value" in df_display.columns:
        styled = styled.format({"estimated_value": lambda v: f"${v:,.0f}" if pd.notna(v) else ""})

    st.dataframe(
        styled,
        use_container_width=True,
        height=600,
        column_config={
            "bid_url": st.column_config.LinkColumn("Bid URL", display_text="Open"),
            "match_score": st.column_config.ProgressColumn(
                "Score", min_value=0, max_value=100, format="%d"
            ),
            "estimated_value": st.column_config.NumberColumn("Est. Value", format="$%.0f"),
        },
    )

    st.caption("🔴 Due ≤7 days | 🟡 Due ≤14 days")

    # Download button
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "⬇️ Download CSV",
        data=csv,
        file_name="bids_export.csv",
        mime="text/csv",
    )

# ===========================================================================
# Tab 2 — Calendar (Plotly timeline / heatmap)
# ===========================================================================

with tab2:
    st.subheader("Bid Deadline Calendar")

    df_cal = df[df["due_date"].notna()].copy()

    if df_cal.empty:
        st.info("No bids with due dates to display.")
    else:
        df_cal["due_date_str"] = df_cal["due_date"].astype(str)
        df_cal["days_left"] = (
            pd.to_datetime(df_cal["due_date_str"]).dt.date.apply(
                lambda d: (d - today).days
            )
        )
        df_cal["county_label"] = df_cal["location_county"].fillna("Unknown")

        # Plotly timeline (Gantt-style grouped by county)
        df_cal["start"] = df_cal["due_date_str"]
        df_cal["end"] = df_cal["due_date_str"]
        df_cal["hover"] = (
            df_cal["title"].str[:60]
            + "<br>" + df_cal["agency"].fillna("")
            + "<br>Score: " + df_cal["match_score"].astype(str)
        )

        fig = px.scatter(
            df_cal,
            x="due_date_str",
            y="county_label",
            color="days_left",
            color_continuous_scale=["red", "orange", "green"],
            range_color=[0, 90],
            hover_name="title",
            hover_data={
                "agency": True,
                "match_score": True,
                "due_date_str": False,
                "county_label": False,
                "days_left": True,
            },
            labels={
                "due_date_str": "Due Date",
                "county_label": "County",
                "days_left": "Days Left",
            },
            title="Bid Deadlines by County",
            height=500,
        )
        fig.update_traces(marker_size=12)
        fig.update_layout(
            xaxis_title="Due Date",
            yaxis_title="County",
            coloraxis_colorbar_title="Days Left",
        )
        st.plotly_chart(fig, use_container_width=True)

        # Heatmap: bids by week × county
        df_cal["week"] = pd.to_datetime(df_cal["due_date_str"]).dt.to_period("W").astype(str)
        pivot = df_cal.pivot_table(
            index="county_label", columns="week", values="id", aggfunc="count", fill_value=0
        )
        if not pivot.empty:
            fig2 = px.imshow(
                pivot,
                color_continuous_scale="Blues",
                title="Bids per Week × County",
                height=400,
                labels={"color": "Bid Count"},
            )
            st.plotly_chart(fig2, use_container_width=True)

# ===========================================================================
# Tab 3 — Stats
# ===========================================================================

with tab3:
    st.subheader("Summary Statistics")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Bids", f"{len(df):,}")
    with col2:
        open_bids = len(df[df["status"] == "open"]) if "status" in df else 0
        st.metric("Open Bids", f"{open_bids:,}")
    with col3:
        due_this_week = len(df[
            df["due_date"].notna()
            & (df["due_date"].apply(lambda d: (d - today).days if d else 999) <= 7)
        ]) if "due_date" in df else 0
        st.metric("Due This Week", f"{due_this_week:,}", delta_color="inverse")
    with col4:
        avg_score = df["match_score"].mean() if "match_score" in df else 0
        st.metric("Avg Match Score", f"{avg_score:.0f}")

    st.markdown("---")

    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        if "location_county" in df.columns:
            county_counts = (
                df[df["status"] == "open"]["location_county"]
                .value_counts()
                .head(15)
                .reset_index()
            )
            county_counts.columns = ["County", "Count"]
            fig = px.bar(
                county_counts,
                x="Count",
                y="County",
                orientation="h",
                title="Open Bids by County",
                height=400,
            )
            st.plotly_chart(fig, use_container_width=True)

    with chart_col2:
        if "agency_type" in df.columns:
            at_counts = (
                df[df["status"] == "open"]["agency_type"]
                .value_counts()
                .reset_index()
            )
            at_counts.columns = ["Agency Type", "Count"]
            fig = px.pie(
                at_counts,
                names="Agency Type",
                values="Count",
                title="Open Bids by Agency Type",
                height=400,
            )
            st.plotly_chart(fig, use_container_width=True)

    if "source_id" in df.columns:
        src_counts = (
            df["source_id"].value_counts().reset_index()
        )
        src_counts.columns = ["Source", "Count"]
        fig = px.bar(
            src_counts,
            x="Source",
            y="Count",
            title="Bids by Source",
            height=300,
        )
        st.plotly_chart(fig, use_container_width=True)
