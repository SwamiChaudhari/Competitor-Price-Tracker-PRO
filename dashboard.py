"""
dashboard.py — Price Monitoring Pipeline Dashboard
=====================================================
Run with:  streamlit run dashboard.py
Requires:  streamlit, plotly, pandas, sqlite3 (stdlib)
"""

import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from datetime import datetime, timedelta
import os
from pathlib import Path

# ─────────────────────────────────────────────
#  PAGE CONFIG
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Price Monitor · Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────
#  CUSTOM CSS
# ─────────────────────────────────────────────
st.markdown(
    """
    <style>
        /* ── Google Font ── */
        @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600;700&display=swap');

        html, body, [class*="css"] {
            font-family: 'IBM Plex Sans', sans-serif;
        }

        /* ── Sidebar ── */
        [data-testid="stSidebar"] {
            background: #0d0d0d;
            border-right: 1px solid #1f1f1f;
        }
        [data-testid="stSidebar"] * { color: #e0e0e0 !important; }
        [data-testid="stSidebar"] .stSelectbox label,
        [data-testid="stSidebar"] .stMultiSelect label { color: #888 !important; font-size: 0.75rem; }

        /* ── Main area ── */
        .main .block-container { padding: 2rem 2.5rem 4rem; background: #0a0a0a; }

        /* ── Page title ── */
        .dash-title {
            font-family: 'IBM Plex Mono', monospace;
            font-size: 1.7rem;
            font-weight: 600;
            color: #f0f0f0;
            letter-spacing: -0.5px;
        }
        .dash-subtitle {
            font-size: 0.8rem;
            color: #555;
            font-family: 'IBM Plex Mono', monospace;
            margin-top: -0.3rem;
            margin-bottom: 1.5rem;
        }

        /* ── Metric cards ── */
        div[data-testid="metric-container"] {
            background: #111;
            border: 1px solid #222;
            border-radius: 10px;
            padding: 1rem 1.25rem;
        }
        div[data-testid="metric-container"] label {
            font-size: 0.7rem !important;
            font-family: 'IBM Plex Mono', monospace !important;
            color: #555 !important;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        div[data-testid="metric-container"] [data-testid="stMetricValue"] {
            font-size: 2rem !important;
            font-family: 'IBM Plex Mono', monospace !important;
            color: #f0f0f0 !important;
        }
        div[data-testid="metric-container"] [data-testid="stMetricDelta"] {
            font-size: 0.75rem !important;
        }

        /* ── HOT badge ── */
        .hot-badge {
            display: inline-block;
            background: #ff3c3c;
            color: #fff;
            font-size: 0.65rem;
            font-family: 'IBM Plex Mono', monospace;
            font-weight: 600;
            padding: 2px 7px;
            border-radius: 4px;
            letter-spacing: 1px;
        }

        /* ── Section headers ── */
        .section-header {
            font-family: 'IBM Plex Mono', monospace;
            font-size: 0.7rem;
            color: #444;
            letter-spacing: 2px;
            text-transform: uppercase;
            padding: 0.5rem 0 0.3rem;
            border-top: 1px solid #1a1a1a;
            margin-top: 0.5rem;
        }

        /* ── Status pill ── */
        .status-ok   { color: #3effa0; font-family: 'IBM Plex Mono', monospace; font-size: 0.8rem; }
        .status-warn { color: #ffcc00; font-family: 'IBM Plex Mono', monospace; font-size: 0.8rem; }
        .status-old  { color: #ff5555; font-family: 'IBM Plex Mono', monospace; font-size: 0.8rem; }

        /* ── Dataframe tweaks ── */
        [data-testid="stDataFrame"] { border-radius: 8px; overflow: hidden; }

        /* ── Divider ── */
        hr { border-color: #1a1a1a !important; }

        /* ── Plotly chart background ── */
        .js-plotly-plot { border-radius: 10px; overflow: hidden; }

        /* ── Search box ── */
        [data-testid="stTextInput"] input {
            background: #111 !important;
            border: 1px solid #222 !important;
            color: #f0f0f0 !important;
            border-radius: 6px;
            font-family: 'IBM Plex Mono', monospace;
            font-size: 0.85rem;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# ─────────────────────────────────────────────
#  CONSTANTS
# ─────────────────────────────────────────────
# Always resolve prices.db relative to this script's own folder
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH  = Path(os.path.join(BASE_DIR, "prices.db"))

PLOTLY_LAYOUT = dict(
    paper_bgcolor="#111111",
    plot_bgcolor="#111111",
    font=dict(family="IBM Plex Mono, monospace", color="#aaaaaa", size=11),
    margin=dict(l=20, r=20, t=40, b=20),
    xaxis=dict(gridcolor="#1e1e1e", linecolor="#2a2a2a", tickcolor="#2a2a2a"),
    yaxis=dict(gridcolor="#1e1e1e", linecolor="#2a2a2a", tickcolor="#2a2a2a"),
    legend=dict(bgcolor="#111111", bordercolor="#222222", borderwidth=1),
)

SOURCE_COLORS = {
    "Flipkart": "#f5a623",
    "Amazon":   "#00a8e8",
    "flipkart": "#f5a623",
    "amazon":   "#00a8e8",
}

# ─────────────────────────────────────────────
#  DATA LAYER
# ─────────────────────────────────────────────
@st.cache_data(ttl=120, show_spinner=False)
def load_data() -> pd.DataFrame:
    """Load all price records from SQLite; return empty frame on missing DB."""
    if not DB_PATH.exists():
        return pd.DataFrame(
            columns=[
                "id", "product_name", "mrp", "sale_price", "discount_pct",
                "source", "scrape_date", "url", "price_change_pct", "alert_status",
            ]
        )
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query("SELECT * FROM prices", conn)
    # Normalise date
    df["scrape_date"] = pd.to_datetime(df["scrape_date"], errors="coerce")
    df["alert_status"] = df["alert_status"].str.upper().fillna("")
    df["source"] = df["source"].str.strip()
    return df


def pipeline_status(df: pd.DataFrame) -> tuple[str, str]:
    """Return (label, css_class) based on freshness of latest scrape."""
    if df.empty or df["scrape_date"].isna().all():
        return "No data", "status-old"
    latest = df["scrape_date"].max()
    age_h  = (datetime.now() - latest).total_seconds() / 3600
    if age_h < 2:
        return f"✔  Live  ({latest.strftime('%b %d, %H:%M')})", "status-ok"
    if age_h < 24:
        return f"⚠  Stale ({int(age_h)}h ago)", "status-warn"
    return f"✘  Old   ({latest.strftime('%b %d')})", "status-old"


# ─────────────────────────────────────────────
#  SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ⚙️ Filters")
    st.markdown("---")

    df_raw = load_data()

    sources = sorted(df_raw["source"].dropna().unique().tolist())
    selected_sources = st.multiselect(
        "Source", sources, default=sources, key="src_filter"
    )

    min_disc, max_disc = 0, 100
    if not df_raw.empty:
        min_disc = int(df_raw["discount_pct"].min() or 0)
        max_disc = int(df_raw["discount_pct"].max() or 100)

    discount_range = st.slider(
        "Discount % range",
        min_value=min_disc, max_value=max_disc,
        value=(min_disc, max_disc),
        key="disc_filter",
    )

    show_hot_only = st.checkbox("HOT deals only", value=False)

    st.markdown("---")
    if st.button("🔄  Refresh data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown(
        "<p style='font-size:0.65rem;color:#333;margin-top:2rem;font-family:IBM Plex Mono,monospace;'>"
        "prices.db · auto-refresh 2 min</p>",
        unsafe_allow_html=True,
    )

# ─────────────────────────────────────────────
#  APPLY FILTERS
# ─────────────────────────────────────────────
df = df_raw.copy()
if selected_sources:
    df = df[df["source"].isin(selected_sources)]
df = df[
    (df["discount_pct"] >= discount_range[0]) &
    (df["discount_pct"] <= discount_range[1])
]
if show_hot_only:
    df = df[df["alert_status"] == "HOT"]

# ─────────────────────────────────────────────
#  HEADER
# ─────────────────────────────────────────────
st.markdown(
    '<p class="dash-title">📊 Price Monitor</p>'
    '<p class="dash-subtitle">Web-Scraping Pipeline · Real-time Price Intelligence</p>',
    unsafe_allow_html=True,
)

# ─────────────────────────────────────────────
#  PANDAS METRICS  (replaces DAX formulas)
# ─────────────────────────────────────────────

# 1. Price Drop % — calculated on the fly from MRP vs Sale Price
df["price_drop_pct"] = ((df["mrp"] - df["sale_price"]) / df["mrp"]) * 100

# 2. Total unique products
total_products = df["product_name"].nunique()

# 3. HOT deals count
hot_deals_count = len(df[df["alert_status"] == "HOT"])

# 4. Market average sale price
market_avg = df["sale_price"].mean()

# 5. Avg price drop % across all products
avg_price_drop = df["price_drop_pct"].mean()

# 6. Pipeline freshness status
status_label, status_class = pipeline_status(df_raw)

# ── Render KPI cards ──────────────────────────
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Products",   f"{total_products:,}")
col2.metric("🔥 HOT Deals",     f"{hot_deals_count:,}")
col3.metric("Market Avg Price", f"₹{market_avg:,.2f}" if not df.empty else "—")
col4.metric("Avg Price Drop",   f"{avg_price_drop:.1f}%" if not df.empty else "—")

with col4:
    st.markdown(
        f'<span class="{status_class}">{status_label}</span>',
        unsafe_allow_html=True,
    )

st.markdown("<div style='margin-top:1.5rem;'></div>", unsafe_allow_html=True)

# ─────────────────────────────────────────────
#  CHARTS ROW
# ─────────────────────────────────────────────
st.markdown('<p class="section-header">Visuals</p>', unsafe_allow_html=True)

chart_col, spacer = st.columns([2, 0.05])

with chart_col:
    with st.container():
        # ── Average Sale Price by Source ──────────────────────
        if not df.empty:
            avg_by_source = (
                df.groupby("source")["sale_price"]
                .mean()
                .reset_index()
                .rename(columns={"sale_price": "avg_sale_price"})
                .sort_values("avg_sale_price", ascending=False)
            )
            color_map = {s: SOURCE_COLORS.get(s, "#888888") for s in avg_by_source["source"]}

            fig_bar = px.bar(
                avg_by_source,
                x="source",
                y="avg_sale_price",
                color="source",
                color_discrete_map=color_map,
                labels={"source": "Source", "avg_sale_price": "Avg Sale Price (₹)"},
                title="Average Sale Price by Source",
                text_auto=".0f",
            )
            fig_bar.update_traces(
                textfont_size=11,
                textposition="outside",
                marker_line_width=0,
            )
            fig_bar.update_layout(
                **PLOTLY_LAYOUT,
                showlegend=False,
                title_font=dict(size=13, color="#aaaaaa"),
                bargap=0.35,
                height=340,
            )
            fig_bar.update_xaxis(title=None)
            fig_bar.update_yaxis(title="₹ Avg Sale Price", tickprefix="₹")

            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info("No data available for the selected filters.")

# ─────────────────────────────────────────────
#  DISCOUNT DISTRIBUTION (secondary chart)
# ─────────────────────────────────────────────
if not df.empty and len(df) > 2:
    fig_hist = px.histogram(
        df,
        x="discount_pct",
        color="source",
        color_discrete_map=SOURCE_COLORS,
        nbins=20,
        labels={"discount_pct": "Discount %", "count": "Count"},
        title="Discount % Distribution",
        opacity=0.85,
    )
    fig_hist.update_layout(
        **PLOTLY_LAYOUT,
        barmode="overlay",
        title_font=dict(size=13, color="#aaaaaa"),
        height=280,
        legend_title_text="Source",
    )
    fig_hist.update_xaxis(title="Discount %", ticksuffix="%")
    fig_hist.update_yaxis(title="# Products")
    st.plotly_chart(fig_hist, use_container_width=True)

# ─────────────────────────────────────────────
#  HOT DEALS TABLE
# ─────────────────────────────────────────────
st.markdown('<p class="section-header">🔥 HOT Deals</p>', unsafe_allow_html=True)

hot_df = df_raw.copy()  # always from full data; filter below
if selected_sources:
    hot_df = hot_df[hot_df["source"].isin(selected_sources)]
hot_df = hot_df[hot_df["alert_status"] == "HOT"].copy()

search_query = st.text_input(
    "🔍  Search products",
    placeholder="e.g. iPhone, Samsung, Nike…",
    key="search_input",
)

if search_query:
    hot_df = hot_df[
        hot_df["product_name"]
        .str.lower()
        .str.contains(search_query.lower(), na=False)
    ]

hot_df_sorted = hot_df.sort_values("discount_pct", ascending=False)

display_cols = {
    "product_name":   "Product",
    "source":         "Source",
    "mrp":            "MRP (₹)",
    "sale_price":     "Sale Price (₹)",
    "discount_pct":   "Discount %",
    "price_change_pct": "Δ Price %",
    "url":            "URL",
    "scrape_date":    "Scraped At",
}
display_df = hot_df_sorted[list(display_cols.keys())].rename(columns=display_cols)

# Format numeric columns
for col in ["MRP (₹)", "Sale Price (₹)"]:
    if col in display_df.columns:
        display_df[col] = display_df[col].apply(
            lambda x: f"₹{x:,.2f}" if pd.notna(x) else "—"
        )
for col in ["Discount %", "Δ Price %"]:
    if col in display_df.columns:
        display_df[col] = display_df[col].apply(
            lambda x: f"{x:.1f}%" if pd.notna(x) else "—"
        )
if "Scraped At" in display_df.columns:
    display_df["Scraped At"] = pd.to_datetime(
        display_df["Scraped At"], errors="coerce"
    ).dt.strftime("%b %d, %H:%M")

st.markdown(f"Showing **{len(display_df)}** HOT deal(s).")

if display_df.empty:
    st.warning("No HOT deals match the current filters.")
else:
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "URL": st.column_config.LinkColumn("URL", display_text="🔗 View"),
        },
    )

# ── Download button ────────────────────────────────────────
csv_bytes = hot_df_sorted.to_csv(index=False).encode("utf-8")
st.download_button(
    label="⬇️  Download HOT Deals CSV",
    data=csv_bytes,
    file_name=f"hot_deals_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
    mime="text/csv",
    use_container_width=False,
)

# ─────────────────────────────────────────────
#  FOOTER
# ─────────────────────────────────────────────
st.markdown("---")
st.markdown(
    "<p style='font-size:0.65rem;color:#2a2a2a;font-family:IBM Plex Mono,monospace;text-align:center;'>"
    "Price Monitor Dashboard · Powered by Streamlit + Plotly"
    "</p>",
    unsafe_allow_html=True,
)