# import pandas as pd
# import plotly.express as px
# import streamlit as st
# from streamlit_gsheets import GSheetsConnection
# from datetime import datetime

# st.set_page_config(
#     page_title="Price Monitor · Dashboard",
#     page_icon="📊",
#     layout="wide",
#     initial_sidebar_state="expanded",
# )

# st.markdown(
#     """
#     <style>
#     @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600;700&display=swap');
#     html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; }
#     [data-testid="stSidebar"] { background: #0d0d0d; border-right: 1px solid #1f1f1f; }
#     [data-testid="stSidebar"] * { color: #e0e0e0 !important; }
#     .main .block-container { padding: 2rem 2.5rem 4rem; background: #0a0a0a; }
#     .dash-title { font-family: 'IBM Plex Mono', monospace; font-size: 1.7rem; font-weight: 600; color: #f0f0f0; letter-spacing: -0.5px; }
#     .dash-subtitle { font-size: 0.8rem; color: #555; font-family: 'IBM Plex Mono', monospace; margin-top: -0.3rem; margin-bottom: 1.5rem; }
#     div[data-testid="metric-container"] { background: #111; border: 1px solid #222; border-radius: 10px; padding: 1rem 1.25rem; }
#     div[data-testid="metric-container"] label { font-size: 0.7rem !important; font-family: 'IBM Plex Mono', monospace !important; color: #555 !important; text-transform: uppercase; letter-spacing: 1px; }
#     div[data-testid="metric-container"] [data-testid="stMetricValue"] { font-size: 2rem !important; font-family: 'IBM Plex Mono', monospace !important; color: #f0f0f0 !important; }
#     .status-ok { color: #3effa0; font-family: 'IBM Plex Mono', monospace; font-size: 0.8rem; }
#     .status-warn { color: #ffcc00; font-family: 'IBM Plex Mono', monospace; font-size: 0.8rem; }
#     .status-old { color: #ff5555; font-family: 'IBM Plex Mono', monospace; font-size: 0.8rem; }
#     .section-header { font-family: 'IBM Plex Mono', monospace; font-size: 0.7rem; color: #444; letter-spacing: 2px; text-transform: uppercase; padding: 0.5rem 0 0.3rem; border-top: 1px solid #1a1a1a; margin-top: 0.5rem; }
#     [data-testid="stTextInput"] input { background: #111 !important; border: 1px solid #222 !important; color: #f0f0f0 !important; border-radius: 6px; font-family: 'IBM Plex Mono', monospace; font-size: 0.85rem; }
#     </style>
#     """,
#     unsafe_allow_html=True,
# )

# PLOTLY_LAYOUT = dict(
#     paper_bgcolor="#111111",
#     plot_bgcolor="#111111",
#     font=dict(family="IBM Plex Mono, monospace", color="#aaaaaa", size=11),
#     margin=dict(l=20, r=20, t=40, b=20),
#     xaxis=dict(gridcolor="#1e1e1e", linecolor="#2a2a2a", tickcolor="#2a2a2a"),
#     yaxis=dict(gridcolor="#1e1e1e", linecolor="#2a2a2a", tickcolor="#2a2a2a"),
#     legend=dict(bgcolor="#111111", bordercolor="#222222", borderwidth=1),
# )

# SOURCE_COLORS = {
#     "flipkart": "#f5a623",
#     "amazon": "#00a8e8",
#     "mobiles": "#7bd88f",
# }

# EMPTY_COLUMNS = [
#     "id", "product_name", "mrp", "sale_price", "discount_pct",
#     "price_change_pct", "alert_status", "source", "scrape_date", "url",
# ]


# def empty_df():
#     return pd.DataFrame(columns=EMPTY_COLUMNS)


# @st.cache_data(ttl=60, show_spinner=False)
# def load_data() -> pd.DataFrame:
#     try:
#         conn = st.connection("gsheets", type=GSheetsConnection)
#         df = conn.read(worksheet="prices")
#     except Exception as e:
#         st.error(f"Could not connect to Google Sheets: {e}")
#         return empty_df()

#     if df is None or df.empty:
#         return empty_df()

#     for c in EMPTY_COLUMNS:
#         if c not in df.columns:
#             df[c] = pd.NA

#     df["scrape_date"] = pd.to_datetime(df["scrape_date"], errors="coerce")
#     for col in ["mrp", "sale_price", "discount_pct", "price_change_pct"]:
#         df[col] = pd.to_numeric(df[col], errors="coerce")
#     df["alert_status"] = df["alert_status"].astype(str).str.upper().str.strip()
#     df["source"] = df["source"].astype(str).str.strip().str.lower()
#     df["product_name"] = df["product_name"].astype(str).str.strip()
#     return df


# def pipeline_status(df: pd.DataFrame) -> tuple[str, str]:
#     if df.empty or df["scrape_date"].isna().all():
#         return "No data", "status-old"
#     latest = df["scrape_date"].max()
#     age_h = (datetime.now() - latest).total_seconds() / 3600
#     if age_h < 2:
#         return f"✔ Live ({latest.strftime('%b %d, %H:%M')})", "status-ok"
#     if age_h < 24:
#         return f"⚠ Stale ({int(age_h)}h ago)", "status-warn"
#     return f"✘ Old ({latest.strftime('%b %d')})", "status-old"


# df_raw = load_data()

# with st.sidebar:
#     st.markdown("### ⚙️ Filters")
#     st.markdown("---")

#     sources = sorted([s for s in df_raw["source"].dropna().unique().tolist() if s and s != "nan"])
#     selected_sources = st.multiselect("Source", sources, default=sources, key="src_filter")

#     if df_raw.empty or df_raw["discount_pct"].dropna().empty:
#         min_disc, max_disc = 0, 100
#     else:
#         min_disc = int(df_raw["discount_pct"].min())
#         max_disc = int(df_raw["discount_pct"].max())

#     discount_range = st.slider(
#         "Discount % range",
#         min_value=min_disc,
#         max_value=max_disc,
#         value=(min_disc, max_disc),
#         key="disc_filter",
#     )
#     show_hot_only = st.checkbox("HOT deals only", value=False)

#     st.markdown("---")
#     if st.button("🔄 Refresh data", use_container_width=True):
#         st.cache_data.clear()
#         st.rerun()

#     st.markdown(
#         "<p style='font-size:0.65rem;color:#333;margin-top:2rem;font-family:IBM Plex Mono,monospace;'>Google Sheets · auto-refresh 60 s</p>",
#         unsafe_allow_html=True,
#     )

# df = df_raw.copy()
# if not df.empty and selected_sources:
#     df = df[df["source"].isin(selected_sources)]
# if not df.empty:
#     df = df[(df["discount_pct"] >= discount_range[0]) & (df["discount_pct"] <= discount_range[1])]
# if show_hot_only and not df.empty:
#     df = df[df["alert_status"] == "HOT"]

# st.markdown(
#     '<p class="dash-title">📊 Price Monitor</p>'
#     '<p class="dash-subtitle">Web-Scraping Pipeline · Real-time Price Intelligence</p>',
#     unsafe_allow_html=True,
# )

# if df.empty:
#     total_products = 0
#     hot_deals_count = 0
#     market_avg = None
#     avg_price_drop = None
# else:
#     df["price_drop_pct"] = ((df["mrp"] - df["sale_price"]) / df["mrp"]) * 100
#     total_products = df["product_name"].nunique()
#     hot_deals_count = int((df["alert_status"] == "HOT").sum())
#     market_avg = df["sale_price"].mean()
#     avg_price_drop = df["price_drop_pct"].mean()

# status_label, status_class = pipeline_status(df_raw)

# c1, c2, c3, c4 = st.columns(4)
# c1.metric("Total Products", f"{total_products:,}")
# c2.metric("🔥 HOT Deals", f"{hot_deals_count:,}")
# c3.metric("Market Avg Price", f"₹{market_avg:,.2f}" if market_avg is not None and pd.notna(market_avg) else "—")
# c4.metric("Avg Price Drop", f"{avg_price_drop:.1f}%" if avg_price_drop is not None and pd.notna(avg_price_drop) else "—")
# with c4:
#     st.markdown(f'<span class="{status_class}">{status_label}</span>', unsafe_allow_html=True)

# st.markdown("<div style='margin-top:1.5rem;'></div>", unsafe_allow_html=True)
# st.markdown('<p class="section-header">Visuals</p>', unsafe_allow_html=True)

# if not df.empty:
#     avg_by_source = (
#         df.groupby("source", as_index=False)["sale_price"]
#         .mean()
#         .rename(columns={"sale_price": "avg_sale_price"})
#         .sort_values("avg_sale_price", ascending=False)
#     )
#     fig_bar = px.bar(
#         avg_by_source,
#         x="source",
#         y="avg_sale_price",
#         color="source",
#         color_discrete_map={k: SOURCE_COLORS.get(k, "#888888") for k in avg_by_source["source"]},
#         labels={"source": "Source", "avg_sale_price": "Avg Sale Price (₹)"},
#         title="Average Sale Price by Source",
#         text_auto=".0f",
#     )
#     fig_bar.update_traces(textposition="outside", marker_line_width=0)
#     fig_bar.update_layout(**PLOTLY_LAYOUT, showlegend=False, title_font=dict(size=13, color="#aaaaaa"), bargap=0.35, height=340)
#     fig_bar.update_xaxes(title=None)
#     fig_bar.update_yaxes(title="₹ Avg Sale Price", tickprefix="₹")
#     st.plotly_chart(fig_bar, use_container_width=True)

#     if len(df) > 2:
#         fig_hist = px.histogram(
#             df,
#             x="discount_pct",
#             color="source",
#             color_discrete_map=SOURCE_COLORS,
#             nbins=20,
#             labels={"discount_pct": "Discount %", "count": "Count"},
#             title="Discount % Distribution",
#             opacity=0.85,
#         )
#         fig_hist.update_layout(**PLOTLY_LAYOUT, barmode="overlay", title_font=dict(size=13, color="#aaaaaa"), height=280, legend_title_text="Source")
#         fig_hist.update_xaxes(title="Discount %", ticksuffix="%")
#         fig_hist.update_yaxes(title="# Products")
#         st.plotly_chart(fig_hist, use_container_width=True)
# else:
#     st.info("No data available for the selected filters.")

# st.markdown('<p class="section-header">🔥 HOT Deals</p>', unsafe_allow_html=True)

# hot_df = df_raw.copy()
# if selected_sources:
#     hot_df = hot_df[hot_df["source"].isin(selected_sources)]
# hot_df = hot_df[hot_df["alert_status"] == "HOT"].copy()

# search_query = st.text_input("🔍 Search products", placeholder="e.g. iPhone, Samsung, Nike…", key="search_input")
# if search_query:
#     hot_df = hot_df[hot_df["product_name"].str.lower().str.contains(search_query.lower(), na=False)]

# hot_df_sorted = hot_df.sort_values("discount_pct", ascending=False)

# display_cols = {
#     "product_name": "Product",
#     "source": "Source",
#     "mrp": "MRP (₹)",
#     "sale_price": "Sale Price (₹)",
#     "discount_pct": "Discount %",
#     "price_change_pct": "Δ Price %",
#     "url": "URL",
#     "scrape_date": "Scraped At",
# }

# display_df = hot_df_sorted[list(display_cols.keys())].rename(columns=display_cols)

# for col in ["MRP (₹)", "Sale Price (₹)"]:
#     display_df[col] = display_df[col].apply(lambda x: f"₹{x:,.2f}" if pd.notna(x) else "—")
# for col in ["Discount %", "Δ Price %"]:
#     display_df[col] = display_df[col].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "—")
# display_df["Scraped At"] = pd.to_datetime(display_df["Scraped At"], errors="coerce").dt.strftime("%b %d, %H:%M")

# st.markdown(f"Showing **{len(display_df)}** HOT deal(s).")

# if display_df.empty:
#     st.warning("No HOT deals match the current filters.")
# else:
#     st.dataframe(
#         display_df,
#         use_container_width=True,
#         hide_index=True,
#         column_config={"URL": st.column_config.LinkColumn("URL", display_text="🔗 View")},
#     )

# csv_bytes = hot_df_sorted.to_csv(index=False).encode("utf-8")
# st.download_button(
#     label="⬇️ Download HOT Deals CSV",
#     data=csv_bytes,
#     file_name=f"hot_deals_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
#     mime="text/csv",
# )

# st.markdown("---")
# st.markdown(
#     "<p style='font-size:0.65rem;color:#2a2a2a;font-family:IBM Plex Mono,monospace;text-align:center;'>Price Monitor Dashboard · Powered by Streamlit + Plotly</p>",
#     unsafe_allow_html=True,
# )



"""
Competitor Price Tracker — Premium Streamlit Dashboard
Clean, modern SaaS-style layout with full filtering, charts, and table.
"""

from __future__ import annotations

import io
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ──────────────────────────────────────────────────────────────
# PAGE CONFIG
# ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="PriceScope · Competitor Tracker",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ──────────────────────────────────────────────────────────────
# GLOBAL STYLES
# ──────────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,wght@0,300;0,400;0,500;0,600;0,700;1,400&family=DM+Mono:wght@400;500&display=swap');

    html, body, [class*="css"] {
        font-family: 'DM Sans', sans-serif;
        color: #e6edf3;
        background-color: #0d1117;
    }
    .main .block-container {
        padding: 0 2.5rem 3rem;
        max-width: 1440px;
        background: #0d1117;
    }
    [data-testid="stAppViewContainer"],
    [data-testid="stAppViewBlockContainer"],
    section[data-testid="stMain"] { background: #0d1117; }

    [data-testid="stSidebar"] {
        background: #0d1117;
        border-right: 1px solid #30363d;
    }
    [data-testid="stSidebar"] .block-container { padding: 1.5rem 1rem; }
    [data-testid="stSidebar"] * { color: #e6edf3 !important; }
    [data-testid="stSidebar"] label {
        font-size: 0.72rem !important;
        font-weight: 600 !important;
        letter-spacing: 0.06em !important;
        text-transform: uppercase !important;
        color: #8b949e !important;
    }
    [data-testid="stSidebar"] span[data-baseweb="tag"] {
        background: #1c2333 !important;
        border: 1px solid #30363d !important;
    }

    .header-strip {
        background: linear-gradient(135deg, #0d1117 0%, #161b22 60%, #1c2333 100%);
        border-bottom: 1px solid #30363d;
        border-radius: 0 0 16px 16px;
        padding: 1.6rem 2.5rem 1.8rem;
        margin: 0 -2.5rem 2rem;
        display: flex;
        align-items: center;
        justify-content: space-between;
    }
    .app-title {
        font-size: 1.6rem;
        font-weight: 700;
        color: #e6edf3;
        letter-spacing: -0.02em;
        margin: 0;
    }
    .app-subtitle {
        font-size: 0.8rem;
        color: #8b949e;
        margin: 2px 0 0;
        font-family: 'DM Mono', monospace;
    }
    .refresh-badge {
        background: #161b22;
        border: 1px solid #30363d;
        border-radius: 8px;
        padding: 0.45rem 0.9rem;
        font-family: 'DM Mono', monospace;
        font-size: 0.72rem;
        color: #8b949e;
        white-space: nowrap;
    }

    .kpi-card {
        background: #161b22;
        border-radius: 14px;
        padding: 1.2rem 1.4rem;
        border: 1px solid #30363d;
        box-shadow: 0 4px 20px rgba(0,0,0,0.5);
        margin-bottom: 1.5rem;
        position: relative;
        overflow: hidden;
    }
    .kpi-card::before {
        content: \'\';
        position: absolute;
        top: 0; left: 0; right: 0;
        height: 3px;
        border-radius: 14px 14px 0 0;
    }
    .kpi-card.blue::before  { background: linear-gradient(90deg, #2f81f7, #79c0ff); }
    .kpi-card.green::before { background: linear-gradient(90deg, #2ea043, #56d364); }
    .kpi-card.red::before   { background: linear-gradient(90deg, #da3633, #ff7b72); }
    .kpi-card.amber::before { background: linear-gradient(90deg, #9e6a03, #e3b341); }
    .kpi-card.purple::before{ background: linear-gradient(90deg, #6e40c9, #bc8cff); }

    .kpi-label {
        font-size: 0.68rem;
        font-weight: 600;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: #8b949e;
        margin-bottom: 0.5rem;
    }
    .kpi-value {
        font-size: 2rem;
        font-weight: 700;
        color: #e6edf3;
        line-height: 1;
        font-family: \'DM Mono\', monospace;
    }
    .kpi-sub {
        font-size: 0.75rem;
        color: #6e7681;
        margin-top: 0.35rem;
    }

    .chart-card {
        background: #161b22;
        border-radius: 14px;
        padding: 1.2rem 1.4rem 0.8rem;
        border: 1px solid #30363d;
        box-shadow: 0 4px 20px rgba(0,0,0,0.4);
        margin-bottom: 1.5rem;
    }
    .chart-title {
        font-size: 0.8rem;
        font-weight: 600;
        letter-spacing: 0.04em;
        text-transform: uppercase;
        color: #8b949e;
        margin-bottom: 0.8rem;
    }

    .section-label {
        font-size: 0.7rem;
        font-weight: 600;
        letter-spacing: 0.1em;
        text-transform: uppercase;
        color: #6e7681;
        margin: 1.8rem 0 0.8rem;
        padding-bottom: 0.4rem;
        border-bottom: 1px solid #21262d;
    }

    .chip-hot   { background:#3d1f1f; color:#ff7b72; border:1px solid #6e2b2b; border-radius:6px; padding:2px 8px; font-size:0.7rem; font-weight:600; }
    .chip-watch { background:#2d2208; color:#e3b341; border:1px solid #5a4000; border-radius:6px; padding:2px 8px; font-size:0.7rem; font-weight:600; }
    .chip-ok    { background:#1a2e1a; color:#56d364; border:1px solid #2a5a2a; border-radius:6px; padding:2px 8px; font-size:0.7rem; font-weight:600; }

    [data-testid="stDataFrame"] { border-radius: 12px; overflow: hidden; border: 1px solid #30363d; }
    [data-testid="stTextInput"] > div > div {
        background: #161b22 !important;
        border-radius: 8px !important;
        border-color: #30363d !important;
        color: #e6edf3 !important;
        font-family: \'DM Sans\', sans-serif !important;
    }

    [data-baseweb="select"] > div {
        background: #161b22 !important;
        border-color: #30363d !important;
        color: #e6edf3 !important;
    }
    [data-baseweb="popover"],
    [data-baseweb="menu"] { background: #1c2333 !important; border-color: #30363d !important; }
    [data-baseweb="menu"] li { color: #e6edf3 !important; }
    [data-baseweb="menu"] li:hover { background: #21262d !important; }

    [data-testid="stDateInput"] input {
        background: #161b22 !important;
        border-color: #30363d !important;
        color: #e6edf3 !important;
    }

    button[kind="secondary"], button[kind="primary"] {
        background: #1c2333 !important;
        border-color: #30363d !important;
        color: #e6edf3 !important;
    }
    button[kind="secondary"]:hover, button[kind="primary"]:hover {
        background: #2f81f7 !important;
        border-color: #2f81f7 !important;
        color: #ffffff !important;
    }

    .sidebar-section {
        font-size: 0.65rem;
        font-weight: 700;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: #6e7681;
        margin: 1.2rem 0 0.5rem;
    }

    .empty-state { text-align:center; padding:3rem 1rem; color:#6e7681; }
    .empty-state-icon { font-size:2.5rem; margin-bottom:0.5rem; }
    .empty-state-title { font-size:1rem; font-weight:600; color:#8b949e; }
    .empty-state-sub { font-size:0.8rem; margin-top:0.25rem; }

    #MainMenu, footer, header { visibility: hidden; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ──────────────────────────────────────────────────────────────
# CONSTANTS
# ──────────────────────────────────────────────────────────────
PLOTLY_BASE = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="DM Sans, sans-serif", color="#64748b", size=11),
    margin=dict(l=0, r=0, t=10, b=0),
    xaxis=dict(gridcolor="#f1f5f9", linecolor="#e2e8f0", tickcolor="#e2e8f0", zeroline=False),
    yaxis=dict(gridcolor="#f1f5f9", linecolor="#e2e8f0", tickcolor="#e2e8f0", zeroline=False),
    legend=dict(bgcolor="rgba(0,0,0,0)", borderwidth=0, orientation="h", y=1.08),
    colorway=["#2563eb", "#059669", "#dc2626", "#d97706", "#7c3aed", "#0891b2"],
)

SOURCE_PALETTE = {
    "flipkart": "#f59e0b",
    "amazon":   "#2563eb",
    "mobiles":  "#059669",
    "myntra":   "#e11d48",
    "snapdeal": "#dc2626",
}

EMPTY_COLS = [
    "id", "product_name", "category", "source",
    "mrp", "current_price", "previous_price",
    "price_change", "price_change_pct",
    "stock_status", "alert_status",
    "scrape_date", "url",
]

# ──────────────────────────────────────────────────────────────
# DATA HELPERS
# ──────────────────────────────────────────────────────────────

def empty_df() -> pd.DataFrame:
    return pd.DataFrame(columns=EMPTY_COLS)


def _try_gsheets() -> pd.DataFrame | None:
    """Try loading from Google Sheets via streamlit-gsheets."""
    try:
        from streamlit_gsheets import GSheetsConnection  # type: ignore
        conn = st.connection("gsheets", type=GSheetsConnection)
        df = conn.read(worksheet="prices")
        if df is not None and not df.empty:
            return df
    except Exception:
        pass
    return None


def _try_csv() -> pd.DataFrame | None:
    """Try loading from prices.csv in working directory."""
    try:
        return pd.read_csv("prices.csv")
    except FileNotFoundError:
        pass
    return None


def _generate_demo() -> pd.DataFrame:
    """Generate realistic-looking demo data so the dashboard is never blank."""
    import random, math
    rng = random.Random(42)
    sources = ["amazon", "flipkart", "mobiles", "myntra", "snapdeal"]
    categories = ["Smartphones", "Laptops", "Headphones", "Tablets", "Smartwatches", "Cameras"]
    statuses = ["IN_STOCK", "IN_STOCK", "IN_STOCK", "OUT_OF_STOCK", "LOW_STOCK"]
    alerts = ["HOT", "WATCH", "OK", "OK", "OK"]

    products = [
        ("Apple iPhone 15 Pro 256GB", 134900, "Smartphones"),
        ("Samsung Galaxy S24 Ultra", 129999, "Smartphones"),
        ("OnePlus 12 5G 256GB", 64999, "Smartphones"),
        ("Realme GT 5 Pro 5G", 34999, "Smartphones"),
        ("Apple MacBook Air M3", 114900, "Laptops"),
        ("Dell XPS 15 9530", 189990, "Laptops"),
        ("HP Spectre x360 14", 159990, "Laptops"),
        ("Lenovo ThinkPad X1 Carbon", 175000, "Laptops"),
        ("Sony WH-1000XM5", 26990, "Headphones"),
        ("Bose QuietComfort 45", 32990, "Headphones"),
        ("Apple AirPods Pro 2nd Gen", 24900, "Headphones"),
        ("JBL Tour One M2", 19999, "Headphones"),
        ("iPad Pro 12.9 M2 256GB", 112900, "Tablets"),
        ("Samsung Galaxy Tab S9 Ultra", 108999, "Tablets"),
        ("Apple Watch Series 9 GPS 45mm", 44900, "Smartwatches"),
        ("Samsung Galaxy Watch 6 Classic", 36999, "Smartwatches"),
        ("Sony Alpha ZV-E10 II", 72990, "Cameras"),
        ("Fujifilm X100VI", 164999, "Cameras"),
        ("GoPro Hero 12 Black", 36000, "Cameras"),
        ("Canon EOS R50", 69995, "Cameras"),
    ]

    rows = []
    base_date = datetime.now()

    for pid, (name, mrp, cat) in enumerate(products, 1):
        for s in rng.sample(sources, rng.randint(2, 4)):
            disc = rng.uniform(0.03, 0.35)
            current = round(mrp * (1 - disc), -1)
            delta_pct = rng.uniform(-0.12, 0.08)
            previous = round(current / (1 + delta_pct), -1)
            change = current - previous
            scrape_dt = base_date - timedelta(hours=rng.uniform(0, 48))
            alert = "HOT" if disc > 0.25 else ("WATCH" if disc > 0.15 else "OK")
            rows.append({
                "id": f"{pid}-{s[:3]}",
                "product_name": name,
                "category": cat,
                "source": s,
                "mrp": mrp,
                "current_price": current,
                "previous_price": previous,
                "price_change": round(change, 2),
                "price_change_pct": round((change / previous) * 100, 2) if previous else 0,
                "discount_pct": round(disc * 100, 1),
                "stock_status": rng.choice(statuses),
                "alert_status": alert,
                "scrape_date": scrape_dt,
                "url": f"https://{s}.com/product/{pid}",
            })

    return pd.DataFrame(rows)


@st.cache_data(ttl=60, show_spinner=False)
def load_data() -> pd.DataFrame:
    df = _try_gsheets()
    if df is None or df.empty:
        df = _try_csv()

    if df is not None and not df.empty:
        # Normalise column names
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
        # Patch column aliases
        aliases = {
            "sale_price": "current_price",
            "old_price": "previous_price",
            "name": "product_name",
        }
        df.rename(columns={k: v for k, v in aliases.items() if k in df.columns}, inplace=True)
        # Fill missing
        for c in EMPTY_COLS:
            if c not in df.columns:
                df[c] = pd.NA
    else:
        df = _generate_demo()

    # Type coercions
    df["scrape_date"] = pd.to_datetime(df["scrape_date"], errors="coerce")
    for col in ["mrp", "current_price", "previous_price", "price_change", "price_change_pct"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "discount_pct" not in df.columns:
        df["discount_pct"] = ((df["mrp"] - df["current_price"]) / df["mrp"] * 100).round(1)

    if "price_change" not in df.columns:
        df["price_change"] = (df["current_price"] - df["previous_price"]).round(2)

    if "price_change_pct" not in df.columns:
        df["price_change_pct"] = (df["price_change"] / df["previous_price"] * 100).round(2)

    df["alert_status"] = df["alert_status"].astype(str).str.upper().str.strip()
    df["stock_status"] = df["stock_status"].astype(str).str.upper().str.strip()
    df["source"] = df["source"].astype(str).str.strip().str.lower()
    df["product_name"] = df["product_name"].astype(str).str.strip()
    if "category" not in df.columns:
        df["category"] = "Uncategorised"
    df["category"] = df["category"].astype(str).str.strip()

    return df

# ──────────────────────────────────────────────────────────────
# COMPONENT FUNCTIONS
# ──────────────────────────────────────────────────────────────

def render_header(last_refreshed: datetime) -> None:
    st.markdown(
        f"""
        <div class="header-strip">
            <div class="header-left">
                <p class="app-title">📈 PriceScope</p>
                <p class="app-subtitle">Competitor Price Intelligence · Real-time Monitoring</p>
            </div>
            <div class="refresh-badge">
                🕐 Last refreshed: {last_refreshed.strftime("%b %d, %Y %H:%M")}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_kpi_cards(df: pd.DataFrame) -> None:
    if df.empty:
        total, hot, drops, oos, avg_change = 0, 0, 0, 0, 0.0
    else:
        total = df["product_name"].nunique()
        hot = int((df["alert_status"] == "HOT").sum())
        drops = int((df["price_change"] < 0).sum())
        oos = int((df["stock_status"] == "OUT_OF_STOCK").sum())
        avg_change = df["price_change_pct"].mean()

    cols = st.columns(5)
    cards = [
        ("blue",   "Total Products",    f"{total:,}",     "unique SKUs tracked"),
        ("amber",  "Active Alerts",     f"{hot:,}",       "HOT deal alerts"),
        ("green",  "Price Drops",       f"{drops:,}",     "since last scan"),
        ("red",    "Out of Stock",      f"{oos:,}",       "items unavailable"),
        ("purple", "Avg Price Change",
         f"{'↓' if avg_change < 0 else '↑'}{abs(avg_change):.1f}%",
         "across all products"),
    ]
    for col, (color, label, value, sub) in zip(cols, cards):
        with col:
            st.markdown(
                f"""
                <div class="kpi-card {color}">
                    <div class="kpi-label">{label}</div>
                    <div class="kpi-value">{value}</div>
                    <div class="kpi-sub">{sub}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def render_sidebar(df_raw: pd.DataFrame) -> dict:
    with st.sidebar:
        st.markdown(
            "<p style='font-size:1rem;font-weight:700;color:#0f172a;margin-bottom:0.2rem;'>⚙ Filters</p>",
            unsafe_allow_html=True,
        )
        st.markdown("<hr style='border-color:#f1f5f9;margin:0.5rem 0 1rem;'>", unsafe_allow_html=True)

        # Source
        st.markdown("<div class='sidebar-section'>Source / Website</div>", unsafe_allow_html=True)
        sources = sorted([s for s in df_raw["source"].dropna().unique() if s and s != "nan"])
        sel_sources = st.multiselect("", sources, default=sources, key="src", label_visibility="collapsed")

        # Category
        st.markdown("<div class='sidebar-section'>Category</div>", unsafe_allow_html=True)
        cats = sorted([c for c in df_raw["category"].dropna().unique() if c and c != "nan"])
        sel_cats = st.multiselect("", cats, default=cats, key="cat", label_visibility="collapsed")

        # Stock status
        st.markdown("<div class='sidebar-section'>Stock Status</div>", unsafe_allow_html=True)
        stock_opts = ["ALL", "IN_STOCK", "LOW_STOCK", "OUT_OF_STOCK"]
        sel_stock = st.radio("", stock_opts, index=0, key="stock", label_visibility="collapsed")

        # Price range
        st.markdown("<div class='sidebar-section'>Current Price (₹)</div>", unsafe_allow_html=True)
        min_p = int(df_raw["current_price"].min()) if not df_raw.empty else 0
        max_p = int(df_raw["current_price"].max()) if not df_raw.empty else 200000
        price_range = st.slider("", min_p, max_p, (min_p, max_p), step=500, key="price_range", label_visibility="collapsed")

        # Date range
        st.markdown("<div class='sidebar-section'>Date Range</div>", unsafe_allow_html=True)
        min_d = df_raw["scrape_date"].min().date() if not df_raw.empty else datetime.today().date()
        max_d = df_raw["scrape_date"].max().date() if not df_raw.empty else datetime.today().date()
        date_range = st.date_input("", (min_d, max_d), min_value=min_d, max_value=max_d, key="date_range", label_visibility="collapsed")

        st.markdown("<hr style='border-color:#f1f5f9;margin:1rem 0;'>", unsafe_allow_html=True)

        if st.button("🔄 Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        st.markdown(
            "<p style='font-size:0.62rem;color:#cbd5e1;margin-top:1rem;font-family:DM Mono,monospace;'>Auto-refresh every 60 s</p>",
            unsafe_allow_html=True,
        )

    return dict(
        sources=sel_sources,
        categories=sel_cats,
        stock=sel_stock,
        price_range=price_range,
        date_range=date_range,
    )


def apply_filters(df: pd.DataFrame, f: dict) -> pd.DataFrame:
    if df.empty:
        return df

    if f["sources"]:
        df = df[df["source"].isin(f["sources"])]
    if f["categories"]:
        df = df[df["category"].isin(f["categories"])]
    if f["stock"] != "ALL":
        df = df[df["stock_status"] == f["stock"]]

    lo, hi = f["price_range"]
    df = df[(df["current_price"] >= lo) & (df["current_price"] <= hi)]

    if isinstance(f["date_range"], (list, tuple)) and len(f["date_range"]) == 2:
        start, end = f["date_range"]
        df = df[
            (df["scrape_date"].dt.date >= start) &
            (df["scrape_date"].dt.date <= end)
        ]
    return df


def render_price_trend(df: pd.DataFrame) -> None:
    if df.empty or "scrape_date" not in df.columns:
        return
    trend = (
        df.dropna(subset=["scrape_date", "current_price"])
        .assign(date=lambda x: x["scrape_date"].dt.date)
        .groupby(["date", "source"], as_index=False)["current_price"]
        .mean()
        .rename(columns={"current_price": "avg_price"})
    )
    if trend.empty:
        return
    fig = px.line(
        trend, x="date", y="avg_price", color="source",
        color_discrete_map=SOURCE_PALETTE,
        labels={"date": "", "avg_price": "Avg Price (₹)", "source": ""},
        line_shape="spline",
    )
    fig.update_traces(line_width=2.5)
    fig.update_layout(**PLOTLY_BASE, height=280)
    fig.update_yaxes(tickprefix="₹")
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def render_stock_chart(df: pd.DataFrame) -> None:
    if df.empty:
        return
    counts = df["stock_status"].value_counts().reset_index()
    counts.columns = ["status", "count"]
    color_map = {"IN_STOCK": "#059669", "LOW_STOCK": "#d97706", "OUT_OF_STOCK": "#dc2626"}
    fig = px.pie(
        counts, names="status", values="count",
        color="status", color_discrete_map=color_map,
        hole=0.6,
    )
    fig.update_traces(textinfo="percent+label", textfont_size=11, showlegend=False,
                      marker=dict(line=dict(color="white", width=3)))
    fig.update_layout(**PLOTLY_BASE, height=260)
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def render_top_drops(df: pd.DataFrame) -> None:
    if df.empty:
        return
    drops = (
        df[df["price_change"] < 0]
        .sort_values("discount_pct", ascending=False)
        .drop_duplicates("product_name")
        .head(8)
    )
    if drops.empty:
        return
    fig = px.bar(
        drops, y="product_name", x="discount_pct",
        orientation="h",
        color="source", color_discrete_map=SOURCE_PALETTE,
        labels={"product_name": "", "discount_pct": "Discount %", "source": ""},
        text="discount_pct",
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
    fig.update_layout(**PLOTLY_BASE, height=300, showlegend=False)
    fig.update_yaxes(categoryorder="total ascending")
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def render_alerts(df: pd.DataFrame) -> None:
    hot = df[df["alert_status"] == "HOT"].sort_values("discount_pct", ascending=False).head(5)
    watch = df[df["alert_status"] == "WATCH"].sort_values("discount_pct", ascending=False).head(5)

    cols = st.columns(2)
    with cols[0]:
        st.markdown("<div class='chart-title'>🔥 HOT Deals</div>", unsafe_allow_html=True)
        if hot.empty:
            st.caption("No HOT deals in current filters.")
        else:
            for _, r in hot.iterrows():
                st.markdown(
                    f"""
                    <div style="display:flex;align-items:center;justify-content:space-between;
                        padding:0.5rem 0.8rem;margin-bottom:0.4rem;background:#1d4ed8;
                        border-radius:8px;border-left:3px solid #ffffff;">
                        <div>
                            <div style="font-size:0.82rem;font-weight:600;color:#ffffff;">{r['product_name'][:45]}…</div>
                            <div style="font-size:0.7rem;color:#ffffff;">{r['source'].capitalize()} · {r['category']}</div>
                        </div>
                        <div style="text-align:right;">
                            <div style="font-size:0.9rem;font-weight:700;color:#ff8c42;">↓ {r['discount_pct']:.1f}%</div>
                            <div style="font-size:0.72rem;color:#8b949e;">₹{r['current_price']:,.0f}</div>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

    with cols[1]:
        st.markdown("<div class='chart-title'>👀 Watch List</div>", unsafe_allow_html=True)
        if watch.empty:
            st.caption("No WATCH alerts in current filters.")
        else:
            for _, r in watch.iterrows():
                st.markdown(
                    f"""
                    <div style="display:flex;align-items:center;justify-content:space-between;
                        padding:0.5rem 0.8rem;margin-bottom:0.4rem;background:#2d2208;
                        border-radius:8px;border-left:3px solid #e3b341;">
                        <div>
                            <div style="font-size:0.82rem;font-weight:600;color:#e3b341;">{r['product_name'][:45]}…</div>
                            <div style="font-size:0.7rem;color:#8b949e;">{r['source'].capitalize()} · {r['category']}</div>
                        </div>
                        <div style="text-align:right;">
                            <div style="font-size:0.9rem;font-weight:700;color:#e3b341;">↓ {r['discount_pct']:.1f}%</div>
                            <div style="font-size:0.72rem;color:#8b949e;">₹{r['current_price']:,.0f}</div>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )


def _fmt_price(x):
    return f"₹{x:,.0f}" if pd.notna(x) else "—"

def _fmt_pct(x):
    if pd.isna(x):
        return "—"
    sign = "↓" if x < 0 else "↑"
    return f"{sign} {abs(x):.1f}%"

def _fmt_stock(x):
    mapping = {"IN_STOCK": "✅ In Stock", "LOW_STOCK": "⚠️ Low", "OUT_OF_STOCK": "❌ OOS"}
    return mapping.get(str(x).upper(), x)

def _fmt_alert(x):
    mapping = {"HOT": "🔥 HOT", "WATCH": "👀 WATCH", "OK": "✓ OK"}
    return mapping.get(str(x).upper(), x)


def render_product_table(df: pd.DataFrame) -> None:
    search = st.text_input(
        "🔍 Search products",
        placeholder="e.g. iPhone, Samsung …",
        key="search",
    )
    tdf = df.copy()
    if search:
        tdf = tdf[tdf["product_name"].str.lower().str.contains(search.lower(), na=False)]

    disp_cols = {
        "product_name": "Product",
        "source": "Source",
        "category": "Category",
        "current_price": "Current (₹)",
        "previous_price": "Previous (₹)",
        "price_change": "Change (₹)",
        "price_change_pct": "Change %",
        "stock_status": "Stock",
        "alert_status": "Alert",
        "scrape_date": "Last Updated",
        "url": "URL",
    }

    present = [c for c in disp_cols if c in tdf.columns]
    out = tdf[present].rename(columns=disp_cols).copy()

    for col in ["Current (₹)", "Previous (₹)", "Change (₹)"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").apply(_fmt_price)
    if "Change %" in out.columns:
        out["Change %"] = pd.to_numeric(out["Change %"], errors="coerce").apply(_fmt_pct)
    if "Stock" in out.columns:
        out["Stock"] = out["Stock"].apply(_fmt_stock)
    if "Alert" in out.columns:
        out["Alert"] = out["Alert"].apply(_fmt_alert)
    if "Last Updated" in out.columns:
        out["Last Updated"] = pd.to_datetime(out["Last Updated"], errors="coerce").dt.strftime("%b %d, %H:%M")
    if "Source" in out.columns:
        out["Source"] = out["Source"].str.capitalize()

    st.markdown(
        f"<p style='font-size:0.78rem;color:#64748b;margin-bottom:0.5rem;'>"
        f"Showing <b>{len(out):,}</b> records</p>",
        unsafe_allow_html=True,
    )

    if out.empty:
        st.markdown(
            "<div class='empty-state'>"
            "<div class='empty-state-icon'>🔍</div>"
            "<div class='empty-state-title'>No products found</div>"
            "<div class='empty-state-sub'>Try adjusting your filters or search query.</div>"
            "</div>",
            unsafe_allow_html=True,
        )
        return

    col_config = {}
    if "URL" in out.columns:
        col_config["URL"] = st.column_config.LinkColumn("URL", display_text="🔗 View")

    st.dataframe(
        out,
        use_container_width=True,
        hide_index=True,
        height=420,
        column_config=col_config,
    )

    csv_buf = io.BytesIO()
    tdf.to_csv(csv_buf, index=False)
    csv_buf.seek(0)
    st.download_button(
        "⬇️ Export CSV",
        data=csv_buf,
        file_name=f"price_data_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
        mime="text/csv",
    )

# ──────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────

def main() -> None:
    df_raw = load_data()
    filters = render_sidebar(df_raw)
    df = apply_filters(df_raw.copy(), filters)

    render_header(datetime.now())
    render_kpi_cards(df)

    # ── Charts row ──────────────────────────────────────────
    st.markdown("<div class='section-label'>Market Overview</div>", unsafe_allow_html=True)

    c1, c2 = st.columns([2, 1])
    with c1:
        st.markdown("<div class='chart-card'><div class='chart-title'>Price Trend by Source</div>", unsafe_allow_html=True)
        render_price_trend(df)
        st.markdown("</div>", unsafe_allow_html=True)

    with c2:
        st.markdown("<div class='chart-card'><div class='chart-title'>Stock Status Breakdown</div>", unsafe_allow_html=True)
        render_stock_chart(df)
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div class='chart-card'><div class='chart-title'>Top Price Drops — Biggest Discounts</div>", unsafe_allow_html=True)
    render_top_drops(df)
    st.markdown("</div>", unsafe_allow_html=True)

    # ── Alerts ──────────────────────────────────────────────
    st.markdown("<div class='section-label'>Alerts</div>", unsafe_allow_html=True)
    st.markdown("<div class='chart-card'>", unsafe_allow_html=True)
    render_alerts(df)
    st.markdown("</div>", unsafe_allow_html=True)

    # ── Table ───────────────────────────────────────────────
    st.markdown("<div class='section-label'>Product Table</div>", unsafe_allow_html=True)
    st.markdown("<div class='chart-card'>", unsafe_allow_html=True)
    render_product_table(df)
    st.markdown("</div>", unsafe_allow_html=True)

    # Footer
    st.markdown(
        "<p style='text-align:center;font-size:0.65rem;color:#cbd5e1;"
        "font-family:DM Mono,monospace;margin-top:2rem;'>"
        "PriceScope · Powered by Streamlit + Plotly · © 2025</p>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()