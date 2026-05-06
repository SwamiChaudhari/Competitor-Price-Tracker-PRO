import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_gsheets import GSheetsConnection
from datetime import datetime

st.set_page_config(
    page_title="Price Monitor · Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; }
    [data-testid="stSidebar"] { background: #0d0d0d; border-right: 1px solid #1f1f1f; }
    [data-testid="stSidebar"] * { color: #e0e0e0 !important; }
    .main .block-container { padding: 2rem 2.5rem 4rem; background: #0a0a0a; }
    .dash-title { font-family: 'IBM Plex Mono', monospace; font-size: 1.7rem; font-weight: 600; color: #f0f0f0; letter-spacing: -0.5px; }
    .dash-subtitle { font-size: 0.8rem; color: #555; font-family: 'IBM Plex Mono', monospace; margin-top: -0.3rem; margin-bottom: 1.5rem; }
    div[data-testid="metric-container"] { background: #111; border: 1px solid #222; border-radius: 10px; padding: 1rem 1.25rem; }
    div[data-testid="metric-container"] label { font-size: 0.7rem !important; font-family: 'IBM Plex Mono', monospace !important; color: #555 !important; text-transform: uppercase; letter-spacing: 1px; }
    div[data-testid="metric-container"] [data-testid="stMetricValue"] { font-size: 2rem !important; font-family: 'IBM Plex Mono', monospace !important; color: #f0f0f0 !important; }
    .status-ok { color: #3effa0; font-family: 'IBM Plex Mono', monospace; font-size: 0.8rem; }
    .status-warn { color: #ffcc00; font-family: 'IBM Plex Mono', monospace; font-size: 0.8rem; }
    .status-old { color: #ff5555; font-family: 'IBM Plex Mono', monospace; font-size: 0.8rem; }
    .section-header { font-family: 'IBM Plex Mono', monospace; font-size: 0.7rem; color: #444; letter-spacing: 2px; text-transform: uppercase; padding: 0.5rem 0 0.3rem; border-top: 1px solid #1a1a1a; margin-top: 0.5rem; }
    [data-testid="stTextInput"] input { background: #111 !important; border: 1px solid #222 !important; color: #f0f0f0 !important; border-radius: 6px; font-family: 'IBM Plex Mono', monospace; font-size: 0.85rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

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
    "flipkart": "#f5a623",
    "amazon": "#00a8e8",
    "mobiles": "#7bd88f",
}

EMPTY_COLUMNS = [
    "id", "product_name", "mrp", "sale_price", "discount_pct",
    "price_change_pct", "alert_status", "source", "scrape_date", "url",
]


def empty_df():
    return pd.DataFrame(columns=EMPTY_COLUMNS)


@st.cache_data(ttl=60, show_spinner=False)
def load_data() -> pd.DataFrame:
    try:
        conn = st.connection("gsheets", type=GSheetsConnection)
        df = conn.read(worksheet="prices")
    except Exception as e:
        st.error(f"Could not connect to Google Sheets: {e}")
        return empty_df()

    if df is None or df.empty:
        return empty_df()

    for c in EMPTY_COLUMNS:
        if c not in df.columns:
            df[c] = pd.NA

    df["scrape_date"] = pd.to_datetime(df["scrape_date"], errors="coerce")
    for col in ["mrp", "sale_price", "discount_pct", "price_change_pct"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["alert_status"] = df["alert_status"].astype(str).str.upper().str.strip()
    df["source"] = df["source"].astype(str).str.strip().str.lower()
    df["product_name"] = df["product_name"].astype(str).str.strip()
    return df


def pipeline_status(df: pd.DataFrame) -> tuple[str, str]:
    if df.empty or df["scrape_date"].isna().all():
        return "No data", "status-old"
    latest = df["scrape_date"].max()
    age_h = (datetime.now() - latest).total_seconds() / 3600
    if age_h < 2:
        return f"✔ Live ({latest.strftime('%b %d, %H:%M')})", "status-ok"
    if age_h < 24:
        return f"⚠ Stale ({int(age_h)}h ago)", "status-warn"
    return f"✘ Old ({latest.strftime('%b %d')})", "status-old"


df_raw = load_data()

with st.sidebar:
    st.markdown("### ⚙️ Filters")
    st.markdown("---")

    sources = sorted([s for s in df_raw["source"].dropna().unique().tolist() if s and s != "nan"])
    selected_sources = st.multiselect("Source", sources, default=sources, key="src_filter")

    if df_raw.empty or df_raw["discount_pct"].dropna().empty:
        min_disc, max_disc = 0, 100
    else:
        min_disc = int(df_raw["discount_pct"].min())
        max_disc = int(df_raw["discount_pct"].max())

    discount_range = st.slider(
        "Discount % range",
        min_value=min_disc,
        max_value=max_disc,
        value=(min_disc, max_disc),
        key="disc_filter",
    )
    show_hot_only = st.checkbox("HOT deals only", value=False)

    st.markdown("---")
    if st.button("🔄 Refresh data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown(
        "<p style='font-size:0.65rem;color:#333;margin-top:2rem;font-family:IBM Plex Mono,monospace;'>Google Sheets · auto-refresh 60 s</p>",
        unsafe_allow_html=True,
    )

df = df_raw.copy()
if not df.empty and selected_sources:
    df = df[df["source"].isin(selected_sources)]
if not df.empty:
    df = df[(df["discount_pct"] >= discount_range[0]) & (df["discount_pct"] <= discount_range[1])]
if show_hot_only and not df.empty:
    df = df[df["alert_status"] == "HOT"]

st.markdown(
    '<p class="dash-title">📊 Price Monitor</p>'
    '<p class="dash-subtitle">Web-Scraping Pipeline · Real-time Price Intelligence</p>',
    unsafe_allow_html=True,
)

if df.empty:
    total_products = 0
    hot_deals_count = 0
    market_avg = None
    avg_price_drop = None
else:
    df["price_drop_pct"] = ((df["mrp"] - df["sale_price"]) / df["mrp"]) * 100
    total_products = df["product_name"].nunique()
    hot_deals_count = int((df["alert_status"] == "HOT").sum())
    market_avg = df["sale_price"].mean()
    avg_price_drop = df["price_drop_pct"].mean()

status_label, status_class = pipeline_status(df_raw)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Products", f"{total_products:,}")
c2.metric("🔥 HOT Deals", f"{hot_deals_count:,}")
c3.metric("Market Avg Price", f"₹{market_avg:,.2f}" if market_avg is not None and pd.notna(market_avg) else "—")
c4.metric("Avg Price Drop", f"{avg_price_drop:.1f}%" if avg_price_drop is not None and pd.notna(avg_price_drop) else "—")
with c4:
    st.markdown(f'<span class="{status_class}">{status_label}</span>', unsafe_allow_html=True)

st.markdown("<div style='margin-top:1.5rem;'></div>", unsafe_allow_html=True)
st.markdown('<p class="section-header">Visuals</p>', unsafe_allow_html=True)

if not df.empty:
    avg_by_source = (
        df.groupby("source", as_index=False)["sale_price"]
        .mean()
        .rename(columns={"sale_price": "avg_sale_price"})
        .sort_values("avg_sale_price", ascending=False)
    )
    fig_bar = px.bar(
        avg_by_source,
        x="source",
        y="avg_sale_price",
        color="source",
        color_discrete_map={k: SOURCE_COLORS.get(k, "#888888") for k in avg_by_source["source"]},
        labels={"source": "Source", "avg_sale_price": "Avg Sale Price (₹)"},
        title="Average Sale Price by Source",
        text_auto=".0f",
    )
    fig_bar.update_traces(textposition="outside", marker_line_width=0)
    fig_bar.update_layout(**PLOTLY_LAYOUT, showlegend=False, title_font=dict(size=13, color="#aaaaaa"), bargap=0.35, height=340)
    fig_bar.update_xaxes(title=None)
    fig_bar.update_yaxes(title="₹ Avg Sale Price", tickprefix="₹")
    st.plotly_chart(fig_bar, use_container_width=True)

    if len(df) > 2:
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
        fig_hist.update_layout(**PLOTLY_LAYOUT, barmode="overlay", title_font=dict(size=13, color="#aaaaaa"), height=280, legend_title_text="Source")
        fig_hist.update_xaxes(title="Discount %", ticksuffix="%")
        fig_hist.update_yaxes(title="# Products")
        st.plotly_chart(fig_hist, use_container_width=True)
else:
    st.info("No data available for the selected filters.")

st.markdown('<p class="section-header">🔥 HOT Deals</p>', unsafe_allow_html=True)

hot_df = df_raw.copy()
if selected_sources:
    hot_df = hot_df[hot_df["source"].isin(selected_sources)]
hot_df = hot_df[hot_df["alert_status"] == "HOT"].copy()

search_query = st.text_input("🔍 Search products", placeholder="e.g. iPhone, Samsung, Nike…", key="search_input")
if search_query:
    hot_df = hot_df[hot_df["product_name"].str.lower().str.contains(search_query.lower(), na=False)]

hot_df_sorted = hot_df.sort_values("discount_pct", ascending=False)

display_cols = {
    "product_name": "Product",
    "source": "Source",
    "mrp": "MRP (₹)",
    "sale_price": "Sale Price (₹)",
    "discount_pct": "Discount %",
    "price_change_pct": "Δ Price %",
    "url": "URL",
    "scrape_date": "Scraped At",
}

display_df = hot_df_sorted[list(display_cols.keys())].rename(columns=display_cols)

for col in ["MRP (₹)", "Sale Price (₹)"]:
    display_df[col] = display_df[col].apply(lambda x: f"₹{x:,.2f}" if pd.notna(x) else "—")
for col in ["Discount %", "Δ Price %"]:
    display_df[col] = display_df[col].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "—")
display_df["Scraped At"] = pd.to_datetime(display_df["Scraped At"], errors="coerce").dt.strftime("%b %d, %H:%M")

st.markdown(f"Showing **{len(display_df)}** HOT deal(s).")

if display_df.empty:
    st.warning("No HOT deals match the current filters.")
else:
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={"URL": st.column_config.LinkColumn("URL", display_text="🔗 View")},
    )

csv_bytes = hot_df_sorted.to_csv(index=False).encode("utf-8")
st.download_button(
    label="⬇️ Download HOT Deals CSV",
    data=csv_bytes,
    file_name=f"hot_deals_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
    mime="text/csv",
)

st.markdown("---")
st.markdown(
    "<p style='font-size:0.65rem;color:#2a2a2a;font-family:IBM Plex Mono,monospace;text-align:center;'>Price Monitor Dashboard · Powered by Streamlit + Plotly</p>",
    unsafe_allow_html=True,
)