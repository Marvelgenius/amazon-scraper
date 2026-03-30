"""
E-commerce market analysis dashboard.

Current data source is primarily Amazon. Supports both legacy OutIn-specific
views and generic brand/category analysis.
Run with: streamlit run dashboard/app.py
"""

import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from src.database import create_db_connection
from src.db_compat import ping_connection
from src import queries as _q

CACHE_TTL = 300  # 5 minutes


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def _cached_generic_filter_options(_conn, brand=None, category_name=None):
    return _q.get_generic_filter_options(_conn, brand=brand, category_name=category_name)


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def _cached_segment_filter_options(_conn, brand=None, segment_name=None):
    return _q.get_segment_filter_options(_conn, brand=brand, segment_name=segment_name)


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def _cached_daily_sales_estimates(_conn, brand=None, category_name=None, marketplace=None, start_date=None, end_date=None, asins=None):
    return _q.get_daily_sales_estimates(_conn, brand=brand, category_name=category_name, marketplace=marketplace, start_date=start_date, end_date=end_date, asins=asins)


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def _cached_segment_daily_sales_estimates(_conn, brand=None, segment_name=None, marketplace=None, start_date=None, end_date=None, asins=None):
    return _q.get_segment_daily_sales_estimates(_conn, brand=brand, segment_name=segment_name, marketplace=marketplace, start_date=start_date, end_date=end_date, asins=asins)


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def _cached_product_daily_metrics(_conn, brand=None, category_name=None, marketplace=None, start_date=None, end_date=None, asins=None):
    return _q.get_product_daily_metrics(_conn, brand=brand, category_name=category_name, marketplace=marketplace, start_date=start_date, end_date=end_date, asins=asins)


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def _cached_segment_product_daily_metrics(_conn, brand=None, segment_name=None, marketplace=None, start_date=None, end_date=None, asins=None):
    return _q.get_segment_product_daily_metrics(_conn, brand=brand, segment_name=segment_name, marketplace=marketplace, start_date=start_date, end_date=end_date, asins=asins)


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def _cached_brand_market_share(_conn, category_name=None, marketplace=None, target_date=None):
    return _q.get_brand_market_share(_conn, category_name=category_name, marketplace=marketplace, target_date=target_date)


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def _cached_brand_market_share_trend(_conn, brand="", category_name=None, marketplace=None, start_date=None, end_date=None):
    return _q.get_brand_market_share_trend(_conn, brand=brand, category_name=category_name, marketplace=marketplace, start_date=start_date, end_date=end_date)


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def _cached_brand_category_share_trend(_conn, brand="", marketplace=None, start_date=None, end_date=None):
    return _q.get_brand_category_share_trend(_conn, brand=brand, marketplace=marketplace, start_date=start_date, end_date=end_date)


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def _cached_segment_market_share(_conn, segment_name=None, marketplace=None, target_date=None):
    return _q.get_segment_market_share(_conn, segment_name=segment_name, marketplace=marketplace, target_date=target_date)


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def _cached_segment_market_share_trend(_conn, brand="", segment_name=None, marketplace=None, start_date=None, end_date=None):
    return _q.get_segment_market_share_trend(_conn, brand=brand, segment_name=segment_name, marketplace=marketplace, start_date=start_date, end_date=end_date)


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def _cached_segment_estimation_stats(_conn, segment_name=None, marketplace=None, target_date=None):
    return _q.get_segment_estimation_stats(_conn, segment_name=segment_name, marketplace=marketplace, target_date=target_date)


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def _cached_trend_alerts(_conn, brand=None, marketplace=None, start_date=None):
    return _q.get_trend_alerts(_conn, brand=brand, marketplace=marketplace, start_date=start_date)

st.set_page_config(
    page_title="电商品牌市场分析",
    page_icon="📊",
    layout="wide",
)


def _inject_styles() -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background: linear-gradient(180deg, #f8fafc 0%, #eef2ff 100%);
        }
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, rgba(255,255,255,0.95), rgba(241,245,249,0.98));
            border-right: 1px solid rgba(148,163,184,0.18);
        }
        .app-hero {
            padding: 1.2rem 1.25rem;
            border-radius: 20px;
            background: rgba(255,255,255,0.82);
            border: 1px solid rgba(148,163,184,0.18);
            box-shadow: 0 18px 40px rgba(15,23,42,0.06);
            margin-bottom: 1rem;
        }
        .app-hero h2 {
            margin: 0 0 0.35rem 0;
            color: #0f172a;
            font-size: 1.45rem;
        }
        .app-hero p {
            margin: 0;
            color: #475569;
            line-height: 1.55;
        }
        .app-filter-bar {
            padding: 0.7rem 0.95rem;
            border-radius: 16px;
            background: rgba(255,255,255,0.74);
            border: 1px solid rgba(148,163,184,0.16);
            margin: 0.55rem 0 1rem 0;
            color: #334155;
        }
        div[data-testid="stMetric"] {
            background: rgba(255,255,255,0.76);
            border: 1px solid rgba(148,163,184,0.18);
            padding: 0.85rem 1rem;
            border-radius: 18px;
            box-shadow: 0 10px 30px rgba(15,23,42,0.04);
        }
        div[data-testid="stForm"] {
            background: rgba(255,255,255,0.72);
            border: 1px solid rgba(148,163,184,0.16);
            border-radius: 18px;
            padding: 0.75rem 0.85rem;
        }
        .stButton>button {
            border-radius: 999px;
            border: 1px solid rgba(79,70,229,0.18);
            background: linear-gradient(180deg, #4338ca, #4f46e5);
            color: white;
            font-weight: 600;
        }
        .stTabs [data-baseweb="tab"] {
            font-weight: 600;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


@st.cache_resource
def _get_connection():
    return create_db_connection()


def _get_healthy_connection():
    conn = _get_connection()
    try:
        ping_connection(conn)
        return conn
    except Exception:
        _get_connection.clear()
        return _get_connection()


def main() -> None:
    conn = _get_healthy_connection()
    _inject_styles()
    _render_generic_page(conn)


# =========================================================================
# Generic brand analysis page
# =========================================================================

def _render_generic_page(conn) -> None:
    st.markdown(
        """
        <div class="app-hero">
            <h2>品牌市场分析</h2>
            <p>统一分析任意品牌在官方类目或自定义细分市场中的日销量估算、价格折扣、消费者评价、市场份额与趋势预警。</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    try:
        category_base_options = _cached_generic_filter_options(conn)
        segment_base_options = _cached_segment_filter_options(conn)
    except Exception as e:
        _get_connection.clear()
        st.error(f"数据库连接或查询失败: {e}")
        st.info("请确认数据库已连接，且 ETL 已运行过至少一次。点击右上角 Rerun 重试。")
        return

    brand_choices = []
    combined_brands = (
        ["OutIn"]
        + category_base_options.get("brands", [])
        + segment_base_options.get("brands", [])
    )
    for brand_name in combined_brands:
        if brand_name and brand_name not in brand_choices:
            brand_choices.append(brand_name)

    with st.sidebar:
        st.header("分析参数")
        scope_type = st.radio(
            "市场范围类型",
            options=["官方类目", "自定义细分市场"],
            key="scope_type",
        )
        preset_brand = st.selectbox(
            "品牌",
            options=[""] + brand_choices,
            format_func=lambda x: "未指定品牌" if not x else x,
            key="brand_quick_pick",
        )
        brand = preset_brand or None

        if scope_type == "官方类目":
            brand_scoped_options = _cached_generic_filter_options(conn, brand=brand)
            scope_choices = brand_scoped_options.get("categories", [])
            scope_pick = st.selectbox(
                "类目名称",
                options=[""] + scope_choices,
                format_func=lambda x: "未指定类目" if not x else x,
                key="scope_pick_category",
            )
            category_name = scope_pick or None
            segment_name = None
            scoped_options = _cached_generic_filter_options(conn, brand=brand, category_name=category_name)
        else:
            brand_scoped_options = _cached_segment_filter_options(conn, brand=brand)
            scope_choices = brand_scoped_options.get("segments", [])
            scope_pick = st.selectbox(
                "细分市场名称",
                options=[""] + scope_choices,
                format_func=lambda x: "未指定细分市场" if not x else x,
                key="scope_pick_segment",
            )
            segment_name = scope_pick or None
            category_name = None
            scoped_options = _cached_segment_filter_options(conn, brand=brand, segment_name=segment_name)
        mp_map = scoped_options.get("marketplace_asin_map", {})
        marketplaces = sorted(mp_map.keys())
        marketplace_pick = st.selectbox(
            "市场",
            options=["全部"] + marketplaces if marketplaces else ["全部"],
            key="gen_mp",
        )
        marketplace = None if marketplace_pick == "全部" else marketplace_pick

        st.divider()
        st.caption("日期范围")
        dates = scoped_options.get("dates", [])
        if len(dates) > 1:
            date_objects = [datetime.strptime(d, "%Y-%m-%d").date() for d in dates]
            start_date_val, end_date_val = st.date_input(
                "开始/结束日期",
                value=(min(date_objects), max(date_objects)),
                min_value=min(date_objects),
                max_value=max(date_objects),
                key="gen_date_range",
            )
            start_str = start_date_val.strftime("%Y-%m-%d")
            end_str = end_date_val.strftime("%Y-%m-%d")
        elif len(dates) == 1:
            st.info(f"当前仅有 **{dates[0]}** 一天的数据")
            start_str = end_str = dates[0]
        else:
            start_str = end_str = None

        available_asins = []
        if marketplace:
            available_asins = mp_map.get(marketplace, [])
        else:
            for items in mp_map.values():
                available_asins.extend(items)

        deduped_asins = []
        seen_asins = set()
        for item in available_asins:
            asin = item.get("asin")
            if asin and asin not in seen_asins:
                seen_asins.add(asin)
                deduped_asins.append(item)

        asin_labels = {
            item["asin"]: (
                f"{item['asin']} - "
                f"{(item.get('brand') or '')} "
                f"{(item.get('title') or '')[:36]}"
            ).strip()
            for item in deduped_asins
        }
        selected_asins = st.multiselect(
            "产品 (ASIN)",
            options=list(asin_labels.keys()),
            format_func=lambda x: asin_labels.get(x, x),
            key="gen_asins",
        ) or None

    active_scope_name = category_name if scope_type == "官方类目" else segment_name

    if not brand and not active_scope_name:
        st.info("请先在左侧选择品牌或市场范围名称开始分析。")
        st.markdown("""
        **使用说明：**
        1. 直接选择 `OutIn`，即可把它作为通用品牌进行分析
        2. 可选择 **官方类目** 或 **自定义细分市场**
        3. 再按市场、日期、ASIN 缩小分析范围
        4. 页面将展示日销量估算、价格折扣率、评价趋势、市场份额与预警
        """)
        return

    active_filters = []
    if brand:
        active_filters.append(f"品牌：`{brand}`")
    if category_name:
        active_filters.append(f"类目：`{category_name}`")
    if segment_name:
        active_filters.append(f"细分市场：`{segment_name}`")
    if marketplace:
        active_filters.append(f"市场：`{marketplace}`")
    if selected_asins:
        active_filters.append(f"ASIN 数：`{len(selected_asins)}`")
    if start_str and end_str:
        active_filters.append(f"日期：`{start_str}` 至 `{end_str}`")
    if active_filters:
        st.markdown(
            f'<div class="app-filter-bar">{" | ".join(active_filters)}</div>',
            unsafe_allow_html=True,
        )

    if scope_type == "自定义细分市场":
        st.info("当前为**自定义细分市场**模式：市场范围由关键词返回结果构成，为样本估计口径，份额与总量会展示不确定性区间与稳定性指标。")

    tab1, tab2, tab3, tab4 = st.tabs([
        "日销量估算", "价格与折扣趋势", "市场份额", "趋势预警",
    ])

    with tab1:
        if scope_type == "官方类目":
            _render_sales_estimate_tab(
                conn, brand, category_name, marketplace, start_str, end_str, selected_asins
            )
        else:
            _render_segment_sales_estimate_tab(
                conn, brand, segment_name, marketplace, start_str, end_str, selected_asins
            )

    with tab2:
        if scope_type == "官方类目":
            _render_price_discount_tab(
                conn, brand, category_name, marketplace, start_str, end_str, selected_asins
            )
        else:
            _render_segment_price_discount_tab(
                conn, brand, segment_name, marketplace, start_str, end_str, selected_asins
            )

    with tab3:
        if scope_type == "官方类目":
            _render_brand_share_tab(conn, brand, category_name, marketplace, start_str, end_str)
        else:
            _render_segment_share_tab(conn, brand, segment_name, marketplace, start_str, end_str)

    with tab4:
        _render_alerts_tab(conn, brand, marketplace, start_str)


def _render_sales_estimate_tab(conn, brand, category_name, marketplace, start_str, end_str, asins):
    est_df = _cached_daily_sales_estimates(
        conn, brand=brand, category_name=category_name,
        marketplace=marketplace, start_date=start_str, end_date=end_str, asins=tuple(asins) if asins else None,
    )

    if est_df.empty:
        st.info("暂无日销量估算数据。请先运行 ETL 管道：build_product_daily_metrics → build_daily_sales_estimates")
        return

    agg = (
        est_df.groupby("estimate_date")
        .agg(
            total_est=("estimated_daily_sales", "sum"),
            total_lower=("estimate_lower_bound", "sum"),
            total_upper=("estimate_upper_bound", "sum"),
            avg_conf=("confidence_score", "mean"),
        )
        .reset_index()
        .sort_values("estimate_date")
    )

    latest = agg.iloc[-1]
    col1, col2, col3 = st.columns(3)
    col1.metric("最新日销量估算", f"{int(latest['total_est']):,}")
    col2.metric(
        "95% 置信区间",
        f"{int(latest['total_lower']):,} ~ {int(latest['total_upper']):,}",
    )
    col3.metric("平均置信度", f"{float(latest['avg_conf']):.0%}")

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=agg["estimate_date"], y=agg["total_upper"],
        mode="lines", line=dict(width=0), showlegend=False,
    ))
    fig.add_trace(go.Scatter(
        x=agg["estimate_date"], y=agg["total_lower"],
        mode="lines", line=dict(width=0), fill="tonexty",
        fillcolor="rgba(55, 128, 191, 0.15)", name="95% 置信区间",
    ))
    fig.add_trace(go.Scatter(
        x=agg["estimate_date"], y=agg["total_est"],
        mode="lines+markers", name="日销量估算",
        line=dict(color="rgb(55, 128, 191)", width=2),
    ))
    fig.update_layout(
        title=f"{'品牌 ' + brand + ' ' if brand else ''}日销量估算趋势",
        xaxis_title="日期", yaxis_title="估算日销量",
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True)

    if est_df["asin"].nunique() > 1:
        top_asins = (
            est_df.groupby(["asin", "brand"], dropna=False)["estimated_daily_sales"]
            .sum()
            .sort_values(ascending=False)
            .head(8)
            .reset_index()
        )
        top_asin_list = top_asins["asin"].tolist()
        detail_df = est_df[est_df["asin"].isin(top_asin_list)].copy()

        fig_detail = px.line(
            detail_df.sort_values("estimate_date"),
            x="estimate_date",
            y="estimated_daily_sales",
            color="asin",
            markers=True,
            title="重点 ASIN 日销量估算趋势",
            hover_data=["brand", "sales_volume_num", "num_ratings_delta"],
        )
        fig_detail.update_layout(xaxis_title="日期", yaxis_title="估算日销量", hovermode="x unified")
        st.plotly_chart(fig_detail, use_container_width=True)

    with st.expander("按 ASIN 查看明细"):
        st.dataframe(
            est_df[["estimate_date", "asin", "brand", "estimated_daily_sales",
                     "estimate_lower_bound", "estimate_upper_bound",
                     "confidence_score", "sales_volume_num", "num_ratings_delta"]]
            .sort_values(["estimate_date", "asin"]),
            use_container_width=True,
        )


def _render_segment_sales_estimate_tab(conn, brand, segment_name, marketplace, start_str, end_str, asins):
    est_df = _cached_segment_daily_sales_estimates(
        conn, brand=brand, segment_name=segment_name,
        marketplace=marketplace, start_date=start_str, end_date=end_str, asins=tuple(asins) if asins else None,
    )

    if est_df.empty:
        st.info("暂无细分市场日销量估算数据。请先运行 segment ETL 管道。")
        return

    est_df["estimated_daily_sales"] = pd.to_numeric(est_df["estimated_daily_sales"], errors="coerce").fillna(0)
    est_df["estimate_lower_bound"] = pd.to_numeric(est_df["estimate_lower_bound"], errors="coerce").fillna(0)
    est_df["estimate_upper_bound"] = pd.to_numeric(est_df["estimate_upper_bound"], errors="coerce").fillna(0)

    agg = (
        est_df.groupby("estimate_date")
        .agg(
            total_est=("estimated_daily_sales", "sum"),
            total_lower=("estimate_lower_bound", "sum"),
            total_upper=("estimate_upper_bound", "sum"),
            avg_conf=("confidence_score", "mean"),
        )
        .reset_index()
        .sort_values("estimate_date")
    )

    latest = agg.iloc[-1]
    col1, col2, col3 = st.columns(3)
    col1.metric("最新样本日销量估算", f"{int(latest['total_est']):,}")
    col2.metric("样本置信区间", f"{int(latest['total_lower']):,} ~ {int(latest['total_upper']):,}")
    col3.metric("平均置信度", f"{float(latest['avg_conf']):.0%}" if pd.notna(latest["avg_conf"]) else "N/A")

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=agg["estimate_date"], y=agg["total_upper"], mode="lines", line=dict(width=0), showlegend=False))
    fig.add_trace(go.Scatter(
        x=agg["estimate_date"], y=agg["total_lower"], mode="lines", line=dict(width=0),
        fill="tonexty", fillcolor="rgba(99, 110, 250, 0.15)", name="样本区间"
    ))
    fig.add_trace(go.Scatter(
        x=agg["estimate_date"], y=agg["total_est"], mode="lines+markers",
        name="样本日销量估算", line=dict(width=2),
    ))
    fig.update_layout(
        title=f"{segment_name or '细分市场'} 日销量估算趋势",
        xaxis_title="日期", yaxis_title="样本估算日销量", hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("按 ASIN 查看样本明细"):
        st.dataframe(
            est_df[["estimate_date", "segment_name", "asin", "brand", "estimated_daily_sales",
                    "estimate_lower_bound", "estimate_upper_bound", "confidence_score"]]
            .sort_values(["estimate_date", "asin"]),
            use_container_width=True,
        )


def _render_price_discount_tab(conn, brand, category_name, marketplace, start_str, end_str, asins):
    metrics_df = _cached_product_daily_metrics(
        conn, brand=brand, category_name=category_name,
        marketplace=marketplace, start_date=start_str, end_date=end_str, asins=tuple(asins) if asins else None,
    )

    if metrics_df.empty:
        st.info("暂无产品指标数据。请先运行 build_product_daily_metrics ETL 任务。")
        return

    fig_price = go.Figure()
    for asin in metrics_df["asin"].unique()[:10]:
        asin_df = metrics_df[metrics_df["asin"] == asin].sort_values("observed_date")
        label = (asin_df["product_title"].iloc[0] or asin)[:40]
        fig_price.add_trace(go.Scatter(
            x=asin_df["observed_date"], y=asin_df["price"],
            mode="lines+markers", name=f"{asin} - {label}",
        ))
    fig_price.update_layout(
        title="价格趋势", xaxis_title="日期", yaxis_title="价格",
        hovermode="x unified", height=400,
    )
    st.plotly_chart(fig_price, use_container_width=True)

    discount_data = metrics_df.dropna(subset=["discount_pct"])
    if not discount_data.empty:
        fig_disc = go.Figure()
        for asin in discount_data["asin"].unique()[:10]:
            asin_df = discount_data[discount_data["asin"] == asin].sort_values("observed_date")
            label = (asin_df["product_title"].iloc[0] or asin)[:40]
            fig_disc.add_trace(go.Scatter(
                x=asin_df["observed_date"], y=asin_df["discount_pct"],
                mode="lines+markers", name=f"{asin} - {label}",
            ))
        fig_disc.update_layout(
            title="折扣率趋势", xaxis_title="日期", yaxis_title="折扣率 (%)",
            hovermode="x unified", height=400,
        )
        st.plotly_chart(fig_disc, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        fig_rating = go.Figure()
        for asin in metrics_df["asin"].unique()[:10]:
            asin_df = metrics_df[metrics_df["asin"] == asin].sort_values("observed_date")
            fig_rating.add_trace(go.Scatter(
                x=asin_df["observed_date"], y=asin_df["star_rating"],
                mode="lines+markers", name=asin,
            ))
        fig_rating.update_layout(title="评分趋势", yaxis_range=[0, 5.5], height=350)
        st.plotly_chart(fig_rating, use_container_width=True)

    with col2:
        fig_ratings_count = go.Figure()
        for asin in metrics_df["asin"].unique()[:10]:
            asin_df = metrics_df[metrics_df["asin"] == asin].sort_values("observed_date")
            fig_ratings_count.add_trace(go.Scatter(
                x=asin_df["observed_date"], y=asin_df["num_ratings"],
                mode="lines+markers", name=asin,
            ))
        fig_ratings_count.update_layout(title="累计评价数趋势", height=350)
        st.plotly_chart(fig_ratings_count, use_container_width=True)


def _render_segment_price_discount_tab(conn, brand, segment_name, marketplace, start_str, end_str, asins):
    metrics_df = _cached_segment_product_daily_metrics(
        conn, brand=brand, segment_name=segment_name,
        marketplace=marketplace, start_date=start_str, end_date=end_str, asins=tuple(asins) if asins else None,
    )

    if metrics_df.empty:
        st.info("暂无细分市场产品指标数据。请先运行 build_segment_product_daily_metrics ETL 任务。")
        return

    fig_price = go.Figure()
    for asin in metrics_df["asin"].unique()[:10]:
        asin_df = metrics_df[metrics_df["asin"] == asin].sort_values("observed_date")
        label = (asin_df["product_title"].iloc[0] or asin)[:40]
        fig_price.add_trace(go.Scatter(
            x=asin_df["observed_date"], y=asin_df["price"],
            mode="lines+markers", name=f"{asin} - {label}",
        ))
    fig_price.update_layout(title="价格趋势", xaxis_title="日期", yaxis_title="价格", hovermode="x unified", height=400)
    st.plotly_chart(fig_price, use_container_width=True)

    discount_data = metrics_df.dropna(subset=["discount_pct"])
    if not discount_data.empty:
        fig_disc = go.Figure()
        for asin in discount_data["asin"].unique()[:10]:
            asin_df = discount_data[discount_data["asin"] == asin].sort_values("observed_date")
            label = (asin_df["product_title"].iloc[0] or asin)[:40]
            fig_disc.add_trace(go.Scatter(
                x=asin_df["observed_date"], y=asin_df["discount_pct"],
                mode="lines+markers", name=f"{asin} - {label}",
            ))
        fig_disc.update_layout(title="折扣率趋势", xaxis_title="日期", yaxis_title="折扣率 (%)", hovermode="x unified", height=400)
        st.plotly_chart(fig_disc, use_container_width=True)


def _render_brand_share_tab(conn, brand, category_name, marketplace, start_str, end_str):
    target_date = end_str
    if not category_name and not brand:
        st.info("请选择类目名称，或先指定品牌以查看跨类目市场份额。")
        return

    share_df = pd.DataFrame()
    if category_name:
        share_df = _cached_brand_market_share(
            conn, category_name=category_name, marketplace=marketplace,
            target_date=target_date,
        )

    if category_name and share_df.empty:
        st.info("暂无市场份额数据。请先运行 build_brand_market_share ETL 任务。")
        return

    col1, col2 = st.columns(2)

    with col1:
        if category_name:
            top_n = 10
            if len(share_df) > top_n:
                top = share_df.nlargest(top_n - 1, "sales_share_pct")
                others_pct = share_df[~share_df["brand"].isin(top["brand"])]["sales_share_pct"].sum()
                others = pd.DataFrame([{"brand": "其他", "sales_share_pct": others_pct}])
                plot_df = pd.concat([top, others], ignore_index=True)
            else:
                plot_df = share_df.copy()

            fig = px.pie(
                plot_df, values="sales_share_pct", names="brand",
                title=f"市场份额 - 类目 {category_name}",
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            if brand:
                mask = plot_df["brand"].str.lower().eq(brand.lower())
                if mask.any():
                    fig.update_traces(pull=[0.08 if m else 0 for m in mask])
            fig.update_traces(textposition="inside", textinfo="label+percent")
            st.plotly_chart(fig, use_container_width=True)
        else:
            category_trend_df = _cached_brand_category_share_trend(
                conn, brand=brand or "", marketplace=marketplace, start_date=start_str, end_date=end_str
            )
            if category_trend_df.empty:
                st.info("暂无该品牌的跨类目历史份额数据。")
            else:
                latest_date = category_trend_df["observed_date"].max()
                latest_df = category_trend_df[category_trend_df["observed_date"] == latest_date].copy()
                latest_df = latest_df.sort_values("sales_share_pct", ascending=False).head(10)
                fig = px.bar(
                    latest_df,
                    x="sales_share_pct",
                    y="category_name",
                    orientation="h",
                    title=f"{brand} 最新跨类目份额",
                    labels={"sales_share_pct": "份额 (%)", "category_name": "类目"},
                )
                st.plotly_chart(fig, use_container_width=True)

    with col2:
        if brand:
            trend_df = _cached_brand_market_share_trend(
                conn,
                brand=brand,
                category_name=category_name,
                marketplace=marketplace,
                start_date=start_str,
                end_date=end_str,
            )
            if not trend_df.empty:
                fig_trend = go.Figure()
                fig_trend.add_trace(go.Scatter(
                    x=trend_df["observed_date"], y=trend_df["sales_share_pct"],
                    mode="lines+markers+text", name="销量份额",
                    text=[f"{v:.1f}%" for v in trend_df["sales_share_pct"]],
                    textposition="top center",
                    line=dict(color="#ff6b6b", width=3),
                ))
                fig_trend.update_layout(
                    title=f"{brand} 市场份额变化趋势",
                    xaxis_title="日期", yaxis_title="份额 (%)",
                )
                st.plotly_chart(fig_trend, use_container_width=True)
            else:
                st.info(f"当前筛选条件下暂无 {brand} 的份额趋势数据")
        else:
            st.info("输入品牌名以查看份额趋势")

    if brand:
        category_trend_df = _cached_brand_category_share_trend(
            conn, brand=brand, marketplace=marketplace, start_date=start_str, end_date=end_str
        )
        if not category_trend_df.empty:
            pivot_df = (
                category_trend_df.pivot_table(
                    index="observed_date",
                    columns="category_name",
                    values="sales_share_pct",
                    aggfunc="mean",
                )
                .fillna(0)
                .sort_index()
            )
            if not pivot_df.empty:
                fig_cat = go.Figure()
                for col in pivot_df.columns[:8]:
                    fig_cat.add_trace(go.Scatter(
                        x=pivot_df.index,
                        y=pivot_df[col],
                        mode="lines+markers",
                        name=str(col),
                    ))
                fig_cat.update_layout(
                    title=f"{brand} 跨类目份额历史",
                    xaxis_title="日期",
                    yaxis_title="份额 (%)",
                    hovermode="x unified",
                )
                st.plotly_chart(fig_cat, use_container_width=True)

    if category_name and not share_df.empty:
        with st.expander("查看完整品牌数据"):
            display_cols = [c for c in [
                "brand", "distinct_asins", "total_estimated_daily_sales",
                "total_num_ratings", "avg_price", "avg_star_rating",
                "sales_share_pct", "rating_share_pct",
            ] if c in share_df.columns]
            st.dataframe(
                share_df[display_cols].sort_values("sales_share_pct", ascending=False).reset_index(drop=True),
                use_container_width=True,
            )


def _render_segment_share_tab(conn, brand, segment_name, marketplace, start_str, end_str):
    target_date = end_str
    if not segment_name:
        st.info("请选择细分市场名称以查看市场份额数据。")
        return

    share_df = _cached_segment_market_share(
        conn, segment_name=segment_name, marketplace=marketplace, target_date=target_date,
    )
    stats_df = _cached_segment_estimation_stats(
        conn, segment_name=segment_name, marketplace=marketplace, target_date=target_date,
    )

    if share_df.empty:
        st.info("暂无细分市场份额数据。请先运行 build_segment_market_share ETL 任务。")
        return

    if not stats_df.empty:
        latest_stats = stats_df.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("样本 ASIN 数", int(latest_stats["sample_asin_count"]))
        c2.metric("估计市场 ASIN 数", int(latest_stats["estimated_market_asin_count"]))
        c3.metric(
            "Bootstrap 区间",
            f"{int(latest_stats['bootstrap_lower_bound']):,} ~ {int(latest_stats['bootstrap_upper_bound']):,}",
        )
        c4.metric("稳定性", f"{float(latest_stats['stability_score']):.0%}")

    col1, col2 = st.columns(2)

    with col1:
        plot_df = share_df.copy()
        if len(plot_df) > 10:
            top = plot_df.nlargest(9, "sales_share_pct")
            others_pct = plot_df[~plot_df["brand"].isin(top["brand"])]["sales_share_pct"].sum()
            plot_df = pd.concat([top, pd.DataFrame([{"brand": "其他", "sales_share_pct": others_pct}])], ignore_index=True)
        fig = px.pie(
            plot_df, values="sales_share_pct", names="brand",
            title=f"细分市场份额 - {segment_name}",
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        if brand:
            mask = plot_df["brand"].str.lower().eq(brand.lower())
            if mask.any():
                fig.update_traces(pull=[0.08 if m else 0 for m in mask])
        fig.update_traces(textposition="inside", textinfo="label+percent")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        if brand:
            trend_df = _cached_segment_market_share_trend(
                conn,
                brand=brand,
                segment_name=segment_name,
                marketplace=marketplace,
                start_date=start_str,
                end_date=end_str,
            )
            if not trend_df.empty:
                fig_trend = go.Figure()
                fig_trend.add_trace(go.Scatter(
                    x=trend_df["observed_date"], y=trend_df["sales_share_pct"],
                    mode="lines+markers+text", name="销量份额",
                    text=[f"{v:.1f}%" for v in trend_df["sales_share_pct"]],
                    textposition="top center",
                    line=dict(color="#ff6b6b", width=3),
                ))
                fig_trend.update_layout(
                    title=f"{brand} 在 {segment_name} 中的份额趋势",
                    xaxis_title="日期", yaxis_title="份额 (%)",
                )
                st.plotly_chart(fig_trend, use_container_width=True)
            else:
                st.info(f"暂无 {brand} 在该细分市场中的份额趋势数据")
        else:
            st.info("输入品牌名以查看细分市场份额趋势")

    with st.expander("查看细分市场估计详情"):
        if not stats_df.empty:
            st.dataframe(stats_df, use_container_width=True)
        st.dataframe(
            share_df.sort_values("sales_share_pct", ascending=False).reset_index(drop=True),
            use_container_width=True,
        )


def _render_alerts_tab(conn, brand, marketplace, start_str):
    alerts_df = _cached_trend_alerts(conn, brand=brand, marketplace=marketplace, start_date=start_str)

    if alerts_df.empty:
        st.success("当前无趋势预警。")
        return

    for _, alert in alerts_df.iterrows():
        level = alert.get("alert_level", "info")
        icon = {"critical": "🔴", "warning": "🟡", "info": "🔵"}.get(level, "⚪")
        msg = alert.get("alert_message", "")
        z = alert.get("z_score", 0)
        change = alert.get("change_pct", 0)

        st.markdown(
            f"{icon} **[{alert['alert_date']}]** {msg} "
            f"(变化 {change:+.1f}%, Z-Score={z:.1f})"
        )

    with st.expander("查看预警明细"):
        st.dataframe(alerts_df, use_container_width=True)


if __name__ == "__main__":
    main()
