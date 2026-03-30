"""
采集配置管理页面 — 更直观的任务控制台。
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pandas as pd
import streamlit as st

from src.config import get_database_backend
from src.db_compat import get_dict_cursor, is_integrity_error, normalize_rows, ping_connection

CONFIG_DB = None if get_database_backend() == "postgresql" else "gurysk_app"
CONFIG_TABLE = "gurysk_app.app_scraper_config" if get_database_backend() == "postgresql" else "app_scraper_config"

TYPE_META = {
    "product_query": {
        "label": "单品/品牌关键词",
        "icon": "🔎",
        "desc": "按关键词搜索商品，适合发现品牌或单品相关结果。",
        "key_label": "搜索关键词",
        "key_placeholder": "例: outin nano",
    },
    "category_query": {
        "label": "品类关键词",
        "icon": "🧭",
        "desc": "按品类关键词搜索，适合宽口径探索市场。",
        "key_label": "品类关键词",
        "key_placeholder": "例: coffee machines",
    },
    "target_asin": {
        "label": "跟踪 ASIN",
        "icon": "🎯",
        "desc": "持续跟踪核心商品详情，适合重点监控。",
        "key_label": "ASIN",
        "key_placeholder": "例: B0BRKFWPF3",
    },
    "brand_search": {
        "label": "品牌搜索",
        "icon": "🏷️",
        "desc": "按品牌+查询词抓取品牌市场表现。",
        "key_label": "品牌名",
        "key_placeholder": "例: OUTIN",
    },
    "category_scan": {
        "label": "类目扫描",
        "icon": "🗂️",
        "desc": "按平台官方类目 ID 扫描，当前主要用于 Amazon 类目份额分析。",
        "key_label": "类目 ID",
        "key_placeholder": "例: 289745",
    },
    "segment_scan": {
        "label": "自定义细分市场",
        "icon": "📦",
        "desc": "按关键词定义业务市场池，适合便携咖啡机这类自定义市场。",
        "key_label": "细分市场关键词",
        "key_placeholder": "例: portable coffee machine",
    },
    "bestseller_scan": {
        "label": "榜单扫描",
        "icon": "📈",
        "desc": "抓取榜单商品，用于识别头部品牌和品类热度。",
        "key_label": "榜单类目",
        "key_placeholder": "例: kitchen/coffee-machines",
    },
    "review_scan": {
        "label": "评论采集",
        "icon": "💬",
        "desc": "抓取重点 ASIN 的评论，用于评价趋势分析。",
        "key_label": "ASIN",
        "key_placeholder": "例: B0BRKFWPF3",
    },
    "offer_scan": {
        "label": "报价采集",
        "icon": "💲",
        "desc": "抓取重点 ASIN 的报价，用于折扣和竞价监控。",
        "key_label": "ASIN",
        "key_placeholder": "例: B0BRKFWPF3",
    },
    "setting": {
        "label": "全局设置",
        "icon": "⚙️",
        "desc": "控制采集节奏与调度参数，不直接抓取商品。",
        "key_label": "设置项名称",
        "key_placeholder": "例: request_delay",
    },
}

SCHEDULE_LABELS = {
    "daily": "每日",
    "weekly": "每周",
    "both": "两者",
}

st.set_page_config(
    page_title="采集配置管理",
    page_icon="⚙️",
    layout="wide",
)


@st.cache_resource
def _get_config_conn():
    from src.database import create_db_connection

    conn = create_db_connection(database=CONFIG_DB)
    return conn


def _healthy_conn():
    conn = _get_config_conn()
    try:
        ping_connection(conn)
        return conn
    except Exception:
        _get_config_conn.clear()
        return _get_config_conn()


def _load_configs(conn) -> pd.DataFrame:
    with get_dict_cursor(conn) as cur:
        cur.execute(f"SELECT * FROM {CONFIG_TABLE} ORDER BY config_type, id")
        rows = normalize_rows(cur.fetchall())
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def _insert_config(conn, data: dict):
    cols = ", ".join(data.keys())
    phs = ", ".join(["%s"] * len(data))
    sql = f"INSERT INTO {CONFIG_TABLE} ({cols}) VALUES ({phs})"
    with conn.cursor() as cur:
        cur.execute(sql, list(data.values()))
    conn.commit()


def _update_config(conn, row_id: int, data: dict):
    sets = ", ".join(f"{k}=%s" for k in data)
    sql = f"UPDATE {CONFIG_TABLE} SET {sets} WHERE id=%s"
    with conn.cursor() as cur:
        cur.execute(sql, list(data.values()) + [row_id])
    conn.commit()


def _delete_config(conn, row_id: int):
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {CONFIG_TABLE} WHERE id=%s", (row_id,))
    conn.commit()


def _parse_config_value(raw_value):
    if raw_value in (None, ""):
        return None
    try:
        return json.loads(raw_value)
    except (TypeError, json.JSONDecodeError):
        return raw_value


def _split_csv(raw: str):
    return [item.strip() for item in (raw or "").split(",") if item.strip()]


def _inject_styles() -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background: linear-gradient(180deg, #f8fafc 0%, #eef2ff 100%);
        }
        .cfg-banner {
            padding: 1.1rem 1.2rem;
            border: 1px solid rgba(120,120,120,0.16);
            border-radius: 22px;
            background: linear-gradient(135deg, rgba(255,255,255,0.9), rgba(238,242,255,0.92));
            box-shadow: 0 20px 45px rgba(15,23,42,0.06);
            margin-bottom: 1rem;
        }
        .cfg-banner h3 {
            margin: 0 0 0.35rem 0;
            font-size: 1.2rem;
            color: #0f172a;
        }
        .cfg-banner p {
            margin: 0;
            color: #475569;
            line-height: 1.45;
        }
        .cfg-card {
            border: 1px solid rgba(120,120,120,0.16);
            border-radius: 18px;
            padding: 1rem 1rem;
            margin: 0.45rem 0 0.75rem 0;
            background: rgba(255,255,255,0.82);
            box-shadow: 0 16px 35px rgba(15,23,42,0.05);
        }
        .cfg-title {
            font-weight: 700;
            font-size: 1.02rem;
            margin-bottom: 0.15rem;
            color: #0f172a;
        }
        .cfg-subtle {
            color: #475569;
            font-size: 0.9rem;
        }
        .cfg-chip-row {
            display: flex;
            flex-wrap: wrap;
            gap: 0.4rem;
            margin-top: 0.6rem;
        }
        .cfg-chip {
            display: inline-block;
            padding: 0.28rem 0.65rem;
            border-radius: 999px;
            font-size: 0.79rem;
            border: 1px solid rgba(120,120,120,0.18);
            background: rgba(248,250,252,0.96);
            color: #334155;
        }
        .cfg-chip-ok {
            color: #166534;
            border-color: rgba(125,220,139,0.35);
            background: rgba(220,252,231,0.75);
        }
        .cfg-chip-pause {
            color: #92400e;
            border-color: rgba(246,199,96,0.35);
            background: rgba(254,243,199,0.85);
        }
        .cfg-form-note {
            border: 1px dashed rgba(120,120,120,0.22);
            border-radius: 16px;
            padding: 0.85rem 1rem;
            background: rgba(255,255,255,0.86);
            color: #334155;
            margin: 0.25rem 0 0.75rem 0;
            line-height: 1.5;
        }
        .cfg-form-note strong {
            color: #0f172a;
        }
        .cfg-meta-list {
            margin-top: 0.55rem;
            color: #475569;
            font-size: 0.85rem;
        }
        .stButton > button, .stForm button[kind="primary"] {
            border-radius: 999px;
            border: 1px solid rgba(79,70,229,0.16);
            background: linear-gradient(180deg, #4338ca, #4f46e5);
            color: white;
            font-weight: 600;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _safe_text(value, default="—"):
    if value is None:
        return default
    text = str(value).strip()
    return text or default


def _render_summary(df: pd.DataFrame) -> None:
    total_count = len(df)
    active_count = int(df["is_active"].fillna(0).astype(int).sum()) if not df.empty else 0
    paused_count = total_count - active_count
    daily_count = len(df[df["schedule_profile"] == "daily"]) if not df.empty else 0
    weekly_count = len(df[df["schedule_profile"] == "weekly"]) if not df.empty else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("总配置数", total_count)
    c2.metric("启用中", active_count)
    c3.metric("已停用", paused_count)
    c4.metric("每日任务", daily_count)

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("每周任务", weekly_count)
    c6.metric("类目扫描", len(df[df["config_type"] == "category_scan"]) if not df.empty else 0)
    c7.metric("自定义细分市场", len(df[df["config_type"] == "segment_scan"]) if not df.empty else 0)
    c8.metric("跟踪 ASIN", len(df[df["config_type"] == "target_asin"]) if not df.empty else 0)


def _render_filters(df: pd.DataFrame) -> pd.DataFrame:
    st.markdown("### 浏览与筛选")
    col1, col2, col3 = st.columns([2.2, 1.2, 1.2])
    with col1:
        keyword = st.text_input("搜索配置", placeholder="搜索关键词、ASIN、说明或类目名称", key="cfg_search")
    with col2:
        type_filter = st.selectbox(
            "类型筛选",
            options=["全部"] + list(TYPE_META.keys()),
            format_func=lambda x: "全部类型" if x == "全部" else TYPE_META[x]["label"],
            key="cfg_type_filter",
        )
    with col3:
        status_filter = st.selectbox(
            "状态筛选",
            options=["全部", "启用", "停用"],
            key="cfg_status_filter",
        )

    filtered = df.copy()
    if keyword.strip():
        search_text = keyword.strip().lower()

        def _row_matches(row) -> bool:
            parsed = _parse_config_value(row.get("config_value"))
            values = [
                row.get("config_key"),
                row.get("description"),
                row.get("user_id"),
                parsed.get("category_name") if isinstance(parsed, dict) else None,
                parsed.get("segment_name") if isinstance(parsed, dict) else None,
            ]
            return any(search_text in str(v).lower() for v in values if v is not None)

        filtered = filtered[filtered.apply(_row_matches, axis=1)]

    if type_filter != "全部":
        filtered = filtered[filtered["config_type"] == type_filter]

    if status_filter == "启用":
        filtered = filtered[filtered["is_active"] == 1]
    elif status_filter == "停用":
        filtered = filtered[filtered["is_active"] != 1]

    st.caption(f"当前显示 {len(filtered)} 条配置")
    return filtered


def _build_config_value(
    new_type: str,
    raw_value: str,
    category_name: str,
    segment_name: str,
    target_brand: str,
    brand_aliases: str,
    query_variants: str,
    related_terms: str,
    repeat_k: int,
    top_n: int,
    sampling_budget: int,
):
    config_value = raw_value.strip() or None
    if new_type not in {"category_scan", "segment_scan", "brand_search", "product_query"}:
        return config_value

    payload = {}
    if config_value:
        try:
            parsed = json.loads(config_value)
            if isinstance(parsed, dict):
                payload.update(parsed)
        except json.JSONDecodeError as exc:
            raise ValueError("附加参数必须是合法 JSON。") from exc

    if new_type == "category_scan" and category_name.strip():
        payload["category_name"] = category_name.strip()
    if new_type == "segment_scan" and segment_name.strip():
        payload["segment_name"] = segment_name.strip()
    if target_brand.strip():
        payload["target_brand"] = target_brand.strip()
    alias_values = _split_csv(brand_aliases)
    if alias_values:
        payload["brand_aliases"] = alias_values
    if new_type == "segment_scan":
        payload["enable_query_perturbation"] = True
        payload["repeat_k"] = int(repeat_k)
        if top_n > 0:
            payload["top_n"] = int(top_n)
        if sampling_budget > 0:
            payload["sampling_budget"] = int(sampling_budget)
        query_variant_values = _split_csv(query_variants)
        if query_variant_values:
            payload["query_variants"] = query_variant_values
        related_term_values = _split_csv(related_terms)
        if related_term_values:
            payload["related_terms"] = related_term_values

    return json.dumps(payload, ensure_ascii=False) if payload else None


def main():
    _inject_styles()
    st.title("采集配置管理")
    st.markdown(
        """
        <div class="cfg-banner">
            <h3>任务控制台</h3>
            <p>在这里统一管理电商数据采集任务。当前数据源以 Amazon 为主，你可以新增关键词、官方类目、自定义细分市场、跟踪 ASIN 和全局设置，修改会在下一轮调度时自动生效。</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    conn = _healthy_conn()
    df = _load_configs(conn)

    _render_summary(df)
    st.divider()

    with st.expander("新增配置项", expanded=False):
        _render_add_form(conn)

    st.divider()
    filtered_df = _render_filters(df)
    st.divider()

    for config_type, meta in TYPE_META.items():
        subset = filtered_df[filtered_df["config_type"] == config_type] if not filtered_df.empty else pd.DataFrame()
        with st.expander(f"{meta['icon']} {meta['label']}  ({len(subset)})", expanded=len(subset) > 0 and config_type in {"segment_scan", "category_scan", "brand_search"}):
            st.caption(meta["desc"])
            if subset.empty:
                st.info("当前筛选条件下暂无配置")
            else:
                _render_config_table(conn, subset, config_type)


def _render_add_form(conn):
    new_type = st.selectbox(
        "类型",
        options=list(TYPE_META.keys()),
        format_func=lambda x: f"{TYPE_META[x]['icon']} {TYPE_META[x]['label']}",
        key="cfg_new_type",
    )
    meta = TYPE_META[new_type]

    st.info(meta["desc"])
    st.markdown(
        f"""
        <div class="cfg-form-note">
            <strong>填写说明</strong><br/>
            1. <strong>{meta['key_label']}</strong>：这条任务的主键值，决定采集什么。<br/>
            2. <strong>目标品牌 / 品牌别名</strong>：用于精确识别品牌，不再依赖标题里的普通词。<br/>
            3. <strong>附加参数 JSON</strong>：仅在需要补充过滤条件或扩展参数时填写。<br/>
            4. <strong>segment 扰动采样</strong>：可配置 query 变体、重复轮次、Top N 和预算。<br/>
            5. <strong>调度频率 / 适用市场 / 搜索页数</strong>：决定采集何时运行、在哪些站点运行、抓多少页。
        </div>
        """,
        unsafe_allow_html=True,
    )

    with st.form("add_config_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        with col1:
            new_key = st.text_input(
                meta["key_label"],
                placeholder=meta["key_placeholder"],
                help="主采集键。关键词类填搜索词；ASIN 类填商品 ASIN；类目扫描填平台类目 ID（当前主要为 Amazon）；细分市场填你定义市场池的关键词。",
            )
            new_value = st.text_area(
                "附加参数 JSON（可选）",
                placeholder='例如：{"query":"coffee maker"}',
                height=80,
                help="用于补充额外参数。brand_search 可写 query/brand；category_scan 可扩展 category_name；segment_scan 可扩展 segment_name。不会写 JSON 可以留空。",
            )
            new_desc = st.text_input(
                "说明",
                placeholder="给这条任务起一个便于识别的说明",
                help="建议描述任务目的，例如“OutIn 核心单品跟踪”或“便携咖啡机市场池采样”。",
            )
        with col2:
            new_schedule = st.selectbox(
                "调度频率",
                options=list(SCHEDULE_LABELS.keys()),
                format_func=lambda x: SCHEDULE_LABELS[x],
                help="决定任务会在每日、每周，还是两种调度中执行。",
            )
            new_countries = st.text_input(
                "适用市场",
                value="ALL",
                placeholder="ALL 或 US,GB,DE",
                help="决定在哪些目标站点采集。可填 ALL，也可填逗号分隔的国家码；当前主要适用于 Amazon 站点。",
            )
            new_pages = st.number_input(
                "搜索页数",
                min_value=1,
                max_value=10,
                value=1,
                help="搜索类任务会抓取的页数。页数越大，API 消耗越高。",
            )
            new_user = st.text_input(
                "操作者 ID",
                placeholder="你的用户名",
                help="用于记录谁创建或维护了这条配置。",
            )

        extra1, extra2 = st.columns(2)
        with extra1:
            category_name = st.text_input(
                "类目名称（仅 category_scan）",
                placeholder="例: Coffee Machines",
                disabled=new_type != "category_scan",
                help="仅 category_scan 需要。用于把官方类目 ID 映射成更易读的名称，便于首页筛选与展示。",
            )
        with extra2:
            segment_name = st.text_input(
                "细分市场名称（仅 segment_scan）",
                placeholder="例: Portable Coffee Machines",
                disabled=new_type != "segment_scan",
                help="仅 segment_scan 需要。用于给自定义关键词市场池命名，后续首页会直接显示这个名称。",
            )

        brand_col1, brand_col2 = st.columns(2)
        with brand_col1:
            target_brand = st.text_input(
                "目标品牌（可选）",
                placeholder="例: OutIn",
                help="用于精确锁定要重点补类目和跟踪份额的品牌。品牌识别会优先依赖 API 返回的 brand 字段。",
            )
        with brand_col2:
            brand_aliases = st.text_input(
                "品牌别名（逗号分隔，可选）",
                placeholder="例: OUTIN, Out In",
                help="用于补充品牌别名清单，后续展示和补采都会按精确品牌名 + 别名集合处理。",
            )

        sampling_col1, sampling_col2 = st.columns(2)
        with sampling_col1:
            query_variants = st.text_input(
                "Query 变体（仅 segment_scan，可选）",
                placeholder="例: travel coffee maker, compact espresso machine",
                disabled=new_type != "segment_scan",
                help="为细分市场手动补充更多搜索词变体。系统也会自动扩展相关词。",
            )
            related_terms = st.text_input(
                "相关词补充（仅 segment_scan，可选）",
                placeholder="例: espresso, travel",
                disabled=new_type != "segment_scan",
                help="用于自动扩展 query 时追加相关词，帮助更接近真实搜索空间。",
            )
        with sampling_col2:
            repeat_k = st.number_input(
                "重复轮次 K（仅 segment_scan）",
                min_value=1,
                max_value=10,
                value=2 if new_type == "segment_scan" else 1,
                disabled=new_type != "segment_scan",
                help="同一 query 重复抓取的轮次，用于减弱排序波动带来的样本偏差。",
            )
            top_n = st.number_input(
                "Top N（仅 segment_scan）",
                min_value=0,
                max_value=200,
                value=50 if new_type == "segment_scan" else 0,
                disabled=new_type != "segment_scan",
                help="近似控制每个 query 抓取的结果深度，系统会按页数换算请求数量。",
            )
            sampling_budget = st.number_input(
                "采样预算上限（仅 segment_scan）",
                min_value=0,
                max_value=200,
                value=0,
                disabled=new_type != "segment_scan",
                help="限制单条 segment_scan 的最大请求数。0 表示按变体、轮次和页数自然展开。",
            )

        submitted = st.form_submit_button("添加配置", type="primary", use_container_width=True)
        if submitted:
            if not new_key.strip():
                st.error("关键词 / ASIN / 类目不能为空")
                return

            try:
                config_value = _build_config_value(
                    new_type,
                    new_value,
                    category_name,
                    segment_name,
                    target_brand,
                    brand_aliases,
                    query_variants,
                    related_terms,
                    int(repeat_k),
                    int(top_n),
                    int(sampling_budget),
                )
            except ValueError as exc:
                st.error(str(exc))
                return

            data = {
                "config_type": new_type,
                "config_key": new_key.strip(),
                "config_value": config_value,
                "schedule_profile": new_schedule,
                "countries": new_countries.strip() or "ALL",
                "pages": new_pages,
                "is_active": 1,
                "description": new_desc.strip() or None,
                "user_id": new_user.strip() or None,
            }
            try:
                _insert_config(conn, data)
                st.success(f"已添加：[{TYPE_META[new_type]['label']}] {new_key}")
                st.rerun()
            except Exception as exc:
                if is_integrity_error(exc):
                    st.error("该配置已存在（类型 + 主键 + 调度频率 组合必须唯一）")
                    return
                st.error(f"添加失败：{exc}")
                return


def _status_chip(is_active: bool) -> str:
    if is_active:
        return '<span class="cfg-chip cfg-chip-ok">启用中</span>'
    return '<span class="cfg-chip cfg-chip-pause">已停用</span>'


def _render_config_table(conn, subset: pd.DataFrame, config_type: str):
    for _, row in subset.iterrows():
        row_id = int(row["id"])
        parsed_value = _parse_config_value(row.get("config_value"))
        display_name = row["config_key"]
        display_subtitle = row.get("description") or "未填写说明"

        if config_type == "category_scan" and isinstance(parsed_value, dict):
            display_subtitle = parsed_value.get("category_name") or display_subtitle
        elif config_type == "segment_scan" and isinstance(parsed_value, dict):
            display_subtitle = parsed_value.get("segment_name") or display_subtitle
        elif config_type == "setting":
            display_subtitle = row.get("config_value") or "未设置值"

        st.markdown(
            f"""
            <div class="cfg-card">
                <div class="cfg-title">{TYPE_META[config_type]['icon']} {display_name}</div>
                <div class="cfg-subtle">{_safe_text(display_subtitle)}</div>
                <div class="cfg-chip-row">
                    <span class="cfg-chip">{SCHEDULE_LABELS.get(row['schedule_profile'], '—')}</span>
                    <span class="cfg-chip">市场 {_safe_text(row.get('countries'))}</span>
                    <span class="cfg-chip">页数 {int(row.get('pages') or 0)}</span>
                    {_status_chip(bool(row.get('is_active')))}
                </div>
                <div class="cfg-meta-list">
                    {f"目标品牌：{_safe_text(parsed_value.get('target_brand'))}<br/>" if isinstance(parsed_value, dict) and parsed_value.get('target_brand') else ""}
                    {f"品牌别名：{', '.join(parsed_value.get('brand_aliases', []))}<br/>" if isinstance(parsed_value, dict) and parsed_value.get('brand_aliases') else ""}
                    {f"采样：K={parsed_value.get('repeat_k', 1)} / TopN={parsed_value.get('top_n', '—')} / 预算={parsed_value.get('sampling_budget', '—')}<br/>" if config_type == "segment_scan" and isinstance(parsed_value, dict) else ""}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        action_col1, action_col2, action_col3 = st.columns([1, 1, 6])
        toggle_label = "停用" if bool(row.get("is_active")) else "启用"
        if action_col1.button(toggle_label, key=f"toggle_{row_id}", use_container_width=True):
            _update_config(conn, row_id, {
                "is_active": 0 if bool(row.get("is_active")) else 1,
                "user_id": "dashboard",
            })
            st.rerun()

        if action_col2.button("删除", key=f"del_{row_id}", use_container_width=True):
            _delete_config(conn, row_id)
            st.rerun()

        with action_col3.expander("查看配置明细", expanded=False):
            detail = {
                "id": row.get("id"),
                "config_type": row.get("config_type"),
                "config_key": row.get("config_key"),
                "config_value": parsed_value,
                "schedule_profile": row.get("schedule_profile"),
                "countries": row.get("countries"),
                "pages": row.get("pages"),
                "is_active": row.get("is_active"),
                "description": row.get("description"),
                "user_id": row.get("user_id"),
            }
            st.json(detail, expanded=False)


if __name__ == "__main__":
    main()
