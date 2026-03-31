# 电商数据采集分析

`ecommerce-data-collector-analysis` · 由 `Yuxuan Fan` 独立维护

Amazon 数据采集、分析与可视化平台。通过 **RapidAPI** 拉取商品、评论、报价、榜单等数据，原始响应归档至 **AWS S3**，结构化数据写入 **PostgreSQL** 四层数仓（Medallion 架构），最终通过 **Streamlit** 仪表盘提供品牌监控、市场份额估算与趋势告警。

> **数据库后端**：新功能以 PostgreSQL 为主（`DB_DIALECT=postgresql`）。旧版 MySQL 链路（`src/etl.py`）仍可切换使用。

---

## 目录

- [项目结构](#项目结构)
- [端到端数据链路](#端到端数据链路)
- [Airflow 工作流](#airflow-工作流)
- [采集机制](#采集机制)
- [数仓结构](#数仓结构)
- [ETL 各层算法详解](#etl-各层算法详解)
- [运维脚本](#运维脚本)
- [快速开始](#快速开始)
- [服务端部署](#服务端部署)

---

## 项目结构

```
├── dags/
│   ├── amazon_postgres_pipeline_dag.py      # 主采集 DAG（每 6 小时）
│   └── amazon_postgres_enrichment_dag.py    # 全量 Enrichment DAG（每周日）
├── src/
│   ├── tasks.py              # Airflow 可调用的任务入口
│   ├── postgres_pipeline.py  # PostgreSQL ETL 核心逻辑
│   ├── sales_estimator.py    # 贝叶斯日销量估算
│   ├── task_config.py        # 细分市场采样与 query 扰动
│   ├── brand_utils.py        # 品牌名规范化
│   ├── rapidapi_client.py    # RapidAPI HTTP 封装
│   └── etl.py                # 旧版 MySQL ETL（向后兼容）
├── sql/postgres/
│   ├── 001_init_schemas.sql  # Schema 初始化
│   ├── 010_raw.sql           # raw 层表 DDL
│   ├── 020_staging.sql       # staging 层
│   ├── 030_core.sql          # core 层
│   ├── 040_mart.sql          # mart 层
│   ├── 050_ops.sql           # ops 运维表
│   └── 060_app_views.sql     # gurysk_app 视图 + 增量列
├── dashboard/app.py          # Streamlit 仪表盘
├── scripts/                  # 初始化与运维脚本
├── main.py                   # 本地/定时采集入口
└── .env.example              # 环境变量模板
```

---

## 端到端数据链路

```
 RapidAPI
    │
    ├──► S3（原始 JSONB 归档，可选）
    │
    └──► raw.api_ingest_event
              │
              ▼
         staging.stg_*          ← 字段解析、类型转换、幂等写入
              │
              ▼
          core.dim_* / fact_*   ← 维度缓慢变化 + 按日快照事实
              │
              ▼
          mart.mart_*            ← 指标聚合、销量估算、市场份额
              │
              ▼
        gurysk_app.v_*          ← 只读视图（仪表盘消费）
              │
              ▼
          Streamlit Dashboard
```

### 两条管线入口的差异


| 入口                                               | 是否包含 Enrichment | 适用场景          |
| ------------------------------------------------ | --------------- | ------------- |
| **主采集 DAG**（`task_build_postgres_staging`）       | ❌ 仅做字段抽取        | 每 6 小时高频采集    |
| **Enrichment DAG**（`task_run_postgres_pipeline`） | ✅ 含品牌/类目/BSR 回填 | 每周深度修复 + 手动触发 |


> Enrichment 步骤（品牌注册表重建、跨 ASIN 类目补全、BSR 回填）较重，不适合放进高频 DAG。每周专跑一次，也可随时在 Airflow UI 手动触发。

---

## Airflow 工作流

### DAG 1：`amazon_postgres_data_platform`（主采集）

**调度**：每 6 小时（默认 `0 */6 * * `*，即 00:00 / 06:00 / 12:00 / 18:00 UTC）

```
collect_search_results
    ├──► collect_product_details  ─┐
    ├──► collect_product_offers   ─┼──► build_postgres_staging
    └──► collect_product_reviews  ─┘         │
                                         build_postgres_core
                                              │
                                         build_postgres_marts
```

搜索任务先跑，通过 **XCom** 将 ASIN 列表传给详情/报价/评论三个采集任务（并行），随后串行跑三层 ETL。


| Task                      | 调用函数                                   | 说明                                                   |
| ------------------------- | -------------------------------------- | ---------------------------------------------------- |
| `collect_search_results`  | `task_fetch_and_store_search`          | 按关键词/站点/页搜索，结果写库并推送 XCom                             |
| `collect_product_details` | `task_fetch_and_store_product_details` | 取前 N 个 ASIN 补采详情（`AIRFLOW_AMAZON_DETAIL_ASIN_LIMIT`） |
| `collect_product_offers`  | `task_fetch_and_store_offers`          | 同上，拉报价（可用 `AIRFLOW_AMAZON_ENABLE_OFFERS=false` 关闭）   |
| `collect_product_reviews` | `task_fetch_and_store_reviews`         | 同上，拉评论（默认关闭）                                         |
| `build_postgres_staging`  | `task_build_postgres_staging`          | 商品 + 评论 + 报价三张 staging 表抽取                           |
| `build_postgres_core`     | `task_build_postgres_core`             | 维度表 + 库存/价格/评论事实表                                    |
| `build_postgres_marts`    | `task_build_postgres_marts`            | 日指标 + 日销量估计 + 品牌/细分份额 + 趋势告警                         |


---

### DAG 2：`amazon_postgres_enrichment_pipeline`（全量 Enrichment）

**调度**：每周日 03:00 UTC（`AIRFLOW_ENRICHMENT_CRON`，默认 `0 3 * * 0`）

- 调度时间刻意与主 DAG 的 00:00 / 06:00 错开，避免同时竞争数据库连接。
- 设置 `AIRFLOW_ENRICHMENT_CRON=None` 可关闭自动调度，仅保留手动触发。
- 手动触发：Airflow UI 点击 ▶ / `airflow dags trigger amazon_postgres_enrichment_pipeline`

```
run_full_pipeline
```

单个任务，调用 `task_run_postgres_pipeline`，完整执行如下步骤（含 Enrichment）：

1. `build_staging_product_snapshot` — 抽取 raw → staging
2. `**backfill_staging_product_enrichment**` — 品牌/类目/BSR 回填 ← **Enrichment 核心**
3. `build_staging_review_event` / `build_staging_offer_snapshot`
4. `build_core_dimensions` → `build_core_inventory_snapshot` → `build_core_price_snapshot` → `build_core_review_fact`
5. `build_mart_product_daily_metrics` → `build_mart_daily_sales_estimate` → `build_mart_brand_market_share` → `build_mart_segment_market_share`
6. `build_ops_trend_alerts`

---

### 常用 Airflow 环境变量

```bash
# 主采集 DAG
AIRFLOW_DAG_AMAZON_SEARCH_CRON=0 */6 * * *
AIRFLOW_AMAZON_SEARCH_QUERY=coffee maker
AIRFLOW_AMAZON_COUNTRY=US
AIRFLOW_AMAZON_SEARCH_PAGE=1
AIRFLOW_AMAZON_DETAIL_ASIN_LIMIT=10
AIRFLOW_AMAZON_OFFER_ASIN_LIMIT=10
AIRFLOW_AMAZON_ENABLE_OFFERS=true
AIRFLOW_AMAZON_ENABLE_REVIEWS=false

# Enrichment DAG
AIRFLOW_ENRICHMENT_CRON=0 3 * * 0   # 每周日 03:00 UTC；设 None 则仅手动触发
```

完整列表见 `.env.example`。

---

## 采集机制

### 配置来源

采集任务由 **数据库配置表**（PostgreSQL 模式）或**环境变量**（回退）驱动：

- **数据库**：`gurysk_app.app_scraper_config`，按 `schedule_profile`（`daily` / `weekly` / `both`）与 `is_active` 筛选；`config_value` 为 JSON，可携带采样参数、目标品牌、别名等。
- **回退**：`AMAZON_QUERIES`、`AMAZON_ASINS`、`AMAZON_COUNTRIES`、`AMAZON_PAGES` 等环境变量。

### 任务类型


| `config_type`     | 作用                             |
| ----------------- | ------------------------------ |
| `product_query`   | 关键词搜索                          |
| `target_asin`     | 追踪指定 ASIN 的详情变化                |
| `category_scan`   | 按 Amazon 官方类目 ID 浏览            |
| `segment_scan`    | 用自定义关键词定义细分市场，支持多 query 采样     |
| `brand_search`    | 搜索 + 品牌过滤（比 product_query 更精准） |
| `bestseller_scan` | 采集榜单数据                         |
| `review_scan`     | 采集评论                           |
| `offer_scan`      | 采集报价与卖家信息                      |


### 细分市场采样（`src/task_config.py`）

`segment_scan` 任务支持对一个细分市场生成多个搜索变体，提高样本覆盖率并减轻单一 query 的偏差。

**流程：**

1. **生成 query 变体**（`build_segment_query_variants`）：种子 query + 手动 `query_variants` + 自动扩展（同义词/相关词，由 `_SEGMENT_TERM_EXPANSIONS` 与 `related_terms` 驱动，可用 `enable_query_perturbation=false` 关闭）。
2. **构建采样计划**（`build_segment_sampling_plan`）：变体 × `repeat_k` 轮 × `sampling_pages` 页 = 请求列表；`sampling_budget` 可截断总数量。
3. **写入元数据**：每次请求都记录 `segment_name`、`search_query`、`query_source`（`seed`/`manual`/`auto`）、`sample_round`、`result_rank` 等，供下游 ETL 做频次去偏。

### 目标品牌与详情补采

- 品牌名规范化：`build_brand_candidates` + `canonicalize_brand_name`（处理大小写、空格、特殊字符），主品牌 + 别名列表一并建索引。
- **自动补采详情**（`enrich_missing_product_details`）：每次主流程结束后，从 `raw` 中筛选缺 categoryid / BSR / 可信品牌信号的 ASIN，按优先级（目标品牌 > 缺关键字段 > 出现频次）排序，批量拉 `product_details`，冷却期 `DETAIL_ENRICHMENT_COOLDOWN_DAYS` 避免重复消耗配额。

---

## 数仓结构

共 **6 个 Schema**，按数据成熟度分层：

### `raw` — 原始事件层


| 表                  | 说明                                                                                                    |
| ------------------ | ----------------------------------------------------------------------------------------------------- |
| `api_ingest_event` | 每次 API 响应一行；请求参数（JSONB）+ 响应体（JSONB）+ 可选 S3 指针；按 `ingest_date` 分区；幂等键 `(ingest_date, idempotency_key)` |


### `staging` — 清洗层


| 表                      | 说明                                                    |
| ---------------------- | ----------------------------------------------------- |
| `stg_product_snapshot` | 商品快照：从响应 JSONB 解析出结构化字段，含 segment/search/sampling 元数据 |
| `stg_review_event`     | 评论事件                                                  |
| `stg_offer_snapshot`   | 报价快照                                                  |


### `core` — 维度与事实层


| 表                                              | 类型      | 说明                               |
| ---------------------------------------------- | ------- | -------------------------------- |
| `dim_platform / dim_account / dim_marketplace` | 维度      | 平台/账号/站点                         |
| `dim_product`                                  | 维度（SCD） | 商品最新状态（品牌、类目）                    |
| `dim_brand_registry`                           | 维度      | 权威品牌注册表，来源于 `product_details` 端点 |
| `bridge_product_identifier`                    | 桥       | ASIN 与其他标识符的映射                   |
| `fact_inventory_snapshot`                      | 事实（分区）  | 每日库存快照                           |
| `fact_price_snapshot`                          | 事实（分区）  | 每日价格与折扣快照                        |
| `fact_review`                                  | 事实（分区）  | 评论事实                             |


### `mart` — 指标与分析层


| 表                               | 说明                                 |
| ------------------------------- | ---------------------------------- |
| `mart_product_daily_metrics`    | 每个 ASIN 的每日聚合指标（价格、星级、BSR、评论数等）    |
| `mart_daily_sales_estimate`     | 贝叶斯日销量估算结果（含区间与置信度）                |
| `mart_brand_market_share`       | 类目维度的品牌市场份额                        |
| `mart_segment_market_share`     | 细分市场维度的品牌份额（含 EB 调整与 Bootstrap 区间） |
| `mart_segment_estimation_stats` | 细分市场估算质量指标（覆盖率、稳定性、先验强度）           |


### `ops` — 运维与治理层

`job_run`（作业日志）、`api_request_log`（请求日志）、`rate_limit_event`、`pagination_checkpoint`、`ingest_dead_letter`、`schema_drift_event`、`brand_manual_override`（手工品牌覆盖）、`trend_alert`。

### `gurysk_app` — 应用层

面向仪表盘的只读 **视图**（列别名映射）：


| 视图                                | 对应 mart 表                                                  |
| --------------------------------- | ---------------------------------------------------------- |
| `v_product_daily_metrics`         | `mart_product_daily_metrics` ⋈ `mart_daily_sales_estimate` |
| `v_segment_product_daily_metrics` | 同上，限定有 segmentname                                         |
| `v_daily_sales_estimate`          | `mart_daily_sales_estimate`                                |
| `v_brand_market_share`            | `mart_brand_market_share`                                  |
| `v_segment_market_share`          | `mart_segment_market_share`                                |
| `v_segment_estimation_stats`      | `mart_segment_estimation_stats`                            |
| `v_trend_alert`                   | `ops.trend_alert`                                          |


配置表：`gurysk_app.app_scraper_config`。

---

## ETL 各层算法详解

### Raw 层

无业务逻辑，原样存储 API 响应，幂等键保证不重复入库。

---

### Staging 层

**目标**：把 JSONB 响应解析成可查询的关系型字段。

#### 商品快照（`build_staging_product_snapshot`）

- 价格、星级、评分数：`REGEXP_REPLACE` 去除非数字字符后转 numeric / integer。
- **BSR**：从多个可能的 JSON 路径（`product_information.Best Sellers Rank`、`bsr_rank`、`bsr` 等）取第一个非空值，正则抽取 `#N` 中的整数。
- **类目**：优先读 `category_path[-1]`（详情端点），回退到 `category.id`，再回退到请求参数里的 `category_id`。
- Segment 采样元数据（`segment_name`、`search_query`、`sample_round` 等）直接从 `request_params_json` 取。
- `ON CONFLICT (raw_event_id)` 时用 `COALESCE` 保留已有的非空值（BSR、类目等），避免用空值覆盖历史数据。

#### Enrichment 回填（`backfill_staging_product_enrichment`）

> 此步骤仅在 **Enrichment 管线**中执行，主采集 DAG 跳过。

分三个子步骤：

**① 品牌值回填（`_backfill_staging_brand_values`）**

同一个 ASIN 可能从多个端点采集，品牌字段质量参差不齐。回填时按以下优先级解析品牌：

```
手工覆盖表（ops.brand_manual_override）
    ↓ 无匹配
权威品牌（extract_authoritative_brand_from_payload）
    — 来自 product_details 端点中 product_information.Brand 或 product_byline
    ↓ 无匹配
标题单品牌库（dim_brand_registry 中标题→唯一品牌的映射）
    ↓ 无匹配
启发式品牌（从响应体其他字段推断）
```

**品牌注册表**（`build_brand_registry`）：扫描所有 `product_details` 响应，以标题规范化键 + 品牌 + 来源类型聚合，写入 `core.dim_brand_registry`，作为「标题单品牌库」的数据源。

**② 类目回填**：用 `DISTINCT ON (marketplace, asin) ORDER BY fetched_at DESC` 取该 ASIN 最新的非空 `category_id` / `category_name`，填补其他行的缺失值。

**③ BSR 回填**：同理，取最新非空 `bsr_rank` 向前填补。

---

### Core 层

**目标**：建立规范的维度与事实表，为 mart 层提供干净的联接主键。

#### 维度表（`build_core_dimensions`）

- `dim_product`：用 `DISTINCT ON (platform, account, asin)` 配合 `ORDER BY`（非空品牌优先 → 非空类目优先 → 时间降序），保证每个 ASIN 只保留一行「最佳」记录，同时记录首次/最近出现时间。
- 其余 `dim_`* 表幂等插入（`ON CONFLICT DO NOTHING`），字典表缓慢变化。

#### 事实表（`build_core_inventory_snapshot` / `build_core_price_snapshot` / `build_core_review_fact`）

直接从 staging 行集按业务键幂等写入，不做聚合，保留每次快照粒度。

---

### Mart 层

#### 1. 日指标聚合（`build_mart_product_daily_metrics`）

以 `fact_inventory_snapshot` 为主表，左连价格快照、评论事实与 `dim_product`，按 `(observed_date, platform, marketplace, asin)` 聚合：

- 均价取 `MAX`（当日最新报价）；折扣取 `MAX`
- 星级取 `AVG`；评论数取 `COUNT DISTINCT`
- BSR 取 `MAX`（通常当日只有一条）
- `BOOL_OR(is_best_seller)`、`BOOL_OR(is_prime)`

---

#### 2. 日销量估算（`build_mart_daily_sales_estimate`）

Amazon 不直接提供日销量，这里结合三个信号用贝叶斯方法推断。

**Step 1 — 构造输入信号**


| 信号                  | 来源               | 处理                                     |
| ------------------- | ---------------- | -------------------------------------- |
| `sales_volume_num`  | 商品页「过去一个月 X 件」文案 | `parse_sales_volume` 正则解析为整数           |
| `num_ratings_delta` | 累计评分数的日变化量       | 按 ASIN 时间序列 `diff`（Pandas）             |
| `bsr_rank`          | Best Seller Rank | 直接使用                                   |
| `prior_daily_sales` | 历史估算结果           | `merge_asof`（向后匹配，不含当日）从历史估算表取前一日估值作先验 |


**Step 2 — 分层校准 BSR 与评论率参数**

为不同「类目 × 站点」桶校准 `CategoryParams`（含 `bsr_gamma`、`bsr_delta`、`bsr_sigma`、`review_rate_alpha`、`review_rate_beta`），分层逻辑：

```
全局参数
  └── 按 marketplace 细化
        └── 按 (marketplace, category_id/name 桶) 再细化
              └── 样本不足时回退到上层
```

- **评论率 Beta 参数**（`calibrate_review_rate_params`）：以月销量 ÷ 30 为日销量锚，计算各行评分增量 / 日销量锚的比值，矩估计拟合 Beta 分布（α = 均值 × 集中度，β = (1 - 均值) × 集中度）。
- **BSR–销量线性参数**（`calibrate_bsr_params`）：对数变换后用 `LinearRegression` 拟合：`ln(BSR) ≈ -γ · ln(日销量) + δ`，γ、δ、σ（残差标准差）即为校准结果。

**Step 3 — MAP 估算（`BayesianDailySalesEstimator`）**

对每个 ASIN 在对数空间 `log_s = ln(日销量)` 上求后验极大值（MAP）。

**后验 = 先验 × 三个似然项 × 软约束**：


| 项      | 分布 / 形式                     | 含义                               |
| ------ | --------------------------- | -------------------------------- |
| 先验     | `Normal(μ_prior, σ_prior)`  | 由历史估值或月销量文案决定均值，无历史则 σ 较大（不确定性高） |
| 评论似然   | `Poisson(λ = 日销量 × 平均评论率)`  | 当日评分增量由泊松过程产生                    |
| BSR 似然 | `Normal(期望 ln(BSR), σ_bsr)` | BSR 与日销量满足对数线性关系                 |
| 月销量软约束 | 超出 ±20% 范围时施加二次惩罚           | 防止估值与文案差距过大                      |


用 `scipy.optimize.minimize_scalar`（bounded）在 `[ln(0.1), ln(100000)]` 区间求 MAP。再对目标函数数值二阶求导近似 Hessian，得 **Laplace 近似**的 95% 置信区间（`[MAP - 1.96σ_post, MAP + 1.96σ_post]`）和置信分数 `1 / (1 + σ_post)`。

---

#### 3. 类目品牌市场份额（`build_mart_brand_market_share`）

**思路**：在同一类目内，用各品牌的估算日销量占比代表市场份额。

1. 以 `mart_product_daily_metrics` 左连 `mart_daily_sales_estimate`，取有 `category_id` 的记录。
2. 按 `(日期, 站点, 类目, 品牌)` 聚合：ASIN 数、估算日销量合计、评分数合计、均价、均星级。
3. 计算三种份额：
  - **销量份额** = 品牌估算日销量 / 类目总估算日销量
  - **评分份额** = 品牌评分总数 / 类目评分总数
  - **ASIN 份额** = 品牌 ASIN 数 / 类目 ASIN 总数

---

#### 4. 细分市场品牌份额（`build_mart_segment_market_share`）

细分市场（segment）的采样存在偏差：热门商品被多个 query 命中，出现次数多。因此需要在聚合前做**去偏**处理，同时要处理**样本不完整**（采到的 ASIN 只是总市场的一部分）。

**① 逆频次采样权重**

```python
sampling_weight = clip(1.0 / appearance_count, min=0.2, max=1.0)
weighted_daily_sales = estimated_daily_sales × sampling_weight
```

出现次数越多的商品权重越低，避免热门商品主导估算。

**② 覆盖率与市场规模估计**

```
estimated_market_asin_count = max(近 30 天内该细分市场历史最大 unique ASIN 数, 当日样本数)
coverage_ratio = 当日样本 ASIN 数 / estimated_market_asin_count
```

**③ 样本稳定性（Jaccard 相似度）**

```
stability_score = (coverage_ratio + Jaccard(当日 ASIN 集, 上日 ASIN 集)) / 2
```

Jaccard = 交集大小 / 并集大小；当日与上日商品集合越相似，稳定性越高。

**④ Bootstrap 市场总销量**

通过有放回重采样估算「如果覆盖整个市场，总销量会是多少」：

```
重复 300 次：
    从当日加权日销量有放回抽样（同样本量）
    × 市场规模放大比 = estimated_market_asin_count / sample_asin_count
    
结果取：均值（bootstrap_mean_sales）、P2.5、P97.5
```

**⑤ 经验贝叶斯品牌份额（EB）**

纯样本份额在覆盖率低时波动大，用历史数据作先验平滑：

```
prior_share   = 过去 30 天该品牌的加权销量占比（无历史则用当日样本）
prior_strength = max(10, 样本总销量 × (1 - coverage_ratio + 0.1))
                      ↑ 覆盖率越低，先验越强

eb_share = (observed_sales + prior_share × prior_strength)
           / (total_observed_sales + prior_strength)

品牌估算日销量 = eb_share × bootstrap_mean_sales
```

---

#### 5. 趋势告警（`build_ops_trend_alerts`）

**方法**：滚动基线 + z-score 异常检测。

对每个「品牌 × 站点」组合，在 `mart_daily_sales_estimate` 中计算三个指标（日总销量、均价、均星级）的：

```
rolling_mean = 过去 30 天移动平均（最少需要 7 个数据点）
rolling_std  = 过去 30 天移动标准差

z_score = (今日值 - rolling_mean) / rolling_std
```

`|z_score| ≥ 阈值`（默认 2.0）时，写入 `ops.trend_alert`，标记告警方向（上升 / 下降）与级别。

---

### 应用层（`gurysk_app`）

**仅视图**，不含额外计算。将 mart / ops 的列名别名化（如 `marketplace_code` → `marketplace`），供 Streamlit 仪表盘查询。

---

## 运维脚本


| 脚本                                  | 说明                                                                  |
| ----------------------------------- | ------------------------------------------------------------------- |
| `scripts/create_config_table.py`    | 初始化 `gurysk_app.app_scraper_config` 配置表                             |
| `scripts/backfill_brand_quality.py` | 按配置范围同步 `product_details`（可选），写入手工品牌覆盖后重算 staging enrichment + mart |


---

## 快速开始

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt

cp .env.example .env             # 填写 RAPIDAPI_KEY 和数据库配置
python scripts/create_config_table.py

python main.py                   # 本地采集
streamlit run dashboard/app.py   # 启动仪表盘
```

手动跑完整 Enrichment 管线（包含 staging 回填）：

```bash
python -c "from src.tasks import task_run_postgres_pipeline; print(task_run_postgres_pipeline())"
```

---

## 服务端部署

1. 克隆仓库，安装依赖，配置与 Airflow worker 共用的 `.env`。
2. Worker / Scheduler 需能访问 RapidAPI、S3（可选）、Secrets Manager（可选）、bastion 与 RDS PostgreSQL。
3. 将 `dags/` 目录置于 Airflow `DAGS_FOLDER` 或挂载仓库目录使其可被扫描。
4. 确保 `PYTHONPATH` 包含仓库根目录（`import src` 可用）。
5. 两个 DAG（采集 + Enrichment）会自动注册，在 UI 中分别启用。

**S3 原始路径约定**：

```
raw/business_domain=thirdparty-market-intelligence/
    source_system=amazon/
    dataset_name=amazon_product_catalog_search_airflow/
    endpoint_name=product_search/
    ...
```

---

## 技术栈


| 类型   | 组件                                 |
| ---- | ---------------------------------- |
| 语言   | Python                             |
| 数据库  | PostgreSQL（主）/ MySQL（旧版可选）         |
| 数据处理 | Pandas、NumPy、SciPy、scikit-learn    |
| 可视化  | Streamlit、Plotly                   |
| 调度   | Apache Airflow                     |
| 数据源  | RapidAPI Real-Time Amazon Data     |
| 存储   | AWS S3（可选）、AWS Secrets Manager（可选） |


---

## 致谢

本项目最初基于开源 Amazon scraper fork 演化，现已独立维护。数据采集依赖 RapidAPI 以及相关 Python 生态。

## 许可

MIT License，见 `LICENSE`。

## 维护者

`Yuxuan Fan`