# 电商数据采集分析

`ecommerce-data-collector-analysis`

由 `Yuxuan Fan` 独立维护的电商数据采集、入库、分析与可视化项目。当前数据源以 Amazon 为主，通过 RapidAPI 获取商品、类目、评论、报价等数据，并结合 MySQL、ETL 与 Streamlit 仪表盘，支持品牌监控、市场份额估算和趋势分析。

## 项目定位

这个仓库已经不再作为上游项目的同步 fork 使用，而是一个独立演化维护的私有项目。当前实现重点包括：

- 配置驱动的数据采集任务
- 商品搜索、详情、类目、榜单、评论、报价采集
- 原始数据落库与指标表构建
- 品牌销量估算、价格折扣、评论趋势与市场份额分析
- 面向业务分析的 Streamlit 仪表盘

## 主要能力

- `product_query`：按关键词搜索商品
- `target_asin`：跟踪指定 ASIN 的详情变化
- `category_scan`：按官方类目 ID 扫描商品
- `segment_scan`：按自定义关键词定义细分市场样本池
- `review_scan`：抓取重点商品评论
- `offer_scan`：抓取报价与卖家信息
- `bestseller_scan`：抓取榜单数据
- ETL：构建日销量估算、市场份额、趋势预警等分析结果
- Dashboard：按品牌、类目、细分市场、站点、日期查看分析结果

## 目录概览

- `src/`：采集客户端、数据库接入、ETL、查询逻辑
- `dashboard/`：Streamlit 分析与配置页面
- `scripts/`：初始化或辅助脚本
- `main.py`：采集任务入口
- `.env.example`：环境变量模板

## 环境准备

```bash
python -m venv .venv
. .venv/Scripts/activate
pip install -r requirements.txt
```

复制环境变量模板并按实际环境填写：

```bash
copy .env.example .env
```

关键配置包括：

- `RAPIDAPI_KEY`
- `DB_HOST`
- `DB_PORT`
- `DB_USER`
- `DB_PASSWORD`
- `DB_NAME`

如果本地需要通过 SSH 隧道访问数据库，还需要配置：

- `SSH_HOST`
- `SSH_PORT`
- `SSH_USERNAME`
- `SSH_PKEY`

## 初始化

创建配置表：

```bash
python scripts/create_config_table.py
```

运行基础采集入口：

```bash
python main.py
```

启动分析仪表盘：

```bash
streamlit run dashboard/app.py
```

## 数据与分析流程

1. 通过配置表定义采集任务、站点、页数和调度频率
2. 通过 RapidAPI 拉取商品与市场原始数据
3. 将原始结果写入数据库
4. 通过 ETL 构建产品指标、销量估算、市场份额与趋势预警
5. 在仪表盘中按品牌、类目或细分市场查看结果

## 当前技术栈

- Python
- Requests
- MySQL / PyMySQL
- Pandas
- Streamlit
- Plotly
- SciPy / scikit-learn / NumPy

## 使用说明

- 当前仓库面向内部分析流程，默认假设你已经具备可用的 RapidAPI Key 和数据库环境
- 项目中的“电商”目前主要指 Amazon 数据采集与分析场景，后续可根据需要扩展到其他平台
- 若需控制 API 配额，建议优先减少扫描页数、缩小站点范围，并合理拆分日常任务与周任务

## 致谢

- 本项目最初基于一个开源 Amazon scraper 项目 fork 演化而来，后续已发生较大改动并转为独立维护
- 感谢原始开源工作提供的早期参考与启发
- 部分数据采集与处理能力依赖 `Botasaurus`、`RapidAPI` 以及相关 Python 生态

## 许可

本项目继续采用 `MIT License`。版权声明见 `LICENSE`。

## 维护者

`Yuxuan Fan`
