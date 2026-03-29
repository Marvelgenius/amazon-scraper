## Advanced Notes

### Project Usage

这个项目当前更适合作为一套内部分析流程，而不是开箱即用的公共 SaaS API。运行前通常需要提前准备：

1. 可用的 `RapidAPI` Key
2. MySQL 数据库或 RDS 连接
3. 必要时的 SSH 隧道配置
4. 已初始化的配置表与调度参数

### Suggested Workflow

1. 先在 `.env` 中配置好 API 和数据库连接
2. 执行 `python scripts/create_config_table.py`
3. 根据业务需要修改配置表中的采集任务
4. 运行 `python main.py` 或调度任务进行采集
5. 运行 `streamlit run dashboard/app.py` 查看分析结果

### Cost Control

由于当前数据采集依赖第三方接口，建议优先从以下几方面控制调用成本：

- 限制搜索页数
- 减少同时扫描的国家站点
- 将高频任务与低频任务拆分
- 对稳定样本优先使用增量采集

### Acknowledgements

- 本项目由早期 fork 版本持续演化而来，现已转为独立维护
- 感谢原始开源工作提供初始参考
- 感谢 `Botasaurus`、`RapidAPI` 与相关 Python 数据生态提供基础能力
