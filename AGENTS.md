# 仓库指南（贡献者）

本文件说明仓库结构、代码约定与开发要点，适用于并发回测系统及随附 `wqb` 客户端库。

## 项目结构

- 根目录
  - `postgresql_database.py` — PostgreSQL 连接池、表结构、CRUD、回测结果处理器
  - `postgresql_alpha_importer.py` — 导入表达式到数据库
  - `concurrent_multi_backtest.py` — 并发回测轮询器（MultiAlpha + 热加载 + 暂停）
  - `wqb/` — WQB SDK（`WQBSession` 等）
  - `expressions/` — Alpha 表达式 JSON 输入目录
  - `requirements.txt` — 运行时依赖
  - `README.md`、`AGENTS.md`、`*.log` — 文档与日志

## 构建与运行

- 依赖安装：`python -m pip install -r requirements.txt`
- 导入表达式：`python postgresql_alpha_importer.py --config config.json`
- 并发回测（轮询）：`python concurrent_multi_backtest.py --config config.json`
- 运行时可通过修改 `config.json` 热加载参数（并发、分组、抓取上限、轮询、暂停）。

## 表达式文件命名与内容规范

- 目录与编码
  - 目录：`expressions/`
  - 编码：UTF-8（建议无 BOM），换行 `LF`

- 文件命名（建议）
  - 模式：`<REGION>_<TOPIC>_<YYYYMMDD>_<SEQ>.json`
  - 约束：`REGION` 取 `USA/EUR/JPN/CHN/...`；`SEQ` 为三位序号（如 `001`）
  - 正则：`^[A-Z]{3}_[A-Z0-9\-]+_[0-9]{8}_[0-9]{3}\.json$`
  - 示例：`USA_MOMENTUM_20241001_001.json`

- 文件内容（JSON 数组）
  - 顶层为数组，每个元素为对象，字段：
    - `expression: str`（必填）
    - `alpha_type: str`（可选，默认 `REGULAR`，可为 `SUPER`）
    - `settings: dict`（可选；可覆盖 `region/universe/delay/decay/neutralization/...`）
  - 建议：`settings.region` 与文件名中的 `REGION` 保持一致

- 规模与分片
  - 单文件建议 200–1000 条，配合 `import_batch_size`（默认 200）
  - 更大数据量请拆分为多文件并顺序编号（`..._001.json`、`..._002.json`）

- 去重与幂等
  - 建议启用 `import_skip_duplicates=true`，以 `expression + settings` 作为重复判断的近似键
  - 不同文件避免重复；可在 `notes/tags` 加入 `batch_id` 以便追踪

- 归档与清理
  - 导入成功后建议移动到 `expressions/archived/` 或 `processed/`，保持待导目录干净
  - 建议保留原始文件以便审计与追溯

## 代码风格

- Python 3.11+，4 空格缩进。
- 文件/模块：`snake_case.py`；类：`PascalCase`；函数/变量：`snake_case`。
- 保持模块职责单一；避免“巨石模块”。
- 日志：统一使用模块内 `logging`；生产日志写入 `.log`，UTF-8 编码。
- 编码：所有源码文件使用 UTF-8。

## 关键实现约定

- MultiAlpha 扁平化与结果处理
  - `concurrent_multi_backtest.py` 将每个组的 `children` 扁平化，与输入一一对应。
  - `BacktestResultProcessor.process_wqb_response` 兼容 HTTP Response 与 `dict`（children 子项）。仅在 HTTP 明确失败或显式错误字段时判为失败；否则提取 `alpha_id` 与指标入库。

- 数据库写回（性能）
  - 回测结果写库为同步提交，但使用 `psycopg2.extras.execute_batch` 批量更新以减少往返。
  - 大批量建议分块提交（如 500–1000 条/批）。

- 状态机
  - `backtest_status`: `pending` → `processing` → `completed|failed`。
  - 取数与标记 `processing` 的原子性见 `PostgreSQLDatabaseManager`；失败可重置回 `pending`。

## 测试与验证

- 单测/集成测试优先使用 `pytest`（如需，手动安装）。
- 覆盖重点：认证流程、并发与队列、重试、数据库适配器。

## 提交规范

- 提交信息：简洁祈使句（≤72 字符）。示例：`Optimize DB writes with execute_batch`。
- 变更涉及行为或配置时，请在正文阐述动机与影响范围。

## 安全与配置

- 生产凭据通过环境变量或安全存储注入；避免提交明文。
- PostgreSQL 建议 UTF-8；确保网络与角色权限符合最小化原则。
