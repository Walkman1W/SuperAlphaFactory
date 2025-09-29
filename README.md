# WorldQuant BRAIN 并发回测系统（PostgreSQL + WQB）

面向 WorldQuant BRAIN 的 Alpha 批量导入与并发回测系统（PostgreSQL + WQB）。当前聚焦两大模块：

- 导入模块：批量将 Alpha 表达式写入 PostgreSQL（lifecycle_stage=backtest, backtest_status=pending）。
- 并发回测模块：从数据库拉取 pending 表达式，按 MultiAlpha 分组并发回测，写回结果与指标。

## 快速开始

- 安装依赖：`python -m pip install -r requirements.txt`
- 准备配置：复制并编辑 `config.json`（数据库与 BRAIN 账号）。

运行导入
- 将 JSON 表达式文件放入 `expressions/` 目录（或自定义目录）。
- 执行：`python postgresql_alpha_importer.py --config config.json`

运行并发回测
- 轮询运行（默认）：`python concurrent_multi_backtest.py --config config.json`
- 单轮运行（脚本化场景）：`python -m concurrent_multi_backtest --config config.json`

暂停/恢复（热加载）
- 运行中修改 `config.json` 的 `pause` 字段，下一轮生效：`false` 继续；`true` 完成本轮后暂停调度。

## 配置说明（摘录）

- 数据库：`db_host`, `db_port`, `db_name`, `db_user`, `db_password`, `max_db_connections`
- BRAIN：`brain_username`, `brain_password`
- 并发回测：
  - `alpha_count_per_slot`（MultiAlpha 分组大小，≥2）
  - `concurrent_count`（并发协程数）
  - `backtest_fetch_limit`（抓取上限，优先于并发×分组）
  - `poll_interval_seconds`（空闲/暂停时的轮询休眠秒数）
  - `use_multi_sim`（是否启用 multi-sim，默认 true）
  - `pause`（热加载暂停）

## 运行机制概览

- Pending 拉取：按目标数量抓取 `pending` 任务；余数为 1 则丢 1 条到下轮避免单组。
- 并发回测：`wqb.to_multi_alphas` 分组，`WQBSession.concurrent_simulate` 并发执行。
- 结果对齐：对每个组的 HTTP 响应读取 `children` 并“扁平化”，与输入一一对应。
- 结果入库：`BacktestResultProcessor` 兼容 HTTP Response 与 `children` 子项字典；按条更新状态与指标。
- 批量写库：使用 `psycopg2.extras.execute_batch` 同步批量更新（减少往返，提升吞吐）。

## 日志

- 回测日志：`concurrent_multi_backtest.log`
- WQB 调用日志：`wqb_concurrent.log`

## 一致性与性能

- 写库为同步提交；崩溃前未提交的数据不会落库（保持一致性），但会丢失内存中的本轮结果。
- 已采用 `execute_batch` 批量更新，降低 DB 往返；建议大批量导入/更新时分块提交（如 500–1000 条/批）。

## 目录结构（简要）

- `postgresql_database.py`：PostgreSQL 连接池、表结构、CRUD、结果写回处理器。
- `postgresql_alpha_importer.py`：从 `expressions/` 扫描 JSON 并导入。
- `concurrent_multi_backtest.py`：并发回测轮询器（含热加载与暂停）。
- `wqb/`：WQB SDK（`WQBSession` 等）。
- `config.json`：示例配置。

## 提示

- 确保 PostgreSQL 使用 UTF-8 编码；本仓库源码统一 UTF-8。
- 生产账号/密码请通过环境变量或安全存储注入，避免明文提交。

