我先逐项解释 config_template.json 每个参数的作用，然后给出“当前代码仓库里是否被使用”的结论，以及如果你只运行 importer 与 concurrent 多回测模块时，可以精简到的最小参数集合。

参数逐行解释（含用途与常用范围）

db_host
数据库主机名或 IP。用于连接 PostgreSQL。
db_port
数据库端口。PostgreSQL 默认 5432。
db_name
数据库名。用于读写 Alpha 与回测结果。
db_user
数据库用户。
db_password
数据库密码。
max_db_connections
数据库连接池最大连接数。常见 10–50。
max_concurrent_tasks
WQB 并发任务最大数（BRAIN/WQB 平台通常限制 ≤ 8）。用于主并发回测系统（concurrent_backtest 包）。
num_workers
工作线程/进程数量（用于并发回测系统内部执行单元的并行度）。
batch_size
每个“多回测任务”内包含的 Alpha 数（WQB 限制 ≤ 10）。与 max_concurrent_tasks 配合控制吞吐。
task_queue_size
任务队列最大深度（用于生产者/消费者架构的并发回测系统）。
result_batch_size
结果处理器一次批量写回数据库的条数。
auth_cooldown_seconds
WQB 认证“有效期/冷却时间”（例如 3.5 小时），在此期间复用认证结果避免频繁登录。
auth_cache_file
WQB 认证缓存文件路径，用于持久化认证信息以减少重复认证。
brain_username
WQB 登录用户名（BRAIN 账户）。
brain_password
WQB 登录密码（BRAIN 账户）。
stats_interval
统计与监控日志输出的间隔（秒）。
verbose
更详细的日志开关（True/False），影响并发系统的调试输出量。
wqb_max_tries
WQB 会话或请求的最大重试次数（并发系统的 worker 中使用）。
wqb_delay_unexpected
遇到非预期错误时 WQB 请求的基础延迟（秒），用于退避。
wqb_simulate_max_tries
WQB simulate 调用的最大尝试次数（避免瞬时失败中断）。
wqb_log_enabled
计划用于启用 WQB 相关日志的开关（在当前代码中未被实际读取）。
wqb_log_file_prefix
计划用于 WQB 日志文件前缀（当前代码中未被实际读取）。
max_retries
写回结果或认证等操作的通用“重试次数”（多处组件使用）。
retry_delay
写回结果、数据库等重试的基础延迟（秒），通常结合指数退避。
producer_check_interval
生产者检查新任务的间隔（秒）。
result_processor_interval
结果处理器检查与写回的间隔（秒）。
log_level
系统日志级别（INFO/DEBUG/WARNING/ERROR）。
log_to_file
是否写日志到文件。
log_to_console
是否输出日志到控制台。
log_file_path
主日志文件路径。
log_max_size
日志轮转的单文件最大字节数（例如 10MB）。
log_backup_count
日志轮转保留的历史文件个数。
import_directory
导入器默认读取表达式 JSON 的目录（递归或按模式过滤）。
import_check_interval
导入器轮询检查新文件的时间间隔（秒）。
import_mode
导入模式：file（单文件）或 directory（目录批量）。
import_file_path
当 import_mode = file 时，指定单个 JSON 文件路径。
import_pattern
当 import_mode = directory 时，匹配 JSON 文件的通配模式（例如 *.json）。
import_skip_duplicates
导入时是否跳过重复表达式（基于 DB 的存在性检查）。
import_batch_size
导入批量大小（每批插入多少条 Alpha）。
default_alpha_priority
新导入 Alpha 的默认优先级（LOW/MEDIUM/HIGH）。
alpha_count_per_slot
仅对“并发多回测（concurrent_multi_backtest.py）”生效：每个 MultiAlpha 包含的 Alpha 数（强制 ≥ 2）。
concurrent_count
仅对“并发多回测”生效：调用 WQB 并发度（多组 MultiAlpha 并发提交数）。
use_multi_sim
仅对“并发多回测”生效：是否使用 MultiAlpha 分组提交（True 时按组跑；False 时逐条跑）。
backtest_fetch_limit
仅对“并发多回测”生效：本轮最大抓取待回测的 Alpha 条数（>0 时覆盖默认“并发×分组大小”的目标值）。
error_rate_threshold
系统可接受的错误率阈值（例如 0.05 即 5%），超出可触发告警或降载策略。
memory_limit_mb
可选的内存使用上限（MB），供监控/限制使用（部分模块读取）。
cpu_limit_percent
可选的 CPU 使用率上限（%），供监控/限制使用（部分模块读取）。
poll_interval_seconds
空闲轮询间隔（秒），对 importer 与“并发多回测”轮询模式均有用。
哪些参数在当前仓库中被实际使用

被并发多回测（concurrent_multi_backtest.py）直接使用
db_host, db_port, db_name, db_user, db_password, max_db_connections
brain_username, brain_password
alpha_count_per_slot, concurrent_count, use_multi_sim, backtest_fetch_limit, poll_interval_seconds
被导入器（postgresql_alpha_importer.py）直接使用
db_host, db_port, db_name, db_user, db_password
import_directory, import_check_interval, import_mode, import_file_path, import_pattern, import_skip_duplicates, import_batch_size
default_alpha_priority
被并发回测主系统（concurrent_backtest 包）使用（如果你运行它）
max_concurrent_tasks, num_workers, batch_size, task_queue_size, result_batch_size
auth_cooldown_seconds, auth_cache_file
stats_interval, verbose
wqb_max_tries, wqb_delay_unexpected, wqb_simulate_max_tries
max_retries, retry_delay, producer_check_interval, result_processor_interval
log_level, log_to_file, log_to_console, log_file_path, log_max_size, log_backup_count
error_rate_threshold, memory_limit_mb, cpu_limit_percent
poll_interval_seconds（队列空闲轮询也会读）
以及上面导入/数据库相关相同参数
看起来“未被任何代码读取”的参数

wqb_log_enabled
wqb_log_file_prefix
说明：它们只出现在配置/文档里，没有在 Python 源码中被引用，删掉不会影响现有模块运行。
如果你只使用“导入器 + 并发多回测（concurrent_multi_backtest.py）”

建议保留的最小参数集
数据库连接
db_host, db_port, db_name, db_user, db_password, max_db_connections
WQB 凭据
brain_username, brain_password
并发多回测控制
alpha_count_per_slot, concurrent_count, use_multi_sim, backtest_fetch_limit, poll_interval_seconds
导入器（若你使用导入脚本）
import_directory, import_check_interval, import_mode, import_file_path, import_pattern, import_skip_duplicates, import_batch_size, default_alpha_priority
日志（可选，若你只看控制台，可以不配）
log_level, log_to_file, log_to_console, log_file_path, log_max_size, log_backup_count
可以删除（对这两个模块无影响）
max_concurrent_tasks, num_workers, batch_size, task_queue_size, result_batch_size
auth_cooldown_seconds, auth_cache_file（concurrent_multi_backtest 自己未使用认证缓存；若只跑这个模块，可删）
stats_interval, verbose
wqb_max_tries, wqb_delay_unexpected, wqb_simulate_max_tries
max_retries, retry_delay, producer_check_interval, result_processor_interval
error_rate_threshold, memory_limit_mb, cpu_limit_percent
wqb_log_enabled, wqb_log_file_prefix（仓库中未使用）
注意

ConcurrentBacktestConfig.load_from_file 会为缺省字段使用内置默认值，并过滤未知字段；所以删掉无用键是安全的。
如果未来你要运行 concurrent_backtest 主系统（producer/worker/monitor），上面“可以删除”的键又会变得有用；届时可用当前模板再加回去。
需要的话我可以基于你现在的实际使用场景，给你生成一份“精简版配置模板”，或者直接替你在 config_template.json 中移除未用项。你更倾向哪种？