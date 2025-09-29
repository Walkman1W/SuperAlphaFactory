{
  "db_host": "localhost",//数据库主机名或 IP。用于连接 PostgreSQL。
  "db_port": 5432,//数据库端口。PostgreSQL 默认 5432。
  "db_name": "concurrent_backtest_fg",//数据库名。用于读写 Alpha 与回测结果。
  "db_user": "postgres",//数据库用户。
  "db_password": "admin",//数据库密码。
  "max_db_connections": 20,//数据库连接池最大连接数。常见 10–50。

  "brain_username": "253553962@qq.com",//WQB 登录用户名（BRAIN 账户）。
  "brain_password": "Gf.4570459",//WQB 登录密码（BRAIN 账户）。

  "alpha_count_per_slot": 10,//每个“多回测任务”内包含的 Alpha 数（WQB 限制 ≤ 10）。与 max_concurrent_tasks 配合控制吞吐。
  "concurrent_count": 8,//同时并发的多回测任务数（WQB 限制 ≤ 8）。用于主并发回测系统（concurrent_backtest 包）。
  "use_multi_sim": true,//是否启用 multi-sim，默认 true。
  "backtest_fetch_limit": 2400,//本轮最大抓取待回测的 Alpha 条数（>0 时覆盖默认“并发×分组大小”的目标值）。
  "poll_interval_seconds": 60, //空闲轮询间隔（秒），对 importer 与“并发多回测”轮询模式均有用。
  "pause": false,//热加载暂停
  "task_queue_size": 50,//任务队列最大深度（用于生产者/消费者架构的并发回测系统）。
  "log_level": "INFO",//日志级别。
  "log_to_file": true,//是否将日志输出到文件。
  "log_to_console": true,//是否将日志输出到控制台。
  "log_file_path": "concurrent_backtest.log",//日志文件路径。
  "log_max_size": 10485760,//日志文件最大大小。
  "log_backup_count": 5,//日志文件备份数量。

  "import_directory": "expressions/test2",//导入目录。当 import_mode = directory 时，指定目录路径。
  "import_check_interval": 30.0,//导入检查间隔（秒）。
  "import_mode": "directory",//导入模式。
  "import_file_path": "expressions/USA_anl44_670.json",//导入文件路径。当 import_mode = file 时，指定单个 JSON 文件路径。
  "import_pattern": "*.json",//导入文件模式。当 import_mode = directory 时，匹配 JSON 文件的通配模式（例如 *.json）。 
  "import_skip_duplicates": true,//是否跳过重复导入。
  "import_batch_size": 200,//导入批次大小。
  "default_alpha_priority": "LOW",//导入默认优先级。
}
