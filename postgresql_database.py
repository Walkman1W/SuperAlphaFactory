"""
PostgreSQL 数据库层实现
替代 DuckDB，支持多连接并发访问与 ACID 事务
实现 Alpha 因子完整生命周期管理
"""

import psycopg2
import psycopg2.pool
import psycopg2.extras
import threading
import queue
import time
import json
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime
from enum import IntEnum
import logging

# 配置日志
logger = logging.getLogger(__name__)


class Priority(IntEnum):
    """优先级枚举"""
    LOW = 1
    MEDIUM = 2
    HIGH = 3


class AlphaStatus:
    """Alpha状态"""
    PENDING = "pending"
    PROCESSING = "processing" 
    COMPLETED = "completed"
    FAILED = "failed"


class LifecycleStage:
    """Alpha生命周期"""
    BACKTEST = "backtest"
    PRODUCTION = "production"
    RETIRED = "retired"


@dataclass
class AlphaFactor:
    """Alpha"""
    
    # id
    id: Optional[int] = None
    alpha_id: Optional[str] = None
    
    # expression
    expression: str = ""
    alpha_type: str = "REGULAR"
    
    # settings
    settings: Dict[str, Any] = None
    
    # lifecycle_stage
    lifecycle_stage: str = LifecycleStage.BACKTEST
    backtest_status: str = AlphaStatus.PENDING
    production_status: Optional[str] = None
    
    # 回测结果
    sharpe_ratio: Optional[float] = None
    annual_return: Optional[float] = None
    max_drawdown: Optional[float] = None
    volatility: Optional[float] = None
    
    # 相关性
    self_corr: Optional[float] = None
    pro_corr: Optional[float] = None
    pnl_daily: Optional[float] = None
    pnl_cumulative: Optional[float] = None
    
    # 回测指标
    backtest_result: Optional[Dict[str, Any]] = None
    production_metrics: Optional[Dict[str, Any]] = None
    
    # 优先级
    priority: int = Priority.MEDIUM
    retry_count: int = 0
    max_retries: int = 3
    worker_id: Optional[int] = None
    batch_id: Optional[str] = None
    
    # 错误信息
    error_message: Optional[str] = None
    processing_time: Optional[float] = None
    
    # 文件
    source_file: Optional[str] = None
    tags: List[str] = None
    notes: Optional[str] = None
    
    # 时间
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    backtest_started_at: Optional[datetime] = None
    backtest_completed_at: Optional[datetime] = None
    production_started_at: Optional[datetime] = None
    
    def __post_init__(self):
        """初始化"""
        if self.settings is None:
            self.settings = {
                "instrumentType": "EQUITY",
                "region": "USA",
                "universe": "TOP3000",
                "delay": 1,
                "decay": 5,
                "neutralization": "INDUSTRY",
                "truncation": round(0.08, 2),
                "pasteurization": "ON",
                "testPeriod": "P0Y0M",
                "unitHandling": "VERIFY",
                "nanHandling": "OFF",
                "maxTrade": "OFF",
                "language": "FASTEXPR",
                "visualization": False
            }
        if self.tags is None:
            self.tags = []
    
    def to_wqb_format(self) -> Dict[str, Any]:
        """转为 WQB 格式"""
        return {
            'type': self.alpha_type,
            'settings': self.settings,
            'regular': self.expression
        }
    
    @classmethod
    def from_json_format(cls, json_data: Dict[str, Any]) -> 'AlphaFactor':
        """从 JSON 格式创建 AlphaFactor"""
        return cls(
            expression=json_data.get('regular', ''),
            alpha_type=json_data.get('type', 'REGULAR'),
            settings=json_data.get('settings', {})
        )
    
    def get_region(self) -> str:
        """获取地区"""
        return self.settings.get('region', 'USA')
    
    def is_backtest_ready(self) -> bool:
        """是否准备好回测"""
        return (self.lifecycle_stage == LifecycleStage.BACKTEST and
                self.backtest_status == AlphaStatus.PENDING and
                bool(self.expression.strip()))


@dataclass
class AlphaBatch:
    """Alpha 批次 - 包含 10 个 Alpha"""
    alphas: List[AlphaFactor]
    batch_id: str
    created_at: datetime
    priority: int
    region: Optional[str] = None
    
    def __post_init__(self):
        """初始化"""
        if len(self.alphas) > 10:
            raise ValueError("Alpha 数量不能超过 10")
        
        # 设置 Alpha 批次 ID
        for alpha in self.alphas:
            alpha.batch_id = self.batch_id
    
    def to_wqb_multi_alpha(self) -> List[Dict[str, Any]]:
        """转为 WQB MultiAlpha 格式"""
        return [alpha.to_wqb_format() for alpha in self.alphas]
    
    def get_alpha_ids(self) -> List[int]:
        """获取 Alpha ID列表"""
        return [alpha.id for alpha in self.alphas if alpha.id is not None]
    
    def get_batch_size(self) -> int:
        """获取批次大小"""
        return len(self.alphas)
    
    def get_regions(self) -> set:
        """获取地区列表"""
        return {alpha.get_region() for alpha in self.alphas}
    
    def is_single_region(self) -> bool:
        """是否单地区"""
        return len(self.get_regions()) == 1
    
    def get_batch_region(self) -> Optional[str]:
        """获取批次地区"""
        regions = self.get_regions()
        return list(regions)[0] if len(regions) == 1 else None


class PostgreSQLConnectionPool:
    """PostgreSQL 连接池 - 并发访问与 ACID 事务"""
    
    def __init__(self, host: str = "localhost", port: int = 5432, 
                 database: str = "concurrent_backtest", user: str = "postgres", 
                 password: str = "admin", max_connections: int = 20):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.max_connections = max_connections
        
        # 初始化连接池
        self.pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=max_connections,
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        
        logger.info(f"PostgreSQL 连接池初始化完成: {host}:{port}/{database}")
    
    def get_connection(self) -> psycopg2.extensions.connection:
        """获取数据库连接"""
        try:
            conn = self.pool.getconn()
            if conn:
                return conn
            else:
                raise Exception("无法从连接池获取连接")
        except Exception as e:
            logger.error(f"获取数据库连接失败: {e}")
            raise
    
    def return_connection(self, conn: psycopg2.extensions.connection) -> None:
        """归还数据库连接"""
        try:
            self.pool.putconn(conn)
        except Exception as e:
            logger.error(f"归还数据库连接失败: {e}")
    
    def close_all(self) -> None:
        """关闭所有连接"""
        try:
            self.pool.closeall()
            logger.info("All PostgreSQL connections closed")
        except Exception as e:
            logger.error(f"关闭数据库连接失败: {e}")
    
    def test_connection(self) -> bool:
        """测试数据库连接"""
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                result = cur.fetchone()
            self.return_connection(conn)
            return result is not None
        except Exception as e:
            logger.error(f"测试数据库连接失败: {e}")
            return False


class PostgreSQLDatabaseManager:
    """PostgreSQL 数据库管理器 - Alpha 状态管理与 ACID 事务"""
    
    def __init__(self, host: str = "localhost", port: int = 5432, 
                 database: str = "concurrent_backtest", user: str = "postgres", 
                 password: str = "admin", max_connections: int = 20):
        
        self.connection_pool = PostgreSQLConnectionPool(
            host, port, database, user, password, max_connections
        )
        self.lock = threading.Lock()
        self._create_schema()
    
    def format_float_to_decimal(self, value: Optional[float]) -> Optional[float]:
        """
        将浮点数格式化为两位小数
        
        Args:
            value: 原始值
            
        Returns:
            格式化后的值
        """
        if value is None:
            return None
        return round(float(value), 2)
    
    def _create_schema(self) -> None:
        """创建 PostgreSQL 模式"""
        
        conn = self.connection_pool.get_connection()
        try:
            with conn.cursor() as cur:
                logger.info("创建 PostgreSQL 模式...")
                
                # 1. 创建 Alpha 表
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS alpha_factors (
                        -- id
                        id BIGSERIAL,
                        alpha_id VARCHAR(50),  -- BRAIN ID
                        
                        -- expression
                        expression TEXT NOT NULL,
                        alpha_type VARCHAR(20) DEFAULT 'REGULAR',  -- REGULAR, COMBO绛?
                        
                        -- settings
                        settings JSONB NOT NULL DEFAULT '{
                            "instrumentType": "EQUITY",
                            "region": "USA",
                            "universe": "TOP3000",
                            "delay": 1,
                            "decay": 5,
                            "neutralization": "INDUSTRY",
                            "truncation": 0.08,
                            "pasteurization": "ON",
                            "testPeriod": "P0Y0M",
                            "unitHandling": "VERIFY",
                            "nanHandling": "OFF",
                            "maxTrade": "OFF",
                            "language": "FASTEXPR",
                            "visualization": false
                        }'::jsonb,
                        
                        -- lifecycle_stage
                        lifecycle_stage VARCHAR(20) DEFAULT 'backtest',  -- backtest, production, retired
                        backtest_status VARCHAR(20) DEFAULT 'pending',   -- pending, processing, completed, failed
                        production_status VARCHAR(20),                   -- submitted, approved, live, paused, stopped
                        
                        -- 回测指标
                        sharpe_ratio DECIMAL(10,6),
                        annual_return DECIMAL(10,6),
                        max_drawdown DECIMAL(10,6),
                        volatility DECIMAL(10,6),
                        
                        -- 相关性
                        self_corr DECIMAL(10,6),        -- 自相关性
                        pro_corr DECIMAL(10,6),         -- 相关性
                        pnl_daily DECIMAL(15,2),        -- 每日pnl
                        pnl_cumulative DECIMAL(15,2),   -- 累计pnl
                        
                        -- 回测结果
                        backtest_result JSONB,          -- 回测结果JSON
                        production_metrics JSONB,       -- 生产指标JSON
                        
                        -- 优先级
                        priority INTEGER DEFAULT 1,
                        retry_count INTEGER DEFAULT 0,
                        max_retries INTEGER DEFAULT 3,
                        worker_id INTEGER,
                        batch_id VARCHAR(50),           -- 批次ID
                        
                        -- 错误信息
                        error_message TEXT,
                        processing_time DECIMAL(10,3),  -- 澶勭悊鑰楁椂(绉?
                        
                        -- 文件
                        source_file TEXT,
                        tags JSONB DEFAULT '[]'::jsonb,
                        notes TEXT,
                        
                        -- 时间
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        backtest_started_at TIMESTAMP,
                        backtest_completed_at TIMESTAMP,
                        production_started_at TIMESTAMP,
                        
                        -- 分区日期
                        partition_date DATE DEFAULT CURRENT_DATE,
                        
                        -- 主键
                        PRIMARY KEY (id, partition_date),
                        UNIQUE (alpha_id, partition_date)
                    ) PARTITION BY RANGE (partition_date)
                """)
                
                # 2. 创建 Alpha 分区表
                # 当前日期和未来3个月的日期
                current_date = datetime.now()
                for month_offset in range(0, 3):  # 当前日期和未来3个月的日期
                    year = current_date.year
                    month = current_date.month + month_offset
                    if month > 12:
                        year += 1
                        month -= 12
                    
                    partition_name = f"alpha_factors_{year}_{month:02d}"
                    start_date = f"{year}-{month:02d}-01"
                    
                    next_month = month + 1
                    next_year = year
                    if next_month > 12:
                        next_year += 1
                        next_month = 1
                    end_date = f"{next_year}-{next_month:02d}-01"
                    
                    cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS {partition_name} PARTITION OF alpha_factors
                        FOR VALUES FROM ('{start_date}') TO ('{end_date}')
                    """)
                
                # 3. 创建索引
                indexes = [
                    "CREATE INDEX IF NOT EXISTS idx_alpha_backtest_status_priority ON alpha_factors(backtest_status, priority DESC) WHERE lifecycle_stage = 'backtest'",
                    "CREATE INDEX IF NOT EXISTS idx_alpha_settings_region ON alpha_factors((settings->>'region'))",
                    "CREATE INDEX IF NOT EXISTS idx_alpha_lifecycle_stage ON alpha_factors(lifecycle_stage)",
                    "CREATE INDEX IF NOT EXISTS idx_alpha_created_date ON alpha_factors(partition_date, created_at)",
                    "CREATE INDEX IF NOT EXISTS idx_alpha_batch_id ON alpha_factors(batch_id) WHERE batch_id IS NOT NULL",
                    "CREATE INDEX IF NOT EXISTS idx_alpha_worker_id ON alpha_factors(worker_id) WHERE worker_id IS NOT NULL",
                    "CREATE INDEX IF NOT EXISTS idx_alpha_sharpe_ratio ON alpha_factors(sharpe_ratio DESC) WHERE sharpe_ratio IS NOT NULL",
                    
                    # 地区状态优先级索引
                    "CREATE INDEX IF NOT EXISTS idx_alpha_region_status_priority ON alpha_factors((settings->>'region'), backtest_status, priority DESC) WHERE lifecycle_stage = 'backtest'",
                    "CREATE INDEX IF NOT EXISTS idx_alpha_region_pending ON alpha_factors((settings->>'region'), created_at) WHERE backtest_status = 'pending' AND lifecycle_stage = 'backtest'",
                    "CREATE INDEX IF NOT EXISTS idx_alpha_region_processing ON alpha_factors((settings->>'region'), backtest_started_at) WHERE backtest_status = 'processing'",
                    "CREATE INDEX IF NOT EXISTS idx_alpha_region_completed ON alpha_factors((settings->>'region'), backtest_completed_at) WHERE backtest_status = 'completed'",
                    
                    # 批次地区状态索引
                    "CREATE INDEX IF NOT EXISTS idx_alpha_batch_region ON alpha_factors(batch_id, (settings->>'region')) WHERE batch_id IS NOT NULL",
                    "CREATE INDEX IF NOT EXISTS idx_alpha_region_batch_status ON alpha_factors((settings->>'region'), batch_id, backtest_status) WHERE batch_id IS NOT NULL"
                ]
                
                for index_sql in indexes:
                    cur.execute(index_sql)
                
                # 4. JSONB 索引
                gin_indexes = [
                    "CREATE INDEX IF NOT EXISTS idx_alpha_settings_gin ON alpha_factors USING GIN(settings)",
                    "CREATE INDEX IF NOT EXISTS idx_alpha_backtest_result_gin ON alpha_factors USING GIN(backtest_result)",
                    "CREATE INDEX IF NOT EXISTS idx_alpha_tags_gin ON alpha_factors USING GIN(tags)"
                ]
                
                for gin_index_sql in gin_indexes:
                    cur.execute(gin_index_sql)
                
                # 5. 更新 updated_at 触发器
                cur.execute("""
                    CREATE OR REPLACE FUNCTION update_updated_at_column()
                    RETURNS TRIGGER AS $$
                    BEGIN
                        NEW.updated_at = CURRENT_TIMESTAMP;
                        RETURN NEW;
                    END;
                    $$ language 'plpgsql'
                """)
                
                cur.execute("""
                    DROP TRIGGER IF EXISTS update_alpha_factors_updated_at ON alpha_factors
                """)
                
                cur.execute("""
                    CREATE TRIGGER update_alpha_factors_updated_at
                        BEFORE UPDATE ON alpha_factors
                        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column()
                """)
                
                # 6. 创建下一个月份分区函数
                cur.execute("""
                    CREATE OR REPLACE FUNCTION create_next_month_partition()
                    RETURNS void AS $$
                    DECLARE
                        next_month_start DATE;
                        next_month_end DATE;
                        partition_name TEXT;
                    BEGIN
                        next_month_start := date_trunc('month', CURRENT_DATE + INTERVAL '1 month');
                        next_month_end := next_month_start + INTERVAL '1 month';
                        partition_name := 'alpha_factors_' || to_char(next_month_start, 'YYYY_MM');
                        
                        EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF alpha_factors
                                        FOR VALUES FROM (%L) TO (%L)',
                                       partition_name, next_month_start, next_month_end);
                    END;
                    $$ LANGUAGE plpgsql
                """)
                
                cur.execute("""
                    CREATE OR REPLACE FUNCTION cleanup_old_partitions()
                    RETURNS void AS $$
                    DECLARE
                        old_partition_date DATE;
                        partition_name TEXT;
                    BEGIN
                        old_partition_date := date_trunc('month', CURRENT_DATE - INTERVAL '2 years');
                        partition_name := 'alpha_factors_' || to_char(old_partition_date, 'YYYY_MM');
                        
                        EXECUTE format('DROP TABLE IF EXISTS %I', partition_name);
                    END;
                    $$ LANGUAGE plpgsql
                """)
                
                conn.commit()
                logger.info("PostgreSQL table schemas created")
                
        except Exception as e:
            conn.rollback()
            logger.error(f"创建 PostgreSQL 模式失败: {e}")
            raise
        finally:
            self.connection_pool.return_connection(conn)
    
    def fetch_pending_alphas_atomic(self, batch_size: int = 10, worker_id: int = None, 
                                   region: Optional[str] = None) -> List[AlphaFactor]:
        """获取待处理 Alpha"""
        
        # 使用锁确保原子性
        with self.lock:
            conn = self.connection_pool.get_connection()
            try:
                with conn.cursor() as cur:
                    # 禁用自动提交
                    conn.autocommit = False
                    
                    # 查询条件
                    where_clause = "lifecycle_stage = %s AND backtest_status = %s"
                    params = [LifecycleStage.BACKTEST, AlphaStatus.PENDING]
                    
                    if region:
                        where_clause += " AND settings->>'region' = %s"
                        params.append(region)
                    
                    # 1. 使用 FOR UPDATE SKIP LOCKED 获取待处理 Alpha
                    query_sql = f"""
                        SELECT id, expression, alpha_type, settings, priority, 
                               source_file, tags, notes, created_at
                        FROM alpha_factors 
                        WHERE {where_clause}
                        ORDER BY priority DESC, created_at ASC 
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED
                    """
                    
                    params.append(batch_size)
                    cur.execute(query_sql, params)
                    results = cur.fetchall()
                    
                    if not results:
                        conn.rollback()
                        return []
                    
                    # 2. 获取 Alpha IDs
                    alpha_ids = [row['id'] for row in results]
                    batch_id = f"batch_{int(time.time())}_{worker_id or 0}"
                    
                    # 3. 更新 Alpha 状态为 processing
                    update_sql = """
                        UPDATE alpha_factors 
                        SET backtest_status = %s, 
                            worker_id = %s, 
                            backtest_started_at = CURRENT_TIMESTAMP,
                            batch_id = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = ANY(%s) AND backtest_status = %s
                    """
                    
                    cur.execute(update_sql, [
                        AlphaStatus.PROCESSING, 
                        worker_id, 
                        batch_id,
                        alpha_ids,
                        AlphaStatus.PENDING
                    ])
                    
                    # 4. 提交事务
                    conn.commit()
                    
                    # 5. 创建 AlphaFactor 列表
                    alphas = []
                    for row in results:
                        alpha = AlphaFactor(
                            id=row['id'],
                            expression=row['expression'],
                            alpha_type=row['alpha_type'],
                            settings=row['settings'],
                            priority=row['priority'],
                            source_file=row['source_file'],
                            tags=row['tags'] or [],
                            notes=row['notes'],
                            created_at=row['created_at'],
                            backtest_status=AlphaStatus.PROCESSING,
                            batch_id=batch_id,
                            worker_id=worker_id
                        )
                        alphas.append(alpha)
                    
                    logger.info(f"获取到 {len(alphas)} 个 Alpha，批次ID: {batch_id}")
                    return alphas
                    
            except Exception as e:
                conn.rollback()
                logger.error(f"获取待处理 Alpha 失败: {e}")
                raise
            finally:
                conn.autocommit = True
                self.connection_pool.return_connection(conn)
    
    def update_backtest_results(self, alphas: List[AlphaFactor]) -> bool:
        """更新回测结果"""
        
        conn = self.connection_pool.get_connection()
        try:
            with conn.cursor() as cur:
                conn.autocommit = False
                
                # 使用 execute_batch 批量更新，减少往返并提高吞吐
                sql = """
                    UPDATE alpha_factors SET
                        backtest_status = %s,
                        alpha_id = %s,
                        sharpe_ratio = %s,
                        annual_return = %s,
                        max_drawdown = %s,
                        volatility = %s,
                        backtest_result = %s,
                        error_message = %s,
                        processing_time = %s,
                        backtest_started_at = %s,
                        backtest_completed_at = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """
                params = [
                    (
                        a.backtest_status,
                        a.alpha_id,
                        self.format_float_to_decimal(a.sharpe_ratio),
                        self.format_float_to_decimal(a.annual_return),
                        self.format_float_to_decimal(a.max_drawdown),
                        self.format_float_to_decimal(a.volatility),
                        json.dumps(a.backtest_result) if a.backtest_result else None,
                        a.error_message,
                        self.format_float_to_decimal(a.processing_time),
                        a.backtest_started_at,
                        a.backtest_completed_at,
                        a.id,
                    )
                    for a in alphas
                ]
                psycopg2.extras.execute_batch(cur, sql, params, page_size=1000)
                
                conn.commit()
                logger.info("Updated backtest results: " + str(len(alphas)))
                return True
                
        except Exception as e:
            conn.rollback()
            logger.error(f"更新回测结果失败: {e}")
            return False
        finally:
            conn.autocommit = True
            self.connection_pool.return_connection(conn)
    
    def insert_alpha_factors(self, alphas: List[AlphaFactor]) -> List[int]:
        """鎵归噺鎻掑叆Alpha鍥犲瓙"""
        
        conn = self.connection_pool.get_connection()
        try:
            with conn.cursor() as cur:
                conn.autocommit = False
                
                inserted_ids = []
                for alpha in alphas:
                    cur.execute("""
                        INSERT INTO alpha_factors (
                            expression, alpha_type, settings, lifecycle_stage,
                            backtest_status, priority, source_file, tags, notes
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s
                        ) RETURNING id
                    """, (
                        alpha.expression, alpha.alpha_type, json.dumps(alpha.settings),
                        alpha.lifecycle_stage, alpha.backtest_status, alpha.priority,
                        alpha.source_file, json.dumps(alpha.tags), alpha.notes
                    ))
                    
                    alpha_id = cur.fetchone()['id']
                    inserted_ids.append(alpha_id)
                
                conn.commit()
                logger.info("Inserted alpha_factors: " + str(len(alphas)))
                return inserted_ids
                
        except Exception as e:
            conn.rollback()
            logger.error(f"插入 Alpha 失败: {e}")
            return []
        finally:
            conn.autocommit = True
            self.connection_pool.return_connection(conn)
    
    def get_alpha_by_id(self, alpha_id: int) -> Optional[AlphaFactor]:
        """根据 ID 获取 Alpha"""
        
        conn = self.connection_pool.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, alpha_id, expression, alpha_type, settings,
                           lifecycle_stage, backtest_status, production_status,
                           sharpe_ratio, annual_return, max_drawdown, volatility,
                           self_corr, pro_corr, pnl_daily, pnl_cumulative,
                           backtest_result, production_metrics, priority,
                           retry_count, max_retries, worker_id, batch_id,
                           error_message, processing_time, source_file,
                           tags, notes, created_at, updated_at,
                           backtest_started_at, backtest_completed_at,
                           production_started_at
                    FROM alpha_factors 
                    WHERE id = %s
                """, [alpha_id])
                
                row = cur.fetchone()
                if not row:
                    return None
                
                return AlphaFactor(
                    id=row['id'],
                    alpha_id=row['alpha_id'],
                    expression=row['expression'],
                    alpha_type=row['alpha_type'],
                    settings=row['settings'],
                    lifecycle_stage=row['lifecycle_stage'],
                    backtest_status=row['backtest_status'],
                    production_status=row['production_status'],
                    sharpe_ratio=row['sharpe_ratio'],
                    annual_return=row['annual_return'],
                    max_drawdown=row['max_drawdown'],
                    volatility=row['volatility'],
                    self_corr=row['self_corr'],
                    pro_corr=row['pro_corr'],
                    pnl_daily=row['pnl_daily'],
                    pnl_cumulative=row['pnl_cumulative'],
                    backtest_result=row['backtest_result'],
                    production_metrics=row['production_metrics'],
                    priority=row['priority'],
                    retry_count=row['retry_count'],
                    max_retries=row['max_retries'],
                    worker_id=row['worker_id'],
                    batch_id=row['batch_id'],
                    error_message=row['error_message'],
                    processing_time=row['processing_time'],
                    source_file=row['source_file'],
                    tags=row['tags'] or [],
                    notes=row['notes'],
                    created_at=row['created_at'],
                    updated_at=row['updated_at'],
                    backtest_started_at=row['backtest_started_at'],
                    backtest_completed_at=row['backtest_completed_at'],
                    production_started_at=row['production_started_at']
                )
                
        except Exception as e:
            logger.error(f"根据 ID 获取 Alpha 失败: {e}")
            return None
        finally:
            self.connection_pool.return_connection(conn)
    
    def get_alphas_by_status(self, status: str, limit: int = 100, 
                            lifecycle_stage: str = LifecycleStage.BACKTEST) -> List[AlphaFactor]:
        """根据状态获取 Alpha"""
        
        conn = self.connection_pool.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, alpha_id, expression, alpha_type, settings,
                           lifecycle_stage, backtest_status, production_status,
                           sharpe_ratio, annual_return, max_drawdown, volatility,
                           priority, retry_count, max_retries, worker_id, batch_id,
                           error_message, processing_time, source_file,
                           tags, notes, created_at, updated_at
                    FROM alpha_factors 
                    WHERE lifecycle_stage = %s AND backtest_status = %s
                    ORDER BY priority DESC, created_at ASC
                    LIMIT %s
                """, [lifecycle_stage, status, limit])
                
                results = cur.fetchall()
                alphas = []
                
                for row in results:
                    alpha = AlphaFactor(
                        id=row['id'],
                        alpha_id=row['alpha_id'],
                        expression=row['expression'],
                        alpha_type=row['alpha_type'],
                        settings=row['settings'],
                        lifecycle_stage=row['lifecycle_stage'],
                        backtest_status=row['backtest_status'],
                        production_status=row['production_status'],
                        sharpe_ratio=row['sharpe_ratio'],
                        annual_return=row['annual_return'],
                        max_drawdown=row['max_drawdown'],
                        volatility=row['volatility'],
                        priority=row['priority'],
                        retry_count=row['retry_count'],
                        max_retries=row['max_retries'],
                        worker_id=row['worker_id'],
                        batch_id=row['batch_id'],
                        error_message=row['error_message'],
                        processing_time=row['processing_time'],
                        source_file=row['source_file'],
                        tags=row['tags'] or [],
                        notes=row['notes'],
                        created_at=row['created_at'],
                        updated_at=row['updated_at']
                    )
                    alphas.append(alpha)
                
                return alphas
                
        except Exception as e:
            logger.error(f"根据状态获取 Alpha 失败: {e}")
            return []
        finally:
            self.connection_pool.return_connection(conn)
    
    def create_alpha_batch(self, batch_size: int = 10, worker_id: int = None, 
                          region: Optional[str] = None) -> Optional[AlphaBatch]:
        """创建 Alpha 批次 - 获取待处理 Alpha"""
        
        alphas = self.fetch_pending_alphas_atomic(batch_size, worker_id, region)
        
        if not alphas:
            return None
        
        batch_id = alphas[0].batch_id
        max_priority = max(alpha.priority for alpha in alphas)
        
        # 如果只有一个地区，则设置为该地区
        batch_region = None
        regions = {alpha.get_region() for alpha in alphas}
        if len(regions) == 1:
            batch_region = list(regions)[0]
        
        return AlphaBatch(
            alphas=alphas,
            batch_id=batch_id,
            created_at=datetime.now(),
            priority=max_priority,
            region=batch_region
        )
    
    def reset_alpha_to_pending(self, alpha_ids: List[int], reason: str = None) -> bool:
        """重置 Alpha 状态为 pending"""
        
        conn = self.connection_pool.get_connection()
        try:
            with conn.cursor() as cur:
                conn.autocommit = False
                
                cur.execute("""
                    UPDATE alpha_factors 
                    SET backtest_status = %s,
                        worker_id = NULL,
                        backtest_started_at = NULL,
                        batch_id = NULL,
                        updated_at = CURRENT_TIMESTAMP,
                        notes = CASE 
                            WHEN %s IS NOT NULL THEN COALESCE(notes, '') || '; Reset: ' || %s
                            ELSE notes 
                        END
                    WHERE id = ANY(%s) AND backtest_status IN (%s, %s)
                """, [
                    AlphaStatus.PENDING, reason, reason, alpha_ids,
                    AlphaStatus.PROCESSING, AlphaStatus.FAILED
                ])
                
                conn.commit()
                logger.info("Reset alphas to pending: " + str(len(alpha_ids)))
                return True
                
        except Exception as e:
            conn.rollback()
            logger.error(f"重置 Alpha 状态为 pending 失败: {e}")
            return False
        finally:
            conn.autocommit = True
            self.connection_pool.return_connection(conn)
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        
        conn = self.connection_pool.get_connection()
        try:
            with conn.cursor() as cur:
                # 获取所有 Alpha 状态
                cur.execute("""
                    SELECT backtest_status, COUNT(*) as count
                    FROM alpha_factors 
                    WHERE lifecycle_stage = %s
                    GROUP BY backtest_status
                """, [LifecycleStage.BACKTEST])
                
                status_counts = {row['backtest_status']: row['count'] for row in cur.fetchall()}
                
                # 获取所有地区
                cur.execute("""
                    SELECT settings->>'region' as region, COUNT(*) as count
                    FROM alpha_factors 
                    WHERE lifecycle_stage = %s
                    GROUP BY settings->>'region'
                """, [LifecycleStage.BACKTEST])
                
                region_counts = {row['region']: row['count'] for row in cur.fetchall()}
                
                # 获取回测指标
                cur.execute("""
                    SELECT 
                        AVG(processing_time) as avg_processing_time,
                        MAX(processing_time) as max_processing_time,
                        COUNT(*) as completed_count
                    FROM alpha_factors 
                    WHERE backtest_status = %s AND processing_time IS NOT NULL
                """, [AlphaStatus.COMPLETED])
                
                processing_stats = cur.fetchone()
                
                return {
                    'status_counts': status_counts,
                    'region_counts': region_counts,
                    'processing_stats': {
                        'avg_processing_time': float(processing_stats['avg_processing_time'] or 0),
                        'max_processing_time': float(processing_stats['max_processing_time'] or 0),
                        'completed_count': processing_stats['completed_count']
                    },
                    'total_alphas': sum(status_counts.values())
                }
                
        except Exception as e:
            logger.error(f"获取统计信息失败: {e}")
            return {}
        finally:
            self.connection_pool.return_connection(conn)


class BacktestResultProcessor:
    """回测结果处理 - 处理 WQB 响应的 Alpha"""
    
    @staticmethod
    def process_wqb_response(alpha_factor: AlphaFactor, wqb_response: Any,
                           started_at: datetime, processing_time: float) -> AlphaFactor:
        """处理 WQB 响应的 Alpha"""
        
        alpha_factor.backtest_started_at = started_at
        alpha_factor.backtest_completed_at = datetime.now()
        alpha_factor.processing_time = processing_time
        
        try:
            if wqb_response is None:
                alpha_factor.backtest_status = AlphaStatus.FAILED
                alpha_factor.error_message = 'WQB response is None'
                return alpha_factor
            
            if hasattr(wqb_response, 'ok') and not wqb_response.ok:
                alpha_factor.backtest_status = AlphaStatus.FAILED
                status_code = getattr(wqb_response, 'status_code', 'Unknown')
                text = getattr(wqb_response, 'text', str(wqb_response))
                alpha_factor.error_message = f"HTTP {status_code}: {text}"
                return alpha_factor
            
            # 处理 WQB 响应
            if isinstance(wqb_response, dict):
                result_data = wqb_response
            elif hasattr(wqb_response, 'json'):
                result_data = wqb_response.json()
            else:
                result_data = wqb_response.__dict__ if hasattr(wqb_response, '__dict__') else {}
            
            # 更新 Alpha 状态为 completed
            alpha_factor.backtest_status = AlphaStatus.COMPLETED
            
            # 提取 alpha_id（若存在）
            try:
                if isinstance(result_data, dict):
                    alpha_factor.alpha_id = result_data.get('alpha') or result_data.get('alpha_id') or alpha_factor.alpha_id
            except Exception:
                pass
            if 'is' in result_data:
                is_data = result_data['is']
                alpha_factor.sharpe_ratio = is_data.get('sharpe')
                alpha_factor.annual_return = is_data.get('returns')
                alpha_factor.max_drawdown = is_data.get('drawdown')
                alpha_factor.volatility = is_data.get('volatility')
            
            # 保存回测结果JSON
            alpha_factor.backtest_result = result_data
            
            # 清除错误信息
            alpha_factor.error_message = None
            
            return alpha_factor
            
        except Exception as e:
            alpha_factor.backtest_status = AlphaStatus.FAILED
            alpha_factor.error_message = f"处理 WQB 响应失败: {str(e)}"
            return alpha_factor
    
    @staticmethod
    def process_batch_results(batch: AlphaBatch, wqb_responses: List[Any],
                            started_at: datetime, processing_time: float) -> List[AlphaFactor]:
        """处理批次回测结果"""
        results = []
        
        for i, alpha in enumerate(batch.alphas):
            response = wqb_responses[i] if i < len(wqb_responses) else None
            processed_alpha = BacktestResultProcessor.process_wqb_response(
                alpha, response, started_at, processing_time
            )
            results.append(processed_alpha)
        
        return results

