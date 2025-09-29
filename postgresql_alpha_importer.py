#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PostgreSQL Alpha Importer (multi-tenant)
- Reads DB settings from `config.json`
"""

from __future__ import annotations

import json
import os
import sys
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
from dataclasses import dataclass

# Make package imports work when running as a script
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from postgresql_database import (  # type: ignore
    PostgreSQLDatabaseManager,
    AlphaFactor,
    Priority,
    AlphaStatus,
    LifecycleStage,
)
import psycopg2
from psycopg2 import sql


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("postgresql_alpha_importer.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


@dataclass
class ImportStats:
    total_processed: int = 0
    successful_imports: int = 0
    duplicates_skipped: int = 0
    errors: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None

    def get_duration(self) -> float:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0

    def get_success_rate(self) -> float:
        if self.total_processed == 0:
            return 0.0
        return (self.successful_imports / self.total_processed) * 100.0


class PostgreSQLAlphaImporter:
    """按用户分库的 PostgreSQL 导入器（不使用 DuckDB）。

    数据库名策略：
    - 使用 `db_name_base` 为基础名；当配置了 `user_db_suffix` 时，目标库为
      `db_name_base_user_db_suffix`；否则使用 `db_name_base`。
    - 表名固定使用配置 `db_table_base`（默认 `alpha_factors`）。
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        config_path: str = "config.json",
    ) -> None:
        # Load config
        cfg: Dict[str, Any] = {}
        try:
            # 兼容旧配置文件名
            if not os.path.exists(config_path) and os.path.exists("config_all.json"):
                config_path = "config_all.json"
            if os.path.exists(config_path):
                # 兼容含 BOM 的 UTF-8 配置
                with open(config_path, "r", encoding="utf-8-sig") as cf:
                    cfg = json.load(cf)
            else:
                logger.warning("Config file not found, using defaults: %s", config_path)
        except Exception as e:
            logger.error("Failed to load config: %s", e)

        # 解析数据库连接参数（传参 > 配置）
        host = host or cfg.get("db_host", "localhost")
        port = port or int(cfg.get("db_port", 5432))
        user = user or cfg.get("db_user", "postgres")
        password = password or cfg.get("db_password", "admin")

        # 解析“按用户分库”的数据库名称
        db_name_base = cfg.get("db_name_base") or cfg.get("db_name") or "concurrent_backtest"
        user_db_suffix = cfg.get("user_db_suffix")
        resolved_db_name = database or (f"{db_name_base}_{user_db_suffix}" if user_db_suffix else db_name_base)

        # 表名（每个库里的基础表名）
        self.table_base: str = cfg.get("db_table_base", "alpha_factors")
        # 默认优先级（LOW/MEDIUM/HIGH）
        self.default_priority_name: str = str(cfg.get("default_alpha_priority", "MEDIUM")).upper()

        # 初始化数据库管理器（会在该库内创建基础表结构/索引/分区等）
        self.database_name = resolved_db_name

        # 若目标数据库不存在，则尝试自动创建
        self._ensure_database_exists(host, port, user, password, self.database_name)

        # 连接到目标数据库并准备表结构
        self.db_manager = PostgreSQLDatabaseManager(host, port, self.database_name, user, password)

        self.stats = ImportStats()
        logger.info("导入器已就绪。目标数据库: %s，目标表: %s", self.database_name, self.table_base)

    # ---------- JSON helpers ----------
    def parse_json_file(self, file_path: str) -> List[Dict[str, Any]]:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            logger.error("Failed to parse JSON %s: %s", file_path, e)
            return []

        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return [data]
        logger.error("Unsupported JSON structure in %s", file_path)
        return []

    def _format_float_values(self, data: Any) -> Any:
        if isinstance(data, float):
            return round(data, 2)
        if isinstance(data, list):
            return [self._format_float_values(x) for x in data]
        if isinstance(data, dict):
            return {k: self._format_float_values(v) for k, v in data.items()}
        return data

    def convert_json_to_alpha(self, json_data: Dict[str, Any], source_file: Optional[str]) -> AlphaFactor:
        expression = json_data.get("regular") or json_data.get("expression") or ""
        if not expression:
            raise ValueError("Alpha expression cannot be empty")

        alpha_type = json_data.get("type", "REGULAR")
        settings = json_data.get("settings", {})

        defaults = {
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
            "visualization": False,
        }
        final_settings = defaults | settings
        final_settings = self._format_float_values(final_settings)

        # 解析默认优先级
        pr_map = {"LOW": Priority.LOW, "MEDIUM": Priority.MEDIUM, "HIGH": Priority.HIGH}
        pr = pr_map.get(self.default_priority_name, Priority.MEDIUM)

        return AlphaFactor(
            expression=expression,
            alpha_type=alpha_type,
            settings=final_settings,
            lifecycle_stage=LifecycleStage.BACKTEST,
            backtest_status=AlphaStatus.PENDING,
            priority=pr,
            source_file=source_file,
            tags=json_data.get("tags", []),
            notes=json_data.get("notes", f"Imported from {source_file}" if source_file else None),
        )

    # ---------- DB ops ----------
    def check_duplicate_expression(self, expression: str, settings: Dict[str, Any]) -> bool:
        conn = self.db_manager.connection_pool.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT COUNT(*) AS count
                    FROM {self.table_base}
                    WHERE expression = %s AND settings = %s::jsonb
                    """,
                    [expression, json.dumps(settings)],
                )
                row = cur.fetchone()
                return (row or {}).get("count", 0) > 0
        finally:
            self.db_manager.connection_pool.return_connection(conn)

    def _insert_alpha_batch(self, alpha_batch: List[AlphaFactor]) -> None:
        conn = self.db_manager.connection_pool.get_connection()
        try:
            with conn.cursor() as cur:
                conn.autocommit = False
                for alpha in alpha_batch:
                    cur.execute(
                        f"""
                        INSERT INTO {self.table_base} (
                            expression, alpha_type, settings, lifecycle_stage,
                            backtest_status, priority, source_file, tags, notes
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            alpha.expression,
                            alpha.alpha_type,
                            json.dumps(alpha.settings),
                            alpha.lifecycle_stage,
                            alpha.backtest_status,
                            int(alpha.priority) if hasattr(alpha, "priority") else int(Priority.MEDIUM),
                            alpha.source_file,
                            json.dumps(getattr(alpha, "tags", []) or []),
                            getattr(alpha, "notes", None),
                        ),
                    )
                conn.commit()
                self.stats.successful_imports += len(alpha_batch)
        except Exception as e:
            conn.rollback()
            logger.error("Batch insert failed: %s", e)
            self.stats.errors += len(alpha_batch)
        finally:
            conn.autocommit = True
            self.db_manager.connection_pool.return_connection(conn)

    def get_database_statistics(self) -> Dict[str, Any]:
        conn = self.db_manager.connection_pool.get_connection()
        try:
            with conn.cursor() as cur:
                stats: Dict[str, Any] = {}

                cur.execute(f"SELECT COUNT(*) AS total FROM {self.table_base}")
                stats["total_alphas"] = cur.fetchone()["total"]

                cur.execute(
                    f"""
                    SELECT backtest_status, COUNT(*) AS count
                    FROM {self.table_base}
                    WHERE lifecycle_stage = 'backtest'
                    GROUP BY backtest_status
                    """
                )
                stats["status_breakdown"] = {r["backtest_status"]: r["count"] for r in cur.fetchall()}

                cur.execute(
                    f"""
                    SELECT settings->>'region' AS region, COUNT(*) AS count
                    FROM {self.table_base}
                    GROUP BY settings->>'region'
                    """
                )
                stats["region_breakdown"] = {r["region"]: r["count"] for r in cur.fetchall()}

                cur.execute(
                    f"""
                    SELECT priority, COUNT(*) AS count
                    FROM {self.table_base}
                    GROUP BY priority
                    """
                )
                stats["priority_breakdown"] = {r["priority"]: r["count"] for r in cur.fetchall()}

                cur.execute(
                    f"""
                    SELECT 
                        COUNT(*) AS today_count,
                        COUNT(*) FILTER (WHERE created_at >= CURRENT_DATE - INTERVAL '7 days') AS week_count,
                        COUNT(*) FILTER (WHERE created_at >= CURRENT_DATE - INTERVAL '30 days') AS month_count
                    FROM {self.table_base}
                    """
                )
                t = cur.fetchone()
                stats["time_breakdown"] = {
                    "today": t["today_count"],
                    "this_week": t["week_count"],
                    "this_month": t["month_count"],
                }

                return stats
        except Exception as e:
            logger.error("Failed to fetch DB statistics: %s", e)
            return {}
        finally:
            self.db_manager.connection_pool.return_connection(conn)

    def print_database_statistics(self) -> None:
        stats = self.get_database_statistics()
        if not stats:
            logger.warning("No statistics available")
            return

        logger.info("=" * 60)
        logger.info("PostgreSQL 统计 - 数据库: %s, 表: %s", self.database_name, self.table_base)
        logger.info("  total_alphas: %s", stats.get("total_alphas", 0))
        logger.info("  status_breakdown: %s", stats.get("status_breakdown", {}))
        logger.info("  region_breakdown: %s", stats.get("region_breakdown", {}))
        logger.info("  priority_breakdown: %s", stats.get("priority_breakdown", {}))
        logger.info("  time_breakdown: %s", stats.get("time_breakdown", {}))
        logger.info("=" * 60)

    def cleanup_failed_alphas(self, max_retries: int = 3) -> int:
        conn = self.db_manager.connection_pool.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT COUNT(*) AS count
                    FROM {self.table_base}
                    WHERE backtest_status = %s AND retry_count >= %s
                    """,
                    [AlphaStatus.FAILED, max_retries],
                )
                count_before = cur.fetchone()["count"]
                if count_before == 0:
                    return 0

                cur.execute(
                    f"""
                    DELETE FROM {self.table_base}
                    WHERE backtest_status = %s AND retry_count >= %s
                    """,
                    [AlphaStatus.FAILED, max_retries],
                )
                conn.commit()
                return int(count_before)
        except Exception as e:
            conn.rollback()
            logger.error("Cleanup failed alphas failed: %s", e)
            return 0
        finally:
            self.db_manager.connection_pool.return_connection(conn)

    def close(self) -> None:
        self.db_manager.connection_pool.close_all()
        logger.info("Importer closed")

    # 分库模式：不需要克隆用户表，DB 管理器在目标库内创建基础表结构
    # 这里无需额外操作

    # ---------- Import flows ----------
    def import_from_json_file(
        self, file_path: str, skip_duplicates: bool = True, batch_size: int = 100
    ) -> ImportStats:
        logger.info("Importing from file: %s", file_path)
        self.stats = ImportStats(start_time=datetime.now())

        items = self.parse_json_file(file_path)
        if not items:
            return self.stats

        self.stats.total_processed = len(items)

        batch: List[AlphaFactor] = []
        for i, jd in enumerate(items, 1):
            try:
                alpha = self.convert_json_to_alpha(jd, file_path)
                if skip_duplicates and self.check_duplicate_expression(alpha.expression, alpha.settings):
                    self.stats.duplicates_skipped += 1
                    continue
                batch.append(alpha)
                if len(batch) >= batch_size:
                    self._insert_alpha_batch(batch)
                    batch = []
            except Exception as e:
                logger.error("Error processing item %s: %s", i, e)
                self.stats.errors += 1

        if batch:
            self._insert_alpha_batch(batch)

        self.stats.end_time = datetime.now()
        logger.info(
            "Done. processed=%s imported=%s dup=%s errors=%s duration=%.2fs",
            self.stats.total_processed,
            self.stats.successful_imports,
            self.stats.duplicates_skipped,
            self.stats.errors,
            self.stats.get_duration(),
        )
        return self.stats

    def import_from_directory(
        self, directory_path: str, pattern: str = "*.json", skip_duplicates: bool = True, batch_size: int = 100
    ) -> ImportStats:
        p = Path(directory_path)
        if not p.exists():
            logger.error("Directory not found: %s", directory_path)
            return ImportStats()

        files = sorted(p.glob(pattern))
        total = ImportStats(start_time=datetime.now())
        for fp in files:
            s = self.import_from_json_file(str(fp), skip_duplicates=skip_duplicates, batch_size=batch_size)
            total.total_processed += s.total_processed
            total.successful_imports += s.successful_imports
            total.duplicates_skipped += s.duplicates_skipped
            total.errors += s.errors
        total.end_time = datetime.now()
        return total

    # ---------- Admin helpers ----------
    def _ensure_database_exists(self, host: str, port: int, user: str, password: str, dbname: str) -> None:
        """确保目标数据库存在。

        实现逻辑：
        1) 连接到 `postgres` 系统库；
        2) 查询 `pg_database` 中是否存在目标库名；
        3) 若不存在，则执行 `CREATE DATABASE <dbname>`；
        4) 提示：需要当前用户具备创建数据库权限（CREATEDB）。
        """
        try:
            conn = psycopg2.connect(host=host, port=port, dbname="postgres", user=user, password=password)
            conn.autocommit = True
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
                    exists = cur.fetchone() is not None
                    if exists:
                        return
                    cur.execute(sql.SQL("CREATE DATABASE {}" ).format(sql.Identifier(dbname)))
                    logger.info("已创建数据库: %s", dbname)
            finally:
                conn.close()
        except Exception as e:
            # 如果没有权限或连接失败，则直接记录告警，后续连接会抛错，提示用户手工创建
            logger.warning("无法自动创建数据库 %s：%s（请确认数据库已创建且当前用户具备权限）", dbname, e)


def main() -> None:
    """
    主函数：PostgreSQL Alpha 导入器的主要执行入口。

    步骤：
    1) 初始化导入器并打印导入前统计；
    2) 读取配置，决定导入模式与参数；
    3) 单文件导入或目录批量导入（支持跳过重复、批量大小）；
    4) 打印导入后统计并关闭连接。
    """
    importer = PostgreSQLAlphaImporter()
    try:
        # 导入前统计
        importer.print_database_statistics()

        # 配置路径优先级：config.json > config_all.json
        if os.path.exists('config.json'):
            cfg_path = 'config.json'
        elif os.path.exists('config_all.json'):
            cfg_path = 'config_all.json'
        else:
            cfg_path = None

        # 读取配置（不存在则用空配置）
        cfg: Dict[str, Any]
        if cfg_path:
            try:
                with open(cfg_path, 'r', encoding='utf-8') as f:
                    cfg = json.load(f)
            except Exception:
                logger.warning("读取配置失败：%s，使用默认参数", cfg_path)
                cfg = {}
        else:
            logger.info("未找到配置文件，使用默认参数")
            cfg = {}

        mode = str(cfg.get('import_mode', 'file')).lower()
        batch_size = int(cfg.get('import_batch_size', 100))
        skip_dup = bool(cfg.get('import_skip_duplicates', True))

        did_import = False
        if mode == 'file':
            file_path = cfg.get('import_file_path')
            if file_path and os.path.exists(file_path):
                importer.import_from_json_file(
                    file_path,
                    skip_duplicates=skip_dup,
                    batch_size=batch_size,
                )
                did_import = True
            else:
                logger.warning("未配置 import_file_path 或文件不存在，尝试目录导入")

        if not did_import:
            directory = cfg.get('import_directory', 'expressions')
            pattern = cfg.get('import_pattern', '*.json')
            importer.import_from_directory(
                directory,
                pattern=pattern,
                skip_duplicates=skip_dup,
                batch_size=batch_size,
            )

        # 导入后统计
        importer.print_database_statistics()
    finally:
        importer.close()


if __name__ == "__main__":
    main()
