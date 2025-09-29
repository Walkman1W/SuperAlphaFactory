#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
并发回测系统配置（PostgreSQL 统一版）

目标：
- 精简为 PostgreSQL 单一后端；不再包含 DuckDB 字段与逻辑。
- 导入与回测共用同一份配置，避免重复维护。
- 过滤未知字段，配置向后兼容且可平滑扩展。
"""

from __future__ import annotations

import os
import json
from dataclasses import dataclass, fields as dc_fields
from typing import Dict, Any, Optional
from pathlib import Path


@dataclass
class ConcurrentBacktestConfig:
    """并发回测系统配置（PostgreSQL-only）"""

    # 数据库（PostgreSQL）
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "concurrent_backtest"
    db_user: str = "postgres"
    db_password: str = "admin"
    max_db_connections: int = 20

    # 并发/工作参数（遵循 WQB 平台限制）
    max_concurrent_tasks: int = 8   # 同时并发的多回测任务数（WQB 限制 ≤ 8）
    batch_size: int = 10            # 每个多回测任务包含的 Alpha 数（WQB 限制 ≤ 10）
    num_workers: int = 8            # 工作线程数

    # 队列与处理
    task_queue_size: int = 50
    result_batch_size: int = 20

    # WQB 认证与会话
    auth_cooldown_seconds: int = 12600
    auth_cache_file: str = ".wqb_auth_cache.json"
    brain_username: str = ""
    brain_password: str = ""
    wqb_max_tries: int = 3
    wqb_delay_unexpected: float = 2.0
    wqb_simulate_max_tries: int = 600

    # 日志
    log_level: str = "INFO"
    log_to_file: bool = True
    log_to_console: bool = True
    log_file_path: str = "concurrent_backtest.log"
    log_max_size: int = 10 * 1024 * 1024
    log_backup_count: int = 5

    # 导入/监控
    import_directory: str = "expressions"
    import_check_interval: float = 30.0
    # 导入控制（从配置触发文件/目录导入）
    import_mode: str = "file"            # file | directory
    import_file_path: Optional[str] = None
    import_pattern: str = "*.json"
    import_skip_duplicates: bool = True
    import_batch_size: int = 100
    stats_interval: int = 60
    verbose: bool = True

    # 错误阈值与资源限制
    error_rate_threshold: float = 0.05
    memory_limit_mb: Optional[int] = None
    cpu_limit_percent: Optional[float] = None

    # 轮询空闲等待（秒）
    poll_interval_seconds: int = 60

    # 生产者与结果处理器节奏
    producer_check_interval: float = 5.0
    result_processor_interval: float = 1.0

    # 回测控制（多回测并行批次/每批Alpha数量/是否使用multi-sim）
    alpha_count_per_slot: int = 10
    concurrent_count: int = 8
    use_multi_sim: bool = True
    backtest_fetch_limit: int = 1000

    # Alpha 导入/回测默认优先级（LOW/MEDIUM/HIGH）
    default_alpha_priority: str = "MEDIUM"

    def __post_init__(self) -> None:
        """基本约束校验 + 目录准备。"""
        if not (1 <= self.max_concurrent_tasks <= 8):
            raise ValueError("max_concurrent_tasks 必须在 1..8 之间（WQB 平台限制）")
        if not (1 <= self.batch_size <= 10):
            raise ValueError("batch_size 必须在 2..10 之间（WQB 平台限制）")
        if self.task_queue_size <= 0:
            raise ValueError("task_queue_size 必须为正数")
        if not (0 < self.error_rate_threshold <= 1):
            raise ValueError("error_rate_threshold 必须在 (0,1] 区间内")

        # 确保日志目录与导入目录存在
        Path(self.log_file_path).parent.mkdir(parents=True, exist_ok=True)
        Path(self.import_directory).mkdir(parents=True, exist_ok=True)

    # ---------------- 工具方法 ----------------
    def to_dict(self) -> Dict[str, Any]:
        """转换为 dict，掩码敏感字段。"""
        out: Dict[str, Any] = {}
        for f in dc_fields(self):
            val = getattr(self, f.name)
            if 'password' in f.name.lower():
                out[f.name] = '***' if val else ''
            else:
                out[f.name] = val
        return out

    def save_to_file(self, file_path: str) -> None:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(self.to_dict(), f, indent=2, ensure_ascii=False)

    @classmethod
    def load_from_file(cls, file_path: str) -> 'ConcurrentBacktestConfig':
        # 兼容含 BOM 的 UTF-8 配置文件
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            data = json.load(f)
        # 允许嵌套结构（database/import/backtest/logging/wqb/queue）与扁平键并存
        data = cls._normalize_nested_sections(data)
        # 过滤未知字段，保证向后兼容
        valid = {f.name for f in dc_fields(cls)}
        filtered = {k: v for k, v in data.items() if k in valid}
        return cls(**filtered)

    @staticmethod
    def _normalize_nested_sections(data: Dict[str, Any]) -> Dict[str, Any]:
        """将可能存在的嵌套配置节（更适合前端表单）映射为扁平字段。

        支持的节：
        - database: {host, port, name, user, password, max_connections}
        - import: {mode, file_path, directory, pattern, skip_duplicates, batch_size, check_interval}
        - backtest: {alpha_count_per_slot, concurrent_count, use_multi_sim, fetch_limit}
        - logging: {level, to_file, to_console, file_path, max_size, backup_count}
        - wqb: {username, password, max_tries, delay_unexpected, simulate_max_tries, auth_cache_file, auth_cooldown_seconds}
        - queue: {task_queue_size, result_batch_size, producer_check_interval, result_processor_interval, poll_interval_seconds}
        """
        out = dict(data)
        db = data.get('database') or {}
        if isinstance(db, dict):
            out.setdefault('db_host', db.get('host'))
            out.setdefault('db_port', db.get('port'))
            out.setdefault('db_name', db.get('name'))
            out.setdefault('db_user', db.get('user'))
            out.setdefault('db_password', db.get('password'))
            out.setdefault('max_db_connections', db.get('max_connections'))

        imp = data.get('import') or {}
        if isinstance(imp, dict):
            out.setdefault('import_mode', imp.get('mode'))
            out.setdefault('import_file_path', imp.get('file_path'))
            out.setdefault('import_directory', imp.get('directory'))
            out.setdefault('import_pattern', imp.get('pattern'))
            out.setdefault('import_skip_duplicates', imp.get('skip_duplicates'))
            out.setdefault('import_batch_size', imp.get('batch_size'))
            out.setdefault('import_check_interval', imp.get('check_interval'))

        bt = data.get('backtest') or {}
        if isinstance(bt, dict):
            out.setdefault('alpha_count_per_slot', bt.get('alpha_count_per_slot'))
            out.setdefault('concurrent_count', bt.get('concurrent_count'))
            out.setdefault('use_multi_sim', bt.get('use_multi_sim'))
            out.setdefault('backtest_fetch_limit', bt.get('fetch_limit'))

        lg = data.get('logging') or {}
        if isinstance(lg, dict):
            out.setdefault('log_level', lg.get('level'))
            out.setdefault('log_to_file', lg.get('to_file'))
            out.setdefault('log_to_console', lg.get('to_console'))
            out.setdefault('log_file_path', lg.get('file_path'))
            out.setdefault('log_max_size', lg.get('max_size'))
            out.setdefault('log_backup_count', lg.get('backup_count'))

        wqb = data.get('wqb') or {}
        if isinstance(wqb, dict):
            out.setdefault('brain_username', wqb.get('username'))
            out.setdefault('brain_password', wqb.get('password'))
            out.setdefault('wqb_max_tries', wqb.get('max_tries'))
            out.setdefault('wqb_delay_unexpected', wqb.get('delay_unexpected'))
            out.setdefault('wqb_simulate_max_tries', wqb.get('simulate_max_tries'))
            out.setdefault('auth_cache_file', wqb.get('auth_cache_file'))
            out.setdefault('auth_cooldown_seconds', wqb.get('auth_cooldown_seconds'))

        q = data.get('queue') or {}
        if isinstance(q, dict):
            out.setdefault('task_queue_size', q.get('task_queue_size'))
            out.setdefault('result_batch_size', q.get('result_batch_size'))
            out.setdefault('producer_check_interval', q.get('producer_check_interval'))
            out.setdefault('result_processor_interval', q.get('result_processor_interval'))
            out.setdefault('poll_interval_seconds', q.get('poll_interval_seconds'))

        return out


class ConfigManager:
    """配置工厂与环境变量覆盖工具（PostgreSQL-only）。"""

    # 预置配置（可按需扩展更多预置）
    PRODUCTION = ConcurrentBacktestConfig(
        max_concurrent_tasks=8,
        num_workers=8,
        batch_size=10,
        task_queue_size=50,
        stats_interval=60,
        log_level="INFO",
        verbose=False,
    )

    DEVELOPMENT = ConcurrentBacktestConfig(
        max_concurrent_tasks=2,
        num_workers=2,
        batch_size=5,
        task_queue_size=20,
        stats_interval=30,
        log_level="DEBUG",
        verbose=True,
    )

    @staticmethod
    def get_config(name: str = "production") -> ConcurrentBacktestConfig:
        mapping = {
            "prod": ConfigManager.PRODUCTION,
            "production": ConfigManager.PRODUCTION,
            "dev": ConfigManager.DEVELOPMENT,
            "development": ConfigManager.DEVELOPMENT,
        }
        base = mapping.get(name.lower(), ConfigManager.PRODUCTION)
        return ConfigManager._load_from_env(base)

    @staticmethod
    def _load_from_env(config: ConcurrentBacktestConfig) -> ConcurrentBacktestConfig:
        """允许用环境变量覆盖关键配置（仅保留有效键）。"""
        env_map: Dict[str, tuple[str, Any] | str] = {
            'CONCURRENT_BACKTEST_DB_HOST': 'db_host',
            'CONCURRENT_BACKTEST_DB_PORT': ('db_port', int),
            'CONCURRENT_BACKTEST_DB_NAME': 'db_name',
            'CONCURRENT_BACKTEST_DB_USER': 'db_user',
            'CONCURRENT_BACKTEST_DB_PASSWORD': 'db_password',
            'CONCURRENT_BACKTEST_MAX_DB_CONNECTIONS': ('max_db_connections', int),
            'CONCURRENT_BACKTEST_MAX_TASKS': ('max_concurrent_tasks', int),
            'CONCURRENT_BACKTEST_BATCH_SIZE': ('batch_size', int),
            'CONCURRENT_BACKTEST_QUEUE_SIZE': ('task_queue_size', int),
            'CONCURRENT_BACKTEST_LOG_LEVEL': 'log_level',
            'CONCURRENT_BACKTEST_VERBOSE': ('verbose', lambda s: s.lower() in ("true","1","yes","on")),
            'CONCURRENT_BACKTEST_STATS_INTERVAL': ('stats_interval', int),
            'CONCURRENT_BACKTEST_POLL_INTERVAL_SECONDS': ('poll_interval_seconds', int),
        }
        for env, mapping in env_map.items():
            val = os.getenv(env)
            if val is None:
                continue
            if isinstance(mapping, tuple):
                key, caster = mapping
                try:
                    setattr(config, key, caster(val))
                except Exception:
                    pass
            else:
                setattr(config, mapping, val)

        ConfigManager._load_credentials(config)
        return config

    @staticmethod
    def _load_credentials(config: ConcurrentBacktestConfig) -> None:
        """从本地凭据文件加载 WQB 账号（可选）。"""
        if config.brain_username and config.brain_password:
            return
        user_file = "useID.txt"
        if os.path.exists(user_file):
            try:
                with open(user_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if isinstance(data, list) and len(data) >= 2:
                    config.brain_username = data[0]
                    config.brain_password = data[1]
            except Exception:
                pass


def create_default_config_file(file_path: str = "concurrent_backtest_config.json") -> None:
    cfg = ConfigManager.get_config("production")
    cfg.save_to_file(file_path)
    print(f"默认配置已保存: {file_path}")


if __name__ == "__main__":
    # 生成一份默认配置，便于快速起步
    create_default_config_file()
