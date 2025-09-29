#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
轻量回测自检（尽量不触发耗时的真实回测）

- 验证 WQB 认证连通性（locate_field('open')）
- 验证 DB 连接与 pending 统计
- 计算当轮计划（不提交 simulate），用于确认配置是否合理
"""

from __future__ import annotations

import argparse
import sys
from typing import Any, Dict

import os
sys.path.append('.')

from concurrent_multi_backtest import ConcurrentMultiBacktest  # type: ignore


def main() -> None:
    parser = argparse.ArgumentParser(description='Smoke test: backtest readiness (no heavy runs)')
    parser.add_argument('--config', type=str, default='config.json', help='Path to config file')
    args = parser.parse_args()

    system = ConcurrentMultiBacktest(args.config)

    # 认证连通性（轻量）
    ok_auth = system.test_authentication()
    print(f"✓ WQB 认证连通性: {'OK' if ok_auth else 'FAILED'}")

    # DB 待回测统计
    try:
        stats = system.db_manager.get_statistics() or {}
        pending_left = int((stats.get('status_counts') or {}).get('pending', 0))
        print(f"✓ 数据库连接正常, pending 条数: {pending_left}")
    except Exception as e:
        print(f"⚠️ 无法获取数据库统计: {e}")

    # 计算一轮计划（不提交 simulate）
    pack = max(2, system.alpha_count_per_slot)
    target = system.backtest_fetch_limit if system.backtest_fetch_limit is not None else max(2, system.concurrent_count) * pack
    print(
        f"计划参数: 并发={system.concurrent_count}, 每组(pack)={pack}, 目标抓取={target}, multi={system.use_multi_sim}, paused={getattr(system, 'paused', False)}"
    )

    # 提示如何真正执行（可选）
    print("提示: 正式回测请运行 concurrent_multi_backtest.py --config ", args.config)


if __name__ == '__main__':
    main()

