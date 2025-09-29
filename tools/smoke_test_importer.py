#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
轻量导入器自检（不依赖 WQB，快速验证 DB 与导入流程）

- 连接 PostgreSQL，并创建 2 条测试 Alpha（source_file 打上 TEST_ 前缀）
- 写入后查询计数并打印结果，然后清理测试数据
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any, Dict, List

import os
sys.path.append('.')

from postgresql_alpha_importer import PostgreSQLAlphaImporter  # type: ignore


def main() -> None:
    parser = argparse.ArgumentParser(description='Smoke test: PostgreSQL importer readiness')
    parser.add_argument('--config', type=str, default='config.json', help='Path to config file')
    parser.add_argument('--keep', action='store_true', help='Do not cleanup inserted test rows')
    args = parser.parse_args()

    importer = PostgreSQLAlphaImporter(config_path=args.config)

    # 构造两条非常简单的 Alpha（不触发真实回测，只写入 DB）
    ts = int(time.time())
    marker = f"TEST_SMOKE_{ts}.json"

    samples: List[Dict[str, Any]] = [
        {
            'type': 'REGULAR',
            'regular': 'rank(close - open)',
            'settings': {'instrumentType': 'EQUITY', 'region': 'USA', 'universe': 'TOP3000'},
            'tags': ['SMOKE', 'IMPORTER'],
            'notes': 'smoke-test-1',
        },
        {
            'type': 'REGULAR',
            'regular': 'rank(volume)',
            'settings': {'instrumentType': 'EQUITY', 'region': 'USA', 'universe': 'TOP3000'},
            'tags': ['SMOKE', 'IMPORTER'],
            'notes': 'smoke-test-2',
        },
    ]

    alphas = [importer.convert_json_to_alpha(x, source_file=marker) for x in samples]

    try:
        # 插入
        importer._insert_alpha_batch(alphas)  # type: ignore[attr-defined]

        # 检查写入数量
        conn = importer.db_manager.connection_pool.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT COUNT(*) AS cnt FROM {importer.table_base} WHERE source_file = %s",
                    [marker],
                )
                row = cur.fetchone() or {}
                inserted = int(row.get('cnt', 0))
                print(f"✓ 导入器写入成功: {inserted} 条测试数据 (source_file={marker})")
        finally:
            importer.db_manager.connection_pool.return_connection(conn)

    finally:
        # 清理测试数据（除非 --keep）
        if not args.keep:
            conn = importer.db_manager.connection_pool.get_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        f"DELETE FROM {importer.table_base} WHERE source_file = %s",
                        [marker],
                    )
                conn.commit()
                print("✓ 已清理测试数据")
            finally:
                importer.db_manager.connection_pool.return_connection(conn)
        importer.close()


if __name__ == '__main__':
    main()

