#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
并发多回测运行器（PostgreSQL + WQB）

本模块用于从 PostgreSQL 数据库批量拉取处于回测阶段（backtest）且状态为待处理（pending）的 Alpha 表达式，
按“多回测（MultiAlpha）”方式打包后提交到 WQB 平台进行并发回测，并把回测结果写回数据库。

功能要点
- 配置来源：默认读取 `config.json`（若不存在则回退 `concurrent_backtest_config.json`）。
- 热加载：每轮结束后重载以下参数：concurrent_count、alpha_count_per_slot、
  backtest_fetch_limit（可为 None）、poll_interval_seconds、use_multi_sim。
- 本轮目标抓取量：
  - 若 backtest_fetch_limit > 0，则 target = backtest_fetch_limit；
  - 否则 target = max(2, concurrent_count) × max(2, alpha_count_per_slot)。
- 多回测专用：只用 multi-sim，不再补跑单测；pack 强制 ≥2；若 remainder==1，丢 1 条到下轮。
- 并发调用：await self.wqb_session.concurrent_simulate(safe_multi, concurrency=self.concurrent_count)。
- 扁平化响应：按输入组块 children 数量对齐，失败用 None 占位；前 3 个失败打印 status/text（截断）。
- 日志：单一日志文件 concurrent_multi_backtest.log；WQB 日志器固定名称 wqb_concurrent；
  每轮打印计划/参数/完成摘要（吞吐耗时与 ETA）。

新手指南（数据流转框架）
1) 数据来源：导入器把 Alpha 写入 PostgreSQL 表 alpha_factors，状态 pending。
2) 拉取与打包：本模块按 target 抓取 pending，计算 pack（≥2），用 wqb.to_multi_alphas 分组，
   丢弃长度为 1 的组（避免 400），余数为 1 时本轮丢 1。
3) 回测与并发：使用 concurrent_simulate 以 concurrent_count 并发提交。
4) 扁平与对齐：把每组 children 扁平为逐条响应，与输入一一对应；失败 None 占位。
5) 落库更新：把结果转为 COMPLETED/FAILED，附指标与原始 JSON，批量更新 DB。
6) 持续轮询：按 poll_interval_seconds 轮询；运行时修改 config，下一轮自动生效。
"""

from __future__ import annotations  # 启用前向引用类型注解，便于类型提示

import os       # 操作系统工具：路径、文件存在性等
import sys      # 修改模块搜索路径等
import json     # 读取/解析配置（热加载时使用）
import time     # 时间工具（此处主要使用 datetime 记录耗时）
import asyncio  # 异步并发与睡眠控制
import logging  # 日志输出到控制台与文件
from typing import List, Dict, Any  # 类型注解：列表、字典、任意类型
from datetime import datetime        # 记录起止时间、计算耗时

sys.path.append('.')  # 确保当前工作目录在模块搜索路径中，支持相对导入

try:
    import wqb  # 供应商 SDK，提供 WQB 会话与多回测并发接口
    WQB_AVAILABLE = True
except ImportError:
    print("未检测到 wqb 包，请先安装后再运行。")
    sys.exit(1)

from config import ConcurrentBacktestConfig
from postgresql_database import (
    PostgreSQLDatabaseManager,
    AlphaStatus,
    AlphaFactor,
    BacktestResultProcessor,
)


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # 输出到控制台
        logging.FileHandler('concurrent_multi_backtest.log', encoding='utf-8'),  # 输出到文件
    ],
)
logger = logging.getLogger(__name__)  # 本模块日志器


class ConcurrentMultiBacktest:
    def __init__(self, config_file: str = 'config.json'):
        # 选择配置路径：优先使用传入 config.json；否则回退 concurrent_backtest_config.json
        self._config_path = config_file if os.path.exists(config_file) else (
            'concurrent_backtest_config.json' if os.path.exists('concurrent_backtest_config.json') else config_file
        )

        # 读取初始配置（数据库、WQB 凭据、并发参数等）
        self.config = ConcurrentBacktestConfig.load_from_file(self._config_path)

        # 数据库管理器（PostgreSQL 连接池）：读取 pending、回写结果、统计等
        self.db_manager = PostgreSQLDatabaseManager(
            host=self.config.db_host,
            port=self.config.db_port,
            database=self.config.db_name,
            user=self.config.db_user,
            password=self.config.db_password,
            max_connections=getattr(self.config, 'max_db_connections', 20),
        )

        # WQB 会话与日志器：首次使用时再初始化
        self.wqb_session = None
        self.wqb_logger = None

        # 运行期参数（支持热加载）：从配置读取，下一轮可被新配置覆盖
        self.poll_interval_seconds = int(getattr(self.config, 'poll_interval_seconds', 60))
        self.alpha_count_per_slot = int(getattr(self.config, 'alpha_count_per_slot', 10))
        self.concurrent_count = int(getattr(self.config, 'concurrent_count', 8))
        try:
            _bfl = getattr(self.config, 'backtest_fetch_limit', None)
            self.backtest_fetch_limit = int(_bfl) if (_bfl is not None and int(_bfl) > 0) else None
        except Exception:
            self.backtest_fetch_limit = None
        self.use_multi_sim = bool(getattr(self.config, 'use_multi_sim', True))  # 是否使用 MultiAlpha 分组回测
        # 运行时暂停标志（热加载），True 表示暂停调度新一轮回测
        self.paused = bool(getattr(self.config, 'pause', False))

        # 统计信息：累计抓取/处理/完成/失败，以及轮起始时间
        self.stats: Dict[str, Any] = {
            'total_loaded': 0,
            'total_processed': 0,
            'total_completed': 0,
            'total_failed': 0,
            'start_time': None,
        }
        # 输入序号 -> 数据库 alpha_id 映射（用于结果对齐与回写）
        self.alpha_id_mapping: Dict[int, int] = {}

        logger.info("Init ok (config=%s)", self._config_path)

    # --------------- 热加载运行参数 ---------------
    def reload_runtime_config(self) -> None:
        # 热加载运行参数：重新读取配置并更新并发/分组/抓取等关键参数；失败则保留旧值
        try:
            path = self._config_path
            if not os.path.exists(path):
                path = 'config.json' if os.path.exists('config.json') else path
            # 以 UTF-8-SIG 打开配置，兼容含 BOM 的文件
            with open(path, 'r', encoding='utf-8-sig') as f:
                data = json.load(f)

            def clamp_int(v, lo, hi, default):
                # 将输入收敛在 [lo, hi] 区间内，异常时回退默认值
                try:
                    v = int(v)
                    if v < lo or v > hi:
                        return default
                    return v
                except Exception:
                    return default

            def nz_int_or_none(v, default):
                # 将非正数视为 None；异常时回退默认值
                try:
                    if v is None:
                        return None
                    v = int(v)
                    return v if v > 0 else None
                except Exception:
                    return default

            new_concurrent = clamp_int(data.get('concurrent_count', self.concurrent_count), 1, 8, self.concurrent_count)
            new_alpha_per_slot = clamp_int(data.get('alpha_count_per_slot', self.alpha_count_per_slot), 1, 10, self.alpha_count_per_slot)
            new_fetch = nz_int_or_none(data.get('backtest_fetch_limit', self.backtest_fetch_limit), self.backtest_fetch_limit)
            try:
                new_poll = int(data.get('poll_interval_seconds', self.poll_interval_seconds))
                if new_poll <= 0:
                    new_poll = self.poll_interval_seconds
            except Exception:
                new_poll = self.poll_interval_seconds
            new_multi = bool(data.get('use_multi_sim', self.use_multi_sim))
            new_pause = bool(data.get('pause', self.paused))

            changes = []
            if new_concurrent != self.concurrent_count:
                changes.append(f"concurrent_count: {self.concurrent_count} -> {new_concurrent}")
                self.concurrent_count = new_concurrent
            if new_alpha_per_slot != self.alpha_count_per_slot:
                changes.append(f"alpha_count_per_slot: {self.alpha_count_per_slot} -> {new_alpha_per_slot}")
                self.alpha_count_per_slot = new_alpha_per_slot
            if new_fetch != self.backtest_fetch_limit:
                changes.append(f"backtest_fetch_limit: {self.backtest_fetch_limit} -> {new_fetch}")
                self.backtest_fetch_limit = new_fetch
            if new_poll != self.poll_interval_seconds:
                changes.append(f"poll_interval_seconds: {self.poll_interval_seconds} -> {new_poll}")
                self.poll_interval_seconds = new_poll
            if new_multi != self.use_multi_sim:
                changes.append(f"use_multi_sim: {self.use_multi_sim} -> {new_multi}")
                self.use_multi_sim = new_multi
            if new_pause != self.paused:
                changes.append(f"pause: {self.paused} -> {new_pause}")
                self.paused = new_pause

            if changes:
                logger.info("Hot-reload params: %s", "; ".join(changes))
        except Exception as e:
            logger.warning("Hot-reload failed, keep previous params: %s", e)

    # --------------- WQB 认证连通性检查 ---------------
    def test_authentication(self) -> bool:
        try:
            if self.wqb_session is None:
                logger.info("Init WQB session...")
                if self.wqb_logger is None:
                    self.wqb_logger = wqb.wqb_logger(name="wqb_concurrent")
                creds = (self.config.brain_username, self.config.brain_password)
                self.wqb_session = wqb.WQBSession(creds, logger=self.wqb_logger)
            resp = self.wqb_session.locate_field('open')
            logger.info("WQB locate_field('open') ok=%s", getattr(resp, 'ok', None))
            return bool(getattr(resp, 'ok', False))
        except Exception as e:
            logger.error("Auth error: %s", e)
            return False

    # --------------- 从 DB 读取待回测 Alphas ---------------
    def load_pending_alphas(self) -> List[AlphaFactor]:
        # pack 至少为 2（MultiAlpha 要求）；desired 由 fetch_limit 或 并发×pack 决定
        pack = max(2, self.alpha_count_per_slot)
        if self.backtest_fetch_limit is not None:
            desired = max(2, int(self.backtest_fetch_limit))  # 若设置了抓取上限则优先使用
        else:
            desired = max(2, self.concurrent_count) * pack  # 否则按并发×分组大小计算目标抓取量
        alphas = self.db_manager.get_alphas_by_status(AlphaStatus.PENDING, limit=desired)
        if not alphas:
            logger.debug("No pending alphas")  # 无待回测任务（降为调试级别，避免打扰正常回测日志）
            return []
        logger.info("Loaded %d pending alphas (target=%d)", len(alphas), desired)
        self.stats['total_loaded'] = len(alphas)  # 记录本轮抓取量
        return alphas

    def _alphas_to_wqb_items(self, alphas: List[AlphaFactor]) -> List[Dict[str, Any]]:
        # 将 AlphaFactor 转为 WQB 请求条目，并建立“输入序号→alpha_id”映射
        items: List[Dict[str, Any]] = []
        self.alpha_id_mapping.clear()
        for i, a in enumerate(alphas):
            # WQB 需要的字段：type（默认 REGULAR）、settings（dict）、regular（表达式）
            items.append({
                'type': a.alpha_type or 'REGULAR',
                'settings': a.settings or {},
                'regular': a.expression,
            })
            self.alpha_id_mapping[i] = a.id  # 记录输入序号对应的数据库主键
        return items

    # --------------- 执行单轮回测（读→打包→并发回测→落库） ---------------
    async def run_concurrent_backtests(self) -> bool:
        try:
            self.stats['start_time'] = datetime.now()  # 记录本轮开始时间
            if not self.test_authentication():
                logger.error("Auth failed, skip round")  # 认证失败，跳过本轮
                return False

            alphas = self.load_pending_alphas()
            if not alphas:
                return True

            pack = max(2, self.alpha_count_per_slot)
            total = len(alphas)
            batches = total // pack
            remainder = total % pack
            drop_last = (remainder == 1 and total > 1)  # 余数为 1：丢 1 条避免单条组（WQB 400）
            drop_n = 1 if drop_last else 0
            if drop_last:
                logger.info("To avoid multi-sim tail size=1, drop last 1 for next round")
            logger.info("Plan this round: total=%d, pack=%d, batches=%d, remainder=%d, drop=%d",
                        total, pack, batches, remainder, drop_n)

            target_count = (self.backtest_fetch_limit if self.backtest_fetch_limit is not None else (max(2, self.concurrent_count) * pack))
            logger.info("Params: concurrency=%d, pack=%d, target=%d, multi=%s",
                        self.concurrent_count, pack, target_count, self.use_multi_sim)

            alphas_run = alphas[:-1] if drop_last else alphas  # 如需丢 1，则本轮不处理最后 1 条
            items = self._alphas_to_wqb_items(alphas_run)

            started_at = datetime.now()
            used_resps: List[Any] = []
            if self.use_multi_sim:
                # 使用 WQB 工具按 pack 分组为 MultiAlpha 列表
                multi = list(wqb.to_multi_alphas(items, pack))
                # 仅保留长度≥2 的组，避免 WQB 400（单个 alpha 的组不被接受）
                safe_multi: List[Any] = [m for m in multi if isinstance(m, list) and len(m) >= 2]
                if safe_multi:
                    # 统一、精简的开始日志，并声明进度频率
                    # 统一、精简的开始日志，并声明进度频率
                    progress_gap = int(getattr(self.config, "progress_log_gap", 10))
                    logger.info("开始并发回测: 组数=%d, 线程=%d, 每%d组打印进度",
                                len(safe_multi), self.concurrent_count, progress_gap)
                    
                    # 并发提交本轮 MultiAlpha 回测，获取各组响应
                    resps_multi = await self.wqb_session.concurrent_simulate(
                        safe_multi,
                        concurrency=self.concurrent_count,
                        log='multi-sim',
                        log_gap=progress_gap,
                    )

                    ok_cnt = 0
                    flat: List[Any] = []
                    extracted_ids_log: List[str] = []
                    for i, resp in enumerate(resps_multi):
                        # 对齐 children 数量到输入组大小，保障一一对应
                        try:
                            child_cnt = len(safe_multi[i])
                        except Exception:
                            child_cnt = pack
                        if hasattr(resp, 'ok') and getattr(resp, 'ok'):
                            ok_cnt += 1
                            try:
                                jd = resp.json()
                                if isinstance(jd, dict) and isinstance(jd.get('children'), list):
                                    if len(jd['children']) == child_cnt:
                                        flat.extend(jd['children'])
                                        # 日志辅助：尝试直接拿到 alpha / alpha_id（部分环境可能直接返回）
                                        alpha_direct = jd.get('alpha') or jd.get('alpha_id')
                                        if alpha_direct:
                                            extracted_ids_log.append(str(alpha_direct))
                                    else:
                                        flat.extend([resp] * child_cnt)
                                else:
                                    # 非 children 返回，尝试 alpha 字段
                                    alpha_direct = None
                                    try:
                                        if isinstance(jd, dict):
                                            alpha_direct = jd.get('alpha') or jd.get('alpha_id')
                                    except Exception:
                                        alpha_direct = None
                                    if alpha_direct:
                                        extracted_ids_log.append(str(alpha_direct))
                                    flat.extend([resp] * child_cnt)
                            except Exception:
                                flat.extend([resp] * child_cnt)
                        else:
                            flat.extend([None] * max(1, child_cnt))
                    logger.info("Multi-sim response: ok=%d/%d, flattened=%d", ok_cnt, len(resps_multi), len(flat))
                    if extracted_ids_log:
                        logger.info("Direct alpha_ids extracted: %s", ",".join(extracted_ids_log))
                    used_resps.extend(flat)
            else:
                logger.info("Single-sim mode size=%d (not recommended)", len(items))
                for it in items:
                    resp = await self.wqb_session.simulate(it)
                    used_resps.append(resp)

            # 映射扁平响应 → Alpha，处理并回写结果
            processed: List[AlphaFactor] = []
            elapsed = (datetime.now() - started_at).total_seconds()
            if elapsed <= 0:
                elapsed = 0.01
            for idx, a in enumerate(alphas_run):
                r: Any = used_resps[idx] if idx < len(used_resps) else None
                if r is not None and hasattr(r, 'ok') and not getattr(r, 'ok') and idx < 3:
                    try:
                        sc = getattr(r, 'status_code', 'NA')
                        txt = getattr(r, 'text', '')
                        logger.warning("Resp failed [%d/%d]: status=%s, text=%.200s", idx+1, len(alphas_run), sc, txt)
                    except Exception:
                        pass
                updated = BacktestResultProcessor.process_wqb_response(a, r, started_at, elapsed)
                processed.append(updated)

            # 批量回写数据库，避免逐条写入的性能损耗
            self.db_manager.update_backtest_results(processed)

            completed = sum(1 for a in processed if a.backtest_status == AlphaStatus.COMPLETED)
            failed = sum(1 for a in processed if a.backtest_status == AlphaStatus.FAILED)
            self.stats['total_processed'] += len(processed)
            self.stats['total_completed'] += completed
            self.stats['total_failed'] += failed
            throughput = (len(processed) / elapsed) if elapsed > 0.5 else 0.0

            # 估算 ETA：基于当前吞吐（条/秒）与剩余 pending 数量预估完成时间
            eta_txt = "N/A"
            try:
                stats = self.db_manager.get_statistics() or {}
                pending_left = int((stats.get('status_counts') or {}).get('pending', 0))
                if throughput > 0 and pending_left > 0:
                    eta_sec = int(pending_left / throughput)
                    h, rem = divmod(eta_sec, 3600)
                    m, s = divmod(rem, 60)
                    eta_txt = f"~{h:02d}:{m:02d}:{s:02d}"
            except Exception:
                pass

            logger.info(
                "Round done: ok=%d, fail=%d, time=%.2fs, rps=%.1f, ETA=%s",
                completed, failed, elapsed, throughput, eta_txt,
            )
            return True
        except Exception as e:
            logger.error("Run error: %s", e)
            return False
    
    def print_final_stats(self) -> None:
        # 打印最终统计：总抓取/处理/完成/失败与成功率
        logger.info("=" * 60)
        logger.info(
            "Final: loaded=%d, processed=%d, completed=%d, failed=%d",
            self.stats['total_loaded'], self.stats['total_processed'],
            self.stats['total_completed'], self.stats['total_failed'],
        )
        if self.stats['total_processed']:
            rate = self.stats['total_completed'] / self.stats['total_processed'] * 100
            logger.info("Success rate: %.1f%%", rate)
        logger.info("=" * 60)

    # --------------- 轮询运行（持续拉取与回测） ---------------
    async def run_forever(self) -> bool:
        # 轮询模式：无 pending 则休眠并热加载；有任务则跑一轮、短暂等待后继续
        if not self.test_authentication():  # 认证失败则不进入轮询
            return False
        logger.info("Enter polling: idle sleep %ds", self.poll_interval_seconds)
        try:
            while True:
                # 若处于暂停状态，则不调度新一轮，等待并热加载直至恢复
                if getattr(self, 'paused', False):
                    logger.info("Paused; not scheduling new backtests. Sleeping %ds...", self.poll_interval_seconds)
                    await asyncio.sleep(self.poll_interval_seconds)
                    self.reload_runtime_config()
                    continue
                alphas = self.load_pending_alphas()
                if not alphas:
                    await asyncio.sleep(self.poll_interval_seconds)  # 空闲：按配置休眠
                    self.reload_runtime_config()
                    continue
                await self.run_concurrent_backtests()
                await asyncio.sleep(1)  # 每轮结束短暂等待
                self.reload_runtime_config()
        except KeyboardInterrupt:
            logger.info("SIGINT: finishing current round then exit...")
            return True
        except asyncio.CancelledError:
            logger.info("Cancelled: graceful exit")
            return True
        except Exception as e:
            logger.error("Polling error: %s", e)
            await asyncio.sleep(self.poll_interval_seconds)
            self.reload_runtime_config()
            return False


async def main() -> None:
    # 一次性运行入口：执行单轮回测并退出（支持 --config 指定配置路径）
    import argparse
    parser = argparse.ArgumentParser(description='Concurrent Multi-Backtest (PostgreSQL + WQB)')
    parser.add_argument('--config', type=str, default='config.json', help='Path to config file')
    args = parser.parse_args()
    system = ConcurrentMultiBacktest(args.config)
    try:
        ok = await system.run_concurrent_backtests()
        logger.info("Done" if ok else "Failed")
    finally:
        try:
            system.db_manager.connection_pool.close_all()
        except Exception:
            pass


async def main_polling() -> None:
    # 轮询运行入口：持续执行 run_forever，直到手动中断
    import argparse
    parser = argparse.ArgumentParser(description='Concurrent Multi-Backtest (Polling)')
    parser.add_argument('--config', type=str, default='config.json', help='Path to config file')
    args = parser.parse_args()
    system = ConcurrentMultiBacktest(args.config)
    try:
        ok = await system.run_forever()
        logger.info("Exit polling ok" if ok else "Exit polling with error")
    finally:
        system.print_final_stats()
        try:
            system.db_manager.connection_pool.close_all()
        except Exception:
            pass


def run_main() -> None:
    # 同步封装：供 __main__ 调用，默认运行轮询版本
    asyncio.run(main_polling())


if __name__ == '__main__':
    run_main()
