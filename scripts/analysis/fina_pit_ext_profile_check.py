#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""验证 fina_pit_ext 表48/44 profile 端到端取数+process（不落库）。"""
from __future__ import annotations
import asyncio, sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from alphahome.common.config_manager import ConfigManager
from alphahome.fetchers.tasks.stock.tinysoft_stock_fina_pit_ext import TinySoftStockFinaPitExtTask
from alphahome.fetchers.tasks.stock.tinysoft_stock_minute import ts_code_to_tinysoft_symbol


async def run_profile(ts_code: str, table_id: int, finance_source: str, start_date: str) -> None:
    cfg = ConfigManager().get_tinysoft_config() or {}
    # 构造只含目标 profile 的 metric_profiles
    target = next(p for p in TinySoftStockFinaPitExtTask.default_metric_profiles if p["table_id"] == table_id)
    metric_profiles = [{"finance_source": finance_source, "table_id": table_id, "metric_defs": target["metric_defs"]}]

    task = TinySoftStockFinaPitExtTask(db_connection=None, tinysoft_config=cfg)
    try:
        batches = await task.get_batch_list(
            ts_codes=[ts_code], start_date=start_date, end_date="2024-01-01",
            metric_profiles=metric_profiles, use_config_symbols=False,
        )
        print(f"[{finance_source}] batches={len(batches)}")
        if not batches:
            return
        # 只跑第一个 batch
        raw = await task.fetch_batch(batches[0])
        if raw is None or raw.empty:
            print(f"  fetch_batch 返回空")
            return
        print(f"  fetch_batch 返回 {len(raw)} 行, 列示例: {list(raw.columns)[:6]}")
        processed = task.process_data(raw, start_date=start_date, end_date="2024-01-01")
        if processed is None or processed.empty:
            print(f"  process_data 返回空")
            return
        print(f"  process_data 返回 {len(processed)} 行")
        # 显示 20220630 这期的现金流字段
        if "report_date" in processed.columns:
            sub = processed[processed["metric_name"].isin(["n_cashflow_act", "n_cashflow_inv_act", "n_cash_flows_fnc_act", "net_profit"])]
            print(f"  关键指标样本:")
            for _, r in sub.head(20).iterrows():
                rd = r.get("report_date")
                print(f"    {rd} {r['metric_name']}={r['metric_value']}")
    finally:
        if hasattr(task, 'api') and task.api:
            await task.api.close()


async def main() -> int:
    ts_code = "002549.SZ"
    print(f"=== 验证 fina_pit_ext 表48 现金流 profile: {ts_code} ===")
    await run_profile(ts_code, 48, "report_48_cashflow", "2022-01-01")
    print()
    print(f"=== 验证 fina_pit_ext 表44 资产负债表 profile: {ts_code} ===")
    await run_profile(ts_code, 44, "report_44_balancesheet", "2022-01-01")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
