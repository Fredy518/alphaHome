#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PIT 财务缺口多源补缺：用 Tinysoft 表44/48 补 tushare 漏采期。

适用场景：tushare 对某股某报告期 source_missing（上游漏采），但该股利润表有该期
（说明公司已披露），且天软表44/48有该期数据。支持两类缺口：
  --table cashflow  : 表48 现金流量表 -> pit.pit_cashflow_quarterly
  --table balance   : 表44 资产负债表 -> pit.pit_balance_quarterly

PIT 时点一致性原则：
- 数值取自天软表44/48（按截止日匹配报告期）
- ann_date 复用 pit.pit_income_quarterly 同股同期的 ann_date（tushare f_ann_date），
  保证三表时点一致；天软"公布日"是数据更新日，不可直接当 ann_date。
- data_source='tinysoft'，主键 (ts_code,end_date,ann_date,data_source) 不与 'report' 冲突。

字段映射单一来源：从 TinySoftStockFinaPitExtTask.default_metric_profiles 读取，
无需在此重复维护。补缺脚本与 fina_pit_ext 任务共享同一份字段定义。

用法：
    # dry-run（默认，只打印不写库）
    python scripts/analysis/pit_cashflow_tinysoft_fill.py --ts-code 002549.SZ --end-date 2022-06-30 --table cashflow
    # 实际写入
    python scripts/analysis/pit_cashflow_tinysoft_fill.py --ts-code 002549.SZ --end-date 2022-06-30 --table cashflow --apply
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import date as date_type
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from alphahome.common.config_manager import ConfigManager
from alphahome.common.db_manager import DBManager
from alphahome.fetchers.sources.tinysoft.tinysoft_opi_api import TinySoftOPIAPI
from alphahome.fetchers.tasks.stock.tinysoft_stock_fina_pit_ext import TinySoftStockFinaPitExtTask
from alphahome.fetchers.tasks.stock.tinysoft_stock_minute import ts_code_to_tinysoft_symbol

# 目标表配置：天软表ID -> PIT表名 + profile finance_source
TABLE_CONFIG = {
    "cashflow": {
        "tinysoft_table_id": 48,
        "pit_table": "pit_cashflow_quarterly",
        "finance_source": "report_48_cashflow",
        "has_year_quarter": True,
    },
    "balance": {
        "tinysoft_table_id": 44,
        "pit_table": "pit_balance_quarterly",
        "finance_source": "report_44_balancesheet",
        "has_year_quarter": True,
    },
}


def _load_field_map(finance_source: str) -> Dict[str, str]:
    """从 TinySoftStockFinaPitExtTask.default_metric_profiles 读取字段映射。"""
    for profile in TinySoftStockFinaPitExtTask.default_metric_profiles:
        if profile["finance_source"] == finance_source:
            return {m["field_name"]: m["metric_name"] for m in profile["metric_defs"]}
    raise ValueError(f"未找到 finance_source={finance_source} 的 metric profile")


def _build_opi_api(cfg: Dict[str, Any]) -> TinySoftOPIAPI:
    return TinySoftOPIAPI(
        user=cfg.get("user"),
        password=cfg.get("password"),
        opi_url=cfg.get("opi_url") or "https://opi.tinysoft.com.cn",
        auth_mode=str(cfg.get("opi_auth_mode") or "basic"),
        session_key=cfg.get("session_key"),
        session_password=cfg.get("session_password"),
        service=str(cfg.get("service") or ""),
        timeout_ms=int(cfg.get("timeout_ms", 60000) or 60000),
        request_interval=float(cfg.get("request_interval", 0.2) or 0.2),
    )


async def fetch_tinysoft_values(
    api: TinySoftOPIAPI,
    ts_code: str,
    end_date_yyyymmdd: int,
    table_id: int,
    field_map: Dict[str, str],
) -> Optional[Dict[str, Any]]:
    """从天软指定表取指定股指定报告期，返回 {pit_field: value}。"""
    stock = ts_code_to_tinysoft_symbol(ts_code)
    where = f'["截止日"]={end_date_yyyymmdd}'
    df = await api.call_dataframe(
        "infoarray",
        table_id,
        stock=stock,
        where_clause=where,
        fields=list(field_map.keys()),
        timeout_ms=120000,
    )
    if df is None or df.empty:
        return None
    row = df.iloc[0]
    result: Dict[str, Any] = {}
    for ts_col, pit_field in field_map.items():
        if ts_col in df.columns:
            v = row[ts_col]
            # 过滤 NaN 和 0（天软对未披露科目常填 0，不应覆盖 PIT 留空语义）
            if pd.notna(v) and v != 0:
                result[pit_field] = float(v)
    return result


async def get_ann_date_from_income(
    db: DBManager, ts_code: str, end_date: date_type
) -> Optional[date_type]:
    """复用 pit.pit_income_quarterly 同股同期的 ann_date。"""
    rows = await db.fetch(
        """
        SELECT ann_date
        FROM pit.pit_income_quarterly
        WHERE ts_code = $1 AND end_date = $2
        ORDER BY ann_date
        LIMIT 1
        """,
        ts_code,
        end_date,
    )
    if not rows:
        return None
    return rows[0]["ann_date"]


async def check_existing_report(
    db: DBManager, pit_table: str, ts_code: str, end_date: date_type
) -> bool:
    """检查目标 PIT 表是否已有该期 report 源（有则不该补）。"""
    rows = await db.fetch(
        f"""
        SELECT 1
        FROM pit.{pit_table}
        WHERE ts_code = $1 AND end_date = $2 AND data_source = 'report'
        LIMIT 1
        """,
        ts_code,
        end_date,
    )
    return bool(rows)


async def upsert_pit_row(
    db: DBManager,
    pit_table: str,
    ts_code: str,
    end_date: date_type,
    ann_date: date_type,
    values: Dict[str, Any],
    quarter: int,
    apply: bool,
) -> None:
    """upsert 一条 data_source='tinysoft' 的记录到目标 PIT 表。"""
    year = end_date.year
    fields = (
        ["ts_code", "end_date", "ann_date", "year", "quarter", "data_source"]
        + list(values.keys())
    )
    field_list = ", ".join(fields)
    placeholders = ", ".join(f"${i+1}" for i in range(len(fields)))
    update_fields = [f for f in fields if f not in ("ts_code", "end_date", "ann_date", "data_source")]
    update_list = ", ".join(f"{f} = EXCLUDED.{f}" for f in update_fields)
    params: List[Any] = [ts_code, end_date, ann_date, year, quarter, "tinysoft"]
    params.extend(values[f] for f in list(values.keys()))
    sql = f"""
    INSERT INTO pit.{pit_table} ({field_list})
    VALUES ({placeholders})
    ON CONFLICT (ts_code, end_date, ann_date, data_source) DO UPDATE SET
        {update_list},
        updated_at = CURRENT_TIMESTAMP
    """
    if apply:
        await db.execute(sql, *params)
        print(f"  [已写入] pit.{pit_table}: {ts_code} / {end_date} / ann={ann_date} / source=tinysoft")
    else:
        print(f"  [DRY-RUN] 将写入: pit.{pit_table}: {ts_code} / {end_date} / ann={ann_date} / source=tinysoft")
        print(f"  [DRY-RUN] 字段({len(values)}): {', '.join(values.keys())}")
        print(f"  [DRY-RUN] SQL: INSERT INTO pit.{pit_table} ({field_list}) ... ON CONFLICT DO UPDATE")


def _to_date(value: str) -> date_type:
    return date_type.fromisoformat(value)


def _quarter_from_end_date(end_date: date_type) -> int:
    return (end_date.month - 1) // 3 + 1


async def main() -> int:
    parser = argparse.ArgumentParser(description="PIT财务缺口天软补缺（cashflow/balance）")
    parser.add_argument("--ts-code", required=True, help="股票代码，如 002549.SZ")
    parser.add_argument("--end-date", required=True, help="报告期截止日 YYYY-MM-DD")
    parser.add_argument(
        "--table",
        required=True,
        choices=list(TABLE_CONFIG.keys()),
        help="补缺目标表类型",
    )
    parser.add_argument("--apply", action="store_true", help="实际写入（默认 dry-run）")
    args = parser.parse_args()

    tc = TABLE_CONFIG[args.table]
    pit_table = tc["pit_table"]
    table_id = tc["tinysoft_table_id"]
    field_map = _load_field_map(tc["finance_source"])

    ts_code = args.ts_code
    end_date = _to_date(args.end_date)
    end_date_int = int(args.end_date.replace("-", ""))
    quarter = _quarter_from_end_date(end_date)

    print(f"=== PIT {args.table} 缺口天软补缺 ===")
    print(f"目标: {ts_code} / {end_date} (Q{quarter}) -> pit.{pit_table} (表{table_id})")
    print(f"模式: {'写入' if args.apply else 'DRY-RUN（不写库）'}")

    cfg = ConfigManager().get_tinysoft_config() or {}
    db_url = ConfigManager().get_database_url()
    if not db_url:
        print("ERROR: 未读取到数据库连接串")
        return 1

    db = DBManager(db_url)
    await db.connect()
    api = _build_opi_api(cfg)

    try:
        # 1. 检查是否已有 report 源（有则不该补）
        if await check_existing_report(db, pit_table, ts_code, end_date):
            print(f"  跳过: pit.{pit_table} 已有 {ts_code}/{end_date} 的 report 源，无需补缺。")
            return 0

        # 2. 从天软取数
        print(f"  从天软表{table_id}取数: {ts_code} / 截止日={end_date_int}")
        values = await fetch_tinysoft_values(api, ts_code, end_date_int, table_id, field_map)
        if not values:
            print(f"  跳过: 天软表{table_id}无 {ts_code}/{end_date} 数据，无法补缺。")
            return 0
        print(f"  天软取到 {len(values)} 个字段:")
        for k, v in values.items():
            print(f"    {k} = {v}")

        # 3. 复用 income 表的 ann_date
        ann_date = await get_ann_date_from_income(db, ts_code, end_date)
        if not ann_date:
            print(f"  跳过: pit.pit_income_quarterly 无 {ts_code}/{end_date} 的 ann_date，无法确定披露日。")
            return 0
        print(f"  ann_date (复用 income 表): {ann_date}")

        # 4. upsert
        await upsert_pit_row(db, pit_table, ts_code, end_date, ann_date, values, quarter, args.apply)
        return 0
    finally:
        await api.close()
        await db.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
