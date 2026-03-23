#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
修复 tushare.fund_share 的 SZ 日期错位（按交易日，而非自然日）。

核心思路：
1) 先做实时探测（仅 start_date / start_date+end_date）判断 bug 是否存在；
2) 若存在，使用交易日历把 SZ 日期映射到前/后一个交易日；
3) 支持两种数据状态：
   - raw: 未做过“自然日 shift”修复，直接按交易日纠偏；
   - natural_shifted: 已做过“自然日 shift”修复，先反向回滚，再按交易日纠偏；
   - auto: 根据 SZ 非交易日占比自动判断状态。

用法：
  python scripts/maintenance/fix_tushare_fund_share_sz_trade_date.py
  python scripts/maintenance/fix_tushare_fund_share_sz_trade_date.py --dry-run
  python scripts/maintenance/fix_tushare_fund_share_sz_trade_date.py --source-state raw
  python scripts/maintenance/fix_tushare_fund_share_sz_trade_date.py --source-state natural_shifted
  python scripts/maintenance/fix_tushare_fund_share_sz_trade_date.py --force-shift-days -1
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict

import pandas as pd

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from alphahome.common.config_manager import get_database_url, get_tushare_token
from alphahome.common.db_manager import create_async_manager
from alphahome.fetchers.sources.tushare.tushare_api import TushareAPI
from alphahome.fetchers.tools.calendar import get_last_trade_day, get_next_trade_day


def quote_ident(name: str) -> str:
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name):
        raise ValueError(f"非法标识符: {name}")
    return f'"{name}"'


def normalize_probe_df(df: pd.DataFrame | None) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["ts_code", "trade_date", "fd_share", "exchange"])

    out = df.copy()
    required = {"ts_code", "trade_date", "fd_share"}
    if not required.issubset(out.columns):
        return pd.DataFrame(columns=["ts_code", "trade_date", "fd_share", "exchange"])

    out["ts_code"] = out["ts_code"].astype(str)
    out["trade_date"] = pd.to_datetime(out["trade_date"], errors="coerce")
    out["fd_share"] = pd.to_numeric(out["fd_share"], errors="coerce").round(6)
    out["exchange"] = out["ts_code"].str[-2:]
    out = out.dropna(subset=["ts_code", "trade_date", "fd_share"])
    return out[["ts_code", "trade_date", "fd_share", "exchange"]]


def build_exchange_mode_summary(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    if df.empty:
        return {}

    per_code_max = (
        df.groupby(["exchange", "ts_code"], as_index=False)["trade_date"]
        .max()
    )

    summary: Dict[str, Dict[str, Any]] = {}
    for exchange, group in per_code_max.groupby("exchange"):
        counts = group["trade_date"].value_counts(dropna=True)
        if counts.empty:
            continue
        mode_date = pd.Timestamp(counts.index[0]).normalize()
        summary[exchange] = {
            "mode_date": mode_date,
            "mode_ratio": float(counts.iloc[0] / len(group)),
            "code_count": int(len(group)),
        }
    return summary


async def infer_shift_days(
    summary: Dict[str, Dict[str, Any]],
    threshold: float = 0.75,
) -> int:
    """
    返回“交易日维度”的修正方向：
    - -1: SZ 日期偏未来一个交易日，修正应回拨 1 个交易日
    - +1: SZ 日期偏过去一个交易日，修正应前移 1 个交易日
    -  0: 未探测到稳定的一日错位
    """
    sz = summary.get("SZ")
    sh = summary.get("SH")
    if not sz or not sh:
        return 0
    if sz["mode_ratio"] < threshold or sh["mode_ratio"] < threshold:
        return 0

    sz_mode = pd.Timestamp(sz["mode_date"]).normalize()
    sh_mode = pd.Timestamp(sh["mode_date"]).normalize()

    if sz_mode == sh_mode:
        return 0

    sz_mode_str = sz_mode.strftime("%Y%m%d")
    prev_trade = await get_last_trade_day(sz_mode_str, n=1, exchange="SZSE")
    next_trade = await get_next_trade_day(sz_mode_str, n=1, exchange="SZSE")

    if prev_trade and pd.to_datetime(prev_trade).normalize() == sh_mode:
        return -1
    if next_trade and pd.to_datetime(next_trade).normalize() == sh_mode:
        return 1

    # 兜底：交易日历不足时退化为自然日比较
    delta_days = int((sz_mode - sh_mode).days)
    if delta_days == 1:
        return -1
    if delta_days == -1:
        return 1
    return 0


def format_summary(summary: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for exchange, item in summary.items():
        out[exchange] = {
            "mode_date": item["mode_date"].strftime("%Y-%m-%d"),
            "mode_ratio": round(float(item["mode_ratio"]), 4),
            "code_count": int(item["code_count"]),
        }
    return out


async def probe_fd_share_bug(token: str, lookback_days: int) -> Dict[str, Any]:
    api = TushareAPI(token)

    end_dt = pd.Timestamp.now().normalize()
    start_dt = end_dt - pd.Timedelta(days=lookback_days)
    start_date = start_dt.strftime("%Y%m%d")
    end_date = end_dt.strftime("%Y%m%d")

    common = {
        "api_name": "fund_share",
        "fields": ["ts_code", "trade_date", "fd_share"],
        "limit": 2000,
    }

    start_only_raw = await api.query(start_date=start_date, **common)
    with_end_raw = await api.query(start_date=start_date, end_date=end_date, **common)

    start_only = normalize_probe_df(start_only_raw)
    with_end = normalize_probe_df(with_end_raw)

    if start_only.empty and with_end.empty:
        return {
            "probe_ok": False,
            "error": "start_only 与 with_end 均为空",
        }

    start_only_summary = build_exchange_mode_summary(start_only)
    with_end_summary = build_exchange_mode_summary(with_end)

    shift_start_only = await infer_shift_days(start_only_summary)
    shift_with_end = await infer_shift_days(with_end_summary)
    shift_days = shift_start_only if shift_start_only != 0 else shift_with_end
    bug_detected = shift_days != 0

    start_end_comparison_not_revealing = (
        shift_start_only != 0 and shift_start_only == shift_with_end
    )

    return {
        "probe_ok": True,
        "start_date": start_date,
        "end_date": end_date,
        "rows_start_only": int(len(start_only)),
        "rows_with_end": int(len(with_end)),
        "shift_start_only": int(shift_start_only),
        "shift_with_end": int(shift_with_end),
        "shift_days": int(shift_days),
        "bug_detected": bool(bug_detected),
        "start_end_comparison_not_revealing": bool(start_end_comparison_not_revealing),
        "start_only_summary": format_summary(start_only_summary),
        "with_end_summary": format_summary(with_end_summary),
    }


async def get_sz_stats(db, full_table: str) -> Dict[str, int]:
    sz_count = int(
        await db.fetch_val(
            f"SELECT COUNT(*) FROM {full_table} WHERE ts_code LIKE '%.SZ'"
        )
        or 0
    )
    non_trade_count = int(
        await db.fetch_val(
            f"""
            SELECT COUNT(*)
            FROM {full_table} s
            LEFT JOIN tushare.others_calendar c
              ON c.exchange = 'SZSE'
             AND c.cal_date = s.trade_date
            WHERE s.ts_code LIKE '%.SZ'
              AND COALESCE(c.is_open, 0) <> 1
            """
        )
        or 0
    )
    return {
        "sz_count": sz_count,
        "non_trade_count": non_trade_count,
    }


def resolve_source_state(
    source_state: str,
    sz_count: int,
    non_trade_count: int,
) -> str:
    if source_state in ("raw", "natural_shifted"):
        return source_state

    if sz_count <= 0:
        return "raw"

    ratio = non_trade_count / sz_count
    # 经验阈值：若 SZ 非交易日占比明显偏高，通常意味着做过自然日 shift
    return "natural_shifted" if ratio >= 0.05 else "raw"


async def repair_database(
    schema: str,
    table: str,
    shift_days: int,
    dry_run: bool,
    source_state: str,
) -> None:
    db_url = get_database_url()
    if not db_url:
        raise RuntimeError("DATABASE_URL 未配置")

    full_table = f"{quote_ident(schema)}.{quote_ident(table)}"
    db = create_async_manager(db_url)
    await db.connect()

    try:
        total_before = int(await db.fetch_val(f"SELECT COUNT(*) FROM {full_table}") or 0)
        sz_stat_before = await db.fetch_one(
            f"""
            SELECT
                COUNT(*) AS cnt,
                MIN(trade_date) AS min_date,
                MAX(trade_date) AS max_date
            FROM {full_table}
            WHERE ts_code LIKE '%.SZ'
            """
        )
        sz_stats = await get_sz_stats(db, full_table)
        actual_source_state = resolve_source_state(
            source_state=source_state,
            sz_count=sz_stats["sz_count"],
            non_trade_count=sz_stats["non_trade_count"],
        )

        print(f"[DB] total rows before: {total_before}")
        print(
            f"[DB] SZ rows before: {sz_stat_before['cnt']}, "
            f"date_range=[{sz_stat_before['min_date']} ~ {sz_stat_before['max_date']}]"
        )
        print(
            "[DB] SZ non-trading rows before: "
            f"{sz_stats['non_trade_count']}/{sz_stats['sz_count']}"
        )
        print(f"[DB] planned trade-day shift for SZ: {shift_days}")
        print(
            f"[DB] source_state requested={source_state}, "
            f"resolved={actual_source_state}"
        )

        async with db.transaction() as conn:
            await conn.execute(
                """
                CREATE TEMP TABLE tmp_fund_share_sz_base
                (
                    ts_code VARCHAR(15) NOT NULL,
                    trade_date DATE NOT NULL,
                    fd_share NUMERIC(20,2),
                    update_time TIMESTAMP
                )
                ON COMMIT DROP
                """
            )

            if actual_source_state == "natural_shifted":
                # 先回滚历史上可能做过的“自然日 shift”
                reverse_natural_days = -shift_days
                await conn.execute(
                    f"""
                    INSERT INTO tmp_fund_share_sz_base (ts_code, trade_date, fd_share, update_time)
                    SELECT
                        ts_code,
                        (trade_date + ($1::int || ' day')::interval)::date AS trade_date,
                        fd_share,
                        update_time
                    FROM {full_table}
                    WHERE ts_code LIKE '%.SZ'
                    """,
                    reverse_natural_days,
                )
            else:
                await conn.execute(
                    f"""
                    INSERT INTO tmp_fund_share_sz_base (ts_code, trade_date, fd_share, update_time)
                    SELECT ts_code, trade_date, fd_share, update_time
                    FROM {full_table}
                    WHERE ts_code LIKE '%.SZ'
                    """
                )

            await conn.execute(
                """
                CREATE TEMP TABLE tmp_fund_share_sz_date_map
                (
                    src_date DATE PRIMARY KEY,
                    dst_date DATE NOT NULL
                )
                ON COMMIT DROP
                """
            )

            if shift_days < 0:
                await conn.execute(
                    """
                    INSERT INTO tmp_fund_share_sz_date_map (src_date, dst_date)
                    WITH distinct_dates AS (
                        SELECT DISTINCT trade_date AS src_date
                        FROM tmp_fund_share_sz_base
                    )
                    SELECT
                        d.src_date,
                        COALESCE(
                            (
                                SELECT c.cal_date
                                FROM tushare.others_calendar c
                                WHERE c.exchange = 'SZSE'
                                  AND c.is_open = 1
                                  AND c.cal_date < d.src_date
                                ORDER BY c.cal_date DESC
                                LIMIT 1
                            ),
                            d.src_date
                        ) AS dst_date
                    FROM distinct_dates d
                    """
                )
            else:
                await conn.execute(
                    """
                    INSERT INTO tmp_fund_share_sz_date_map (src_date, dst_date)
                    WITH distinct_dates AS (
                        SELECT DISTINCT trade_date AS src_date
                        FROM tmp_fund_share_sz_base
                    )
                    SELECT
                        d.src_date,
                        COALESCE(
                            (
                                SELECT c.cal_date
                                FROM tushare.others_calendar c
                                WHERE c.exchange = 'SZSE'
                                  AND c.is_open = 1
                                  AND c.cal_date > d.src_date
                                ORDER BY c.cal_date ASC
                                LIMIT 1
                            ),
                            d.src_date
                        ) AS dst_date
                    FROM distinct_dates d
                    """
                )

            await conn.execute(
                """
                CREATE TEMP TABLE tmp_fund_share_sz_shifted
                ON COMMIT DROP
                AS
                SELECT ts_code, trade_date, fd_share, update_time
                FROM (
                    SELECT
                        b.ts_code,
                        m.dst_date AS trade_date,
                        b.fd_share,
                        b.update_time,
                        b.trade_date AS src_trade_date,
                        ROW_NUMBER() OVER (
                            PARTITION BY b.ts_code, m.dst_date
                            ORDER BY b.update_time DESC NULLS LAST, b.trade_date DESC
                        ) AS rn
                    FROM tmp_fund_share_sz_base b
                    JOIN tmp_fund_share_sz_date_map m
                      ON b.trade_date = m.src_date
                ) t
                WHERE t.rn = 1
                """
            )

            base_cnt = int(
                await conn.fetchval("SELECT COUNT(*) FROM tmp_fund_share_sz_base") or 0
            )
            map_cnt = int(
                await conn.fetchval("SELECT COUNT(*) FROM tmp_fund_share_sz_date_map") or 0
            )
            shifted_cnt = int(
                await conn.fetchval("SELECT COUNT(*) FROM tmp_fund_share_sz_shifted") or 0
            )
            dedup_dropped = base_cnt - shifted_cnt

            sample_map = await conn.fetch(
                """
                SELECT src_date, dst_date
                FROM tmp_fund_share_sz_date_map
                ORDER BY src_date DESC
                LIMIT 10
                """
            )
            print(
                f"[DB] temp stats: base={base_cnt}, map_dates={map_cnt}, "
                f"shifted={shifted_cnt}, dedup_dropped={dedup_dropped}"
            )
            if sample_map:
                print("[DB] mapping sample (latest):")
                for r in sample_map:
                    print(f"  {r['src_date']} -> {r['dst_date']}")

            if dry_run:
                print("[DB] dry-run 模式，不执行写入")
            else:
                await conn.execute(f"DELETE FROM {full_table} WHERE ts_code LIKE '%.SZ'")
                await conn.execute(
                    f"""
                    INSERT INTO {full_table} (ts_code, trade_date, fd_share, update_time)
                    SELECT ts_code, trade_date, fd_share, update_time
                    FROM tmp_fund_share_sz_shifted
                    """
                )
                print("[DB] write completed: replaced SZ rows with trade-day corrected rows")

        if dry_run:
            return

        total_after = int(await db.fetch_val(f"SELECT COUNT(*) FROM {full_table}") or 0)
        sz_stat_after = await db.fetch_one(
            f"""
            SELECT
                COUNT(*) AS cnt,
                MIN(trade_date) AS min_date,
                MAX(trade_date) AS max_date
            FROM {full_table}
            WHERE ts_code LIKE '%.SZ'
            """
        )
        sz_stats_after = await get_sz_stats(db, full_table)

        print(f"[DB] total rows after: {total_after}")
        print(
            f"[DB] SZ rows after: {sz_stat_after['cnt']}, "
            f"date_range=[{sz_stat_after['min_date']} ~ {sz_stat_after['max_date']}]"
        )
        print(
            "[DB] SZ non-trading rows after: "
            f"{sz_stats_after['non_trade_count']}/{sz_stats_after['sz_count']}"
        )

    finally:
        await db.close()


def resolve_token() -> str:
    token = os.environ.get("TUSHARE_TOKEN")
    if token:
        return token
    token = get_tushare_token()
    if token:
        return token
    raise RuntimeError("TUSHARE_TOKEN 未配置")


async def main(args: argparse.Namespace) -> int:
    token = resolve_token()

    probe = await probe_fd_share_bug(token=token, lookback_days=args.lookback_days)
    print("[Probe] result:")
    print(probe)

    if not probe.get("probe_ok", False):
        print("[Probe] failed, stop.")
        return 1

    shift_days = args.force_shift_days
    if shift_days is None:
        shift_days = int(probe.get("shift_days", 0))

    if shift_days == 0:
        print("[Probe] 未探测到稳定的一天错位，不执行数据库修复。")
        return 0

    print(
        "[Probe] 检测到 bug，准备按交易日修复数据库。"
        f" shift_days={shift_days}, "
        f"start_end_comparison_not_revealing={probe.get('start_end_comparison_not_revealing')}"
    )

    await repair_database(
        schema=args.schema,
        table=args.table,
        shift_days=shift_days,
        dry_run=args.dry_run,
        source_state=args.source_state,
    )
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fix SZ trade_date shift in tushare.fund_share by trade calendar"
    )
    parser.add_argument("--schema", default="tushare", help="schema name")
    parser.add_argument("--table", default="fund_share", help="table name")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=15,
        help="probe lookback window in natural days",
    )
    parser.add_argument("--dry-run", action="store_true", help="probe and preview only")
    parser.add_argument(
        "--force-shift-days",
        type=int,
        choices=[-1, 1],
        default=None,
        help="force shift direction, skip probe inference",
    )
    parser.add_argument(
        "--source-state",
        choices=["auto", "raw", "natural_shifted"],
        default="auto",
        help=(
            "source data state: raw=未做过自然日shift; "
            "natural_shifted=已做过自然日shift; auto=按非交易日占比自动判断"
        ),
    )

    raise SystemExit(asyncio.run(main(parser.parse_args())))

