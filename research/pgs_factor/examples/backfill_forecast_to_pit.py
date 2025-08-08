#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
回填 forecast 行到 pgs_factors.pit_income_quarterly（全市场/按股票）
================================================================

口径说明：
- 单位统一：net_profit_min/max 为“万元”，回填前转换为“元”（×10000）。
- 最新公告：同一 (ts_code, end_date) 仅取截止 as_of 前的最后一次 ann_date。
- 单季化：
  - Q1：n_income_single = mid_yuan
  - Q2~Q4：n_income_single = mid_yuan − sum(report 单季 n_income，Q1..Q_{q-1})

使用方式：
  python -m research.pgs_factor.examples.backfill_forecast_to_pit --as-of 2025-08-01 --batch-size 1000
  可选：--stocks 000001.SZ,600519.SH  仅回填指定列表
"""

from __future__ import annotations

import argparse
from typing import List, Optional
import pandas as pd

from research.tools.context import ResearchContext
from research.pgs_factor.database.db_manager import PGSFactorDBManager


def _chunk(items: List[str], size: int) -> List[List[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)] if items else []


def _to_numeric(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def load_latest_forecast(ctx: ResearchContext, as_of: Optional[str], stocks: Optional[List[str]]) -> pd.DataFrame:
    """加载截止 as_of 的最新 forecast 行（每个报告期一条），并生成 mid_yuan 与 year/quarter。"""
    q = """
        WITH base AS (
            SELECT ts_code,
                   ann_date::date AS ann_date,
                   end_date::date AS end_date,
                   ((net_profit_min + net_profit_max)/2.0) AS mid_wan
            FROM tushare.fina_forecast
            {where}
        ), latest AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY ts_code, end_date ORDER BY ann_date DESC) rn
            FROM base
        )
        SELECT ts_code, end_date, ann_date, mid_wan
        FROM latest
        WHERE rn = 1
    """
    wheres = []
    params: dict = {}
    if as_of:
        wheres.append("ann_date <= %(as_of)s")
        params["as_of"] = as_of
    if stocks:
        wheres.append("ts_code = ANY(%(stocks)s)")
        params["stocks"] = stocks
    where_clause = ("WHERE " + " AND ".join(wheres)) if wheres else ""
    df = ctx.query_dataframe(q.format(where=where_clause), params)
    if df is None or df.empty:
        return pd.DataFrame(columns=["ts_code", "end_date", "ann_date", "mid_wan"])  # 保持列

    df["mid_yuan"] = pd.to_numeric(df["mid_wan"], errors="coerce") * 10000.0
    df["year"] = pd.to_datetime(df["end_date"]).dt.year
    df["quarter"] = pd.to_datetime(df["end_date"]).dt.quarter
    return df.drop(columns=["mid_wan"])  # 丢弃万元列


def load_report_single_income(ctx: ResearchContext, stocks: List[str]) -> pd.DataFrame:
    """读取 report 单季 n_income，并按 (ts_code, year, quarter) 聚合（防御性）。"""
    q = """
        SELECT ts_code, year, quarter, SUM(n_income) AS n_income_single
        FROM pgs_factors.pit_income_quarterly
        WHERE data_source='report' AND ts_code = ANY(%(stocks)s)
        GROUP BY ts_code, year, quarter
    """
    df = ctx.query_dataframe(q, {"stocks": stocks})
    return _to_numeric(df, ["n_income_single"]) if df is not None else pd.DataFrame(
        columns=["ts_code", "year", "quarter", "n_income_single"]
    )


def compute_single_from_forecast(fc_df: pd.DataFrame, rpt_df: pd.DataFrame) -> pd.DataFrame:
    if fc_df is None or fc_df.empty:
        return pd.DataFrame()
    work = fc_df.copy()
    work = _to_numeric(work, ["mid_yuan", "year", "quarter"])  # year/quarter 已是数值

    # 对报告单季求“年内截至上一季”的累计和
    if rpt_df is None or rpt_df.empty:
        rpt = pd.DataFrame(columns=["ts_code", "year", "quarter", "n_income_single", "ytd_sum_before"]).copy()
    else:
        rpt = rpt_df.copy()
        rpt = _to_numeric(rpt, ["n_income_single"]).sort_values(["ts_code", "year", "quarter"])  # 保序
        rpt["ytd_sum_before"] = (
            rpt.groupby(["ts_code", "year"], dropna=False)["n_income_single"]
               .cumsum() - rpt["n_income_single"]
        )

    merged = work.merge(
        rpt[["ts_code", "year", "quarter", "ytd_sum_before"]],
        on=["ts_code", "year", "quarter"], how="left"
    )
    merged["ytd_sum_before"] = merged["ytd_sum_before"].fillna(0.0)

    # 单季推导
    merged["n_income_single"] = merged.apply(
        lambda r: r["mid_yuan"] if int(r["quarter"]) == 1 else r["mid_yuan"] - r["ytd_sum_before"],
        axis=1
    )
    return merged


def upsert_into_pit(ctx: ResearchContext, mgr: PGSFactorDBManager, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    out = pd.DataFrame({
        "ts_code": df["ts_code"],
        "end_date": pd.to_datetime(df["end_date"]).dt.date,
        "ann_date": pd.to_datetime(df["ann_date"]).dt.date,
        "data_source": "forecast",
        "year": df["year"].astype(int),
        "quarter": df["quarter"].astype(int),
        "n_income": pd.to_numeric(df["n_income_single"], errors="coerce"),
        "net_profit_mid": pd.to_numeric(df["mid_yuan"], errors="coerce"),
    })
    mgr.save_pit_income_quarterly(out)
    return len(out)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--as-of", type=str, default=None, help="截止日期（含），如 2025-08-01")
    parser.add_argument("--stocks", type=str, default=None, help="逗号分隔股票列表，可选")
    parser.add_argument("--batch-size", type=int, default=1000, help="批大小（按股票分批）")
    args = parser.parse_args()

    stocks: Optional[List[str]] = None
    if args.stocks:
        stocks = [s.strip() for s in args.stocks.split(',') if s.strip()]

    with ResearchContext() as ctx:
        mgr = PGSFactorDBManager(ctx)

        # 确定股票池（默认为源表并集）
        if not stocks:
            df_codes = ctx.query_dataframe(
                """
                SELECT DISTINCT ts_code FROM (
                  SELECT ts_code FROM tushare.fina_forecast
                  UNION
                  SELECT ts_code FROM pgs_factors.pit_income_quarterly WHERE data_source='report'
                ) t
                """,
                None,
            )
            stocks = df_codes["ts_code"].tolist() if df_codes is not None and not df_codes.empty else []

        total = 0
        for idx, chunk in enumerate(_chunk(stocks, max(1, int(args.batch_size))), 1):
            fc = load_latest_forecast(ctx, args.as_of, chunk)
            if fc.empty:
                continue
            rpt = load_report_single_income(ctx, chunk)
            calc = compute_single_from_forecast(fc, rpt)
            affected = upsert_into_pit(ctx, mgr, calc)
            total += affected
            print(f"Batch {idx}: upserted {affected} forecast rows")

        print(f"Done. Total forecast rows upserted: {total}")


if __name__ == "__main__":
    main()


