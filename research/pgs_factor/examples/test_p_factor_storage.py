#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
P因子存储测试脚本
==================

- 构造少量示例P因子数据（归母净利润口径下的ROE/ROA计算结果应来自主流程，这里仅演示字段）
- 通过 PGSFactorDBManager.save_p_factor 写入数据库
- 再回查 pgs_factors.p_factor 验证写入

用法：
  python research/pgs_factor/examples/test_p_factor_storage.py --ann 2025-08-01 --calc 2025-08-01
"""

import argparse
import pandas as pd
from datetime import datetime, date

from research.tools.context import ResearchContext
from research.pgs_factor.database.db_manager import PGSFactorDBManager


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ann", type=str, required=False, default=date.today().strftime("%Y-%m-%d"), help="公告日期 YYYY-MM-DD")
    parser.add_argument("--calc", type=str, required=False, default=date.today().strftime("%Y-%m-%d"), help="计算日期 YYYY-MM-DD")
    parser.add_argument("--source", type=str, required=False, default="report", choices=["report", "express", "forecast"], help="数据来源")
    args = parser.parse_args()

    ann_date = datetime.strptime(args.ann, "%Y-%m-%d").date()
    calc_date = datetime.strptime(args.calc, "%Y-%m-%d").date()

    # 示例数据（真实计算应由 PGSFactorCalculator 产生）
    # 构造更贴近真实口径的示例：p_score = (roe_ttm + roa_ttm)/2，并统一保留两位小数
    base = [
        {"ts_code": "000001.SZ", "calc_date": calc_date, "ann_date": ann_date, "roe_ttm": 12.3, "roa_ttm": 6.5, "gross_margin": 18.2, "confidence": 0.9},
        {"ts_code": "600000.SH", "calc_date": calc_date, "ann_date": ann_date, "roe_ttm": 15.1, "roa_ttm": 7.4, "gross_margin": 20.7, "confidence": 0.85},
    ]
    for row in base:
        row["p_score"] = round((row["roe_ttm"] + row["roa_ttm"]) / 2, 2)
        row["roe_ttm"] = round(row["roe_ttm"], 2)
        row["roa_ttm"] = round(row["roa_ttm"], 2)
        row["gross_margin"] = round(row["gross_margin"], 2)
    df = pd.DataFrame(base)

    ctx = ResearchContext()
    mgr = PGSFactorDBManager(ctx)

    # 写入
    mgr.save_p_factor(df, ann_date=ann_date, data_source=args.source)

    # 回查
    query = """
        SELECT ts_code, calc_date, ann_date, data_source, roe_ttm, roa_ttm, gross_margin, p_score, confidence, data_quality
        FROM pgs_factors.p_factor
        WHERE calc_date = %(calc_date)s AND ts_code = ANY(%(stocks)s)
        ORDER BY ts_code
    """
    result = ctx.query_dataframe(query, {
        "calc_date": calc_date,
        "stocks": df["ts_code"].tolist()
    })

    print("\nWritten rows:")
    print(result.to_string(index=False))

    ctx.close()


if __name__ == "__main__":
    main()


