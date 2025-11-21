#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
补数脚本：tushare.stock_limitlist 2007-01-01 ~ 2019-12-31

根据现有数据表：
- "tushare"."stock_limitprice"  (每日涨跌停价格，接口 stk_limit，见 https://tushare.pro/document/2?doc_id=183)
- "tushare"."stock_daily"       (日线行情)
- "tushare"."stock_dailybasic"  (每日基本面，如市值、换手率)
- "tushare"."stock_basic"       (静态基本信息：行业、名称)
- "tushare"."stock_st"          (2016+ ST 列表，接口 stock_st，见 https://tushare.pro/document/2?doc_id=397)

推导并补充 "tushare"."stock_limitlist"（涨跌停列表，接口 limit_list_d，见 https://tushare.pro/document/2?doc_id=298）
在 2007-01-01 ~ 2019-12-31 区间内缺失的数据。

核心规则概览：
- 使用 close 与 up_limit/down_limit 的相对误差判断该日是否涨停/跌停 (limit='U'/'D')
- 使用涨跌停幅度约为 ±5% 的特征识别 ST 日（5% 涨跌停），并结合 2016+ 的 stock_st 表精确过滤 ST 股票
- 仅补充 trade_date < 2020-01-01 的记录，使用 ON CONFLICT (trade_date, ts_code) DO NOTHING，保证幂等
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime
from typing import Optional
import argparse
from alphahome.common.config_manager import get_database_url
from alphahome.common.db_manager import DBManager


START_DATE = date(2007, 1, 1)
END_DATE = date(2019, 12, 31)

# 涨跌停判定的价格相对误差容忍度
EPS_PRICE = 5e-4

# ST 涨跌停幅判定带宽（围绕 5%）
EPS_BAND = 0.005


async def backfill_range(
    db: DBManager,
    start_date: date,
    end_date: date,
    eps_price: float = EPS_PRICE,
    eps_band: float = EPS_BAND,
) -> None:
    """
    在给定日期区间内，按计划补充 tushare.stock_limitlist 的数据。

    所有计算在数据库端完成：
    - 连接 limitprice / daily / dailybasic / basic / st
    - 推导涨跌停标记 (limit)
    - 识别 ST 日并过滤
    - 插入到 stock_limitlist，遇到已存在记录则忽略
    """
    if start_date > end_date:
        return

    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    # 仅处理 2020 年前的数据，SQL 中再加一层保护
    sql = f"""
    INSERT INTO "tushare"."stock_limitlist" (
        trade_date,
        ts_code,
        industry,
        name,
        close,
        pct_chg,
        amount,
        limit_amount,
        float_mv,
        total_mv,
        turnover_ratio,
        fd_amount,
        first_time,
        last_time,
        open_times,
        up_stat,
        limit_times,
        "limit"
    )
    SELECT
        p.trade_date,
        p.ts_code,
        COALESCE(sw_industry.l2_name, sb.industry) AS industry,
        COALESCE(snc.name, sb.name) AS stock_name,
        d.close,
        d.pct_chg,
        d.amount,
        NULL::NUMERIC,            -- limit_amount 无法可靠重建
        dbasic.circ_mv  AS float_mv,
        dbasic.total_mv AS total_mv,
        dbasic.turnover_rate,
        NULL::NUMERIC,            -- fd_amount
        NULL::VARCHAR,            -- first_time
        NULL::VARCHAR,            -- last_time
        NULL::INTEGER,            -- open_times
        NULL::VARCHAR,            -- up_stat
        NULL::INTEGER,            -- limit_times
        CASE
            WHEN ABS(d.close - p.up_limit) <= {eps_price} * p.up_limit
                THEN 'U'
            WHEN ABS(d.close - p.down_limit) <= {eps_price} * ABS(p.down_limit)
                THEN 'D'
            ELSE NULL
        END AS limit_flag
    FROM "tushare"."stock_limitprice" AS p
    JOIN "tushare"."stock_daily" AS d
      ON p.trade_date = d.trade_date
     AND p.ts_code    = d.ts_code
    LEFT JOIN "tushare"."stock_dailybasic" AS dbasic
      ON p.trade_date = dbasic.trade_date
     AND p.ts_code    = dbasic.ts_code
    LEFT JOIN "tushare"."stock_basic" AS sb
      ON p.ts_code = sb.ts_code
    LEFT JOIN LATERAL (
        SELECT name
        FROM "tushare"."stock_namechange" AS snc_raw
        WHERE snc_raw.ts_code = p.ts_code
          AND (snc_raw.start_date IS NULL OR snc_raw.start_date <= p.trade_date)
          AND (snc_raw.end_date   IS NULL OR snc_raw.end_date   >= p.trade_date)
        ORDER BY snc_raw.start_date DESC NULLS LAST
        LIMIT 1
    ) AS snc ON TRUE
    LEFT JOIN LATERAL (
        SELECT l2_name
        FROM "tushare"."index_swmember" AS sw_raw
        WHERE sw_raw.ts_code = p.ts_code
          AND (sw_raw.in_date IS NULL OR sw_raw.in_date <= p.trade_date)
          AND (sw_raw.out_date IS NULL OR sw_raw.out_date >= p.trade_date)
        ORDER BY sw_raw.in_date DESC NULLS LAST
        LIMIT 1
    ) AS sw_industry ON TRUE
    LEFT JOIN "tushare"."stock_st" AS st
      ON p.trade_date = st.trade_date
     AND p.ts_code    = st.ts_code
    WHERE
        p.trade_date BETWEEN $1 AND $2
        AND p.trade_date < DATE '2020-01-01'
        AND p.trade_date >= DATE '2007-01-01'
        -- 过滤 ST 股票：2016+ 使用 stock_st + 涨跌停幅，2016- 使用涨跌停幅
        AND (
            CASE
                WHEN p.pre_close IS NOT NULL AND p.pre_close > 0 THEN
                    (p.up_limit - p.pre_close) / p.pre_close
                ELSE NULL
            END
        ) IS NOT NULL
        AND (
            CASE
                WHEN p.pre_close IS NOT NULL AND p.pre_close > 0 THEN
                    (p.pre_close - p.down_limit) / p.pre_close
                ELSE NULL
            END
        ) IS NOT NULL
        -- 先计算 5% 涨跌停幅是否命中
        AND NOT (
            (
                -- 5% 上涨停幅
                ((p.up_limit - p.pre_close) / p.pre_close)
                BETWEEN (0.05 - {eps_band}) AND (0.05 + {eps_band})
            )
            OR
            (
                -- 5% 下跌停幅
                ((p.pre_close - p.down_limit) / p.pre_close)
                BETWEEN (0.05 - {eps_band}) AND (0.05 + {eps_band})
            )
            OR
            (
                -- 2016-01-01 之后，stock_st 明确标记为 ST
                p.trade_date >= DATE '2016-01-01'
                AND st.ts_code IS NOT NULL
            )
        )
        -- 仅保留能明确判断出涨停或跌停的记录
        AND (
            ABS(d.close - p.up_limit)  <= {eps_price} * p.up_limit
            OR
            ABS(d.close - p.down_limit) <= {eps_price} * ABS(p.down_limit)
        )
        -- 避免插入已存在的主键 (trade_date, ts_code)
        AND NOT EXISTS (
            SELECT 1
            FROM "tushare"."stock_limitlist" AS existing
            WHERE existing.trade_date = p.trade_date
              AND existing.ts_code    = p.ts_code
        )
    ;
    """

    print(
        f"[backfill] 处理中日期区间: {start_str} ~ {end_str} "
        f"(eps_price={eps_price}, eps_band={eps_band})"
    )
    await db.execute(sql, start_date, end_date)


async def main() -> None:
    """主入口：按年度分段补数 2007-2019，可通过 --year 仅跑单一年。"""
    parser = argparse.ArgumentParser(
        description="Backfill tushare.stock_limitlist for 2007-2019 using existing price tables."
    )
    parser.add_argument(
        "--year",
        type=int,
        help="仅补数指定年份（例如 --year 2007）。未指定时补数 2007-2019 全部年份。",
    )
    args = parser.parse_args()
    db_url = get_database_url()
    if not db_url:
        print("错误：无法获取数据库连接字符串，请检查配置文件。")
        return

    db = DBManager(db_url, mode="async")
    await db.connect()

    try:
        # 按年份分段处理，避免单次处理窗口过大
        years = (
            [args.year]
            if args.year is not None
            else list(range(START_DATE.year, END_DATE.year + 1))
        )

        for year in years:
            batch_start = date(year, 1, 1)
            batch_end = date(year, 12, 31)
            # 裁剪到总范围
            if batch_end < START_DATE or batch_start > END_DATE:
                continue
            if batch_start < START_DATE:
                batch_start = START_DATE
            if batch_end > END_DATE:
                batch_end = END_DATE

            await backfill_range(db, batch_start, batch_end)

        print("全部年度区间补数流程已完成。")

    except Exception as exc:
        print(f"补数过程中发生错误: {exc}")
        import traceback

        traceback.print_exc()
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())


