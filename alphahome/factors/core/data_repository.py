"""Read-only repositories for P/G source data and eligible universes."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, List

import pandas as pd

from alphahome.common.schema_names import FACTOR_SCHEMA, PIT_SCHEMA


class PFactorDataRepository:
    """Own all PIT and stock-universe reads used by P v2.0."""

    def __init__(self, context: Any, logger: logging.Logger):
        self.context = context
        self.logger = logger

    def trading_stock_codes(self, calc_date: str) -> List[str]:
        optimized_codes: List[str] = []
        try:
            optimized = self.context.query_dataframe(
                "SELECT * FROM get_trading_stocks_optimized(%s)", (calc_date,)
            )
            optimized_codes = (
                optimized["ts_code"].dropna().tolist()
                if optimized is not None and not optimized.empty
                else []
            )
        except Exception as exc:
            # The optimized helper is optional in older databases.  The
            # authoritative stock_basic query below still gives a complete,
            # reproducible fallback universe.
            self.logger.warning(
                "%s 优化股票池查询失败，使用stock_basic: %s", calc_date, exc
            )

        try:
            stock_basic = self.context.query_dataframe(
                """
                SELECT ts_code
                FROM tushare.stock_basic
                WHERE list_date <= %s
                  AND (delist_date IS NULL OR delist_date > %s)
                ORDER BY ts_code
                """,
                (calc_date, calc_date),
            )
            stock_basic_codes = (
                stock_basic["ts_code"].dropna().tolist()
                if stock_basic is not None and not stock_basic.empty
                else []
            )
        except Exception as exc:
            raise RuntimeError(f"{calc_date} stock_basic股票池查询失败") from exc

        result = sorted(set(optimized_codes) | set(stock_basic_codes))
        if not result:
            self.logger.warning("%s 未找到在交易股票数据", calc_date)
        return result

    def eligible_indicators(
        self, as_of_date: str, stock_codes: List[str]
    ) -> pd.DataFrame:
        try:
            result = self.context.query_dataframe(
                f"""
                WITH latest_indicators AS (
                    SELECT
                        pit.ts_code, pit.end_date, pit.ann_date, pit.data_source,
                        pit.gpa_ttm, pit.roe_excl_ttm, pit.roa_excl_ttm,
                        pit.net_margin_ttm, pit.operating_margin_ttm, pit.roi_ttm,
                        pit.asset_turnover_ttm, pit.equity_multiplier,
                        pit.debt_to_asset_ratio, pit.equity_ratio,
                        pit.revenue_yoy_growth, pit.n_income_yoy_growth,
                        pit.operate_profit_yoy_growth, pit.data_quality,
                        pit.calculation_status,
                        ROW_NUMBER() OVER (
                            PARTITION BY pit.ts_code
                            ORDER BY pit.ann_date DESC, pit.end_date DESC,
                                     CASE pit.data_source
                                         WHEN 'report' THEN 1
                                         WHEN 'express' THEN 2
                                         WHEN 'forecast' THEN 3
                                         ELSE 9
                                     END
                        ) AS rn
                    FROM {PIT_SCHEMA}.pit_financial_indicators pit
                    INNER JOIN tushare.stock_basic sb ON pit.ts_code = sb.ts_code
                    WHERE pit.ann_date <= %s
                      AND pit.ts_code = ANY(%s)
                      AND pit.calculation_status = 'success'
                      AND pit.data_quality IN (
                          'high', 'normal', 'outlier_high', 'outlier_low'
                      )
                      AND pit.end_date >= (%s::date - INTERVAL '10 months')
                      AND sb.list_date <= %s
                      AND (sb.delist_date IS NULL OR sb.delist_date > %s)
                )
                SELECT
                    ts_code, end_date, ann_date, data_source,
                    gpa_ttm, roe_excl_ttm, roa_excl_ttm,
                    net_margin_ttm, operating_margin_ttm, roi_ttm,
                    asset_turnover_ttm, equity_multiplier,
                    debt_to_asset_ratio, equity_ratio,
                    revenue_yoy_growth, n_income_yoy_growth,
                    operate_profit_yoy_growth, data_quality, calculation_status
                FROM latest_indicators
                WHERE rn = 1
                ORDER BY ts_code
                """,
                (
                    as_of_date,
                    stock_codes,
                    as_of_date,
                    as_of_date,
                    as_of_date,
                ),
            )
            return result if result is not None else pd.DataFrame()
        except Exception as exc:
            self.logger.error(
                "查询MVP预计算指标失败 (PIT时点: %s): %s", as_of_date, exc
            )
            raise RuntimeError(f"{as_of_date} P因子PIT指标查询失败") from exc

    def industry_classification(
        self, stock_codes: List[str], as_of_date: str
    ) -> pd.DataFrame:
        try:
            result = self.context.query_dataframe(
                "SELECT * FROM get_industry_classification_batch_pit_optimized(%s, %s, 'sw')",
                (stock_codes, as_of_date),
            )
        except Exception as exc:
            self.logger.warning("优化PIT行业分类查询失败，使用成员表回退: %s", exc)
            result = pd.DataFrame()
        if result is not None and not result.empty:
            return result
        self.logger.warning(
            "未通过优化函数获取到行业分类，尝试回退到Tushare行业成员表查询"
        )
        try:
            return self._fallback_industry(stock_codes, as_of_date)
        except Exception as fallback_exc:
            raise RuntimeError(f"{as_of_date} P因子行业分类查询失败") from fallback_exc

    def _fallback_industry(
        self, stock_codes: List[str], as_of_date: str
    ) -> pd.DataFrame:
        active = self.context.query_dataframe(
            """
            SELECT ts_code, l1_name AS industry_level1,
                   l2_name AS industry_level2, l3_name AS industry_level3,
                   l1_code AS industry_code1, l2_code AS industry_code2,
                   l3_code AS industry_code3, in_date
            FROM tushare.index_swmember
            WHERE ts_code = ANY(%s) AND l1_name IS NOT NULL
              AND in_date <= %s AND (out_date IS NULL OR out_date > %s)
            ORDER BY ts_code, in_date DESC
            """,
            (stock_codes, as_of_date, as_of_date),
        )
        collected: dict[str, dict] = {}
        if active is not None and not active.empty:
            for _, row in active.groupby("ts_code").first().reset_index().iterrows():
                collected[row["ts_code"]] = row.to_dict()

        remaining = [code for code in stock_codes if code not in collected]
        if remaining:
            past = self.context.query_dataframe(
                """
                SELECT DISTINCT ON (ts_code)
                       ts_code, l1_name AS industry_level1,
                       l2_name AS industry_level2, l3_name AS industry_level3,
                       l1_code AS industry_code1, l2_code AS industry_code2,
                       l3_code AS industry_code3, in_date
                FROM tushare.index_swmember
                WHERE ts_code = ANY(%s) AND l1_name IS NOT NULL AND in_date <= %s
                ORDER BY ts_code, in_date DESC
                """,
                (remaining, as_of_date),
            )
            if past is not None and not past.empty:
                for _, row in past.iterrows():
                    collected[row["ts_code"]] = row.to_dict()

        remaining = [code for code in stock_codes if code not in collected]
        if remaining:
            earliest = self.context.query_dataframe(
                """
                SELECT DISTINCT ON (ts_code)
                       ts_code, l1_name AS industry_level1,
                       l2_name AS industry_level2, l3_name AS industry_level3,
                       l1_code AS industry_code1, l2_code AS industry_code2,
                       l3_code AS industry_code3, in_date
                FROM tushare.index_swmember
                WHERE ts_code = ANY(%s) AND l1_name IS NOT NULL
                ORDER BY ts_code, in_date ASC
                """,
                (remaining,),
            )
            if earliest is not None and not earliest.empty:
                for _, row in earliest.iterrows():
                    collected[row["ts_code"]] = row.to_dict()
        if not collected:
            return pd.DataFrame()

        result = pd.DataFrame.from_records(list(collected.values()))
        financial_keywords = (
            "银行",
            "证券",
            "保险",
            "信托",
            "期货",
            "基金",
            "金融",
            "投资",
            "资产管理",
            "财务公司",
        )
        industry_text = (
            result["industry_level1"].fillna("").astype(str)
            + " "
            + result["industry_level2"].fillna("").astype(str)
        )
        result["requires_special_gpa_handling"] = industry_text.apply(
            lambda value: any(key in value for key in financial_keywords)
        )
        result["gpa_calculation_method"] = result["requires_special_gpa_handling"].map(
            {True: "null", False: "standard"}
        )
        result["data_source"] = "sw"
        result["obs_date"] = as_of_date
        columns = [
            "ts_code",
            "obs_date",
            "data_source",
            "industry_level1",
            "industry_level2",
            "industry_level3",
            "requires_special_gpa_handling",
            "gpa_calculation_method",
        ]
        for column in columns:
            if column not in result:
                result[column] = None
        return result[columns]


class GFactorDataRepository:
    """Own same-day P eligibility and legal-Friday history reads for G v1.1."""

    def __init__(self, context: Any, logger: logging.Logger):
        self.context = context
        self.logger = logger

    def same_day_p_codes(self, calc_date: str) -> List[str]:
        try:
            frame = self.context.query_dataframe(
                f"""
                SELECT DISTINCT ts_code
                FROM {FACTOR_SCHEMA}.p_factor
                WHERE calc_date = %s
                ORDER BY ts_code
                """,
                (calc_date,),
            )
            if frame is not None and not frame.empty:
                return sorted(set(frame["ts_code"].dropna().tolist()))
            self.logger.error("%s 缺少同日P因子，G因子按依赖契约停止", calc_date)
            return []
        except Exception as exc:
            self.logger.error("获取 %s 同日P因子股票集合失败: %s", calc_date, exc)
            raise RuntimeError(f"{calc_date} 同日P因子股票集合查询失败") from exc

    def p_history(self, as_of_date: str, stock_codes: List[str]) -> pd.DataFrame:
        try:
            start_date = (
                datetime.strptime(as_of_date, "%Y-%m-%d") - timedelta(days=730)
            ).strftime("%Y-%m-%d")
            result = self.context.query_dataframe(
                f"""
                SELECT ts_code, calc_date, p_score, data_source, ann_date,
                       gpa, roe_excl, roa_excl,
                       revenue_yoy_growth, n_income_yoy_growth
                FROM {FACTOR_SCHEMA}.p_factor
                WHERE ts_code = ANY(%s)
                  AND calc_date BETWEEN %s AND %s
                  AND EXTRACT(ISODOW FROM calc_date) = 5
                  AND p_score IS NOT NULL
                ORDER BY ts_code, calc_date
                """,
                (stock_codes, start_date, as_of_date),
            )
            return result if result is not None else pd.DataFrame()
        except Exception as exc:
            self.logger.error(
                "查询P因子历史数据失败 (PIT时点: %s): %s", as_of_date, exc
            )
            raise RuntimeError(f"{as_of_date} G因子P历史查询失败") from exc


__all__ = ["GFactorDataRepository", "PFactorDataRepository"]
