"""Read-only live-contract smoke test for the FTTM pipeline."""

from __future__ import annotations

import pandas as pd
import pytest

from alphahome.pit.calculators.industry_fttm_calculator import IndustryFTTMCalculator
from alphahome.pit.calculators.index_fttm_calculator import IndexFTTMCalculator
from alphahome.pit.calculators.stock_fttm_calculator import StockFTTMCalculator
from alphahome.pit.context import PITContext
from alphahome.pit.pit_index_fttm_manager import (
    IMPORTANT_INDEX_SPECS,
    PITIndexFTTMManager,
    configured_universe_count,
)


pytestmark = [pytest.mark.integration, pytest.mark.requires_db]


def test_live_alphadb_sample_obeys_stock_and_industry_pit_contracts():
    try:
        context = PITContext()
    except Exception as exc:  # pragma: no cover - environment-specific skip
        pytest.skip(f"AlphaDB unavailable: {exc}")

    with context:
        date_frame = context.query_dataframe(
            """
            SELECT MAX(obs_date)::date AS obs_date
            FROM pit.pit_industry_classification
            WHERE data_source = 'sw'
              AND industry_code1 IS NOT NULL
              AND industry_code2 IS NOT NULL
            """
        )
        if date_frame.empty or pd.isna(date_frame.iloc[0]["obs_date"]):
            pytest.skip("No SW PIT classification snapshot")
        obs_date = pd.Timestamp(date_frame.iloc[0]["obs_date"]).normalize()
        window_start = obs_date - pd.DateOffset(months=6)

        code_frame = context.query_dataframe(
            """
            SELECT r.ts_code, COUNT(*)::bigint AS report_rows
            FROM rawdata.stock_report_rc r
            JOIN pit.pit_industry_classification c
              ON c.ts_code = r.ts_code
             AND c.obs_date = %s
             AND c.data_source = 'sw'
             AND c.industry_code1 IS NOT NULL
             AND c.industry_code2 IS NOT NULL
            WHERE r.report_date > %s
              AND r.report_date <= %s
              AND r.quarter ~ '^[0-9]{4}Q4$'
            GROUP BY r.ts_code
            ORDER BY report_rows DESC, r.ts_code
            LIMIT 20
            """,
            (obs_date.date(), window_start.date(), obs_date.date()),
        )
        if code_frame.empty:
            pytest.skip("No live research-report sample in current PIT window")
        codes = code_frame["ts_code"].tolist()

        forecasts = context.query_dataframe(
            """
            SELECT r.ts_code, r.org_name, r.author_name, r.report_date,
                   r.quarter, r.np, r.eps, d.total_share,
                   r.ann_date, r.end_date, r.report_title, r.report_type,
                   r.classify, r.create_time, r.update_time
            FROM rawdata.stock_report_rc r
            LEFT JOIN rawdata.stock_dailybasic d
              ON d.ts_code = r.ts_code
             AND d.trade_date = r.report_date
            WHERE r.ts_code = ANY(%s)
              AND r.report_date >= %s
              AND r.report_date <= %s
            """,
            (codes, window_start.date(), obs_date.date()),
        )
        stock_result = StockFTTMCalculator().calculate(forecasts, [obs_date])
        assert not stock_result.empty
        assert not stock_result.duplicated(["ts_code", "org_name", "obs_date"]).any()
        assert stock_result["selected_report_date"].le(obs_date).all()
        assert stock_result["selected_report_date"].gt(window_start).all()
        assert stock_result["fy1_weight"].add(stock_result["fy2_weight"]).eq(1).all()
        assert stock_result["fttm_np"].notna().all()

        classifications = context.query_dataframe(
            """
            SELECT ts_code, obs_date, data_source,
                   industry_code1, industry_level1,
                   industry_code2, industry_level2
            FROM pit.pit_industry_classification
            WHERE obs_date = %s
              AND data_source = 'sw'
              AND ts_code = ANY(%s)
            """,
            (obs_date.date(), codes),
        )
        basics = context.query_dataframe(
            """
            SELECT ts_code, list_date, delist_date, exchange, curr_type
            FROM rawdata.stock_basic
            WHERE ts_code = ANY(%s)
            """,
            (codes,),
        )
        weights = context.query_dataframe(
            """
            WITH weight_date AS (
                SELECT MAX(trade_date)::date AS trade_date
                FROM rawdata.stock_dailybasic
                WHERE trade_date <= %s
                  AND trade_date >= %s - INTERVAL '31 days'
            )
            SELECT %s::date AS obs_date, d.ts_code,
                   w.trade_date AS weight_trade_date, d.total_mv
            FROM weight_date w
            JOIN rawdata.stock_dailybasic d ON d.trade_date = w.trade_date
            WHERE d.ts_code = ANY(%s)
            """,
            (obs_date.date(), obs_date.date(), obs_date.date(), codes),
        )
        industry_result = IndustryFTTMCalculator().calculate(
            classifications,
            basics,
            weights,
            stock_result,
            obs_dates=[obs_date],
        )
        assert not industry_result.empty
        assert set(industry_result["industry_level"]) == {"L1", "L2"}
        assert not industry_result.duplicated(
            [
                "obs_date",
                "classification_source",
                "industry_level",
                "industry_code",
                "weight_basis",
            ]
        ).any()
        assert industry_result["weight_trade_date"].dropna().le(obs_date).all()
        assert industry_result["source_max_report_date"].dropna().le(obs_date).all()


def test_live_alphadb_sample_obeys_index_and_all_a_pit_contracts():
    try:
        context = PITContext()
    except Exception as exc:  # pragma: no cover - environment-specific skip
        pytest.skip(f"AlphaDB unavailable: {exc}")

    with context:
        manager = PITIndexFTTMManager()
        manager.context = context
        latest = manager._latest_available_month()
        if latest is None:
            pytest.skip("No common complete month for index FTTM")
        previous = manager.previous_month_end(latest)
        months = [previous, latest]
        sources = manager._load_sources(months)
        manager._validate_dependencies(sources["members"], sources["stock_fttm"], months)

        result = IndexFTTMCalculator().calculate(
            sources["members"],
            sources["stock_basic"],
            sources["stock_fttm"],
            obs_dates=months,
        )
        latest_rows = result.loc[result["obs_date"].eq(pd.Timestamp(latest))]

        assert len(latest_rows) == configured_universe_count(latest)
        assert latest_rows["universe_code"].nunique() == 1 + len(IMPORTANT_INDEX_SPECS)
        assert set(latest_rows["universe_type"]) == {"index", "all_a"}
        assert not latest_rows.duplicated(
            ["obs_date", "universe_type", "universe_code", "weight_basis"]
        ).any()
        assert latest_rows["weight_trade_date"].dropna().le(pd.Timestamp(latest)).all()
        assert latest_rows["source_max_report_date"].dropna().le(pd.Timestamp(latest)).all()
        assert latest_rows["diffusion_up"].dropna().between(0, 1).all()
        assert (
            latest_rows["matched_org_count"]
            == latest_rows["up_org_count"] + latest_rows["down_or_flat_org_count"]
        ).all()
