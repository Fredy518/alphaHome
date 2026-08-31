"""Acceptance checks for the industry FAPI PIT history."""

from __future__ import annotations

import json
from datetime import date

import pandas as pd

from alphahome.pit.pit_industry_fapi_manager import PITIndustryFAPIManager


EXPECTED_START = date(2014, 2, 28)
SAMPLE_MONTHS = (date(2014, 2, 28), date(2020, 3, 31), date(2026, 7, 31))


SUMMARY_SQL = """
WITH expected_months AS (
    SELECT (date_trunc('month', value)::date + INTERVAL '1 month - 1 day')::date
               AS obs_date
    FROM generate_series(%s::date, %s::date, INTERVAL '1 month') AS value
), monthly AS (
    SELECT obs_date,
           COUNT(*)::integer AS row_count,
           COUNT(*) FILTER (WHERE fapi_spread_equal IS NOT NULL)::integer
               AS valued_count,
           COUNT(*) FILTER (WHERE is_eligible)::integer AS eligible_count
    FROM pit.pit_industry_fapi_monthly
    GROUP BY obs_date
)
SELECT
    (SELECT COUNT(*)::bigint FROM pit.pit_industry_fapi_monthly) AS row_count,
    (SELECT COUNT(*)::integer FROM monthly) AS month_count,
    (SELECT MIN(obs_date) FROM monthly) AS min_obs_date,
    (SELECT MAX(obs_date) FROM monthly) AS max_obs_date,
    (SELECT COUNT(*)::integer
     FROM expected_months e LEFT JOIN monthly m USING (obs_date)
     WHERE m.obs_date IS NULL) AS missing_month_count,
    (SELECT MIN(row_count) FROM monthly) AS min_rows_per_month,
    (SELECT MAX(row_count) FROM monthly) AS max_rows_per_month,
    (SELECT MIN(valued_count) FROM monthly) AS min_valued_per_month,
    (SELECT MAX(valued_count) FROM monthly) AS max_valued_per_month,
    (SELECT MIN(eligible_count) FROM monthly) AS min_eligible_per_month,
    (SELECT MAX(eligible_count) FROM monthly) AS max_eligible_per_month
"""


INTEGRITY_SQL = """
WITH structural AS (
    SELECT obs_date,
           data_source AS classification_source,
           'L1'::text AS industry_level,
           industry_code1 AS industry_code
    FROM pit.pit_industry_classification
    WHERE obs_date BETWEEN %s AND %s
      AND data_source = 'sw'
      AND industry_code1 IS NOT NULL
    UNION
    SELECT obs_date, data_source, 'L2'::text, industry_code2
    FROM pit.pit_industry_classification
    WHERE obs_date BETWEEN %s AND %s
      AND data_source = 'sw'
      AND industry_code2 IS NOT NULL
), actual AS (
    SELECT obs_date, classification_source, industry_level, industry_code
    FROM pit.pit_industry_fapi_monthly
), structural_comparison AS (
    SELECT s.industry_code AS expected_code, a.industry_code AS actual_code
    FROM structural s
    FULL JOIN actual a
      USING (obs_date, classification_source, industry_level, industry_code)
)
SELECT
    (SELECT COUNT(*) FROM (
        SELECT obs_date, classification_source, industry_level, industry_code,
               benchmark_code, method_version
        FROM pit.pit_industry_fapi_monthly
        GROUP BY 1, 2, 3, 4, 5, 6
        HAVING COUNT(*) > 1
    ) duplicate_keys)::bigint AS duplicate_key_groups,
    COUNT(*) FILTER (WHERE source_max_report_date > obs_date)::bigint
        AS future_current_source,
    COUNT(*) FILTER (WHERE previous_source_max_report_date >= obs_date)::bigint
        AS future_previous_source,
    COUNT(*) FILTER (WHERE equity_trade_date > obs_date)::bigint
        AS future_equity_date,
    COUNT(*) FILTER (WHERE benchmark_weight_trade_date > obs_date)::bigint
        AS future_benchmark_date,
    COUNT(*) FILTER (
        WHERE fapi_spread_equal NOT BETWEEN 0 AND 1
           OR fapi_spread_weighted NOT BETWEEN 0 AND 1
           OR fapi_ratio_equal NOT BETWEEN 0 AND 1
           OR fapi_ratio_weighted NOT BETWEEN 0 AND 1
    )::bigint AS fapi_out_of_range,
    COUNT(*) FILTER (
        WHERE matched_org_count <>
              spread_up_org_count + spread_down_or_flat_org_count
    )::bigint AS spread_count_mismatch,
    COUNT(*) FILTER (
        WHERE ratio_matched_org_count <>
              ratio_up_org_count + ratio_down_or_flat_org_count
    )::bigint AS ratio_count_mismatch,
    COUNT(*) FILTER (
        WHERE spread_up_org_weight < 0
           OR spread_total_org_weight < spread_up_org_weight
           OR ratio_up_org_weight < 0
           OR ratio_total_org_weight < ratio_up_org_weight
    )::bigint AS weight_mismatch,
    COUNT(*) FILTER (
        WHERE is_eligible
          AND (matched_org_count < 5
               OR fapi_spread_equal IS NULL
               OR jsonb_array_length(quality_reasons) > 0)
    )::bigint AS false_eligible,
    (SELECT COUNT(*) FROM structural_comparison WHERE actual_code IS NULL)::bigint
        AS expected_structure_missing,
    (SELECT COUNT(*) FROM structural_comparison WHERE expected_code IS NULL)::bigint
        AS unexpected_structure_rows
FROM pit.pit_industry_fapi_monthly
"""


DIRECT_RECOMPUTE_SQL = """
WITH months(obs_date) AS (
    VALUES (%s::date), (%s::date), (%s::date)
), equity_dates AS (
    SELECT m.obs_date, MAX(d.trade_date)::date AS equity_trade_date
    FROM months m
    LEFT JOIN rawdata.stock_dailybasic d
      ON d.trade_date <= m.obs_date
     AND d.trade_date >= m.obs_date - INTERVAL '31 days'
    GROUP BY m.obs_date
), stock_equity AS (
    SELECT d.obs_date, s.ts_code,
           s.total_mv / NULLIF(s.pb, 0)::double precision AS book_equity
    FROM equity_dates d
    JOIN rawdata.stock_dailybasic s ON s.trade_date = d.equity_trade_date
    WHERE s.total_mv > 0 AND s.pb > 0
), benchmark_dates AS (
    SELECT m.obs_date, MAX(w.trade_date)::date AS weight_trade_date
    FROM months m
    LEFT JOIN rawdata.index_weight w
      ON w.index_code = '000906.SH'
     AND w.trade_date <= m.obs_date
     AND w.trade_date >= m.obs_date - INTERVAL '65 days'
    GROUP BY m.obs_date
), benchmark_members AS (
    SELECT d.obs_date, w.con_code AS ts_code
    FROM benchmark_dates d
    JOIN rawdata.index_weight w
      ON w.index_code = '000906.SH'
     AND w.trade_date = d.weight_trade_date
    WHERE w.weight > 0
    GROUP BY d.obs_date, w.con_code
), matched AS (
    SELECT c.obs_date, c.ts_code, c.org_name,
           c.fttm_np::double precision AS current_fttm_np,
           p.fttm_np::double precision AS previous_fttm_np,
           GREATEST((c.obs_date - c.selected_report_date), 0)::double precision
               AS report_age_days
    FROM months m
    JOIN pit.pit_stock_fttm_monthly c ON c.obs_date = m.obs_date
    JOIN pit.pit_stock_fttm_monthly p
      ON p.ts_code = c.ts_code
     AND p.org_name = c.org_name
     AND p.obs_date = (date_trunc('month', c.obs_date) - INTERVAL '1 day')::date
), industry_stock AS (
    SELECT m.obs_date, m.ts_code, m.org_name,
           c.industry_code1 AS industry_code,
           m.current_fttm_np, m.previous_fttm_np, m.report_age_days,
           e.book_equity
    FROM matched m
    JOIN pit.pit_industry_classification c
      ON c.obs_date = m.obs_date
     AND c.ts_code = m.ts_code
     AND c.data_source = 'sw'
    JOIN stock_equity e
      ON e.obs_date = m.obs_date
     AND e.ts_code = m.ts_code
    WHERE c.industry_code1 IS NOT NULL
), industry_org AS (
    SELECT obs_date, industry_code, org_name,
           COUNT(DISTINCT ts_code)::integer AS common_stock_count,
           SUM(current_fttm_np) / NULLIF(SUM(book_equity), 0)
               AS current_industry_roe,
           SUM(previous_fttm_np) / NULLIF(SUM(book_equity), 0)
               AS previous_industry_roe,
           AVG(report_age_days) AS report_age_days
    FROM industry_stock
    GROUP BY obs_date, industry_code, org_name
    HAVING COUNT(DISTINCT ts_code) >= 2 AND SUM(book_equity) > 0
), benchmark_stock AS (
    SELECT m.obs_date, m.ts_code, m.org_name,
           m.current_fttm_np, m.previous_fttm_np, e.book_equity
    FROM matched m
    JOIN benchmark_members b
      ON b.obs_date = m.obs_date
     AND b.ts_code = m.ts_code
    JOIN stock_equity e
      ON e.obs_date = m.obs_date
     AND e.ts_code = m.ts_code
), benchmark_org AS (
    SELECT obs_date, org_name,
           SUM(current_fttm_np) / NULLIF(SUM(book_equity), 0)
               AS current_benchmark_roe,
           SUM(previous_fttm_np) / NULLIF(SUM(book_equity), 0)
               AS previous_benchmark_roe
    FROM benchmark_stock
    GROUP BY obs_date, org_name
    HAVING COUNT(DISTINCT ts_code) >= 20 AND SUM(book_equity) > 0
), relative_org AS (
    SELECT i.*,
           i.current_industry_roe - b.current_benchmark_roe
               AS current_relative_spread,
           i.previous_industry_roe - b.previous_benchmark_roe
               AS previous_relative_spread,
           CASE WHEN ABS(b.current_benchmark_roe) > 1e-12
                THEN i.current_industry_roe / b.current_benchmark_roe END
               AS current_relative_ratio,
           CASE WHEN ABS(b.previous_benchmark_roe) > 1e-12
                THEN i.previous_industry_roe / b.previous_benchmark_roe END
               AS previous_relative_ratio,
           LN(1.0 + i.common_stock_count)
             * POWER(0.5, i.report_age_days / 183.0) AS org_weight
    FROM industry_org i
    JOIN benchmark_org b USING (obs_date, org_name)
), direct AS (
    SELECT obs_date, industry_code,
           AVG((current_relative_spread > previous_relative_spread)::integer::double precision)
               AS fapi_spread_equal,
           SUM(org_weight * (current_relative_spread > previous_relative_spread)::integer)
             / NULLIF(SUM(org_weight), 0) AS fapi_spread_weighted,
           AVG((current_relative_ratio > previous_relative_ratio)::integer::double precision)
               FILTER (WHERE current_relative_ratio IS NOT NULL
                         AND previous_relative_ratio IS NOT NULL)
               AS fapi_ratio_equal,
           SUM(org_weight * (current_relative_ratio > previous_relative_ratio)::integer)
               FILTER (WHERE current_relative_ratio IS NOT NULL
                         AND previous_relative_ratio IS NOT NULL)
             / NULLIF(SUM(org_weight) FILTER (
                   WHERE current_relative_ratio IS NOT NULL
                     AND previous_relative_ratio IS NOT NULL), 0)
               AS fapi_ratio_weighted,
           AVG(current_industry_roe) AS expected_roe_equal,
           SUM(org_weight * current_industry_roe) / NULLIF(SUM(org_weight), 0)
               AS expected_roe_weighted
    FROM relative_org
    GROUP BY obs_date, industry_code
)
SELECT COUNT(*)::integer AS compared_rows,
       COUNT(*) FILTER (WHERE p.industry_code IS NULL)::integer
           AS missing_persisted_rows,
       MAX(ABS(d.fapi_spread_equal - p.fapi_spread_equal))
           AS max_abs_fapi_spread_equal,
       MAX(ABS(d.fapi_spread_weighted - p.fapi_spread_weighted))
           AS max_abs_fapi_spread_weighted,
       MAX(ABS(d.fapi_ratio_equal - p.fapi_ratio_equal))
           AS max_abs_fapi_ratio_equal,
       MAX(ABS(d.fapi_ratio_weighted - p.fapi_ratio_weighted))
           AS max_abs_fapi_ratio_weighted,
       MAX(ABS(d.expected_roe_equal - p.expected_roe_equal))
           AS max_abs_expected_roe_equal,
       MAX(ABS(d.expected_roe_weighted - p.expected_roe_weighted))
           AS max_abs_expected_roe_weighted
FROM direct d
LEFT JOIN pit.pit_industry_fapi_monthly p
  ON p.obs_date = d.obs_date
 AND p.industry_level = 'L1'
 AND p.industry_code = d.industry_code
 AND p.benchmark_code = '000906.SH'
 AND p.method_version = 'source_adapted_sw_csi800_v1'
"""


def _row(frame):
    if frame.empty:
        raise AssertionError("validation query returned no rows")
    return frame.iloc[0].to_dict()


def validate() -> dict:
    manager = PITIndustryFAPIManager()
    manager.__enter__()
    try:
        latest = manager._latest_available_month()
        if latest is None:
            raise AssertionError("no complete dependency month")
        summary = _row(
            manager.context.query_dataframe(SUMMARY_SQL, (EXPECTED_START, latest))
        )
        integrity = _row(
            manager.context.query_dataframe(
                INTEGRITY_SQL,
                (EXPECTED_START, latest, EXPECTED_START, latest),
            )
        )
        recompute = _row(
            manager.context.query_dataframe(DIRECT_RECOMPUTE_SQL, SAMPLE_MONTHS)
        )
    finally:
        manager.__exit__(None, None, None)

    expected_months = len(pd.date_range(EXPECTED_START, latest, freq="ME"))
    assert summary["min_obs_date"] == EXPECTED_START
    assert summary["max_obs_date"] == latest
    assert int(summary["month_count"]) == expected_months
    assert int(summary["missing_month_count"]) == 0
    for key, value in integrity.items():
        assert int(value or 0) == 0, f"{key}={value}"
    assert int(recompute["compared_rows"]) > 0
    assert int(recompute["missing_persisted_rows"]) == 0
    for key, value in recompute.items():
        if key.startswith("max_abs_"):
            assert float(value or 0.0) <= 1e-9, f"{key}={value}"
    return {
        "status": "passed",
        "latest_complete_month": latest,
        "sample_months": SAMPLE_MONTHS,
        "summary": summary,
        "integrity": integrity,
        "direct_recompute": recompute,
    }


if __name__ == "__main__":
    print(json.dumps(validate(), ensure_ascii=False, default=str, indent=2))
