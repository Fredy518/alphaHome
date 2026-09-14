"""Read-only eligibility reconciliation used by factor readiness checks."""

INPUT_RELATIONS = (
    "pit.pit_income_quarterly", "pit.pit_balance_quarterly",
    "pit.pit_financial_indicators", "tushare.stock_basic",
)


def financial_input_gap_sql(placeholder="%s"):
    if placeholder not in {"%s", "$1"}:
        raise ValueError("Unsupported query parameter convention")
    return f"""
        WITH boundary AS (SELECT {placeholder}::date AS cutoff),
        candidates AS (
            SELECT DISTINCT i.ts_code, i.end_date, i.ann_date, i.data_source,
                   COALESCE(i.conversion_status, '') = 'RPT_ORIG' AS unconverted,
                   sb.ts_code IS NULL AS missing_master,
                   sb.list_date > boundary.cutoff AS not_yet_listed,
                   sb.delist_date <= boundary.cutoff AS delisted,
                   EXISTS (
                       SELECT 1 FROM pit.pit_balance_quarterly b
                       WHERE b.ts_code=i.ts_code AND b.end_date=i.end_date
                         AND b.ann_date<=i.ann_date AND b.data_source IN ('report','express')
                         AND b.tot_assets IS NOT NULL
                   ) AS has_balance
            FROM pit.pit_income_quarterly i CROSS JOIN boundary
            LEFT JOIN tushare.stock_basic sb ON sb.ts_code=i.ts_code
            WHERE i.ann_date<=boundary.cutoff
              AND i.end_date>=boundary.cutoff - INTERVAL '10 months'
              AND i.data_source IN ('report','express')
        ), eligible AS (
            SELECT DISTINCT ON (ts_code) * FROM candidates WHERE NOT missing_master AND NOT unconverted
              AND NOT COALESCE(not_yet_listed, TRUE) AND NOT COALESCE(delisted, FALSE)
              AND has_balance
            ORDER BY ts_code, ann_date DESC, end_date DESC,
                     CASE data_source WHEN 'report' THEN 0 ELSE 1 END
        ), missing AS (
            SELECT e.ts_code FROM eligible e CROSS JOIN boundary
            WHERE NOT EXISTS (
                SELECT 1 FROM pit.pit_financial_indicators f
                WHERE f.ts_code=e.ts_code AND f.end_date=e.end_date
                  AND f.ann_date>=e.ann_date AND f.ann_date<=boundary.cutoff
                  AND f.data_source IN ('report','express')
                  AND f.calculation_status='success'
                  AND f.data_quality IN ('high','normal','outlier_high','outlier_low')
            )
        )
        SELECT /* eligible_pit_input_gaps */
            (SELECT count(DISTINCT ts_code) FROM eligible)::bigint AS eligible_count,
            (SELECT count(DISTINCT ts_code) FROM missing)::bigint AS eligible_missing,
            count(DISTINCT ts_code) FILTER (WHERE missing_master)::bigint AS missing_master,
            count(DISTINCT ts_code) FILTER (WHERE delisted)::bigint AS delisted,
            count(DISTINCT ts_code) FILTER (WHERE not_yet_listed)::bigint AS not_yet_listed,
            count(DISTINCT ts_code) FILTER (WHERE unconverted)::bigint AS unconverted,
            count(DISTINCT ts_code) FILTER (WHERE NOT has_balance)::bigint AS missing_balance
        FROM candidates
    """
