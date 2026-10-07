"""Causal rank SQL builders with explicit, caller-approved policy arguments.

These builders do not choose a strategy's window or warmup. Daily availability
is reconstructed from public dates, not ingestion/last-update timestamps.
"""
import re


def _minimum(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 2:
        raise ValueError('Rank warmup must be an explicit integer of at least two')
    return value


def fund_holdings_sql(*, min_history_periods: int) -> str:
    """Expand over distinct report periods, using each period's as-of snapshot."""
    minimum = _minimum(min_history_periods)
    return f"""
        CREATE MATERIALIZED VIEW features.mv_fund_holdings_quarterly AS
        WITH holdings AS (
            -- Retain zero/NULL revisions until after version selection so an
            -- obsolete positive position cannot be resurrected.
            SELECT DISTINCT ON (source.symbol, source.ts_code, source.end_date, source.ann_date)
                source.symbol AS ts_code, source.ts_code AS fund_code,
                source.end_date, source.ann_date,
                source.mkv AS holding_value, source.amount AS holding_shares,
                source.stk_mkv_ratio
            FROM rawdata.fund_portfolio source
            WHERE source.symbol IS NOT NULL AND source.ts_code IS NOT NULL
              AND source.end_date IS NOT NULL AND source.ann_date IS NOT NULL
            ORDER BY source.symbol, source.ts_code, source.end_date, source.ann_date,
                     source.update_time DESC NULLS LAST, md5(row_to_json(source)::text) DESC
        ),
        position_versions AS (
            SELECT *, LEAD(ann_date) OVER (
                PARTITION BY ts_code, fund_code, end_date ORDER BY ann_date
            ) AS next_ann_date
            FROM holdings
        ),
        period_events AS (
            SELECT DISTINCT ts_code, end_date, ann_date FROM holdings
        ),
        period_snapshots AS (
            -- One version per stock/fund/report period at this publication.
            SELECT event.ts_code, event.end_date, event.ann_date,
                   COUNT(position.fund_code) FILTER (WHERE position.holding_value > 0) AS fund_count,
                   COALESCE(SUM(position.holding_value) FILTER (WHERE position.holding_value > 0), 0) AS total_holding_value,
                   SUM(position.holding_shares) FILTER (WHERE position.holding_value > 0) AS total_holding_shares,
                   AVG(position.stk_mkv_ratio) FILTER (WHERE position.holding_value > 0) AS avg_fund_ratio,
                   MAX(position.stk_mkv_ratio) FILTER (WHERE position.holding_value > 0) AS max_fund_ratio
            FROM period_events event
            JOIN position_versions position
              ON position.ts_code = event.ts_code AND position.end_date = event.end_date
             AND position.ann_date <= event.ann_date
             AND (position.next_ann_date IS NULL OR position.next_ann_date > event.ann_date)
            GROUP BY event.ts_code, event.end_date, event.ann_date
        ),
        stock_events AS (
            SELECT DISTINCT ts_code, ann_date FROM period_events
        ),
        period_vintages AS (
            SELECT snapshot.*, LEAD(ann_date) OVER (
                PARTITION BY ts_code,end_date ORDER BY ann_date
            ) AS next_snapshot_date
            FROM period_snapshots snapshot
        ),
        known_periods AS (
            -- Interval join chooses one known vintage per report period.
            -- A single hash join avoids reparsing a stock's entire JSON
            -- history, and sorting it, once per announcement.
            SELECT event.ts_code,event.ann_date,snapshot.end_date,
                   snapshot.fund_count,snapshot.total_holding_value,
                   snapshot.total_holding_shares,snapshot.avg_fund_ratio,
                   snapshot.max_fund_ratio,
                   ROW_NUMBER() OVER (
                       PARTITION BY event.ts_code,event.ann_date
                       ORDER BY snapshot.end_date DESC
                   ) AS period_order,
                   FIRST_VALUE(snapshot.fund_count) OVER (
                       PARTITION BY event.ts_code,event.ann_date
                       ORDER BY snapshot.end_date DESC
                   ) AS current_fund_count
            FROM stock_events event
            JOIN period_vintages snapshot
              ON snapshot.ts_code=event.ts_code
             AND snapshot.ann_date<=event.ann_date
             AND (snapshot.next_snapshot_date>event.ann_date OR snapshot.next_snapshot_date IS NULL)
        ),
        asof_snapshots AS (
            SELECT ts_code,ann_date,
                   MAX(end_date) FILTER (WHERE period_order = 1) AS end_date,
                   MAX(fund_count) FILTER (WHERE period_order = 1) AS fund_count,
                   MAX(total_holding_value) FILTER (WHERE period_order = 1) AS total_holding_value,
                   MAX(total_holding_shares) FILTER (WHERE period_order = 1) AS total_holding_shares,
                   MAX(avg_fund_ratio) FILTER (WHERE period_order = 1) AS avg_fund_ratio,
                   MAX(max_fund_ratio) FILTER (WHERE period_order = 1) AS max_fund_ratio,
                   MAX(fund_count) FILTER (WHERE period_order = 2) AS previous_fund_count,
                   MAX(total_holding_value) FILTER (WHERE period_order = 2) AS previous_holding_value,
                   COUNT(*) AS fund_count_pctl_sample_count,
                   COUNT(*) FILTER (WHERE fund_count < current_fund_count) AS less_count
            FROM known_periods
            GROUP BY ts_code,ann_date
        ),
        with_chg AS (
            SELECT *, fund_count - previous_fund_count AS fund_count_chg,
                   total_holding_value - previous_holding_value AS holding_value_chg,
                   CASE WHEN fund_count_pctl_sample_count >= {minimum}
                        THEN less_count::double precision / (fund_count_pctl_sample_count - 1)
                        ELSE NULL END AS fund_count_pctl
            FROM asof_snapshots
        )
        SELECT ts_code, end_date, ann_date,
               ann_date AS query_start_date,
               COALESCE(LEAD(ann_date) OVER (PARTITION BY ts_code ORDER BY ann_date) - 1,
                        '2099-12-31'::date) AS query_end_date,
               fund_count, fund_count_chg, total_holding_value, holding_value_chg,
               total_holding_shares, avg_fund_ratio, max_fund_ratio, fund_count_pctl,
               fund_count_pctl_sample_count,
               CASE WHEN fund_count_chg > 0 AND fund_count_pctl > 0.8 THEN 'CROWDED_UP'
                    WHEN fund_count_chg < 0 AND fund_count_pctl < 0.3 THEN 'UNCROWDED_DOWN'
                    WHEN fund_count_pctl > 0.9 THEN 'HIGHLY_CROWDED'
                    ELSE 'NORMAL' END AS crowd_signal,
               'rawdata.fund_portfolio' AS _source_table,
               NOW() AS _processed_at, CURRENT_DATE AS _data_version
        FROM with_chg
    """


def ah_premium_sql(*, history_interval: str, min_observations: int) -> str:
    """PERCENT_RANK-equivalent strict-less counts within an inclusive window."""
    minimum = _minimum(min_observations)
    if not isinstance(history_interval, str) or not re.fullmatch(r'[1-9][0-9]* (?:day|month|year)s?', history_interval):
        raise ValueError('Explicit history interval must be positive days, months or years')
    return f"""
        CREATE MATERIALIZED VIEW features.mv_ah_premium_daily AS
        WITH stock_level AS NOT MATERIALIZED (
            SELECT trade_date, ts_code, hk_code, name,
                   close AS a_close, hk_close, pct_chg AS a_pct_chg, hk_pct_chg,
                   ah_comparison, ah_premium
            FROM rawdata.stock_ahcomparison
            WHERE trade_date IS NOT NULL AND ah_premium IS NOT NULL
        ),
        lookbacks AS (
            -- Keep the bounded date window in a PostgreSQL window aggregate;
            -- do not re-scan the source relation once for every output row.
            SELECT *, ARRAY_AGG(ah_premium) OVER (
                PARTITION BY ts_code ORDER BY trade_date::timestamp
                RANGE BETWEEN INTERVAL '{history_interval}' PRECEDING AND CURRENT ROW
            ) AS window_premiums
            FROM stock_level
        ),
        with_pctl AS (
            SELECT current.*,
                   history.sample_count AS ah_premium_pctl_sample_count,
                   CASE WHEN history.sample_count >= {minimum}
                        THEN history.less_count::double precision / (history.sample_count - 1)
                        ELSE NULL END AS ah_premium_pctl,
                   ah_premium - AVG(ah_premium) OVER (
                       PARTITION BY current.ts_code ORDER BY current.trade_date
                       ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                   ) AS ah_premium_dev_ma20
            FROM lookbacks current
            CROSS JOIN LATERAL (
                SELECT COUNT(*) AS sample_count,
                       COUNT(*) FILTER (WHERE prior.ah_premium < current.ah_premium) AS less_count
                FROM UNNEST(current.window_premiums) AS prior(ah_premium)
            ) history
        ),
        market_agg AS (
            SELECT trade_date, COUNT(*) AS ah_stock_count,
                   AVG(ah_premium) AS ah_premium_avg,
                   PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ah_premium) AS ah_premium_median,
                   STDDEV(ah_premium) AS ah_premium_std,
                   COUNT(*) FILTER (WHERE ah_premium > 50) * 100.0 / COUNT(*) AS high_premium_pct,
                   COUNT(*) FILTER (WHERE ah_premium < 0) * 100.0 / COUNT(*) AS discount_pct
            FROM stock_level GROUP BY trade_date
        )
        SELECT s.trade_date, s.ts_code, s.hk_code, s.name, s.a_close, s.hk_close,
               s.a_pct_chg, s.hk_pct_chg, s.ah_comparison, s.ah_premium,
               s.ah_premium_pctl, s.ah_premium_dev_ma20,
               s.ah_premium_pctl_sample_count,
               m.ah_stock_count, m.ah_premium_avg AS market_ah_premium_avg,
               m.ah_premium_median AS market_ah_premium_median,
               m.ah_premium_std AS market_ah_premium_std,
               m.high_premium_pct, m.discount_pct,
               CASE WHEN s.ah_premium_pctl > 0.9 THEN 'A_EXPENSIVE'
                    WHEN s.ah_premium_pctl < 0.1 THEN 'A_CHEAP'
                    ELSE 'NEUTRAL' END AS arbitrage_signal,
               'rawdata.stock_ahcomparison' AS _source_table,
               NOW() AS _processed_at, CURRENT_DATE AS _data_version
        FROM with_pctl s JOIN market_agg m ON s.trade_date = m.trade_date
    """
