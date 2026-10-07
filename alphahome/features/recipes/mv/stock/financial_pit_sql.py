"""Shared public-version windows for the financial statement feature projections."""

from hashlib import sha256

from alphahome.pit.disclosure import FINANCIAL_PIT_CONTRACT, public_date_sql


def financial_statement_sql(target, sources, fields):
    """One active version per (stock, period), with report > express > forecast.

    All distinct public events remain in the projection. Same-day lower-priority
    sources remain in raw/PIT tables but do not create overlapping feature rows.
    Different report periods have independent windows, including simultaneous
    annual/Q1 publications. Receipt timestamps only break same-event ties.
    """
    selects = []
    for source, table, expressions in sources:
        report = source == "report"
        date = public_date_sql() if report else "ann_date"
        actual = "f_ann_date" if report else "NULL::date"
        report_filter = "AND (report_type = 1 OR report_type IS NULL)" if report else ""
        metrics = ", ".join(
            f"{expr} AS {name}" for name, expr in zip(fields, expressions)
        )
        selects.append(
            f"""
            SELECT ts_code, end_date, {date} AS pit_ann_date,
                   ann_date AS source_ann_date, {actual} AS source_f_ann_date,
                   update_time AS source_update_time,
                   md5(row_to_json(source)::text) AS source_version_hash,
                   '{source}'::text AS data_source, '{table}'::text AS _source_table,
                   {metrics}
            FROM {table} source
            WHERE ts_code IS NOT NULL AND end_date IS NOT NULL
              AND {date} IS NOT NULL {report_filter}
        """
        )
    union = "\nUNION ALL\n".join(selects)
    metrics = ", ".join(f"a.{name}" for name in fields)
    query = f"""
        WITH all_data AS ({union}), ranked AS (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY ts_code, end_date, pit_ann_date
                ORDER BY CASE data_source WHEN 'report' THEN 1 WHEN 'express' THEN 2
                          WHEN 'forecast' THEN 3 ELSE 9 END,
                         source_update_time DESC NULLS LAST, source_version_hash DESC
            ) AS rn
            FROM all_data
        ), events AS (
            SELECT * FROM ranked WHERE rn = 1
        ), distinct_dates AS (
            SELECT DISTINCT ts_code, end_date, pit_ann_date FROM events
        ), next_dates AS (
            SELECT ts_code, end_date, pit_ann_date,
                   LEAD(pit_ann_date) OVER (
                       PARTITION BY ts_code, end_date ORDER BY pit_ann_date
                   ) AS next_ann_date
            FROM distinct_dates
        )
        SELECT a.ts_code, a.pit_ann_date AS ann_date, a.end_date AS report_period,
               a.pit_ann_date AS query_start_date,
               COALESCE(n.next_ann_date - 1, DATE '2099-12-31') AS query_end_date,
               {metrics}, a.data_source,
               a.source_ann_date, a.source_f_ann_date, a.source_update_time,
               a.source_version_hash, a._source_table,
               NOW() AS _processed_at, CURRENT_DATE AS _data_version,
               '{FINANCIAL_PIT_CONTRACT}'::text AS _pit_contract_version,
               '{{definition_signature}}'::text AS _pit_definition_hash
        FROM events a JOIN next_dates n
          ON a.ts_code = n.ts_code AND a.end_date = n.end_date
         AND a.pit_ann_date = n.pit_ann_date
    """
    signature = sha256(" ".join(query.split()).encode()).hexdigest()
    query = query.replace("{definition_signature}", signature)
    return f"CREATE MATERIALIZED VIEW {target} AS {query}"
