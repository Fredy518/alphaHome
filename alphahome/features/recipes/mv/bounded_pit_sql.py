"""Bounded computations with unchanged availability and version rules.

Each lateral computation reads retained source rows for one stock, fund, or
trading day. The holdings recipe retains deterministic same-key deduplication;
zero/NULL revisions suppress obsolete positive positions. NAV dates remain
eligible only after their announcement day. These transformations do not
certify historical publication, receipt times, or overwritten source vintages.
"""
def index_daily_sql(sql):
 target,query=sql.split(' AS',1)
 left=query.index('weight_dates AS (')
 right=query.index('-- PIT 权重\n',left)
 replacement='''pit_weight_lookup AS (
            SELECT repair_day.trade_date, i.idx_code AS index_code, latest.weight_date AS pit_weight_date
            FROM core_indexes i
            CROSS JOIN LATERAL (
                SELECT trade_date AS weight_date FROM tushare.index_weight
                WHERE index_code=i.idx_code AND trade_date<=repair_day.trade_date
                ORDER BY trade_date DESC LIMIT 1
            ) latest
        ),
        '''
 query=query[:left]+replacement+query[right:]
 query=query.replace('WHERE pe_ttm IS NOT NULL OR pb IS NOT NULL OR dv_ratio IS NOT NULL',
   'WHERE trade_date=repair_day.trade_date AND (pe_ttm IS NOT NULL OR pb IS NOT NULL OR dv_ratio IS NOT NULL)',1)
 query=query.replace('stock_valuation AS (','stock_valuation AS MATERIALIZED (',1)
 # Per-day normalization remains exactly the same; no metadata normalization.
 query=query.replace('WITH NO DATA','')
 return target+''' AS SELECT result.* FROM
        (SELECT DISTINCT trade_date FROM tushare.stock_dailybasic WHERE trade_date IS NOT NULL) repair_day
        CROSS JOIN LATERAL ('''+query+''') result WITH NO DATA'''

def etf_event_timeline_sql(sql):
 left=sql.index('        -- 净值数据')
 right=sql.index('        -- 计算净申赎',left)
 replacement='''        -- A NAV is first usable on max(nav_date, ann_date+1).
        -- Event-time running MAX picks the latest eligible NAV date even when
        -- an older NAV has a delayed announcement. Current retained NAVs only.
        nav_events AS (
            SELECT n.ts_code,GREATEST(n.nav_date,n.ann_date+1) AS event_date,MAX(n.nav_date) AS nav_date
            FROM tushare.fund_nav n JOIN dynamic_etfs e ON n.ts_code=e.ts_code
            WHERE n.unit_nav IS NOT NULL AND n.unit_nav>0 AND n.ann_date IS NOT NULL
            GROUP BY n.ts_code,GREATEST(n.nav_date,n.ann_date+1)
        ),
        timeline AS (
            SELECT ts_code,trade_date AS event_date FROM shares
            UNION SELECT ts_code,event_date FROM nav_events
        ),
        visible_nav AS (
            SELECT t.ts_code,t.event_date,
                   MAX(n.nav_date) OVER(PARTITION BY t.ts_code ORDER BY t.event_date ROWS UNBOUNDED PRECEDING) AS nav_date
            FROM timeline t LEFT JOIN nav_events n USING(ts_code,event_date)
        ),
        merged AS (
            SELECT s.ts_code,s.trade_date,s.fd_share,n.unit_nav,
                   s.fd_share*n.unit_nav/10000 AS aum,
                   LAG(s.fd_share) OVER(PARTITION BY s.ts_code ORDER BY s.trade_date) AS prev_share,
                   LAG(s.fd_share*n.unit_nav/10000) OVER(PARTITION BY s.ts_code ORDER BY s.trade_date) AS prev_aum
            FROM shares s LEFT JOIN visible_nav v ON v.ts_code=s.ts_code AND v.event_date=s.trade_date
            LEFT JOIN tushare.fund_nav n ON n.ts_code=v.ts_code AND n.nav_date=v.nav_date
        ),
        '''
 return sql[:left]+replacement+sql[right:]

def etf_bounded_fund_sql(sql):
 sql=etf_event_timeline_sql(sql)
 left=sql.index('        shares AS (')
 right=sql.index('        -- 按日期汇总',left)
 inner=sql[left:right].strip().rstrip(',')
 marker='JOIN dynamic_etfs e ON s.ts_code = e.ts_code'
 assert inner.count(marker)==1
 inner=inner.replace(marker,'').replace('WHERE s.fd_share IS NOT NULL','WHERE s.ts_code=repair_etf.ts_code AND s.fd_share IS NOT NULL',1).replace('e.list_date','repair_etf.list_date').replace('e.index_code','repair_etf.index_code')
 marker='FROM tushare.fund_nav n JOIN dynamic_etfs e ON n.ts_code=e.ts_code'
 assert inner.count(marker)==1
 inner=inner.replace(marker,'FROM nav_source n',1)
 inner=inner.replace('LEFT JOIN tushare.fund_nav n ON n.ts_code=v.ts_code AND n.nav_date=v.nav_date','LEFT JOIN nav_source n ON n.ts_code=v.ts_code AND n.nav_date=v.nav_date',1)
 nav='''nav_source AS MATERIALIZED (
        SELECT ts_code,nav_date,ann_date,unit_nav FROM tushare.fund_nav
        WHERE ts_code=repair_etf.ts_code AND unit_nav>0 AND ann_date IS NOT NULL
    ),'''
 return sql[:left]+'''flows AS (
        SELECT result.* FROM dynamic_etfs repair_etf CROSS JOIN LATERAL (WITH '''+nav+inner+''' SELECT * FROM flows) result
    ),
    '''+sql[right:]


def bounded_stock_source_sql(sql):
    target,query=sql.split(' AS',1)
    query=query.replace('WHERE source.symbol IS NOT NULL','WHERE source.symbol=repair_stock.symbol AND source.symbol IS NOT NULL',1)
    return target+''' AS SELECT result.*
        FROM (SELECT DISTINCT symbol FROM rawdata.fund_portfolio
              WHERE symbol IS NOT NULL AND ts_code IS NOT NULL AND ann_date IS NOT NULL AND end_date IS NOT NULL) repair_stock
        CROSS JOIN LATERAL ('''+query+''') result'''

def cumulative_position_delta_sql(sql):
    """Per-fund revision deltas avoid expanding every position at every event.

    MAX ratio uses the current source's latest eligible fund/period version;
    the direct-source lookup is backed by a bounded partial index in production.
    Exact numeric sums preserve NULL sum/average semantics via observation counts.
    """
    left=sql.index('        position_versions AS (')
    right=sql.index('        stock_events AS (',left)
    replacement='''        normalized_positions AS (
            SELECT *,CASE WHEN holding_value>0 THEN 1 ELSE 0 END AS positive_count,
                CASE WHEN holding_value>0 THEN holding_value ELSE 0 END AS positive_value,
                CASE WHEN holding_value>0 THEN holding_shares END AS positive_shares,
                CASE WHEN holding_value>0 THEN stk_mkv_ratio END AS positive_ratio
            FROM holdings
        ),
        position_changes AS (
            SELECT ts_code,end_date,ann_date,
                positive_count-COALESCE(LAG(positive_count) OVER version_order,0) AS count_delta,
                positive_value-COALESCE(LAG(positive_value) OVER version_order,0) AS value_delta,
                COALESCE(positive_shares,0)-COALESCE(LAG(positive_shares) OVER version_order,0) AS shares_delta,
                (CASE WHEN positive_shares IS NOT NULL THEN 1 ELSE 0 END)-COALESCE(LAG(CASE WHEN positive_shares IS NOT NULL THEN 1 ELSE 0 END) OVER version_order,0) AS shares_count_delta,
                COALESCE(positive_ratio,0)-COALESCE(LAG(positive_ratio) OVER version_order,0) AS ratio_delta,
                (CASE WHEN positive_ratio IS NOT NULL THEN 1 ELSE 0 END)-COALESCE(LAG(CASE WHEN positive_ratio IS NOT NULL THEN 1 ELSE 0 END) OVER version_order,0) AS ratio_count_delta
            FROM normalized_positions
            WINDOW version_order AS(PARTITION BY ts_code,fund_code,end_date ORDER BY ann_date)
        ),
        period_deltas AS (
            SELECT ts_code,end_date,ann_date,SUM(count_delta) AS count_delta,
                SUM(value_delta) AS value_delta,SUM(shares_delta) AS shares_delta,
                SUM(shares_count_delta) AS shares_count_delta,SUM(ratio_delta) AS ratio_delta,
                SUM(ratio_count_delta) AS ratio_count_delta
            FROM position_changes GROUP BY ts_code,end_date,ann_date
        ),
        period_events AS(SELECT ts_code,end_date,ann_date FROM period_deltas),
        cumulative_period AS (
            SELECT ts_code,end_date,ann_date,SUM(count_delta) OVER period_order AS fund_count,
                SUM(value_delta) OVER period_order AS total_holding_value,
                SUM(shares_delta) OVER period_order AS shares_sum,
                SUM(shares_count_delta) OVER period_order AS shares_count,
                SUM(ratio_delta) OVER period_order AS ratio_sum,
                SUM(ratio_count_delta) OVER period_order AS ratio_count
            FROM period_deltas WINDOW period_order AS(PARTITION BY ts_code,end_date ORDER BY ann_date ROWS UNBOUNDED PRECEDING)
        ),
        period_snapshots AS (
            SELECT event.ts_code,event.end_date,event.ann_date,event.fund_count::bigint,
                event.total_holding_value,
                CASE WHEN event.shares_count>0 THEN event.shares_sum END AS total_holding_shares,
                CASE WHEN event.ratio_count>0 THEN event.ratio_sum/event.ratio_count END AS avg_fund_ratio,
                ratio.stk_mkv_ratio AS max_fund_ratio
            FROM cumulative_period event
            LEFT JOIN LATERAL (
                SELECT source.stk_mkv_ratio FROM rawdata.fund_portfolio source
                WHERE source.symbol=event.ts_code AND source.end_date=event.end_date
                  AND source.ann_date<=event.ann_date AND source.mkv>0 AND source.stk_mkv_ratio IS NOT NULL
                  AND NOT EXISTS(SELECT 1 FROM rawdata.fund_portfolio later
                    WHERE later.symbol=source.symbol AND later.ts_code=source.ts_code
                      AND later.end_date=source.end_date AND later.ann_date<=event.ann_date
                      AND (later.ann_date>source.ann_date OR
                        (later.ann_date=source.ann_date AND
                         ROW(later.update_time IS NOT NULL,COALESCE(later.update_time,'-infinity'::timestamp),md5(row_to_json(later)::text))
                           > ROW(source.update_time IS NOT NULL,COALESCE(source.update_time,'-infinity'::timestamp),md5(row_to_json(source)::text)))))
                ORDER BY source.stk_mkv_ratio DESC LIMIT 1
            ) ratio ON TRUE
        ),
        '''
    return sql[:left]+replacement+sql[right:]
