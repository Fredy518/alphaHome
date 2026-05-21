# Tinysoft Task Priority Review - 2026-05-22

## Decision Rules

- P0: Keep and backfill first. These are source-of-truth tables, hard-to-reconstruct event data, mappings, memberships, or PIT metadata.
- P1: Keep, but backfill after P0. These are useful source tables with larger volume, narrower use, or partial overlap with existing data.
- P2: Optional. Keep registered, but do not include in routine FULL backfills unless a research need appears.
- P3: Prefer local computation. Do not run OPI FULL collection for these unless a vendor-vs-local reconciliation is explicitly needed.
- EXCLUDE: Minute-line tasks. Keep as manually scoped jobs, never part of broad FULL backfills.
- REMOVED: Delete the registered fetch task and any partial table/view created by the old task.

## Priority List

| Priority | Task | Reason | Suggested Action |
|---|---|---|---|
| P0 | `tinysoft_stock_basic_ext` | Stock static metadata and exchange/industry links are source fields. | Keep |
| P0 | `tinysoft_stock_suspend` | Suspension events are PIT-sensitive and not safely derived from prices. | Keep |
| P0 | `tinysoft_stock_industry_versioned` | Versioned industry membership is core as-of metadata. | Keep |
| P0 | `tinysoft_stock_fina_pit_ext` | PIT financial announcement metadata is source-critical. | Keep |
| P0 | `tinysoft_stock_hsgt_daily` | North/southbound channel turnover is a source table. | Keep |
| P0 | `tinysoft_stock_hsgt_hold` | Channel holdings have disclosure-cycle differences and are hard to rebuild. | Keep |
| P0 | `tinysoft_stock_hsgt_top10` | Broker/channel active-trading events are source records. | Keep |
| P0 | `tinysoft_stock_hsgt_short_balance` | HSGT short-balance data is a source-specific field. | Keep |
| P0 | `tinysoft_stock_lending_balance` | Securities lending balance is a source state table. | Keep |
| P0 | `tinysoft_stock_public_trade_info` | Public trading event details are hard to reconstruct. | Keep |
| P0 | `tinysoft_stock_holder_change_ext` | Shareholder change events need announcement semantics. | Keep |
| P0 | `tinysoft_stock_repurchase_ext` | Repurchase event process fields are source-specific. | Keep |
| P0 | `tinysoft_stock_unlock_schedule` | Future unlock schedule is event data, not derived from OHLCV. | Keep |
| P0 | `tinysoft_fund_basic_ext` | Fund static metadata and share-class links are foundational. | Keep |
| P0 | `tinysoft_fund_classification_info` | Classification dictionary/version source. | Keep |
| P0 | `tinysoft_fund_classification_member` | Historical classification membership is PIT-sensitive. | Keep |
| P0 | `tinysoft_fund_manager_ext` | Manager identity/resume/tenure metadata is source data. | Keep |
| P0 | `tinysoft_fund_asset_alloc` | Reported asset allocation is source disclosure. | Keep |
| P0 | `tinysoft_fund_industry_alloc` | Reported industry allocation is source disclosure. | Keep |
| P0 | `tinysoft_fund_stock_holding_detail` | Fund holdings are disclosed source records. | Keep |
| P0 | `tinysoft_fund_bond_holding_detail` | Fund bond holdings are disclosed source records. | Keep |
| P0 | `tinysoft_fund_holder_structure` | Holder structure is disclosed source data. | Keep |
| P0 | `tinysoft_index_basic_ext` | Index metadata is source data. | Keep |
| P0 | `tinysoft_index_member_versioned` | Index membership history is PIT-sensitive source data. | Keep |
| P0 | `tinysoft_market_calendar_multi` | Multi-market calendar cannot be safely inferred from A-share calendar alone. | Keep |
| P0 | `tinysoft_future_basic_ext` | Futures contract lifecycle metadata is source data. | Keep |
| P0 | `tinysoft_future_product_mapping_ext` | Product/main-contract mapping is operational source metadata. | Keep |
| P0 | `tinysoft_option_basic_daily_ext` | Daily option contract status is source data; keep financial ETF/index options only. | Keep |
| P0 | `tinysoft_bond_basic_ext` | Bond and convertible static metadata is source data. | Keep |
| P1 | `tinysoft_stock_lending_summary` | Useful aggregate/source fields, but lower priority than balances. | Keep after P0 |
| P1 | `tinysoft_stock_lending_trade` | Detailed lending trades can be large; keep for event-level research. | Keep after P0 |
| P1 | `tinysoft_stock_pledge_detail` | Source event detail; useful but lower priority than core PIT tables. | Keep after P0 |
| P1 | `tinysoft_stock_pledge_balance` | Source balance table; useful for pledge state. | Keep after P0 |
| P1 | `tinysoft_stock_pledge_summary` | Aggregate summary overlaps with detail/balance but may provide official totals. | Keep, low-frequency |
| P1 | `tinysoft_stock_pledge_rate` | Partly aggregate/derived, but official rate can be useful for reconciliation. | Keep, low-frequency |
| P1 | `tinysoft_fund_bond_alloc` | Reported bond allocation is source disclosure but large. | Keep after core holdings |
| P1 | `tinysoft_fund_financial_quarterly_ext` | Fund financial statement extensions are source fields but lower frequency. | Keep after P0 |
| P1 | `tinysoft_fund_top_holder` | Major-holder disclosure is source data but narrower use. | Keep after P0 |
| P1 | `tinysoft_fund_broker_seat` | Source disclosure and high volume; run only with COPY/index-drop optimization. | Keep, controlled FULL |
| P2 | `tinysoft_fund_abs_holding_detail` | Narrow asset class; optional unless ABS exposure is needed. | Keep optional |
| P2 | `tinysoft_fund_cbond_holding_detail` | Useful if convertible-bond fund exposure is in scope; otherwise optional. | Keep optional |
| P2 | `tinysoft_fund_fof_holding_detail` | Useful for FOF research; optional outside FOF workflows. | Keep optional |
| P2 | `tinysoft_fund_stock_trade_summary` | Cumulative trade summary overlaps with disclosed holdings and turnover analysis. | Keep optional |
| EXCLUDE | `tinysoft_stock_minute` | Minute-line task; high-volume operational collection only. | Exclude from broad FULL |
| EXCLUDE | `tinysoft_fund_minute` | Minute-line task; high-volume operational collection only. | Exclude from broad FULL |
| EXCLUDE | `tinysoft_index_minute` | Minute-line task; high-volume operational collection only. | Exclude from broad FULL |
| REMOVED | `tinysoft_fund_risk_adj_ext` | Risk metrics can be computed locally and OPI FULL collection is too expensive. | Removed |
| REMOVED | `tinysoft_fund_style_ext` | Style labels can be computed from holdings and local factor/style definitions. | Removed |
| REMOVED | `tinysoft_fund_category_index_ret` | Category index returns can be rebuilt from fund classifications and NAV panels. | Removed |
| REMOVED | `tinysoft_fund_manager_perf_ext` | Manager-period returns/ranks can be computed from manager tenure, fund NAV, and category benchmarks. | Removed |
| REMOVED | `tinysoft_fund_manager_risk_ext` | Risk-adjusted manager metrics are derived from NAV/benchmark return series. | Removed |
| REMOVED | `tinysoft_index_financial_agg` | Index financial aggregates can be computed from constituents, weights, and stock PIT financials. | Removed |

## Follow-Up Candidates

- Convert P3 tasks into processor tasks under a local factor/analytics layer instead of Tinysoft fetch tasks.
- Restrict broad FULL backfill runners to P0 and selected P1 by default.
- Require explicit allowlist for P2/P3 and EXCLUDE groups.
- For high-volume P1 tasks such as `tinysoft_fund_broker_seat`, use COPY/INSERT mode only after truncation and rebuild indexes afterward.
