"""
Database DDL files for Clean Layer schema.

This package contains SQL DDL files for creating the clean schema
and its core tables. These files are designed to be executed in
dev/staging environments. Production deployment requires DBA approval.

Tables:
- clean.index_valuation_base: Index valuation base data
- clean.index_volatility_base: Index volatility base data
- clean.industry_base: Industry index base data
- clean.money_flow_base: Market money flow base data
- clean.market_technical_base: Market technical base data

All tables include standard lineage columns:
- _source_table: Source table name(s)
- _processed_at: Processing timestamp (UTC)
- _data_version: Data version identifier
- _ingest_job_id: Task execution instance ID
"""
