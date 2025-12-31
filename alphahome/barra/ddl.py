from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BarraDDL:
    """DDL generator for Barra schema.

    Note: Industry one-hot column set is generated dynamically from DB content
    (distinct SW L1 codes). This class only holds templates.
    """

    schema: str = "barra"

    def create_schema_sql(self) -> str:
        return f"CREATE SCHEMA IF NOT EXISTS {self.schema};"

    def create_pit_sw_view_sql(self) -> str:
        # out_date is confirmed as the last effective day
        return f"""
CREATE OR REPLACE VIEW {self.schema}.pit_sw_industry_member_mv AS
SELECT
  ts_code,
  in_date::date  AS query_start_date,
  COALESCE(out_date::date, '2099-12-31'::date) AS query_end_date,
  l1_code,
  l1_name,
  l2_code,
  l2_name,
  l3_code,
  l3_name
FROM rawdata.index_swmember
WHERE ts_code IS NOT NULL
  AND in_date IS NOT NULL;
""".strip()

    def create_industry_dim_table_sql(self) -> str:
        return f"""
CREATE TABLE IF NOT EXISTS {self.schema}.industry_l1_dim (
  l1_code TEXT PRIMARY KEY,
  l1_name TEXT,
  column_name TEXT NOT NULL UNIQUE,
  updated_at TIMESTAMP DEFAULT NOW()
);
""".strip()

    def create_partitioned_table_sql(self, table_name: str, columns_sql: str, pk_sql: str) -> str:
        return f"""
CREATE TABLE IF NOT EXISTS {self.schema}.{table_name} (
{columns_sql},
{pk_sql}
) PARTITION BY RANGE (trade_date);
""".strip()

    def create_default_partition_sql(self, table_name: str) -> str:
        return f"""
CREATE TABLE IF NOT EXISTS {self.schema}.{table_name}_default
PARTITION OF {self.schema}.{table_name} DEFAULT;
""".strip()
