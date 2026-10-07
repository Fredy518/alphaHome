"""Dated active-security snapshots; historical queries are not PIT certification."""

import pandas as pd

from alphahome.common.task_system.task_decorator import task_register
from alphahome.fetchers.sources.massive import MassiveAPIError, MassiveTask


@task_register()
class MassiveStockUsBasicTask(MassiveTask):
    name = "massive_stock_us_basic"
    description = "美股证券资料快照（Massive，保留普通股/ETF/ADR 类型）"
    table_name = "stock_us_basic"
    primary_keys = ["ticker", "snapshot_date"]
    date_column = "snapshot_date"
    snapshot_only = True
    replace_date_snapshots = True
    smart_lookback_days = 1
    schema_def = {
        "ticker": {"type": "VARCHAR(64)", "constraints": "NOT NULL"},
        "snapshot_date": {"type": "DATE", "constraints": "NOT NULL"},
        "name": {"type": "TEXT", "constraints": "NOT NULL"},
        "security_type": {"type": "VARCHAR(32)", "constraints": "NOT NULL"},
        "primary_exchange": {"type": "VARCHAR(16)", "constraints": "NOT NULL"},
        "currency_name": {"type": "VARCHAR(32)"},
        "active": {"type": "BOOLEAN", "constraints": "NOT NULL"},
        "cik": {"type": "VARCHAR(32)"},
        "composite_figi": {"type": "VARCHAR(32)"},
        "share_class_figi": {"type": "VARCHAR(32)"},
        "source_updated_at": {"type": "TIMESTAMPTZ"},
        "observed_at": {"type": "TIMESTAMPTZ", "constraints": "NOT NULL"},
    }
    indexes = [
        {
            "name": "idx_stock_us_basic_date_type",
            "columns": ["snapshot_date", "security_type"],
        }
    ]

    async def fetch_batch(self, params, stop_event=None):
        rows = await self.api.list_records(
            "/v3/reference/tickers",
            {
                "market": "stocks",
                "active": "true",
                "date": params["date"],
                "limit": 1000,
                "sort": "ticker",
                "order": "asc",
            },
            stop_event,
        )
        if not rows:
            raise MassiveAPIError("证券资料快照为空，拒绝推进快照日期")
        data = pd.DataFrame(rows)
        required = (
            "ticker",
            "name",
            "type",
            "primary_exchange",
            "active",
            "locale",
            "market",
        )
        if any(column not in data for column in required):
            raise MassiveAPIError("证券资料缺少必需分类字段")
        # OTC is excluded from grouped bars; keep only US exchange-listed issues.
        data = data[
            (data["locale"] == "us")
            & (data["market"] == "stocks")
            & data["primary_exchange"].notna()
        ].copy()
        if data.empty:
            raise MassiveAPIError("证券资料中没有美国交易所上市证券")
        minimum = max(1, int(self.task_specific_config.get("min_basic_records", 1000)))
        if len(data) < minimum:
            raise MassiveAPIError(
                f"证券快照仅 {len(data)} 条，低于全市场下限 {minimum}"
            )
        data = data.rename(
            columns={"type": "security_type", "last_updated_utc": "source_updated_at"}
        )
        # Historical reference responses can omit the classification of a few
        # otherwise valid listed issues. Preserve their membership explicitly;
        # never infer CS/ETF from the name or drop them from the dated universe.
        missing_type = data["security_type"].isna() | data["security_type"].astype(
            str
        ).str.strip().eq("")
        data.loc[missing_type, "security_type"] = "UNKNOWN"
        data["snapshot_date"] = pd.Timestamp(params["date"]).date()
        data["observed_at"] = self.now()
        for column in self.schema_def:
            if column not in data:
                data[column] = None
        data["source_updated_at"] = pd.to_datetime(
            data["source_updated_at"], utc=True, errors="raise"
        ).dt.floor(
            "us"
        )  # PostgreSQL TIMESTAMPTZ stores microseconds, not nanoseconds.
        return data[list(self.schema_def)]

    def process_data(self, data, **kwargs):
        data = super().process_data(data, **kwargs)
        if data is None or data.empty:
            return data
        for field in ("name", "security_type", "primary_exchange"):
            if (
                data[field].isna().any()
                or data[field].astype(str).str.strip().eq("").any()
            ):
                raise MassiveAPIError(f"证券资料 {field} 为空，拒绝不完整分类")
        if not data["active"].eq(True).all():
            raise MassiveAPIError("活跃证券查询返回了非活跃记录")
        return data
