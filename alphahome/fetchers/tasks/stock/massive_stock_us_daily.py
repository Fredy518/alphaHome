"""All US listed securities, one complete date per request; raw share basis."""

import pandas as pd

from alphahome.common.task_system.task_decorator import task_register
from alphahome.fetchers.sources.massive import MassiveAPIError, MassiveTask


@task_register()
class MassiveStockUsDailyTask(MassiveTask):
    name = "massive_stock_us_daily"
    description = "美股全市场日线（Massive，未复权，不含 OTC）"
    table_name = "stock_us_daily"
    primary_keys = ["ticker", "trade_date"]
    date_column = "trade_date"
    replace_date_snapshots = True
    schema_def = {
        "ticker": {"type": "VARCHAR(64)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        **{
            field: {"type": "DOUBLE PRECISION", "constraints": "NOT NULL"}
            for field in ("open", "high", "low", "close", "volume")
        },
        "vwap": {"type": "DOUBLE PRECISION"},
        "transactions": {"type": "BIGINT"},
        "bar_timestamp": {"type": "BIGINT", "constraints": "NOT NULL"},
        "adjusted": {"type": "BOOLEAN", "constraints": "NOT NULL"},
        "observed_at": {"type": "TIMESTAMPTZ", "constraints": "NOT NULL"},
    }
    indexes = [{"name": "idx_stock_us_daily_date", "columns": "trade_date"}]

    async def fetch_batch(self, params, stop_event=None):
        day = params["date"]
        payload = await self.api.request(
            f"/v2/aggs/grouped/locale/us/market/stocks/{day}",
            {"adjusted": "false", "include_otc": "false"},
            stop_event,
        )
        if payload.get("adjusted") is not False or payload.get("next_url"):
            raise MassiveAPIError("全市场日线必须是完整的未复权响应")
        rows = payload["results"]
        minimum = max(1, int(self.task_specific_config.get("min_daily_records", 1000)))
        if len(rows) < minimum:
            raise MassiveAPIError(
                f"交易日 {day} 仅返回 {len(rows)} 条，低于全市场下限 {minimum}"
            )
        data = pd.DataFrame(rows).rename(
            columns={
                "T": "ticker",
                "o": "open",
                "h": "high",
                "l": "low",
                "c": "close",
                "v": "volume",
                "vw": "vwap",
                "n": "transactions",
                "t": "bar_timestamp",
            }
        )
        required = ["ticker", "open", "high", "low", "close", "volume", "bar_timestamp"]
        if any(field not in data for field in required):
            raise MassiveAPIError("全市场日线缺少必需字段")
        if "otc" in data and data["otc"].fillna(False).any():
            raise MassiveAPIError("未包含 OTC 的请求返回了 OTC 记录")
        dates = (
            pd.to_datetime(data["bar_timestamp"], unit="ms", utc=True, errors="coerce")
            .dt.tz_convert("America/New_York")
            .dt.date
        )
        if not dates.eq(pd.Timestamp(day).date()).all():
            raise MassiveAPIError("行情时间戳与请求的美股交易日不一致")
        data["trade_date"] = dates
        data["adjusted"] = False
        data["observed_at"] = self.now()
        for field in ("vwap", "transactions"):
            if field not in data:
                data[field] = None
        return data[list(self.schema_def)]

    def process_data(self, data, **kwargs):
        data = super().process_data(data, **kwargs)
        if data is None or data.empty:
            return data
        self.require_numbers(data, ("open", "high", "low", "close"))
        self.require_numbers(data, ("volume",), allow_zero=True)
        self.require_numbers(data, ("vwap",), optional=True)
        self.require_numbers(data, ("transactions",), allow_zero=True, optional=True)
        valid = (data["low"] <= data[["open", "close"]].min(axis=1)) & (
            data["high"] >= data[["open", "close"]].max(axis=1)
        )
        if not valid.all():
            raise MassiveAPIError("日线 OHLC 价格关系异常，拒绝保存整个交易日")
        if (data["transactions"].dropna() % 1 != 0).any():
            raise MassiveAPIError("transactions 必须是整数")
        data["transactions"] = data["transactions"].astype("Int64")
        return data
