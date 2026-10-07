"""Split events supporting point-in-time share-basis reconstruction."""

import pandas as pd

from alphahome.common.task_system.task_decorator import task_register
from alphahome.fetchers.sources.massive import MassiveAPIError, MassiveTask


@task_register()
class MassiveStockUsSplitTask(MassiveTask):
    name = "massive_stock_us_split"
    description = "美股拆股/并股事件（Massive，支持日线拆股复权）"
    table_name = "stock_us_split"
    primary_keys = ["event_id"]
    date_column = "execution_date"
    smart_lookback_days = 30
    smart_initial_lookback_days = 30
    schema_def = {
        "event_id": {"type": "VARCHAR(128)", "constraints": "NOT NULL"},
        "ticker": {"type": "VARCHAR(64)", "constraints": "NOT NULL"},
        "execution_date": {"type": "DATE", "constraints": "NOT NULL"},
        "split_from": {"type": "DOUBLE PRECISION", "constraints": "NOT NULL"},
        "split_to": {"type": "DOUBLE PRECISION", "constraints": "NOT NULL"},
        "adjustment_type": {"type": "VARCHAR(32)", "constraints": "NOT NULL"},
        "observed_at": {"type": "TIMESTAMPTZ", "constraints": "NOT NULL"},
    }
    indexes = [
        {
            "name": "idx_stock_us_split_ticker_date",
            "columns": ["ticker", "execution_date"],
        }
    ]

    async def get_batch_list(self, **kwargs):
        start, end = self.checked_window(kwargs)
        return (
            [{"start_date": start.isoformat(), "end_date": end.isoformat()}]
            if start <= end
            else []
        )

    async def fetch_batch(self, params, stop_event=None):
        rows = await self.api.list_records(
            "/stocks/v1/splits",
            {
                "execution_date.gte": params["start_date"],
                "execution_date.lte": params["end_date"],
                "limit": 1000,
                "sort": "execution_date.asc",
            },
            stop_event,
        )
        if not rows:
            self._smart_skip_reason = "Massive 成功返回该窗口无拆股事件"
            return None
        data = pd.DataFrame(rows).rename(columns={"id": "event_id"})
        required = set(self.schema_def) - {"observed_at"}
        if required - set(data.columns):
            raise MassiveAPIError("拆股事件缺少必需字段")
        dates = pd.to_datetime(data["execution_date"], errors="raise").dt.date
        start, end = (
            pd.Timestamp(params["start_date"]).date(),
            pd.Timestamp(params["end_date"]).date(),
        )
        if not dates.between(start, end).all():
            raise MassiveAPIError("拆股事件超出请求窗口")
        data["execution_date"] = dates
        data["observed_at"] = self.now()
        # Do not store vendor cumulative factors: future splits would stale them.
        return data[list(self.schema_def)]

    def process_data(self, data, **kwargs):
        data = super().process_data(data, **kwargs)
        if data is None or data.empty:
            return data
        if (
            data["ticker"].isna().any()
            or data["ticker"].astype(str).str.strip().eq("").any()
        ):
            raise MassiveAPIError("拆股事件 ticker 为空")
        if (
            not data["adjustment_type"]
            .isin({"forward_split", "reverse_split", "stock_dividend"})
            .all()
        ):
            raise MassiveAPIError("未知的拆股事件类型")
        self.require_numbers(data, ("split_from", "split_to"))
        return data
