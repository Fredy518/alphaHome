"""Shared US session planning and strict data validation for Massive tasks."""

from abc import ABC
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

from alphahome.common.constants import UpdateTypes
from alphahome.fetchers.base.fetcher_task import FetcherTask

from .massive_api import MassiveAPI, MassiveAPIError

NEW_YORK = ZoneInfo("America/New_York")


class MassiveTask(FetcherTask, ABC):
    data_source = "massive"
    domain = "stock"
    default_concurrent_limit = 1
    # Retry only inside the API client (429/network/5xx, never auth failures).
    default_max_retries = 1
    default_stream_batches = True
    default_stream_update_types = (
        UpdateTypes.FULL,
        UpdateTypes.SMART,
        UpdateTypes.MANUAL,
    )
    default_stream_save_batch_size = 1  # Commit each complete trading day atomically.
    smart_initial_lookback_days = 7
    smart_lookback_days = 3
    snapshot_only = False
    replace_date_snapshots = False

    def __init__(self, db_connection, api=None, **kwargs):
        super().__init__(db_connection, **kwargs)
        # Do not advance past a failed day within a run. Historical holes below
        # an existing MAX(date) require explicit date coverage, not SMART resume.
        self.concurrent_limit = 1
        self.max_retries = 1
        self.continue_on_stream_batch_failure = False
        self.allow_partial_batch_save = False
        self.api = api or MassiveAPI(
            request_interval=self.task_specific_config.get("request_interval", 13.0),
            max_attempts=self.task_specific_config.get("api_max_attempts", 3),
            logger=self.logger,
        )
        self.history_years = int(self.task_specific_config.get("history_years", 2))
        if not 1 <= self.history_years <= 30:
            raise ValueError("history_years 必须在 1 到 30 之间，且不能超过账户权限")
        self.default_start_date = self.history_floor().strftime("%Y%m%d")

    @staticmethod
    def now():
        return datetime.now(timezone.utc)

    def history_floor(self):
        # Keep one day clear of the rolling, potentially time-of-day entitlement.
        return (
            self.now().date()
            - relativedelta(years=self.history_years)
            + timedelta(days=1)
        )

    def supports_incremental_update(self) -> bool:
        """Expose the bounded SMART path to the production collector."""
        return True

    @staticmethod
    def sessions(start, end):
        if start > end:
            return []
        try:
            import exchange_calendars as xcals
        except ImportError:
            raise RuntimeError(
                "美股任务需要安装 alphahome[massive]（exchange-calendars）"
            ) from None
        calendar = xcals.get_calendar(
            "XNYS", start=start - timedelta(days=7), end=end + timedelta(days=7)
        )
        return [value.date() for value in calendar.sessions_in_range(start, end)]

    def latest_complete_session(self):
        # Basic is EOD. Never request the current New York date, even after 16:00.
        cutoff = self.now().astimezone(NEW_YORK).date() - timedelta(days=1)
        days = self.sessions(cutoff - timedelta(days=15), cutoff)
        if not days:
            raise RuntimeError("美股交易日历未找到最近完整交易日")
        return days[-1]

    async def _pre_execute(self, stop_event=None, **kwargs):
        MassiveAPI.check_cancelled(stop_event)
        if kwargs.get("continue_on_stream_batch_failure"):
            raise ValueError("Massive 任务不能跳过失败批次继续推进日期")
        if kwargs.get("use_insert_mode") or self.use_insert_mode:
            raise ValueError("Massive 任务必须使用可重入的 UPSERT 模式")

    def _continue_on_stream_batch_failure(self, kwargs=None):
        return False

    async def _determine_date_range(self):
        self._smart_skip_reason = None
        floor, end = self.history_floor(), self.latest_complete_session()
        if self.update_type == UpdateTypes.MANUAL:
            if not self.start_date or not self.end_date:
                raise ValueError("手动更新必须提供 start_date 和 end_date")
            start = pd.Timestamp(self.start_date).date()
            requested_end = pd.Timestamp(self.end_date).date()
            if start > requested_end:
                raise ValueError("start_date 不能晚于 end_date")
            if start < floor:
                raise ValueError(f"起始日超出配置的历史权限窗口，最早支持 {floor}")
            end = min(end, requested_end)
        elif self.update_type == UpdateTypes.FULL:
            # FULL metadata means a complete latest universe. Historical daily
            # snapshots require an explicit MANUAL window to avoid huge requests.
            start = end if self.snapshot_only else floor
        elif self.update_type == UpdateTypes.SMART:
            latest = await self.get_latest_date_for_task()
            if latest:
                latest = min(pd.Timestamp(latest).date(), end)
                start = max(
                    floor, latest - timedelta(days=max(0, self.smart_lookback_days - 1))
                )
            else:
                start = (
                    end
                    if self.snapshot_only
                    else max(
                        floor,
                        end
                        - timedelta(days=(self.smart_initial_lookback_days or 7) - 1),
                    )
                )
        else:
            raise ValueError(f"不支持的更新类型: {self.update_type}")
        if start > end:
            self._smart_skip_reason = "请求窗口内没有已完成的美股交易日"
            return None
        return {
            "start_date": start.strftime("%Y%m%d"),
            "end_date": end.strftime("%Y%m%d"),
        }

    async def _fetch_data(self, stop_event=None, **kwargs):
        # Keep the same NY cutoff/entitlement logic if streaming is disabled.
        batches = await self._get_effective_batch_list(**kwargs)
        results = await self._execute_batches(batches, stop_event)
        return pd.concat(results, ignore_index=True) if results else None

    def checked_window(self, kwargs):
        if any(kwargs.get(k) for k in ("ts_code", "ts_codes", "ticker", "symbols")):
            raise ValueError("美股任务按全市场采集，不支持单证券过滤")
        start = pd.Timestamp(kwargs.get("start_date") or self.history_floor()).date()
        requested_end = pd.Timestamp(
            kwargs.get("end_date") or self.latest_complete_session()
        ).date()
        if start > requested_end:
            raise ValueError("start_date 不能晚于 end_date")
        if start < self.history_floor():
            raise ValueError("起始日超出配置的历史权限窗口")
        return start, min(requested_end, self.latest_complete_session())

    async def get_batch_list(self, **kwargs):
        start, end = self.checked_window(kwargs)
        days = self.sessions(start, end)
        if not days:
            self._smart_skip_reason = "请求窗口内没有已完成的美股交易日"
        return [{"date": day.isoformat()} for day in days]

    async def prepare_params(self, batch):
        return dict(batch)

    async def _save_to_database(self, data, stop_event=None, **kwargs):
        if not self.replace_date_snapshots:
            return await super()._save_to_database(
                data, stop_event=stop_event, **kwargs
            )
        # Re-fetching a complete day must also remove rows withdrawn by the
        # vendor. DELETE + COPY share one transaction, preserving other dates.
        dates = sorted(set(data[self.date_column]))
        columns = list(data.columns)

        def db_value(value):
            if pd.isna(value):
                return None
            if isinstance(value, pd.Timestamp):
                return value.to_pydatetime()
            return value.item() if isinstance(value, np.generic) else value

        records = [
            tuple(db_value(v) for v in row)
            for row in data.itertuples(index=False, name=None)
        ]
        async with self.db.pool.acquire() as connection:
            async with connection.transaction():
                # Serialize replacement of this task's snapshots across workers.
                await connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtext($1))",
                    self.get_full_table_name(),
                )
                MassiveAPI.check_cancelled(stop_event)
                await connection.execute(
                    f'DELETE FROM {self.get_full_table_name()} WHERE "{self.date_column}" = ANY($1::date[])',
                    dates,
                )
                await connection.copy_records_to_table(
                    self.table_name,
                    schema_name=self.data_source,
                    columns=columns,
                    records=records,
                )
                MassiveAPI.check_cancelled(stop_event)
        return len(data)

    def process_data(self, data, **kwargs):
        data = super().process_data(data, **kwargs)
        if data is None or data.empty:
            return data
        data = data.copy()
        for key in self.primary_keys:
            if key not in data or data[key].isna().any():
                raise MassiveAPIError(f"{self.name}: 主键 {key} 缺失")
            if (
                data[key]
                .map(
                    lambda value: isinstance(value, str)
                    and (
                        not value.strip()
                        or value != value.strip()
                        or any(c in value for c in "\r\n\t")
                    )
                )
                .any()
            ):
                raise MassiveAPIError(f"{self.name}: 主键 {key} 包含空白或控制字符")
        if data.duplicated(self.primary_keys).any():
            raise MassiveAPIError(f"{self.name}: 响应包含重复业务键，拒绝保存")
        return data

    @staticmethod
    def require_numbers(data, columns, *, allow_zero=False, optional=False):
        for column in columns:
            values = pd.to_numeric(data[column], errors="coerce")
            valid = np.isfinite(values) & (values >= 0 if allow_zero else values > 0)
            if optional:
                valid |= data[column].isna()
            if not valid.all():
                raise MassiveAPIError(f"Massive {column} 存在缺失、非数值或越界数据")
            data[column] = values
