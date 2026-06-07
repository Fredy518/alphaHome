import pandas as pd
import pytest
from datetime import date, datetime

from alphahome.common.constants import UpdateTypes
import alphahome.fetchers.base.fetcher_task as fetcher_task_module
from alphahome.fetchers.base.fetcher_task import FetcherTask


class _FailingBatchFetcherTask(FetcherTask):
    name = "failing_batch_fetcher"
    table_name = "failing_batch_fetcher"
    date_column = "trade_date"
    default_start_date = "20100101"

    async def get_batch_list(self, **kwargs):
        return []

    async def prepare_params(self, batch):
        return {"batch": batch}

    async def fetch_batch(self, params, stop_event=None):
        if params["batch"] == "bad":
            raise RuntimeError("batch boom")
        return pd.DataFrame({"batch": [params["batch"]]})


class _StreamingDB:
    def __init__(self, events=None):
        self.saved_chunks = []
        self.events = events

    async def get_latest_date(self, task, date_column):
        return None

    async def table_exists(self, target):
        return True

    async def ensure_table_schema_compatible(self, target):
        return None

    async def check_table_exists(self, schema, table):
        return False

    async def create_rawdata_view(self, *args, **kwargs):
        return None

    async def upsert(self, df, target, conflict_columns, update_columns, timestamp_column=None):
        self.saved_chunks.append(df.copy())
        if self.events is not None:
            self.events.append(("save", len(df)))
        return len(df)


class _StreamingBatchFetcherTask(_FailingBatchFetcherTask):
    name = "streaming_batch_fetcher"
    table_name = "streaming_batch_fetcher"
    primary_keys = ["batch"]
    schema_def = {
        "batch": {"type": "INTEGER", "constraints": "NOT NULL"},
        "value": {"type": "INTEGER"},
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.process_call_lengths = []

    async def get_batch_list(self, **kwargs):
        return [1, 2, 3]

    async def fetch_batch(self, params, stop_event=None):
        batch = params["batch"]
        return pd.DataFrame({"batch": [batch], "value": [batch * 10]})

    def process_data(self, data, **kwargs):
        self.process_call_lengths.append(len(data))
        return super().process_data(data, **kwargs)


class _DefaultStreamingBatchFetcherTask(_StreamingBatchFetcherTask):
    default_stream_batches = True


@pytest.mark.asyncio
async def test_execute_batches_raises_when_any_batch_exhausts_retries():
    task = _FailingBatchFetcherTask(
        db_connection=object(),
        task_config={"concurrent_limit": 2, "max_retries": 2, "retry_delay": 0},
    )

    with pytest.raises(RuntimeError, match="1/2 batches failed"):
        await task._execute_batches(["ok", "bad"])


@pytest.mark.asyncio
async def test_streaming_execute_processes_each_batch_and_buffers_saves():
    db = _StreamingDB()
    task = _StreamingBatchFetcherTask(
        db_connection=db,
        update_type=UpdateTypes.FULL,
        task_config={"stream_batches": True, "concurrent_limit": 1, "stream_save_batch_size": 2},
    )

    result = await task.execute()

    assert result["status"] == "success"
    assert result["stream_batches"] is True
    assert result["rows"] == 3
    assert result["saved_batches"] == 2
    assert task.process_call_lengths == [1, 1, 1]
    assert [len(chunk) for chunk in db.saved_chunks] == [2, 1]


@pytest.mark.asyncio
async def test_streaming_progress_advances_after_flush(monkeypatch):
    events = []

    class _ProgressBar:
        def __init__(self, *args, **kwargs):
            events.append(("open", kwargs.get("total")))

        def update(self, value):
            events.append(("progress", value))

        def set_postfix(self, *args, **kwargs):
            return None

        def close(self):
            events.append(("close", None))

    monkeypatch.setattr(fetcher_task_module, "tqdm", _ProgressBar)
    db = _StreamingDB(events=events)
    task = _StreamingBatchFetcherTask(
        db_connection=db,
        update_type=UpdateTypes.FULL,
        task_config={"stream_batches": True, "concurrent_limit": 1, "stream_save_batch_size": 1},
    )

    result = await task.execute()

    assert result["status"] == "success"
    save_positions = [idx for idx, event in enumerate(events) if event[0] == "save"]
    progress_positions = [idx for idx, event in enumerate(events) if event[0] == "progress"]
    assert len(save_positions) == 3
    assert len(progress_positions) == 3
    assert all(save_idx < progress_idx for save_idx, progress_idx in zip(save_positions, progress_positions))


@pytest.mark.asyncio
async def test_streaming_defaults_to_full_update_type_only():
    db = _StreamingDB()
    task = _DefaultStreamingBatchFetcherTask(
        db_connection=db,
        update_type=UpdateTypes.SMART,
        task_config={"concurrent_limit": 1},
    )

    result = await task.execute()

    assert result["status"] == "success"
    assert "stream_batches" not in result
    assert result["rows"] == 3
    assert task.process_call_lengths == [3]
    assert [len(chunk) for chunk in db.saved_chunks] == [3]


@pytest.mark.asyncio
async def test_runtime_stream_batches_override_respects_update_type_gate():
    db = _StreamingDB()
    task = _DefaultStreamingBatchFetcherTask(
        db_connection=db,
        update_type=UpdateTypes.SMART,
        task_config={"concurrent_limit": 1, "stream_save_batch_size": 2},
    )

    result = await task.execute(stream_batches=True)

    assert result["status"] == "success"
    assert "stream_batches" not in result
    assert task.process_call_lengths == [3]
    assert [len(chunk) for chunk in db.saved_chunks] == [3]


@pytest.mark.asyncio
async def test_runtime_stream_update_types_can_explicitly_stream_smart():
    db = _StreamingDB()
    task = _DefaultStreamingBatchFetcherTask(
        db_connection=db,
        update_type=UpdateTypes.SMART,
        task_config={"concurrent_limit": 1, "stream_save_batch_size": 2},
    )

    result = await task.execute(stream_batches=True, stream_update_types="smart")

    assert result["status"] == "success"
    assert result["stream_batches"] is True
    assert task.process_call_lengths == [1, 1, 1]
    assert [len(chunk) for chunk in db.saved_chunks] == [2, 1]


class _LatestDateDB:
    def __init__(self, latest_date):
        self.latest_date = latest_date

    async def get_latest_date(self, task, date_column):
        return self.latest_date


class _RecentUpdateDB:
    def __init__(self, latest_update_time, latest_date=None):
        self.latest_update_time = latest_update_time
        self.latest_date = latest_date
        self.latest_date_calls = 0

    async def table_exists(self, target):
        return True

    async def get_latest_update_time(self, target):
        return self.latest_update_time

    async def get_latest_date(self, task, date_column):
        self.latest_date_calls += 1
        return self.latest_date


class _FrozenDateTime:
    @classmethod
    def now(cls):
        return datetime(2026, 5, 20)

    @classmethod
    def strptime(cls, value, fmt):
        return datetime.strptime(value, fmt)


@pytest.mark.asyncio
async def test_smart_date_range_uses_today_anchor_when_latest_date_is_future(monkeypatch):
    monkeypatch.setattr(fetcher_task_module, "datetime", _FrozenDateTime)
    task = _FailingBatchFetcherTask(
        db_connection=_LatestDateDB(date(2026, 12, 31)),
        update_type=UpdateTypes.SMART,
        task_config={"smart_lookback_days": 30},
    )

    date_range = await task._determine_date_range()

    assert date_range == {"start_date": "20260421", "end_date": "20260520"}


@pytest.mark.asyncio
async def test_smart_refresh_interval_skips_recently_updated_table(monkeypatch):
    monkeypatch.setattr(fetcher_task_module, "datetime", _FrozenDateTime)
    db = _RecentUpdateDB(datetime(2026, 5, 19, 12, 0), latest_date=date(2026, 5, 10))
    task = _FailingBatchFetcherTask(
        db_connection=db,
        update_type=UpdateTypes.SMART,
        task_config={"smart_refresh_interval_days": 7},
    )

    date_range = await task._determine_date_range()

    assert date_range is None
    assert db.latest_date_calls == 0


@pytest.mark.asyncio
async def test_smart_refresh_interval_allows_expired_table(monkeypatch):
    monkeypatch.setattr(fetcher_task_module, "datetime", _FrozenDateTime)
    db = _RecentUpdateDB(datetime(2026, 5, 1, 12, 0), latest_date=date(2026, 5, 10))
    task = _FailingBatchFetcherTask(
        db_connection=db,
        update_type=UpdateTypes.SMART,
        task_config={"smart_lookback_days": 3, "smart_refresh_interval_days": 7},
    )

    date_range = await task._determine_date_range()

    assert date_range == {"start_date": "20260508", "end_date": "20260520"}
    assert db.latest_date_calls == 1
