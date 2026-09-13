from datetime import datetime, timedelta

import pytest

from alphahome.fetchers.tasks.index.tushare_index_weight import (
    TushareIndexWeightTask,
)


@pytest.mark.asyncio
async def test_index_weight_single_day_batch_does_not_expand_to_year_start(
    monkeypatch,
):
    task = TushareIndexWeightTask(
        db_connection=object(),
        api_token="test-token",
        api=object(),
    )

    async def _index_codes():
        return ["000300.SH", "000905.SH"]

    monkeypatch.setattr(task, "get_index_codes", _index_codes)

    batches = await task.get_batch_list(
        start_date="20260901",
        end_date="20260901",
    )

    assert batches == [
        {
            "index_code": "000300.SH",
            "start_date": "20260901",
            "end_date": "20260901",
        },
        {
            "index_code": "000905.SH",
            "start_date": "20260901",
            "end_date": "20260901",
        },
    ]


@pytest.mark.asyncio
async def test_index_weight_manual_batch_honours_explicit_index_codes(monkeypatch):
    task = TushareIndexWeightTask(
        db_connection=object(),
        api_token="test-token",
        api=object(),
    )

    async def _unexpected_index_discovery():
        raise AssertionError("explicit index codes must not trigger ETF discovery")

    monkeypatch.setattr(task, "get_index_codes", _unexpected_index_discovery)

    batches = await task.get_batch_list(
        start_date="20211201",
        end_date="20211231",
        index_codes=["H30531.CSI", "000846.CSI", "H30531.CSI"],
    )

    assert batches == [
        {
            "index_code": "H30531.CSI",
            "start_date": "20211201",
            "end_date": "20211231",
        },
        {
            "index_code": "000846.CSI",
            "start_date": "20211201",
            "end_date": "20211231",
        },
    ]


def test_index_weight_explicit_codes_accept_comma_separated_values():
    assert TushareIndexWeightTask._normalize_requested_index_codes(
        " h30531.csi, 000846.csi ;h30531.csi"
    ) == ["H30531.CSI", "000846.CSI"]


@pytest.mark.asyncio
async def test_index_weight_batch_uses_configured_codes(monkeypatch):
    task = TushareIndexWeightTask(
        db_connection=object(),
        api_token="test-token",
        api=object(),
        task_config={"index_codes": ["H30531.CSI"]},
    )

    async def _unexpected_index_discovery():
        raise AssertionError("configured index codes must not trigger ETF discovery")

    monkeypatch.setattr(task, "get_index_codes", _unexpected_index_discovery)

    batches = await task.get_batch_list(
        start_date="20220101",
        end_date="20220131",
    )

    assert [batch["index_code"] for batch in batches] == ["H30531.CSI"]


@pytest.mark.asyncio
async def test_index_weight_batch_appends_supplemental_codes(monkeypatch):
    task = TushareIndexWeightTask(
        db_connection=object(),
        api_token="test-token",
        api=object(),
        task_config={"extra_index_codes": ["H30531.CSI", "000300.SH"]},
    )

    async def _index_discovery():
        return ["000300.SH", "000905.SH"]

    monkeypatch.setattr(task, "get_index_codes", _index_discovery)

    batches = await task.get_batch_list(
        start_date="20220101",
        end_date="20220131",
    )

    assert [batch["index_code"] for batch in batches] == [
        "000300.SH",
        "000905.SH",
        "H30531.CSI",
    ]


def test_index_weight_long_range_chunks_without_gaps_or_boundary_expansion():
    batches = TushareIndexWeightTask._split_exact_date_range(
        "20250115",
        "20260220",
    )

    assert batches[0]["start_date"] == "20250115"
    assert batches[-1]["end_date"] == "20260220"

    for current, following in zip(batches, batches[1:]):
        current_end = datetime.strptime(current["end_date"], "%Y%m%d")
        following_start = datetime.strptime(following["start_date"], "%Y%m%d")
        assert following_start == current_end + timedelta(days=1)

    for batch in batches:
        start = datetime.strptime(batch["start_date"], "%Y%m%d")
        end = datetime.strptime(batch["end_date"], "%Y%m%d")
        assert (end - start).days + 1 <= TushareIndexWeightTask.max_batch_days
