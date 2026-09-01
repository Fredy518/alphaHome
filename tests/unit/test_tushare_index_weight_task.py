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
