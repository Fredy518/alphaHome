from datetime import date
from unittest.mock import Mock

import pytest
import requests

from alphahome.common.constants import UpdateTypes
from alphahome.fetchers.sources.akshare import index_cci_cx_ext as source
from alphahome.fetchers.sources.akshare.akshare_api import AkShareAPI
from alphahome.fetchers.tasks.macro.akshare_macro_cci import AkShareMacroCciTask


def _mock_response(monkeypatch, payload):
    response = Mock()
    response.json.return_value = payload
    post = Mock(return_value=response)
    monkeypatch.setattr(source.requests, "post", post)
    return post, response


def _payload(dates, values):
    return {"code": 0, "data": {"month": dates, "data": values}}


def test_cci_new_source_sorts_and_deduplicates_before_percentage_change(monkeypatch):
    post, response = _mock_response(
        monkeypatch,
        _payload(
            [
                "2009-01-13 00:00:00",
                "2009-01-09 00:00:00",
                "2009-01-12 00:00:00",
                "2009-01-12 00:00:00",
            ],
            [99, 100, 109, "110"],
        ),
    )
    data = source.index_cci_cx()

    post.assert_called_once_with(
        "https://yun.ccxe.com.cn/dataindices/cci", data={"month": ""}, timeout=(10, 30)
    )
    response.raise_for_status.assert_called_once()
    assert list(data.columns) == ["日期", "大宗商品指数", "变化值"]
    assert data["日期"].tolist() == [
        date(2009, 1, 9),
        date(2009, 1, 12),
        date(2009, 1, 13),
    ]
    assert data["大宗商品指数"].tolist() == [100, 110, 99]
    assert data["变化值"].tolist() == pytest.approx([0, 10, -10])


@pytest.mark.asyncio
@pytest.mark.parametrize("update_type", [UpdateTypes.SMART, UpdateTypes.MANUAL])
async def test_cci_fetch_uses_new_source_and_retains_window_first_day_change(
    monkeypatch, update_type
):
    _mock_response(
        monkeypatch,
        _payload(["2009-01-09", "2009-01-12", "2009-01-13"], [100, 110, 99]),
    )
    task = AkShareMacroCciTask(
        db_connection=Mock(),
        update_type=update_type,
        api=AkShareAPI(request_interval=0, max_retries=1),
    )
    task._effective_start_date = date(2009, 1, 12)
    task._effective_end_date = date(2009, 1, 12)
    batches = await task.get_batch_list(start_date="20090112", end_date="20090112")
    params = await task.prepare_params(batches[0])
    data = task.process_data(await task.fetch_batch(params))

    assert params == {}
    assert data["date"].tolist() == [date(2009, 1, 12)]
    assert data["cci"].tolist() == [110]
    assert data["change"].tolist() == pytest.approx([10])


@pytest.mark.parametrize(
    "payload",
    [
        [],
        {"code": 500},
        {"code": 0, "data": None},
        _payload([], []),
        _payload(["2009-01-09"], []),
        _payload("2009-01-09", [100]),
        _payload(["2009-01-09"], "100"),
        _payload(["bad-date"], [100]),
        _payload([None], [100]),
        _payload(["2009-01-09"], [None]),
        _payload(["2009-01-09"], ["bad-value"]),
        _payload(["2009-01-09"], [float("inf")]),
        _payload(["2009-01-09"], [0]),
        _payload(["2009-01-09"], [-100]),
        _payload(["2026-09-03"], [371.053173]),
        _payload(["2009-01-09"], [101]),
    ],
)
def test_cci_rejects_failed_malformed_or_truncated_history(monkeypatch, payload):
    _mock_response(monkeypatch, payload)
    with pytest.raises(ValueError):
        source.index_cci_cx()


def test_cci_html_response_has_actionable_error(monkeypatch):
    _, response = _mock_response(monkeypatch, None)
    response.json.side_effect = ValueError("Expecting value")
    with pytest.raises(ValueError, match="未返回 JSON"):
        source.index_cci_cx()


def test_cci_http_failure_is_not_treated_as_no_data(monkeypatch):
    _, response = _mock_response(monkeypatch, None)
    response.raise_for_status.side_effect = requests.HTTPError("503 unavailable")
    with pytest.raises(requests.HTTPError):
        source.index_cci_cx()
    response.json.assert_not_called()
