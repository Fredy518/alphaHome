import asyncio
import logging
from unittest.mock import AsyncMock, Mock

import pytest
import requests

from alphahome.fetchers.sources.massive.massive_api import MassiveAPI, MassiveAPIError


@pytest.fixture
def api():
    client = MassiveAPI(api_key="unit-test-secret")
    client._wait_for_rate_limit = AsyncMock()
    client._sleep = AsyncMock()
    return client


def response(payload=None, status=200, headers=None):
    result = Mock(status_code=status, headers=headers or {})
    result.json.return_value = (
        payload if payload is not None else {"status": "OK", "results": []}
    )
    return result


@pytest.mark.asyncio
async def test_pagination_retains_header_auth_and_strips_url_key(api, monkeypatch):
    get = Mock(
        side_effect=[
            response(
                {
                    "status": "OK",
                    "count": 1,
                    "results": [{"ticker": "A"}],
                    "next_url": "https://api.massive.com/v3/reference/tickers?cursor=next&apiKey=old-secret",
                }
            ),
            response({"status": "OK", "count": 1, "results": [{"ticker": "B"}]}),
        ]
    )
    monkeypatch.setattr(requests, "get", get)
    rows = await api.list_records("/v3/reference/tickers", {"limit": 1})
    assert [row["ticker"] for row in rows] == ["A", "B"]
    assert get.call_args_list[1].args == (
        "https://api.massive.com/v3/reference/tickers?cursor=next",
    )
    for call in get.call_args_list:
        assert call.kwargs["headers"]["Authorization"] == "Bearer unit-test-secret"
        assert call.kwargs["allow_redirects"] is False
        assert "secret" not in call.args[0]


@pytest.mark.asyncio
async def test_pagination_failure_does_not_return_partial_snapshot(api):
    api._get = Mock(
        side_effect=[
            response(
                {
                    "status": "OK",
                    "results": [{"ticker": "A"}],
                    "next_url": "https://api.massive.com/v3/reference/tickers?cursor=next",
                }
            ),
            response(status=403),
        ]
    )
    with pytest.raises(MassiveAPIError, match="403"):
        await api.list_records("/v3/reference/tickers")
    assert api._get.call_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "next_url",
    [
        "https://example.com/steal",
        "http://api.massive.com/data",
        "https://api.massive.com@evil.example/data",
        "https://api.massive.com/v3/reference/tickers",
    ],
)
async def test_pagination_rejects_foreign_or_cyclic_urls(api, next_url):
    api._get = Mock(
        return_value=response({"status": "OK", "results": [], "next_url": next_url})
    )
    with pytest.raises(MassiveAPIError):
        await api.list_records("/v3/reference/tickers")
    assert api._get.call_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [401, 403, 302, 400])
async def test_non_retryable_status_is_not_retried_or_logged_with_secret(
    api, status, caplog
):
    api._get = Mock(return_value=response({"message": "unit-test-secret"}, status))
    with caplog.at_level(logging.DEBUG), pytest.raises(MassiveAPIError) as caught:
        await api.request("/v3/reference/tickers")
    assert api._get.call_count == 1
    assert "unit-test-secret" not in str(caught.value) + caplog.text


@pytest.mark.asyncio
async def test_429_retries_with_server_backoff(api):
    api._get = Mock(
        side_effect=[response(status=429, headers={"Retry-After": "75"}), response()]
    )
    assert (await api.request("/v3/reference/tickers"))["results"] == []
    api._sleep.assert_awaited_once_with(75.0, None)
    assert api._wait_for_rate_limit.await_count == 2


@pytest.mark.asyncio
async def test_transport_exception_is_redacted(api):
    api._get = Mock(side_effect=requests.ConnectionError("url?apiKey=unit-test-secret"))
    with pytest.raises(MassiveAPIError) as caught:
        await api.request("/v3/reference/tickers")
    assert "unit-test-secret" not in str(caught.value)
    assert api._get.call_count == 3


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {},
        [],
        {"status": "NOT_AUTHORIZED"},
        {"status": "OK", "results": {}},
        {"status": "OK", "results": [None]},
        {"status": "OK", "results": [], "count": 2},
    ],
)
async def test_malformed_success_is_rejected(api, payload):
    api._get = Mock(return_value=response(payload))
    with pytest.raises(MassiveAPIError):
        await api.request("/v3/reference/tickers")


@pytest.mark.asyncio
async def test_cancelled_request_never_touches_network():
    api = MassiveAPI(api_key="cancel-test")
    api._get = Mock()
    event = asyncio.Event()
    event.set()
    with pytest.raises(asyncio.CancelledError):
        await api.request("/v3/reference/tickers", stop_event=event)
    api._get.assert_not_called()


@pytest.mark.asyncio
async def test_instances_share_rate_limit_across_same_key(monkeypatch):
    from alphahome.fetchers.sources.massive import massive_api as module

    clock = [100.0]
    monkeypatch.setattr(module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(MassiveAPI, "_next_request", {})
    first, second = MassiveAPI(api_key="shared"), MassiveAPI(api_key="shared")
    sleeps = []

    async def sleep(seconds, event):
        sleeps.append(seconds)
        clock[0] += seconds

    second._sleep = sleep
    await first._wait_for_rate_limit("shared", None)
    await second._wait_for_rate_limit("shared", None)
    assert sleeps == [13.0]
