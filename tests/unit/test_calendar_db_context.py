from datetime import date

import pytest

from alphahome.fetchers.tools import calendar


class _FakeDbManager:
    def __init__(self):
        self.calls = []

    async def fetch(self, query, *args):
        self.calls.append((query, args))
        return [
            {
                "exchange": "SSE",
                "cal_date": date(2024, 5, 6),
                "is_open": 1,
                "pretrade_date": date(2024, 4, 30),
            },
            {
                "exchange": "SSE",
                "cal_date": date(2024, 5, 7),
                "is_open": 0,
                "pretrade_date": date(2024, 5, 6),
            },
        ]


@pytest.mark.asyncio
async def test_trade_calendar_uses_injected_db_manager(monkeypatch):
    calendar._TRADE_CAL_CACHE.clear()
    db_manager = _FakeDbManager()

    async def fail_if_legacy_pool_is_used():
        raise AssertionError("legacy pool should not be used")

    monkeypatch.setattr(calendar, "_get_db_pool", fail_if_legacy_pool_is_used)

    trade_days = await calendar.get_trade_days_between(
        "20240506",
        "20240507",
        db_manager=db_manager,
    )

    assert trade_days == ["20240506"]
    assert len(db_manager.calls) == 1
    assert db_manager.calls[0][1] == ("SSE", "20240506", "20240507")

