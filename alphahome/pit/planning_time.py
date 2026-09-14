"""Worker-local business time; wall-clock logging remains real time."""

from contextlib import contextmanager
from contextvars import ContextVar
from datetime import date, datetime
from zoneinfo import ZoneInfo

_business_date = ContextVar("pit_business_date", default=None)
_date_range = ContextVar("pit_planned_date_range", default=None)


def business_date():
    return _business_date.get() or datetime.now(ZoneInfo("Asia/Shanghai")).date()


def planned_date_range():
    return _date_range.get()


@contextmanager
def frozen_pit_time(cutoff=None, date_range=None):
    if isinstance(cutoff, str):
        cutoff = date.fromisoformat(cutoff)
    time_token = _business_date.set(cutoff)
    range_token = _date_range.set(tuple(date_range) if date_range else None)
    try:
        yield
    finally:
        _date_range.reset(range_token)
        _business_date.reset(time_token)
