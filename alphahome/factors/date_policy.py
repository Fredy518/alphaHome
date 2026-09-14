"""Calendar-date contract for production P/G factor snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Iterable, List
from zoneinfo import ZoneInfo


FACTOR_TIMEZONE = ZoneInfo("Asia/Shanghai")


def coerce_date(value: date | datetime | str) -> date:
    """Normalize supported date inputs without silently accepting bad formats."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value), "%Y-%m-%d").date()


@dataclass(frozen=True)
class FactorDatePolicy:
    """Production factor snapshots are calendar Fridays, including holidays."""

    timezone: ZoneInfo = FACTOR_TIMEZONE

    @staticmethod
    def is_valid(value: date | datetime | str) -> bool:
        return coerce_date(value).weekday() == 4

    @classmethod
    def require_valid(cls, value: date | datetime | str) -> date:
        resolved = coerce_date(value)
        if resolved.weekday() != 4:
            raise ValueError(f"生产因子calc_date必须是自然周五: {resolved.isoformat()}")
        return resolved

    @staticmethod
    def fridays(
        start_date: date | datetime | str,
        end_date: date | datetime | str,
    ) -> List[date]:
        start = coerce_date(start_date)
        end = coerce_date(end_date)
        if start > end:
            raise ValueError("start_date must be <= end_date")
        current = start + timedelta(days=(4 - start.weekday()) % 7)
        result: List[date] = []
        while current <= end:
            result.append(current)
            current += timedelta(days=7)
        return result

    def automatic_cutoff(self, batch_started_at: datetime | date | None = None) -> date:
        """Return the latest Friday strictly before the local batch date."""
        if batch_started_at is None:
            local_date = datetime.now(self.timezone).date()
        elif isinstance(batch_started_at, datetime):
            if batch_started_at.tzinfo is None:
                local_date = batch_started_at.date()
            else:
                local_date = batch_started_at.astimezone(self.timezone).date()
        else:
            local_date = batch_started_at

        days_back = (local_date.weekday() - 4) % 7
        if days_back == 0:
            days_back = 7
        return local_date - timedelta(days=days_back)

    @classmethod
    def normalize_many(cls, values: Iterable[date | datetime | str]) -> List[date]:
        normalized = sorted({cls.require_valid(value) for value in values})
        return normalized


__all__ = ["FACTOR_TIMEZONE", "FactorDatePolicy", "coerce_date"]
