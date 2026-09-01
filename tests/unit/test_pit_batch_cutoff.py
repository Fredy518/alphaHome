from datetime import date

from alphahome.pit.base.monthly_snapshot_manager import PITMonthlySnapshotManager
from alphahome.pit.pit_industry_classification_manager import (
    PITIndustryClassificationManager,
)


def test_complete_month_cutoff_never_advances_past_frozen_batch_month(monkeypatch):
    monkeypatch.setattr(
        PITMonthlySnapshotManager,
        "latest_complete_month",
        staticmethod(lambda today=None: date(2026, 8, 31)),
    )

    assert PITMonthlySnapshotManager.complete_month_cutoff("2026-07-31") == date(
        2026, 7, 31
    )


def test_industry_classification_affected_months_respect_frozen_cutoff(monkeypatch):
    manager = PITIndustryClassificationManager()
    monkeypatch.setattr(
        PITMonthlySnapshotManager,
        "latest_complete_month",
        staticmethod(lambda today=None: date(2026, 8, 31)),
    )

    months = manager._get_affected_months("2026-04-03", cutoff_date="2026-07-31")

    assert months == [
        date(2026, 4, 1),
        date(2026, 5, 1),
        date(2026, 6, 1),
        date(2026, 7, 1),
    ]
