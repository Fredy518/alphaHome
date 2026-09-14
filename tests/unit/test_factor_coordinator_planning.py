from datetime import date

from alphahome.factors.coordinator import FactorCoordinator
from alphahome.factors.date_policy import FactorDatePolicy


class _Governance:
    def schema_issues(self):
        return []


class _Repository:
    def snapshot_xmin(self):
        return 100

    def source_watermarks(self, contract):
        return {}

    def readiness(self, contract, effective_end, dates, dates_by_task):
        return []

    def row_counts_by_date(self, contract, dates):
        return {}

    def table_date_stats(self, contract):
        return {"latest_date_row_count": 1}


def test_plan_propagates_changed_p_dates_into_g_dates(monkeypatch):
    coordinator = FactorCoordinator.__new__(FactorCoordinator)
    coordinator.governance = _Governance()
    coordinator.repository = _Repository()
    coordinator.date_policy = FactorDatePolicy()
    coordinator.max_automatic_dates = 26
    coordinator.expand_dependencies = lambda names: ["factor_p", "factor_g"]

    def planned(contract, mode, *, requested_start, effective_end):
        return [date(2026, 9, 4)] if contract.task_name == "factor_p" else []

    monkeypatch.setattr(coordinator, "_plan_dates", planned)
    result = coordinator.plan(
        ["factor_p", "factor_g"],
        mode="smart",
        start_date="2026-09-01",
        end_date="2026-09-11",
    )

    assert result.status == "ready"
    plans = {item.task_name: item for item in result.task_plans}
    assert plans["factor_p"].dates == [date(2026, 9, 4)]
    assert plans["factor_g"].dates == [date(2026, 9, 4), date(2026, 9, 11)]
