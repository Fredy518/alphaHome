from datetime import date

from alphahome.factors.coordinator import FactorCoordinator
from alphahome.factors.repository import FactorRepository


class _ReadOnlyDB:
    def fetch_sync(self, *_args, **_kwargs):
        raise AssertionError("readiness should not fetch result rows")

    def fetch_val_sync(self, *_args, **_kwargs):
        return 1


def test_p_readiness_checks_every_planned_date_not_only_range_end(monkeypatch):
    repository = FactorRepository(_ReadOnlyDB())
    monkeypatch.setattr(repository, "relation_exists", lambda _relation: True)
    checked = []

    def gaps(calc_date):
        checked.append(calc_date)
        return {
            "status": "checked",
            "eligible_missing": 1 if calc_date == date(2024, 10, 11) else 0,
        }

    monkeypatch.setattr(repository, "financial_input_gaps", gaps)
    dates = [date(2024, 10, 11), date(2024, 10, 18)]
    blockers = repository.readiness(
        FactorCoordinator.contracts()["factor_p"], dates[-1], dates, {}
    )

    assert checked == dates
    assert blockers == ["pit_input_eligibility:missing=1:2024-10-11"]
