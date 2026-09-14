from datetime import date, datetime, timezone

import pandas as pd
import pytest

from alphahome.common.task_system import UnifiedTaskFactory
from alphahome.factors.base import FactorTaskContract
from alphahome.factors.date_policy import FactorDatePolicy
from alphahome.factors.tasks import discover_tasks
from alphahome.factors.validation import FactorValidationError, validate_factor_frame


def _p_frame(calc_date="2026-09-11"):
    return pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "calc_date": [calc_date],
            "ann_date": ["2026-08-31"],
            "calculation_status": ["success"],
            "p_score": [50.0],
            "p_rank": [1],
        }
    )


def test_factor_date_policy_uses_previous_complete_friday():
    policy = FactorDatePolicy()
    assert policy.automatic_cutoff(date(2026, 9, 14)) == date(2026, 9, 11)
    assert policy.automatic_cutoff(date(2026, 9, 12)) == date(2026, 9, 11)
    assert policy.automatic_cutoff(date(2026, 9, 11)) == date(2026, 9, 4)
    assert policy.automatic_cutoff(
        datetime(2026, 9, 11, 16, tzinfo=timezone.utc)
    ) == date(2026, 9, 11)


def test_factor_date_policy_accepts_holiday_friday_and_rejects_other_days():
    assert FactorDatePolicy.require_valid("2025-10-03") == date(2025, 10, 3)
    with pytest.raises(ValueError, match="自然周五"):
        FactorDatePolicy.require_valid("2025-10-02")


def test_factor_validation_enforces_pit_and_eligible_universe():
    result = validate_factor_frame(
        _p_frame(), "p", "2026-09-11", expected_codes=["000001.SZ"]
    )
    assert result.row_count == 1
    assert result.coverage_rate == 1.0

    bad = _p_frame()
    bad.loc[0, "ann_date"] = "2026-09-12"
    with pytest.raises(FactorValidationError, match="ann_date"):
        validate_factor_frame(bad, "p", "2026-09-11")

    with pytest.raises(FactorValidationError, match="资格集合"):
        validate_factor_frame(
            _p_frame(),
            "p",
            "2026-09-11",
            expected_codes=["000001.SZ", "600000.SH"],
        )


def test_factor_task_contracts_are_discoverable_and_dependency_aware():
    discover_tasks(force_reload=True)
    registry = UnifiedTaskFactory._task_registry
    assert {"factor_p", "factor_g"} <= set(registry)
    p_contract = registry["factor_p"].contract
    g_contract = registry["factor_g"].contract
    assert isinstance(p_contract, FactorTaskContract)
    assert p_contract.formula_version == "v2.0"
    assert p_contract.date_strategy == "calendar_friday_last_complete"
    assert p_contract.readiness_dependencies == (
        "pit_financial_indicators",
        "pit_industry_classification",
    )
    assert g_contract.formula_version == "v1.1"
    assert g_contract.dependencies == ("factor_p",)
    assert g_contract.history_lookback_days == 730


def test_factor_contract_round_trip_preserves_calculator_path():
    discover_tasks()
    contract = UnifiedTaskFactory._task_registry["factor_g"].contract
    restored = FactorTaskContract.from_dict(contract.to_dict())
    assert restored.task_name == "factor_g"
    assert restored.resolve_calculator_class().__name__ == "GFactorCalculator"
