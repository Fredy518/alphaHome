from dataclasses import FrozenInstanceError, replace
from datetime import date, datetime, timezone

import pytest

from alphahome.common.run_models import RunPlan, RunRequest, RunResult, RunUnit, SourceBoundary, fingerprint, target_fingerprint


DAY = date(2026, 9, 11)


def make_plan(**overrides):
    request = RunRequest("factors", ("factor_g",), "smart", fingerprint("test_target"))
    units = (RunUnit("factor_p", (DAY,)), RunUnit("factor_g", (DAY,), ("factor_p",)))
    return RunPlan.build(request, units, DAY, schema=overrides.get("schema", {"v": 1}),
                         sources=overrides.get("sources", {"xmin": 1}), config=overrides.get("config", {"formula": "v2.0"}))


def test_canonical_plans_are_immutable_and_independent_of_mapping_order():
    plan = make_plan(schema={"b": 1, "a": 2})
    assert plan.plan_hash == make_plan(schema={"a": 2, "b": 1}).plan_hash
    with pytest.raises(FrozenInstanceError):
        plan.effective_cutoff = DAY
    assert isinstance(plan.units, tuple)
    assert isinstance(plan.units[0].dates, tuple)
    exposed = plan.to_dict()
    exposed["units"][0]["dates"].clear()
    assert plan.units[0].dates == (DAY,)


@pytest.mark.parametrize("changed", ["schema", "sources", "config"])
def test_changed_execution_inputs_require_a_new_preview(changed):
    before = make_plan()
    after = make_plan(**{changed: {"new": 2}})
    with pytest.raises(RuntimeError, match="plan changed"):
        after.require_matching(before.plan_hash)


def test_dependency_order_cutoff_and_selected_tasks_are_enforced():
    plan = make_plan()
    with pytest.raises(ValueError, match="dependencies"):
        replace(plan, units=tuple(reversed(plan.units)))
    with pytest.raises(ValueError, match="cutoff"):
        replace(plan, effective_cutoff=date(2026, 9, 4))
    with pytest.raises(ValueError, match="omits"):
        replace(plan, units=plan.units[:1])


def test_database_fingerprint_excludes_password_but_detects_target_and_role_changes():
    first = target_fingerprint("postgresql://reader:first@127.0.0.1:15432/test")
    assert first == target_fingerprint("host=127.0.0.1 port=15432 dbname=test user=reader password=second")
    assert first != target_fingerprint("postgresql://writer:first@127.0.0.1:15432/test")
    assert first != target_fingerprint("postgresql://reader:first@127.0.0.1:15432/other")
    with pytest.raises(ValueError, match="explicit"):
        target_fingerprint("dbname=test")


def test_consumed_boundary_requires_actual_snapshot_and_scope():
    boundary = SourceBoundary(fingerprint("target"), datetime.now(timezone.utc), 12, fingerprint({}), "repeatable_read")
    with pytest.raises(ValueError, match="completed scope"):
        replace(boundary, consumed=True)
    assert replace(boundary, consumed=True, scope=("2026-09-11",)).consumed
    with pytest.raises(ValueError, match="timezone"):
        replace(boundary, observed_at=datetime(2026, 9, 11))


@pytest.mark.parametrize("kwargs", [{"status": "success", "error_count": 1},
                                   {"status": "unknown"},
                                   {"status": "partial_success", "attempted_rows": 3, "committed_rows": 4}])
def test_invalid_terminal_results_cannot_masquerade_as_success(kwargs):
    with pytest.raises(ValueError):
        RunResult(**kwargs)


def test_unknown_commit_count_remains_unknown():
    result = RunResult("error", attempted_rows=5, error_count=1)
    assert result.to_dict()["committed_rows"] is None
