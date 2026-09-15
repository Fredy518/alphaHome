from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from alphahome.gui.services import fundpos_service as service


def _write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _config(state_dir):
    return SimpleNamespace(
        state_dir=state_dir,
        scope="v3-pilot",
        families=("fixed_income_plus", "enhanced_index"),
        expected_scope_versions={
            "fixed_income_plus": "fixed-v3",
            "enhanced_index": "index-v3",
        },
        expected_universe_counts={
            "fixed_income_plus": 90,
            "enhanced_index": 20,
        },
    )


def test_snapshot_keeps_latest_result_per_family_after_check(monkeypatch, tmp_path):
    state = tmp_path / "state"
    _write_json(
        state / "latest.json",
        {
            "status": "passed",
            "mode": "check",
            "finished_at": "2026-09-15T08:00:00+08:00",
            "families": [],
        },
    )
    _write_json(
        state / "runs" / "20260915_070000.json",
        {
            "status": "passed",
            "mode": "shadow",
            "finished_at": "2026-09-15T07:00:00+08:00",
            "families": [
                {
                    "family": "fixed_income_plus",
                    "run_id": "fixed-run",
                    "assessment": {
                        "valuation_date": "2026-09-14",
                        "count_coverage": 0.9,
                    },
                    "reconciliation": {"status": "passed"},
                },
                {
                    "family": "enhanced_index",
                    "run_id": "index-run",
                    "assessment": {
                        "valuation_date": "2026-09-14",
                        "count_coverage": 1.0,
                    },
                    "reconciliation": {"status": "passed"},
                },
            ],
        },
    )
    _write_json(
        state / "observation.json",
        {"successful_distinct_valuation_days": 3, "required_shadow_days": 10},
    )
    monkeypatch.setattr(service, "load_production_config", lambda _path: _config(state))

    snapshot = service.get_fundpos_snapshot(tmp_path / "config.json")

    assert snapshot["status"] == "ready"
    assert snapshot["latest_run_mode"] == "check"
    assert snapshot["observation_days"] == 3
    assert [item["status"] for item in snapshot["families"]] == ["成功", "成功"]
    assert [item["run_id"] for item in snapshot["families"]] == [
        "fixed-run",
        "index-run",
    ]


@pytest.mark.asyncio
async def test_gui_never_allows_fundpos_publish():
    with pytest.raises(ValueError, match="不允许正式发布"):
        await service.execute_fundpos(mode="publish")


@pytest.mark.asyncio
async def test_shadow_execution_uses_selected_families(monkeypatch, tmp_path):
    calls = []

    class Runner:
        def __init__(self, config):
            self.config = config

        def run(self, **kwargs):
            calls.append(kwargs)
            return {"status": "passed"}

    monkeypatch.setattr(
        service, "load_production_config", lambda _path: _config(tmp_path)
    )
    monkeypatch.setattr(service, "FundposProductionRunner", Runner)
    service._is_running = False

    result = await service.execute_fundpos(
        mode="shadow",
        families=["enhanced_index"],
        cutoff="2026-09-15",
        config_path=tmp_path / "config.json",
    )

    assert result["status"] == "passed"
    assert calls == [
        {
            "mode": "shadow",
            "date": "latest",
            "cutoff": "2026-09-15",
            "families": ("enhanced_index",),
        }
    ]
