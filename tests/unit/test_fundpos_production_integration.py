from __future__ import annotations

import hashlib
import json

import pytest

from alphahome.integrations.fundpos.production import (
    CommandOutput,
    FundposProductionConfig,
    FundposProductionError,
    FundposProductionRunner,
    assess_run_manifest,
    parse_cli_json,
)
from alphahome.integrations.fundpos.state import (
    FundposStateError,
    bootstrap_fundpos_state,
)


def _config(tmp_path, **updates):
    values = {
        "repository_root": str(tmp_path),
        "engine_root": str(tmp_path / "packages" / "fundpos"),
        "python_executable": str(tmp_path / "python.exe"),
        "engine_config": "config/v3.toml",
        "release_manifest": str(tmp_path / "release.json"),
        "expected_tag": "v0.3.0",
        "expected_package_version": "0.3.0",
        "state_dir": str(tmp_path / "state"),
        "families": ["fixed_income_plus", "enhanced_index"],
        "expected_universe_counts": {
            "fixed_income_plus": 90,
            "enhanced_index": 20,
        },
        "expected_scope_versions": {
            "fixed_income_plus": "fundpos_v3_fixed_income_plus_20260912",
            "enhanced_index": "fundpos_v3_enhanced_index_20260912",
        },
        "non_applicable_reasons": {
            "fixed_income_plus": ["UNIVERSE_RULES_NOT_MET"],
        },
    }
    values.update(updates)
    return FundposProductionConfig.from_mapping(values)


def test_parse_cli_json_uses_final_object_after_progress():
    payload = {"status": "passed", "run": "x"}
    output = "读取 AlphaDB\nprogress {not json}\n" + json.dumps(payload)

    assert parse_cli_json(output) == payload


def test_config_rejects_unknown_or_duplicate_families(tmp_path):
    with pytest.raises(FundposProductionError, match="Unknown"):
        _config(tmp_path, families=["unknown"])
    with pytest.raises(FundposProductionError, match="unique"):
        _config(tmp_path, families=["enhanced_index", "enhanced_index"])


def test_config_requires_a_frozen_count_and_scope_for_every_family(tmp_path):
    with pytest.raises(FundposProductionError, match="expected_universe_counts"):
        _config(tmp_path, families=["convertible_dominant"])


def test_config_resolves_engine_config_below_release_checkout(tmp_path):
    config = _config(tmp_path)

    assert (
        config.engine_config
        == (tmp_path / "packages" / "fundpos" / "config" / "v3.toml").resolve()
    )
    assert config.repository_root == tmp_path.resolve()
    assert config.engine_root == (tmp_path / "packages" / "fundpos").resolve()
    assert config.expected_revision is None
    assert config.minimum_count_coverage == 0.80
    assert config.mode == "shadow"


def test_production_estimate_never_generates_file_reports(tmp_path):
    commands = []

    def command_runner(args, cwd, timeout_seconds, environment):
        commands.append(tuple(args))
        return CommandOutput(tuple(args), 0, json.dumps({"run": "run"}), "")

    runner = FundposProductionRunner(_config(tmp_path), command_runner=command_runner)
    result = runner._estimate("enhanced_index", "2026-09-11", "2026-09-12")

    assert result["run"] == "run"
    assert "--report" not in commands[0]
    assert "--no-excel" not in commands[0]


def test_assess_manifest_accepts_owner_approved_shadow_coverage():
    manifest = {
        "model_family": "fixed_income_plus",
        "valuation_date": "2026-09-11",
        "information_cutoff": "2026-09-14",
        "scope_version": "fundpos_v3_fixed_income_plus_20260912",
        "git_revision": "a" * 40,
        "status_counts": {"degraded": 81, "unavailable": 19},
        "provenance": {"provider": "alphadb", "sql_mode": "read_only"},
        "input_hash": "input",
        "code_hash": "code",
    }

    result = assess_run_manifest(
        manifest,
        family="fixed_income_plus",
        minimum_count_coverage=0.80,
        expected_universe_count=100,
        expected_scope_version="fundpos_v3_fixed_income_plus_20260912",
        expected_revision="a" * 40,
    )

    assert result["available_count"] == 81
    assert result["count_coverage"] == pytest.approx(0.81)
    assert result["complete"] is False


def test_assess_manifest_fails_closed_on_coverage_or_writeable_source():
    manifest = {
        "model_family": "equity",
        "valuation_date": "2026-09-11",
        "information_cutoff": "2026-09-14",
        "scope_version": "fundpos_v3_enhanced_index_20260912",
        "git_revision": "a" * 40,
        "status_counts": {"degraded": 7, "unavailable": 3},
        "provenance": {"provider": "alphadb", "sql_mode": "read_only"},
    }
    with pytest.raises(FundposProductionError, match="below"):
        assess_run_manifest(
            manifest, family="enhanced_index", minimum_count_coverage=0.80
        )

    manifest["status_counts"] = {"degraded": 10}
    manifest["provenance"]["sql_mode"] = "read_write"
    with pytest.raises(FundposProductionError, match="read-only"):
        assess_run_manifest(
            manifest, family="enhanced_index", minimum_count_coverage=0.80
        )


def test_assess_manifest_fails_when_frozen_scope_loses_a_product():
    manifest = {
        "model_family": "convertible_dominant",
        "scope_version": "fundpos_v3_convertible_dominant_20260913",
        "git_revision": "a" * 40,
        "valuation_date": "2026-09-11",
        "information_cutoff": "2026-09-14",
        "status_counts": {"degraded": 9},
        "provenance": {"provider": "alphadb", "sql_mode": "read_only"},
    }

    with pytest.raises(FundposProductionError, match="frozen scope count"):
        assess_run_manifest(
            manifest,
            family="convertible_dominant",
            minimum_count_coverage=0.80,
            expected_universe_count=10,
        )


def test_assess_manifest_reports_but_excludes_out_of_scope_controls_from_gate():
    manifest = {
        "model_family": "fixed_income_plus",
        "valuation_date": "2026-09-11",
        "information_cutoff": "2026-09-14",
        "status_counts": {"degraded": 68, "unavailable": 22},
        "reason_counts": {
            "UNIVERSE_RULES_NOT_MET": 20,
            "STALE_NAV": 2,
        },
        "provenance": {"provider": "alphadb", "sql_mode": "read_only"},
    }

    result = assess_run_manifest(
        manifest,
        family="fixed_income_plus",
        minimum_count_coverage=0.95,
        expected_universe_count=90,
        non_applicable_reasons=("UNIVERSE_RULES_NOT_MET",),
    )

    assert result["raw_count_coverage"] == pytest.approx(68 / 90)
    assert result["count_coverage"] == pytest.approx(68 / 70)
    assert result["not_applicable_count"] == 20


def test_state_bootstrap_is_idempotent_and_rejects_corruption(tmp_path):
    engine = tmp_path / "engine"
    seed = engine / "resources" / "supplements_seed"
    seed.mkdir(parents=True)
    content = b"evidence"
    (seed / "evidence.bin").write_bytes(content)
    digest = hashlib.sha256(content).hexdigest()
    (seed / "manifest.json").write_text(
        json.dumps({"files": [{"file": "evidence.bin", "sha256": digest}]}),
        encoding="utf-8",
    )
    state = tmp_path / "state"

    first = bootstrap_fundpos_state(engine, state)
    second = bootstrap_fundpos_state(engine, state)

    assert first["status"] == "initialized_from_versioned_seed"
    assert second["status"] == "existing_valid_state"
    (state / "supplements" / "evidence.bin").write_bytes(b"changed")
    with pytest.raises(FundposStateError, match="hash differs"):
        bootstrap_fundpos_state(engine, state)
