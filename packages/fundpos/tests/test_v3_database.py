import copy
import json

import numpy as np
import pandas as pd
import pytest

from fundpos.config import Settings
from fundpos.constants import ASSETS, FIXED_INCOME_OUTPUTS
from fundpos.database import (
    FundposDatabase,
    _compare_frames,
    _exposure_quality,
    _json,
    _owner_exception,
    _scenario_exposure_records,
)
from fundpos.errors import DataUnavailable, ProtocolError
from fundpos.storage import atomic_json, atomic_parquet


def make_run(path, settings):
    path.mkdir()
    row = {"fund_code": "F", "master_code": "F", "fund_name": "示例", "category": "偏股混合",
           "valuation_date": "2024-06-28", "status": "degraded", "reason": "CONTRACT_NOT_VERIFIED",
           "aum": 100.0, **dict.fromkeys(ASSETS, 0.0)}
    row["cash"] = 1.0
    atomic_parquet(path / "estimates.parquet", pd.DataFrame([row]))
    aggregate = {"valuation_date": "2024-06-28", "category": "全部主动权益", "weighting": "equal",
                 "status": "partial", "universe_count": 1, "valid_count": 0, "count_coverage": 0.0,
                 "aum_coverage": None, "aum_known_count": 1, "known_aum": 100.0,
                 **dict.fromkeys(ASSETS, None)}
    atomic_parquet(path / "aggregates.parquet", pd.DataFrame([aggregate]))
    atomic_json(path / "manifest.json", {"run_id": "run-1", "created_at": "2024-06-29T01:00:00+08:00",
        "valuation_date": "2024-06-28", "information_cutoff": "2024-06-29", "model_family": "equity",
        "scope_version": "test", "input_hash": "input", "config_hash": "config", "code_hash": "code",
        "formal_publication": False, "configuration": {"model": {"name": "personalized"}}})
    return path


def test_database_run_preflight_is_read_only_and_deterministic(settings, tmp_path):
    run = make_run(tmp_path / "run", settings)
    db = FundposDatabase(settings)
    first, second = db.preflight_run(run), db.preflight_run(run)
    assert first["logical_run_key"] == second["logical_run_key"]
    assert first["estimate_rows"] == first["aggregate_rows"] == 1


def test_database_preflight_uses_fixed_income_assets_for_convertible_family(
    settings, tmp_path
):
    run = make_run(tmp_path / "run", settings)
    estimates = pd.read_parquet(run / "estimates.parquet")
    for asset in FIXED_INCOME_OUTPUTS:
        if asset not in estimates:
            estimates[asset] = 0.0
    estimates["primary_scenario"] = "no_financing"
    atomic_parquet(run / "estimates.parquet", estimates)
    aggregates = pd.read_parquet(run / "aggregates.parquet")
    for asset in FIXED_INCOME_OUTPUTS:
        if asset not in aggregates:
            aggregates[asset] = None
    atomic_parquet(run / "aggregates.parquet", aggregates)
    manifest = json.loads((run / "manifest.json").read_text(encoding="utf8"))
    manifest["model_family"] = "convertible_dominant"
    atomic_json(run / "manifest.json", manifest)
    atomic_parquet(
        run / "scenarios.parquet",
        pd.DataFrame(
            [
                {
                    "fund_code": "F",
                    "master_code": "F",
                    "valuation_date": "2024-06-28",
                    "scenario": "no_financing",
                    "cash": 1.0,
                    "financing": 0.0,
                }
            ]
        ),
    )
    checked = FundposDatabase(settings).preflight_run(run)
    assert checked["assets"] == FIXED_INCOME_OUTPUTS
    records = _scenario_exposure_records(checked["scenarios"], checked["estimates"])
    assert len(records) == 2
    assert all(record["is_primary"] for record in records)


def test_database_preflight_requires_fixed_income_scenario_evidence(settings, tmp_path):
    run = make_run(tmp_path / "run", settings)
    estimates = pd.read_parquet(run / "estimates.parquet")
    for asset in FIXED_INCOME_OUTPUTS:
        if asset not in estimates:
            estimates[asset] = 0.0
    atomic_parquet(run / "estimates.parquet", estimates)
    manifest = json.loads((run / "manifest.json").read_text(encoding="utf8"))
    manifest["model_family"] = "fixed_income_plus"
    atomic_json(run / "manifest.json", manifest)

    with pytest.raises(DataUnavailable, match="scenarios.parquet"):
        FundposDatabase(settings).preflight_run(run)


def test_database_preflight_rejects_duplicate_products(settings, tmp_path):
    run = make_run(tmp_path / "run", settings)
    estimates = pd.read_parquet(run / "estimates.parquet")
    atomic_parquet(run / "estimates.parquet", pd.concat([estimates, estimates], ignore_index=True))
    with pytest.raises(DataUnavailable, match="RUN_SCHEMA"):
        FundposDatabase(settings).preflight_run(run)


def test_validation_preflight_accepts_conditional_diagnostics_without_publication(
    settings, tmp_path
):
    path = tmp_path / "validation.json"
    atomic_json(
        path,
        {
            "status": "conditional_complete",
            "model_family": "fixed_income_plus",
            "model_version": "v3-test",
            "phase": "selection",
            "scope_version": "pilot",
            "truth_version": "truth",
            "protocol_hash": "protocol",
            "recalculation_code_hash": "code",
            "metrics": {"candidate": {"stock_mae": 0.08}},
            "final_holdout_opened": False,
        },
    )
    checked = FundposDatabase(settings).preflight_validation(path)
    assert checked["status"] == "conditional_complete"
    assert checked["formal_publication_eligible"] is False
    blocked = json.loads(path.read_text(encoding="utf8"))
    blocked["status"] = "blocked_data"
    atomic_json(path, blocked)
    with pytest.raises(ProtocolError):
        FundposDatabase(settings).preflight_validation(path)


def test_project_owner_exception_is_formally_eligible_without_faking_final_holdout(
    settings, tmp_path
):
    override = {
        "authorized": True,
        "authorization_type": "project_owner_explicit",
        "authorized_at": "2026-09-13T00:00:00+08:00",
        "policy_version": "v3.1",
        "policy_sha256": "policy",
        "reason": "Explicitly approved relaxed acceptance",
        "waived_gates": ["final_holdout"],
        "allow_partial_groups": True,
    }
    assert _owner_exception(override) == override
    path = tmp_path / "approved.json"
    atomic_json(
        path,
        {
            "status": "passed",
            "model_family": "fixed_income_plus",
            "model_version": "v3-approved",
            "phase": "approval",
            "scope_version": "pilot",
            "truth_version": "truth",
            "protocol_hash": "protocol",
            "recalculation_code_hash": "code",
            "metrics": {"candidate": {"cbond_mae": 0.065}},
            "final_holdout_opened": False,
            "acceptance_override": override,
        },
    )
    checked = FundposDatabase(settings).preflight_validation(path)
    assert checked["formal_publication_eligible"] is True
    assert checked["phase"] == "approval"


def test_project_owner_exception_requires_complete_audit_fields():
    with pytest.raises(ProtocolError, match="Incomplete"):
        _owner_exception({"authorized": True})


def test_database_json_serializes_numpy_array_diagnostics():
    assert json.loads(_json({"failures": np.array(["a", "b"])})) == {
        "failures": ["a", "b"]
    }


def test_convertible_exposure_quality_matches_ingestion_and_reconciliation():
    row = {
        "cbond_quality": "estimated",
        "cbond_reliability": "model_disagreement",
    }
    assert _exposure_quality(row, "convertible_bond") == "diagnostic"
    row["cbond_reliability"] = "agreement_qualified"
    assert _exposure_quality(row, "convertible_bond") == "estimated"


def test_migration_contains_partition_and_publication_guards(settings):
    sql = (settings.root / "migrations/001_fundpos_v3.sql").read_text(encoding="utf8")
    assert "PARTITION BY RANGE (valuation_date)" in sql
    assert "ux_publication_current" in sql
    assert "fundpos.published_current" in sql
    assert "rawdata." not in sql


def test_scenario_migration_persists_financing_diagnostics(settings):
    sql = (settings.root / "migrations/005_fund_scenario_exposure.sql").read_text(
        encoding="utf8"
    )
    assert "fundpos.fund_scenario_exposure" in sql
    assert "scenario_code" in sql
    assert "REFERENCES fundpos.fund_estimate" in sql
    grants = (
        settings.root / "migrations/006_fund_scenario_privileges.sql"
    ).read_text(encoding="utf8")
    assert "fundpos_writer" in grants
    assert "fundpos_reader" in grants


def test_database_roles_are_schema_scoped(settings):
    sql = (settings.root / "migrations/002_fundpos_roles.sql").read_text(encoding="utf8")
    assert "fundpos_reader" in sql and "fundpos_writer" in sql and "fundpos_migrator" in sql
    assert "REVOKE ALL ON SCHEMA fundpos FROM PUBLIC" in sql
    assert "rawdata" not in sql


def test_write_connection_never_falls_back_to_source_config(
    settings, tmp_path, monkeypatch
):
    values = copy.deepcopy(settings.values)
    values["data"]["alphahome_config"] = str(tmp_path / "missing.json")
    for name in ("FUNDPOS_WRITE_DATABASE_URL", "FUNDPOS_MIGRATION_DATABASE_URL"):
        monkeypatch.delenv(name, raising=False)
    database = FundposDatabase(Settings(settings.root, values))
    with pytest.raises(DataUnavailable, match="NO_FUNDPOS_WRITE_CONFIG"):
        database.connect()
    with pytest.raises(DataUnavailable, match="NO_FUNDPOS_MIGRATION_CONFIG"):
        database.connect(migration=True)


def test_v3_protocol_does_not_open_final_holdout(settings):
    protocol = json.loads((settings.root / "config/v3_validation_protocol.json").read_text(encoding="utf8"))
    assert protocol["final_holdout_opened"] is False
    assert protocol["acceptance"]["stock_mae_max"] == .05
    assert protocol["acceptance"]["convertible_bond_mae_max"] == .05


def test_database_reconciliation_distinguishes_null_and_zero():
    expected = pd.DataFrame(
        [{"date": "2024-06-28", "id": "A", "value": None}]
    )
    actual = pd.DataFrame(
        [{"date": pd.Timestamp("2024-06-28"), "id": "A", "value": 0.0}]
    )
    result = _compare_frames(
        expected,
        actual,
        keys=["date", "id"],
        columns=["value"],
    )
    assert result["value_mismatches"] == 1
