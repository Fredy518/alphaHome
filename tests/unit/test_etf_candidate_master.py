from copy import deepcopy
from datetime import datetime, timezone

import pytest

from alphahome.curation.etf_candidate_master import (
    CandidateMasterValidationError,
    COVERAGE_VIEW_SQL,
    ENRICHED_VIEW_SQL,
    SCHEMA_SQL,
    load_candidate_master_snapshot,
    validate_payload,
)


def sample_payload():
    record = {
        "source_row_number": 7,
        "source_rank": 1,
        "asset_class": "境内权益",
        "allocation_module": "核心宽基",
        "allocation_role": "核心锚",
        "region_market": "中国A股",
        "level1_group": "A股核心",
        "level2_group": "全市场综合",
        "exposure_name": "中证A500",
        "exposure_id": "CN_CORE_A500",
        "fund_code": "563360.SH",
        "fund_name": "ETF",
        "tracking_index_code": "000510.SH",
        "tracking_index_name": "中证A500",
        "product_role": "主工具",
        "candidate_status": "正式候选",
        "exposure_relationship": "核心锚主实现",
        "parent_fund_code": None,
        "budget_scope": "A股核心锚共享预算",
        "snapshot_aum_100m": 397.4,
        "snapshot_amount_20d_100m": 42.7,
        "snapshot_age_months": 22.6,
        "snapshot_total_fee_pct": 0.2,
        "snapshot_mean_abs_premium_60d": None,
        "snapshot_product_auxiliary_state": "强",
        "snapshot_premium_observation_label": "不适用",
        "product_facts_as_of": "2026-09-04",
        "source_supplement": None,
        "manual_review_status": "待复核",
        "inclusion_reason": "reason",
        "risk_boundary": "boundary",
        "update_frequency": "quarterly",
        "execution_check": "check before execution",
        "research_permission": "候选池研究",
        "duplicate_check": "OK",
        "data_source_id": "ALPHADB_20260904",
    }
    return {
        "contract_version": "etf_candidate_master_snapshot_v1",
        "snapshot_id": "etf_candidate_master_20260905_" + "a" * 12,
        "source": {
            "source_version": "v1",
            "source_file_name": "master.xlsx",
            "source_file_path": "Z:/not-present/master.xlsx",
            "source_file_sha256": "a" * 64,
            "workbook_generated_on": "2026-09-05",
            "product_facts_as_of": "2026-09-04",
            "structure_baseline_as_of": "2026-07-31",
            "exported_at": "2026-09-13T00:00:00Z",
        },
        "governance": {
            "research_stage": "EXPLORE",
            "authority_scope": "CANDIDATE_POOL_ONLY",
            "capital_authority": False,
            "order_authority": False,
        },
        "thresholds": {
            "strong_aum_100m": 20,
            "strong_amount_20d_100m": 1,
            "minimum_age_months": 12,
            "usable_aum_100m": 5,
            "usable_amount_20d_100m": 0.3,
            "low_premium_upper": 0.04,
            "medium_premium_upper": 0.08,
            "premium_is_candidate_gate": False,
        },
        "quality": {
            "row_count": 1,
            "exposure_count": 1,
            "check_count": 1,
            "all_checks_ok": True,
            "checks": [{"check_id": "OVERALL", "status": "OK"}],
        },
        "records": [record],
    }


def test_validate_payload_accepts_research_only_snapshot():
    validate_payload(sample_payload())


@pytest.mark.parametrize("authority_field", ["capital_authority", "order_authority"])
def test_validate_payload_rejects_execution_authority(authority_field):
    payload = sample_payload()
    payload["governance"][authority_field] = True
    with pytest.raises(CandidateMasterValidationError):
        validate_payload(payload)


def test_validate_payload_rejects_status_permission_mismatch():
    payload = deepcopy(sample_payload())
    payload["records"][0]["research_permission"] = "仅观察"
    with pytest.raises(CandidateMasterValidationError):
        validate_payload(payload)


def test_schema_isolated_from_legacy_latest_snapshot():
    assert "etf_candidate_master_latest_batch" in SCHEMA_SQL
    assert "latest_snapshot_batch" not in SCHEMA_SQL
    assert "features.mv_etf_product_facts_current" in ENRICHED_VIEW_SQL
    assert "confirmation_status" in SCHEMA_SQL
    assert "AI_CONFIRMED" in SCHEMA_SQL
    assert "HUMAN_REJECTED" in SCHEMA_SQL
    assert "WHERE s.include_in_candidate_pool" in SCHEMA_SQL


def test_validate_payload_accepts_legacy_record_without_confirmation_fields():
    payload = sample_payload()
    validate_payload(payload, verify_source_file=False)


def test_validate_payload_requires_ai_provenance():
    payload = sample_payload()
    payload["records"][0].update(
        {
            "confirmation_status": "AI_CONFIRMED",
            "confirmation_actor": "deepseek:deepseek-flash",
            "confirmation_at": "2026-09-16T00:00:00+00:00",
            "ai_run_id": "etf_ai_202609_example",
            "ai_model": "deepseek-flash",
            "ai_confidence": 0.9,
            "ai_decision_hash": "not-a-hash",
            "human_review_note": None,
        }
    )
    with pytest.raises(CandidateMasterValidationError, match="ai_decision_hash"):
        validate_payload(payload, verify_source_file=False)

    payload["records"][0]["ai_decision_hash"] = "b" * 64
    validate_payload(payload, verify_source_file=False)


def test_validate_payload_accepts_auditable_human_rejection():
    payload = sample_payload()
    payload["records"][0].update(
        {
            "confirmation_status": "HUMAN_REJECTED",
            "confirmation_actor": "wuh",
            "confirmation_at": "2026-09-16T00:00:00+00:00",
            "human_review_note": "产品执行条件不满足",
            "include_in_candidate_pool": False,
        }
    )

    validate_payload(payload, verify_source_file=False)

    payload["records"][0]["include_in_candidate_pool"] = True
    with pytest.raises(
        CandidateMasterValidationError, match="leave the candidate pool"
    ):
        validate_payload(payload, verify_source_file=False)


def test_coverage_view_exposes_missing_direct_route_without_reconstruction():
    assert "etf_candidate_index_coverage_current" in COVERAGE_VIEW_SQL
    assert "MISSING_DIRECT_ROUTE" in COVERAGE_VIEW_SQL
    assert "mv_etf_exposure_technical_current_universe_daily" in COVERAGE_VIEW_SQL
    assert "mv_index_direct_valuation_daily" in COVERAGE_VIEW_SQL


def test_validate_payload_checks_accessible_source_hash_unless_skipped(tmp_path):
    source = tmp_path / "master.xlsx"
    source.write_bytes(b"current workbook bytes")
    payload = sample_payload()
    payload["source"]["source_file_path"] = str(source)

    with pytest.raises(CandidateMasterValidationError, match="SHA-256 mismatch"):
        validate_payload(payload)

    validate_payload(payload, verify_source_file=False)


def test_loaded_snapshot_is_immutable_idempotent_noop(monkeypatch):
    loaded_at = datetime(2026, 9, 16, 11, 26, tzinfo=timezone.utc)

    class Cursor:
        def __init__(self):
            self.sql = []
            self.result = None

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, _params=None):
            self.sql.append(sql)
            if sql == "SHOW transaction_isolation":
                self.result = ("read committed",)
            elif "pg_advisory_xact_lock" in sql:
                self.result = (None,)
            elif "SELECT source_file_sha256" in sql:
                assert any("pg_advisory_xact_lock" in item for item in self.sql)
                self.result = ("a" * 64, "loaded", loaded_at)
            elif "COUNT(*) AS row_count" in sql:
                self.result = (1, 1, 1, 0, 0)
            else:
                raise AssertionError(f"unexpected write during idempotent load: {sql}")

        def fetchone(self):
            return self.result

    class Connection:
        def __init__(self):
            self.cursor_instance = Cursor()
            self.commit_count = 0

        def cursor(self):
            return self.cursor_instance

        def commit(self):
            self.commit_count += 1

        def rollback(self):
            raise AssertionError("idempotent no-op must not roll back")

    connection = Connection()
    monkeypatch.setattr(
        "alphahome.curation.etf_candidate_master.ensure_candidate_master_schema",
        lambda _connection: None,
    )

    result = load_candidate_master_snapshot(
        connection,
        sample_payload(),
        verify_source_file=False,
    )

    assert result["idempotent_noop"] is True
    assert result["loaded_at"] == loaded_at.isoformat()
    assert connection.commit_count == 1
    assert not any("DELETE FROM" in sql for sql in connection.cursor_instance.sql)
