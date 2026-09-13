from copy import deepcopy

import pytest

from alphahome.curation.etf_candidate_master import (
    CandidateMasterValidationError,
    COVERAGE_VIEW_SQL,
    ENRICHED_VIEW_SQL,
    SCHEMA_SQL,
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
