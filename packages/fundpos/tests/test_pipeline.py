import json

import numpy as np
import pandas as pd
import pytest

from fundpos.aggregation import aggregate_estimates, custom_portfolio
from fundpos.constants import ASSETS, FIXED_INCOME_OUTPUTS
from fundpos.data import DataBundle
from fundpos.errors import DataUnavailable
from fundpos.operations import completed_valuation_date, record_observation
from fundpos.pipeline import compute_date, run_estimate
from fundpos.reporting import export_report, report_payload


def test_full_pipeline_recovers_three_fund_types(settings, bundle, base_bundle):
    result = compute_date(settings, bundle, "2023-09-29", "2023-09-30")
    assert len(result) == 3 and result.status.eq("ok").all(), (
        result[["reason", "error_detail"]] if "error_detail" in result else result.reason
    )
    true = base_bundle[1].set_index("fund_code").loc[result.fund_code, list(ASSETS)]
    np.testing.assert_allclose(result[list(ASSETS)], true, atol=1e-5)
    assert result.loc[result.fund_code == "DEMO001.OF", "aum"].iloc[0] == 1.25e8
    agg = aggregate_estimates(result)
    assert agg.status.eq("complete").all()
    assert np.allclose(agg[list(ASSETS)].sum(axis=1), 1, atol=1e-6)


def test_future_market_data_and_announcements_do_not_change_cutoff(settings, bundle):
    before = compute_date(settings, bundle, "2023-06-30", "2023-07-01")
    for name, column in (("nav", "date"), ("prices", "date"), ("factors", "date")):
        data = bundle.frames[name]
        selected = pd.to_datetime(data[column]) > pd.Timestamp("2023-07-01")
        field = {"nav": "adj_nav", "prices": "adjusted_close", "factors": "return"}[name]
        data.loc[selected, field] = 0.5
    h = bundle.frames["holdings"]
    h.loc[h.ann_date > pd.Timestamp("2023-07-01"), "weight"] *= 0.5
    after = compute_date(settings, bundle, "2023-06-30", "2023-07-01")
    pd.testing.assert_frame_equal(before, after)


def test_snapshot_roundtrip_and_integrity(bundle, tmp_path):
    bundle.save(tmp_path / "snapshot")
    loaded = DataBundle.load(tmp_path / "snapshot")
    assert loaded.fingerprint == bundle.fingerprint
    file = tmp_path / "snapshot/nav.parquet"
    nav = pd.read_parquet(file)
    nav.loc[0, "adj_nav"] += 0.5
    nav.to_parquet(file)
    with pytest.raises(DataUnavailable, match="INPUT_HASH_CHANGED"):
        DataBundle.load(tmp_path / "snapshot")


def test_snapshot_fingerprint_uses_parquet_roundtrip_for_list_columns(tmp_path):
    bundle = DataBundle(
        {"funds": pd.DataFrame({"fund_code": ["F"], "share_codes": [["F", "FC"]]})}
    )
    directory = tmp_path / "list_snapshot"
    bundle.save(directory)
    loaded = DataBundle.load(directory)
    assert list(loaded["funds"].share_codes.iloc[0]) == ["F", "FC"]


def test_incremental_identity_cutoff_and_partial_publication(settings, bundle):
    full = run_estimate(settings, bundle, "2023-09-28", "2023-09-29")
    modified_time = (full / "estimates.parquet").stat().st_mtime_ns
    repeat = run_estimate(settings, bundle, "2023-09-28", "2023-09-29")
    assert full == repeat and (repeat / "estimates.parquet").stat().st_mtime_ns == modified_time
    later_cutoff = run_estimate(settings, bundle, "2023-09-28", "2023-09-30")
    assert later_cutoff != full
    bundle.frames["factors"] = bundle.frames["factors"].loc[
        bundle.frames["factors"].date < pd.Timestamp("2023-09-29")
    ]
    partial = run_estimate(settings, bundle, "2023-09-29", "2023-09-30")
    current = json.loads(
        (settings.path("output_dir") / "latest_complete.json").read_text(encoding="utf-8")
    )
    assert current["valuation_date"] == "2023-09-28"
    assert not json.loads((partial / "manifest.json").read_text(encoding="utf-8"))["complete"]


def test_missing_aum_denominator_blocks_formal_aggregation(settings, bundle):
    result = compute_date(settings, bundle, "2023-09-29", "2023-09-30")
    result.loc[0, "aum"] = None
    agg = aggregate_estimates(result)
    all_group = agg.loc[agg.category == "全部主动权益"]
    assert all_group.status.eq("partial").all()
    assert all_group.aum_coverage.isna().all()


def test_failed_fund_is_not_zero_and_custom_weights_not_renormalized(settings, bundle):
    data = bundle.frames["nav"]
    bundle.frames["nav"] = data.loc[
        ~((data.fund_code == "DEMO001.OF") & (data.date == data.date.max()))
    ]
    result = compute_date(settings, bundle, "2023-09-29", "2023-09-30")
    failed = result.loc[result.fund_code == "DEMO001.OF"]
    assert failed.reason.iloc[0] == "STALE_NAV" and failed[list(ASSETS)].isna().all().all()
    custom = pd.DataFrame({"master_code": ["DEMO001.OF", "DEMO002.OF"], "weight": [0.5, 0.5]})
    with pytest.raises(DataUnavailable, match="CUSTOM_PORTFOLIO_INCOMPLETE"):
        custom_portfolio(result, custom, "2023-09-29")


def test_holiday_aware_target_and_live_observation_boundary(settings, bundle):
    calendar = pd.to_datetime(["2023-09-25", "2023-09-26", "2023-09-27", "2023-09-28"])
    assert completed_valuation_date(calendar, "2023-09-30") == pd.Timestamp("2023-09-28")
    run = run_estimate(settings, bundle, "2023-09-29", "2023-09-30")
    with pytest.raises(DataUnavailable, match="OBSERVATION_SCOPE"):
        record_observation(settings, run)


def test_html_numbers_come_from_same_estimates(settings, bundle):
    run = run_estimate(settings, bundle, "2023-09-29", "2023-09-30")
    files = export_report(settings.root, run)
    payload = json.loads((run / "report/report_data.json").read_text(encoding="utf-8"))
    first = payload["estimates"][0]
    assert first["stock_weight"] == pytest.approx(0.88, abs=1e-5)
    html = (run / "report/report.html").read_text(encoding="utf-8")
    assert "合成示例基金01A" in html and "88.00%" in html
    assert files["html"].endswith("report.html")
    assert not (run / "report/report.xlsx").exists()


def test_convertible_dominant_report_uses_dedicated_family_and_diagnostics(
    settings, bundle
):
    run = run_estimate(settings, bundle, "2023-09-29", "2023-09-30")
    estimates = pd.read_parquet(run / "estimates.parquet")
    aggregates = pd.read_parquet(run / "aggregates.parquet")
    for asset in FIXED_INCOME_OUTPUTS:
        if asset not in estimates:
            estimates[asset] = 0.0
        if asset not in aggregates:
            aggregates[asset] = 0.0
    estimates["cbond_control_weight"] = 0.8
    estimates["cbond_mark_to_market_weight"] = 0.82
    estimates["cbond_reliability"] = "agreement_qualified"
    estimates["cbond_priced_coverage"] = 0.9
    estimates.to_parquet(run / "estimates.parquet", index=False)
    aggregates.to_parquet(run / "aggregates.parquet", index=False)
    manifest_path = run / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["model_family"] = "convertible_dominant"
    manifest["configuration"]["model"]["name"] = "cbond_state_space_v1"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    payload = report_payload(run)
    overview = dict(payload["sheets"][0]["rows"])
    detail_keys = payload["sheets"][1]["keys"]
    assert payload["title"].startswith("公募基金转债主导仓位测算")
    assert payload["assets"] == list(FIXED_INCOME_OUTPUTS)
    assert overview["模型"] == "转债主导状态路径模型"
    assert overview["模型族"] == "转债主导专属仓位"
    assert "cbond_reliability" in detail_keys
    assert "cbond_mark_to_market_weight" in detail_keys
