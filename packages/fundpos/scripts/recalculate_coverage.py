"""Versioned monetary-weight and data-coverage repair, on the original frozen universe.

prepare -> freeze -> run (disjoint resumable ranges) -> summarize.
No writes to AlphaDB, original full-test artifacts or the final holdout.
"""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

import full_report_test as harness
import pandas as pd
from filelock import FileLock

from fundpos.config import Settings
from fundpos.constants import SW_CODES
from fundpos.data import AlphaDB, DataBundle
from fundpos.normalization import (
    historical_membership_fallback,
    normalize_holdings,
    separate_nav_availability,
)
from fundpos.pit import dates
from fundpos.storage import atomic_json, atomic_parquet, code_fingerprint, file_hash

ROOT = Path(__file__).resolve().parents[1]
OLD = ROOT / "outputs/full_test"
OLD_CACHE = ROOT / "data/full_test"
OUT = ROOT / "outputs/full_test_v2"
CACHE = ROOT / "data/full_test_v2"
PROTOCOL = ROOT / "config/coverage_repair_protocol.json"
KEY = ["fund_code", "report_date"]


def prepare():
    if (OUT / "inventory.json").exists():
        raise RuntimeError("Inputs are already frozen; run or summarize the existing version")
    plan = json.loads((OLD / "inventory.json").read_text(encoding="utf8"))
    OUT.mkdir(parents=True, exist_ok=True)
    CACHE.mkdir(parents=True, exist_ok=True)
    sources = CACHE / "sources"
    sources.mkdir(exist_ok=True)
    financial = pd.read_parquet(ROOT / "outputs/coverage_diagnosis/financial_reports_source.parquet")
    atomic_parquet(sources / "financial_reports.parquet", financial)
    path = sources / "membership_events.parquet"
    if not path.exists():
        securities = set()
        for i in range(len(plan["batches"])):
            securities.update(pd.read_parquet(OLD_CACHE / f"{i:03d}/holdings.parquet",
                                               columns=["security_code"]).security_code.dropna())
        query = """SELECT ts_code security_code,trade_date in_date,industry_source,industry_l1
                   FROM rawdata.stock_industry_versioned WHERE industry_source='SWHY'
                   AND ts_code=ANY(%s) AND trade_date<=%s"""
        events = AlphaDB(Settings.load()).query(query, (sorted(securities), "2024-06-30"))
        events["in_date"] = pd.to_datetime(events.in_date)
        atomic_parquet(path, events)
        atomic_json(sources / "provenance.json", {
            "queried_at": pd.Timestamp.now(tz="Asia/Shanghai").isoformat(),
            "query": query, "query_security_count": len(securities), "end": "2024-06-30",
            "financial_source": "outputs/coverage_diagnosis/financial_reports_source.parquet",
            "history_kind": "dated_change_events_reconstructed_from_current_vendor_history",
            "final_holdout_opened": False})
    events = pd.read_parquet(path)
    fallback = historical_membership_fallback(events)
    atomic_parquet(sources / "membership_fallback.parquet", fallback)
    for i in range(len(plan["batches"])):
        target = CACHE / f"{i:03d}"
        if (target / "manifest.json").exists():
            print(f"prepared {i+1}/48 already present", flush=True)
            continue
        original = DataBundle.load(OLD_CACHE / f"{i:03d}")
        frames = dict(original.frames)
        frames["holdings"] = normalize_holdings(frames["holdings"], frames["asset_reports"])
        frames["nav"] = separate_nav_availability(frames["nav"])
        frames["financial_reports"] = financial.loc[
            financial.fund_code.isin(frames["funds"].fund_code)].copy()
        frames["membership_fallback"] = fallback.loc[
            fallback.security_code.isin(frames["holdings"].security_code)].copy()
        # Fund NAVs, factor returns, stock prices and the historical universe are unchanged.
        repaired = DataBundle(frames, {**original.provenance, "coverage_repair": "v2",
            "parent_input_hash": original.fingerprint,
            "repair_source_hashes": {p.name: file_hash(p) for p in sources.glob("*.parquet")},
            "weight_basis": "security_market_value_over_report_product_NAV",
            "nav_timing": "separate_nav_and_aum_availability; no inferred historical daily release"})
        repaired.save(target)
        print(f"prepared {i+1}/48", flush=True)
    for name in ["universe.parquet", "turnover_proxy.parquet", "turnover_proxy_audit.json",
                 "ppt_evidence.json", "reference_report.json"]:
        shutil.copy2(OLD / name, OUT / name)
    (OUT / "legacy").mkdir(exist_ok=True)
    shutil.copy2(OLD / "legacy/comparison.json", OUT / "legacy/comparison.json")


def freeze():
    original = json.loads((OLD / "inventory.json").read_text(encoding="utf8"))
    if not all((CACHE / f"{i:03d}/manifest.json").exists() for i in range(len(original["batches"]))):
        raise RuntimeError("All repaired input batches must exist before freeze")
    if (OUT / "inventory.json").exists():
        verify_freeze()
        return
    protocol = json.loads((ROOT / "config/full_test_protocol.json").read_text(encoding="utf8"))
    protocol.update(protocol_id="report_date_full_universe_coverage_repair_v2",
        parent_protocol_hash=original["protocol_hash"],
        industry_labels="Independent monetary reconciliation; complete SW labels separated from partial labels",
        repairs=["precise_security_value_NAV_weights", "quarterly_financial_AUM_with_field_timing",
                 "dated_SWHY_gap_fill_primary_precedence", "small_unknown_A_equity_separate_diagnostics"],
        max_unclassified_nav_weight=.001,
        input_modes={"recorded_ann_date": "Daily NAV availability inherits original vendor date; AUM has independent availability",
                     "nav_date_assumption": "NAV available next calendar day sensitivity only; original AUM availability and holding announcements unchanged"})
    atomic_json(PROTOCOL, protocol)
    atomic_json(OUT / "inventory.json", {**original,
        "created_at": pd.Timestamp.now(tz="Asia/Shanghai").isoformat(),
        "parent_inventory_hash": file_hash(OLD / "inventory.json"),
        "parent_summary_hash": file_hash(OLD / "summary.json"),
        "code_hash": code_fingerprint(ROOT), "protocol_hash": file_hash(PROTOCOL),
        "test_script_hash": file_hash(Path(__file__)),
        "harness_hash": file_hash(Path(harness.__file__)),
        "input_manifests": {f"{i:03d}": file_hash(CACHE / f"{i:03d}/manifest.json")
                            for i in range(len(original["batches"]))}})
    print("Frozen v2 on the original 3,828 products and 34,540 fund-quarters", flush=True)


def verify_freeze():
    plan = json.loads((OUT / "inventory.json").read_text(encoding="utf8"))
    for expected, actual in [(plan["code_hash"], code_fingerprint(ROOT)),
                             (plan["protocol_hash"], file_hash(PROTOCOL)),
                             (plan["test_script_hash"], file_hash(Path(__file__))),
                             (plan["harness_hash"], file_hash(Path(harness.__file__))),
                             (plan["parent_summary_hash"], file_hash(OLD / "summary.json"))]:
        if expected != actual:
            raise RuntimeError("Frozen code, protocol, harness or original result changed")
    return plan


def nav_sensitivity(bundle):
    frames = dict(bundle.frames)
    n = separate_nav_availability(bundle["nav"])
    next_day = n.date + pd.Timedelta(days=1)
    changed = n.nav_available_at.isna() | n.nav_available_at.gt(next_day)
    n.loc[changed, "nav_available_at"] = next_day[changed]
    n.loc[changed, "nav_availability_source"] = "next_calendar_day_assumption"
    n["nav_announcement_assumed"] = changed
    frames["nav"] = n
    return DataBundle(frames, {**bundle.provenance, "nav_timing_assumption": True})


def independent_labels(bundle, requested, universe):
    """Reconstruct labels from money independently of the normalization implementation."""
    h = dates(bundle["holdings"], ("report_date", "ann_date"))
    assets = dates(bundle["asset_reports"], ("report_date", "ann_date"))
    membership = dates(bundle["membership"], ("in_date", "out_date"))
    fallback = dates(bundle["membership_fallback"], ("in_date", "out_date"))
    hg = {k: g for k, g in h.groupby(KEY)}
    ag = {k: g for k, g in assets.groupby(KEY)}
    complete, partial, stock, excluded = [], [], [], []
    for fund in universe.loc[universe.fund_code.isin(requested)].itertuples(index=False):
        report_date = pd.Timestamp(fund.report_date)
        key = (fund.fund_code, report_date)
        header = {"fund_code": fund.fund_code, "valuation_date": fund.valuation_date,
                  "report_date": fund.report_date}
        control = ag.get(key, pd.DataFrame())
        valid_control = len(control) == 1 and pd.notna(control.iloc[0].stock_weight)
        if valid_control:
            stock.append({**header, "stock_true": float(control.iloc[0].stock_weight),
                          "label_aum": control.iloc[0].aum, "label_ann_date": None})
        else:
            excluded.append({**header, "label_kind": "quarterly_stock",
                             "reason": "MISSING_OR_AMBIGUOUS_ASSET_LABEL"})
        if report_date.month not in (6, 12):
            continue
        group = hg.get(key, pd.DataFrame()).copy()
        reason = None
        if group.empty:
            reason = "NO_HOLDINGS_LABEL"
        elif group.security_code.isna().any() or group.security_code.duplicated().any():
            reason = "MISSING_OR_DUPLICATE_SECURITY"
        elif not valid_control or pd.isna(control.iloc[0].aum) or control.iloc[0].aum <= 0:
            reason = "NO_INDEPENDENT_ASSET_CONTROL"
        elif (group.market_value.isna().any() or (group.market_value < 0).any()
              or group.weight_disclosed.isna().any() or (group.weight_disclosed < 0).any()):
            reason = "INVALID_LABEL_WEIGHT"
        else:
            a = control.iloc[0]
            amount = float(group.market_value.sum())
            if (pd.isna(a.stock_market_value)
                or abs(amount - a.stock_market_value) > max(1., abs(a.stock_market_value)*1e-8)
                or abs(amount / a.aum - a.stock_weight) > 1e-6):
                reason = "INCOMPLETE_HOLDINGS_VALUE"
        if reason:
            excluded.append({**header, "label_kind": "industry", "reason": reason})
            continue
        # Label calculation does not use the model's normalized weight column.
        group["true_weight"] = group.market_value.astype(float) / float(a.aum)
        current = membership.loc[(membership.in_date <= report_date)
            & (membership.out_date.isna() | (report_date < membership.out_date)),
            ["security_code", "industry"]].drop_duplicates()
        if not fallback.empty:
            extra = fallback.loc[(fallback.in_date <= report_date)
                & (fallback.out_date.isna() | (report_date < fallback.out_date)),
                ["security_code", "industry"]].drop_duplicates()
            current = pd.concat([current, extra.loc[~extra.security_code.isin(current.security_code)]])
        if current.security_code.duplicated().any():
            raise RuntimeError("Ambiguous historical membership in independent label construction")
        group = group.merge(current, on="security_code", how="left", validate="many_to_one")
        group.loc[group.security_code.str.endswith(".HK"), "industry"] = "hk"
        missing = group.industry.isna() & group.true_weight.gt(0)
        unknown = float(group.loc[missing, "true_weight"].sum())
        weights = group.groupby("industry").true_weight.sum()
        label = {**header, **{s: float(weights.get(s, 0)) for s in SW_CODES},
            "hk_true": float(weights.get("hk", 0)), "stock_true": float(group.true_weight.sum()),
            "label_aum": float(a.aum), "label_ann_verified": bool(group.ann_date.notna().all()),
            "label_ann_date": str(group.ann_date.max().date()) if group.ann_date.notna().all() else None,
            "control_difference": float(group.true_weight.sum() - a.stock_weight),
            "unclassified_weight": unknown, "label_weight_basis": "independent_market_value_over_report_NAV"}
        if not missing.any():
            complete.append(label)
        else:
            a_only = group.loc[missing].security_code.str.fullmatch(r"\d{6}\.(SH|SZ|BJ)").all()
            small = bool(a_only and unknown <= .001)
            if small:
                partial.append({**label, "label_kind": "partial_A_industry",
                    "unclassified_securities": ",".join(group.loc[missing, "security_code"])})
            excluded.append({**header, "label_kind": "industry",
                "reason": "SMALL_UNCLASSIFIED_A_PARTIAL_LABEL" if small else "UNMAPPED_SW_LABEL",
                "missing_weight": unknown, "securities": ",".join(group.loc[missing, "security_code"])})
    return pd.DataFrame(complete), pd.DataFrame(stock), pd.DataFrame(excluded), pd.DataFrame(partial)


def run(first, last):
    plan = verify_freeze()
    universe = pd.read_parquet(OUT / "universe.parquet")
    harness.OUT, harness.CACHE, harness.PROTOCOL = OUT, CACHE, PROTOCOL
    harness.nav_sensitivity = nav_sensitivity
    for i in range(first, min(last or len(plan["batches"]), len(plan["batches"]))):
        with FileLock(str(OUT / f"batch_{i:03d}.lock"), timeout=0):
            if file_hash(CACHE / f"{i:03d}/manifest.json") != plan["input_manifests"][f"{i:03d}"]:
                raise RuntimeError("Derived input manifest changed")
            target = OUT / f"batches/{i:03d}"
            target.mkdir(parents=True, exist_ok=True)

            def labels(data, codes, unused_universe):
                complete, stock, excluded, partial = independent_labels(data, codes, universe)
                atomic_parquet(target / "partial_industry_labels.parquet", partial)
                return complete, stock, excluded

            harness.independent_labels = labels
            harness.run(i, i+1)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("phase", choices=["prepare", "freeze", "run", "summarize"])
    p.add_argument("--first", type=int, default=0)
    p.add_argument("--last", type=int)
    args = p.parse_args()
    if args.phase == "prepare":
        prepare()
    elif args.phase == "freeze":
        freeze()
    elif args.phase == "run":
        run(args.first, args.last)
    else:
        verify_freeze()
        import full_test_summary
        full_test_summary.OUT = OUT
        full_test_summary.summarize()


if __name__ == "__main__":
    main()
