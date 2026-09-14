"""Exhaustive report-date baseline diagnostic; preserves formal holdout state.

Run inventory, run (resumable batches), then summarize. Raw snapshots and every
exclusion are retained. A NAV-availability sensitivity never changes production.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import numpy as np
import pandas as pd

from fundpos.config import Settings
from fundpos.constants import SW_CODES
from fundpos.data import AlphaDB, DataBundle
from fundpos.pipeline import compute_date
from fundpos.pit import dates, eligible_universe
from fundpos.storage import atomic_json, atomic_parquet, code_fingerprint, file_hash
from fundpos.validation import choose_dates

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "outputs/full_test"
CACHE = ROOT / "data/full_test"
PROTOCOL = ROOT / "config/full_test_protocol.json"


def announce(text):
    print(text, flush=True)


def inventory(batch_size=80):
    protocol = json.loads(PROTOCOL.read_text(encoding="utf8"))
    db = AlphaDB(Settings.load())
    funds, classification = db.universe()
    calendar = db.query(
        "SELECT cal_date date FROM rawdata.others_calendar WHERE exchange='SSE' "
        "AND is_open=1 AND cal_date BETWEEN %s AND %s ORDER BY cal_date",
        (protocol["prediction_start"], protocol["prediction_end"]),
    )
    targets = choose_dates(calendar.date, protocol["prediction_start"],
                           protocol["prediction_end"], "quarterly")
    parts = []
    for day in targets:
        u = eligible_universe(funds, classification, day)
        u["valuation_date"] = str(day.date())
        u["report_date"] = str(day.to_period("Q").end_time.date())
        parts.append(u)
    universe = pd.concat(parts, ignore_index=True)
    codes = sorted(universe.fund_code.unique())
    OUT.mkdir(exist_ok=True, parents=True)
    CACHE.mkdir(exist_ok=True, parents=True)
    atomic_parquet(OUT / "universe.parquet", universe)
    atomic_parquet(CACHE / "funds.parquet", funds)
    atomic_parquet(CACHE / "classification.parquet", classification)
    result = {
        "created_at": pd.Timestamp.now(tz="Asia/Shanghai").isoformat(),
        "protocol_hash": file_hash(PROTOCOL), "code_hash": code_fingerprint(ROOT),
        "test_script_hash": file_hash(Path(__file__)),
        "unique_products": len(codes), "fund_report_pairs": len(universe),
        "batch_size": batch_size,
        "batches": [codes[i:i+batch_size] for i in range(0, len(codes), batch_size)],
        "dates": [str(d.date()) for d in targets],
        "by_date": universe.groupby("valuation_date").size().to_dict(),
        "final_holdout_opened": False,
    }
    atomic_json(OUT / "inventory.json", result)
    announce({k: v for k, v in result.items() if k != "batches"})


def independent_labels(bundle, requested, universe):
    """Build ex-post labels independently of fundpos.validation.disclosed_labels.

    Control totals and all security mappings are mandatory. Unknown publication
    dates may label a diagnostic, but are retained and cannot certify strict PIT.
    """
    h = dates(bundle["holdings"], ("report_date", "ann_date"))
    assets = dates(bundle["asset_reports"], ("report_date", "ann_date"))
    m = dates(bundle["membership"], ("in_date", "out_date"))
    stock_rows, industry_rows, exclusions = [], [], []
    hgroups = {k: v for k, v in h.groupby(["fund_code", "report_date"])}
    agroups = {k: v for k, v in assets.groupby(["fund_code", "report_date"])}
    for u in universe.loc[universe.fund_code.isin(requested)].itertuples(index=False):
        key = (u.fund_code, pd.Timestamp(u.report_date))
        header = {"fund_code": u.fund_code, "valuation_date": u.valuation_date,
                  "report_date": u.report_date}
        a = agroups.get(key, pd.DataFrame())
        if len(a) == 1 and pd.notna(a.iloc[0].stock_weight):
            stock_rows.append({**header, "stock_true": float(a.iloc[0].stock_weight),
                               "label_aum": a.iloc[0].aum,
                               "label_ann_date": str(a.iloc[0].ann_date.date())
                               if pd.notna(a.iloc[0].ann_date) else None})
        else:
            exclusions.append({**header, "label_kind": "quarterly_stock",
                               "reason": "MISSING_OR_AMBIGUOUS_ASSET_LABEL"})
        if key[1].month not in (6, 12):
            continue
        g = hgroups.get(key, pd.DataFrame())
        reason = None
        if g.empty:
            reason = "NO_HOLDINGS_LABEL"
        elif g.security_code.isna().any() or g.security_code.duplicated().any():
            reason = "MISSING_OR_DUPLICATE_SECURITY"
        elif g.weight.isna().any() or (g.weight < 0).any():
            reason = "INVALID_LABEL_WEIGHT"
        elif len(a) != 1 or pd.isna(a.iloc[0].stock_weight):
            reason = "NO_INDEPENDENT_ASSET_CONTROL"
        elif abs(g.weight.sum() - a.iloc[0].stock_weight) > .001:
            reason = "INCOMPLETE_HOLDINGS_TOTAL"
        if reason:
            exclusions.append({**header, "label_kind": "industry", "reason": reason})
            continue
        current = m.loc[(m.in_date <= key[1]) &
                        (m.out_date.isna() | (key[1] < m.out_date)) &
                        m.security_code.isin(g.security_code),
                        ["security_code", "industry"]].drop_duplicates()
        if current.security_code.duplicated().any():
            exclusions.append({**header, "label_kind": "industry", "reason": "AMBIGUOUS_SW_LABEL"})
            continue
        joined = g.merge(current, on="security_code", how="left", validate="one_to_one")
        joined.loc[joined.security_code.str.endswith(".HK"), "industry"] = "hk"
        missing = joined.industry.isna()
        if missing.any():
            exclusions.append({**header, "label_kind": "industry", "reason": "UNMAPPED_SW_LABEL",
                               "missing_weight": float(joined.loc[missing, "weight"].sum()),
                               "securities": ",".join(joined.loc[missing, "security_code"])})
            continue
        weights = joined.groupby("industry").weight.sum()
        industry_rows.append({
            **header, **{s: float(weights.get(s, 0)) for s in SW_CODES},
            "hk_true": float(weights.get("hk", 0)), "stock_true": float(g.weight.sum()),
            "label_aum": float(a.iloc[0].aum) if pd.notna(a.iloc[0].aum) else None,
            "label_ann_verified": bool(g.ann_date.notna().all()),
            "label_ann_date": str(g.ann_date.max().date()) if g.ann_date.notna().all() else None,
            "control_difference": float(g.weight.sum() - a.iloc[0].stock_weight),
        })
    return pd.DataFrame(industry_rows), pd.DataFrame(stock_rows), pd.DataFrame(exclusions)


def nav_sensitivity(bundle):
    frames = dict(bundle.frames)
    nav = dates(bundle["nav"], ("date", "ann_date"))
    next_day = nav.date + pd.Timedelta(days=1)
    changed = nav.ann_date.isna() | (nav.ann_date > next_day)
    nav["original_ann_date"] = nav.ann_date
    # Late report-scale data must not leak through the assumed NAV availability.
    nav.loc[changed, [c for c in ("net_asset", "total_netasset") if c in nav]] = np.nan
    nav.loc[changed, "ann_date"] = next_day[changed]
    nav["nav_announcement_assumed"] = changed
    frames["nav"] = nav
    return DataBundle(frames, {**bundle.provenance, "nav_timing_assumption": True})


def run(first=0, last=None):
    plan = json.loads((OUT / "inventory.json").read_text(encoding="utf8"))
    protocol = json.loads(PROTOCOL.read_text(encoding="utf8"))
    if plan["protocol_hash"] != file_hash(PROTOCOL):
        raise RuntimeError("Test protocol changed after inventory freeze")
    if plan["code_hash"] != code_fingerprint(ROOT):
        raise RuntimeError("Implementation changed after inventory freeze")
    universe = pd.read_parquet(OUT / "universe.parquet")
    settings = Settings.load()
    db = AlphaDB(settings)
    count = len(plan["batches"])
    for i in range(first, min(last if last is not None else count, count)):
        codes = plan["batches"][i]
        destination = OUT / "batches" / f"{i:03d}"
        if (destination / "done.json").exists():
            announce(f"batch {i+1}/{count}: resume completed")
            continue
        started = time.perf_counter()
        source = CACHE / f"{i:03d}"
        announce(f"batch {i+1}/{count}: load {len(codes)} products")
        if (source / "manifest.json").exists():
            bundle = DataBundle.load(source)
        else:
            bundle = db.load(protocol["prediction_start"], protocol["prediction_end"], codes)
            # Keep exactly the requested price/industry universe in this batch.
            held = bundle.frames["holdings"].security_code.unique()
            bundle.frames["membership"] = bundle.frames["membership"].loc[
                bundle.frames["membership"].security_code.isin(held)].copy()
            bundle.save(source)
        destination.mkdir(exist_ok=True, parents=True)
        labels, stock, excluded = independent_labels(bundle, codes, universe)
        atomic_parquet(destination / "industry_labels.parquet", labels)
        atomic_parquet(destination / "stock_labels.parquet", stock)
        atomic_parquet(destination / "label_exclusions.parquet", excluded)
        sensitivity = nav_sensitivity(bundle)
        for mode, data in (("recorded_ann_date", bundle), ("nav_date_assumption", sensitivity)):
            for model in protocol["models"]:
                path = destination / f"{mode}_{model}.parquet"
                if path.exists():
                    continue
                records = []
                recorded = (pd.read_parquet(destination / f"recorded_ann_date_{model}.parquet")
                            if mode == "nav_date_assumption" else None)
                for day in plan["dates"]:
                    target = pd.Timestamp(day)
                    cutoff = target + pd.Timedelta(days=1)
                    reusable = pd.DataFrame()
                    day_data = data
                    original = None
                    if recorded is not None:
                        original = recorded.loc[recorded.valuation_date == day]
                        raw_nav = bundle.frames["nav"]
                        revised_nav = data.frames["nav"]
                        availability = "nav_available_at" if "nav_available_at" in raw_nav else "ann_date"
                        newly_available = ((raw_nav.date <= target)
                            & (raw_nav[availability].isna() | (raw_nav[availability] > cutoff))
                            & (revised_nav[availability] <= cutoff))
                        affected = set(raw_nav.loc[newly_available, "fund_code"])
                        reusable = original.loc[~original.fund_code.isin(affected)].copy()
                        required = original.loc[original.fund_code.isin(affected), "fund_code"]
                        if required.empty:
                            estimates = reusable.copy()
                        else:
                            f = data.frames["funds"]
                            families = f.loc[f.fund_code.isin(required), "master_code"]
                            frames = dict(data.frames)
                            frames["funds"] = f.loc[f.master_code.isin(families) | f.fund_code.isin(required)]
                            day_data = DataBundle(frames, data.provenance)
                            fresh = compute_date(settings.with_model(name=model), day_data, target, cutoff)
                            estimates = pd.concat([reusable, fresh], ignore_index=True)
                        # Restore the recorded run's PIT AUM, which was computed even
                        # when its period-end NAV failed. NAV sensitivity changes only
                        # the NAV observation's timing, never report-scale availability.
                        aum_lookup = original.set_index("fund_code")
                        for col in ("aum", "aum_date", "aum_available_at", "aum_source", "aum_reason"):
                            if col in aum_lookup:
                                estimates[col] = estimates.fund_code.map(aum_lookup[col])
                    else:
                        estimates = compute_date(settings.with_model(name=model), day_data, target, cutoff)
                    if estimates.empty:
                        continue
                    estimates = estimates.loc[estimates.fund_code.isin(codes)].copy()
                    estimates["input_mode"] = mode
                    estimates["test_scope"] = "conditional_diagnostic"
                    records.append(estimates)
                    announce(f"batch {i+1}/{count} {mode} {model} {day}: "
                             f"{estimates.status.value_counts().to_dict()}")
                result = pd.concat(records, ignore_index=True) if records else pd.DataFrame()
                atomic_parquet(path, result)
                announce(f"batch {i+1}/{count} {mode} {model}: "
                         f"{result.status.value_counts().to_dict() if len(result) else {}}")
        atomic_json(destination / "done.json", {
            "input_hash": bundle.fingerprint, "seconds": time.perf_counter()-started,
            "protocol_hash": plan["protocol_hash"], "code_hash": plan["code_hash"],
            "industry_labels": len(labels), "stock_labels": len(stock),
            "harness_hash": file_hash(Path(__file__)),
            "files": {p.name: file_hash(p) for p in sorted(destination.glob("*.parquet"))},
        })
        announce(f"batch {i+1}/{count}: done in {time.perf_counter()-started:.1f}s")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=["inventory", "run"])
    parser.add_argument("--batch-size", type=int, default=80)
    parser.add_argument("--first", type=int, default=0)
    parser.add_argument("--last", type=int)
    args = parser.parse_args()
    if args.phase == "inventory":
        inventory(args.batch_size)
    else:
        run(args.first, args.last)


if __name__ == "__main__":
    main()
