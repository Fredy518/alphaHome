"""Independent error arithmetic, coverage accounting and full-test artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from fundpos.constants import ASSETS, SW_CODES
from fundpos.storage import atomic_json, atomic_parquet, file_hash

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "outputs/full_test"
KEYS = ["fund_code", "valuation_date"]
ACCEPTED = ["ok", "degraded"]


def attach_turnover_proxy(errors, proxy):
    """Use an earlier report only, explicitly as an ex-post descriptive field."""
    right = proxy.loc[proxy.turnover_proxy.notna(), ["fund_code", "report_date", "turnover_proxy"]].copy()
    right = right.rename(columns={"report_date": "turnover_report_date"})
    right["turnover_report_date"] = pd.to_datetime(right.turnover_report_date)
    if right.duplicated(["fund_code", "turnover_report_date"]).any():
        raise ValueError("Ambiguous historical turnover proxy")
    left = errors.copy()
    left["_turnover_date"] = pd.to_datetime(left.valuation_date)
    left["_original_order"] = np.arange(len(left))
    result = pd.merge_asof(
        left.sort_values("_turnover_date"), right.sort_values("turnover_report_date"),
        left_on="_turnover_date", right_on="turnover_report_date", by="fund_code",
        allow_exact_matches=False, direction="backward", tolerance=pd.Timedelta(days=400),
    ).sort_values("_original_order").drop(columns=["_turnover_date", "_original_order"])
    result["turnover_proxy_group"] = pd.cut(
        result.turnover_proxy, [-np.inf, 1, 3, np.inf],
        labels=["1倍以内", "1至3倍", "3倍以上"],
    ).astype(object)
    return result


def matched_errors(predictions, labels):
    p = predictions.loc[predictions.status.isin(ACCEPTED)].copy()
    if p.duplicated(KEYS).any() or labels.duplicated(KEYS).any():
        raise ValueError("Duplicate fund/report keys")
    result = labels.merge(p, on=KEYS, how="inner", suffixes=("_true", "_estimated"),
                          validate="one_to_one")
    actual = result[[f"{s}_true" for s in SW_CODES]].to_numpy(dtype=float)
    estimated = result[[f"{s}_estimated" for s in SW_CODES]].to_numpy(dtype=float)
    if not np.isfinite(actual).all() or not np.isfinite(estimated).all():
        raise ValueError("Missing truth or estimate cannot be filled with zero")
    delta = estimated - actual
    result["industry_l1"] = np.abs(delta).sum(axis=1)
    result["per_industry_mae"] = result.industry_l1 / len(SW_CODES)
    result["signed_industry_total_error"] = delta.sum(axis=1)
    result["stock_error"] = abs(result.stock_weight - result.stock_true)
    result["hk_error"] = abs(result.hk - result.hk_true)
    result["a_stock_true"] = actual.sum(axis=1)
    result["a_stock_estimated"] = estimated.sum(axis=1)
    # Additional equity-normalized metric; never substitute it for NAV exposure L1.
    normalized = np.full(len(result), np.nan)
    valid = (actual.sum(axis=1) > 1e-8) & (estimated.sum(axis=1) > 1e-8)
    normalized[valid] = np.abs(
        actual[valid]/actual[valid].sum(axis=1)[:, None]
        - estimated[valid]/estimated[valid].sum(axis=1)[:, None]
    ).sum(axis=1)
    result["equity_normalized_industry_l1"] = normalized
    result["meets_20pp"] = result.industry_l1 <= .2 + 1e-12
    return result


def statistics(frame):
    if frame.empty:
        return {"count": 0}
    return {
        "count": len(frame), "unique_products": frame.fund_code.nunique(),
        "industry_l1_mean": float(frame.industry_l1.mean()),
        "industry_l1_median": float(frame.industry_l1.median()),
        "industry_l1_p90": float(frame.industry_l1.quantile(.9)),
        "industry_l1_p95": float(frame.industry_l1.quantile(.95)),
        "per_industry_mae": float(frame.per_industry_mae.mean()),
        "fraction_le_20pp": float(frame.meets_20pp.mean()),
        "stock_mae": float(frame.stock_error.mean()),
        "hk_mae": float(frame.hk_error.mean()),
        "label_ann_verified_count": int(frame.label_ann_verified.sum()),
        "equity_normalized_industry_l1_mean": float(frame.equity_normalized_industry_l1.mean())
        if frame.equity_normalized_industry_l1.notna().any() else None,
    }


def grouped_bias(frame, weight_column=None, equity_normalized=False):
    if equity_normalized:
        frame = frame.loc[(frame.a_stock_true > 1e-8) & (frame.a_stock_estimated > 1e-8)]
    if frame.empty:
        return {"count": 0, "status": "no_nonzero_equity"}
    if weight_column:
        frame = frame.loc[frame[weight_column].notna() & (frame[weight_column] > 0)]
        if frame.empty:
            return {"count": 0, "status": "missing_weight"}
        w = frame[weight_column].to_numpy(dtype=float)
    else:
        w = np.ones(len(frame))
    w /= w.sum()
    actual = frame[[f"{s}_true" for s in SW_CODES]].to_numpy()
    estimated = frame[[f"{s}_estimated" for s in SW_CODES]].to_numpy()
    if equity_normalized:
        actual = actual / actual.sum(axis=1)[:, None]
        estimated = estimated / estimated.sum(axis=1)[:, None]
    delta = estimated - actual
    bias = w @ delta
    return {
        "count": len(frame), "industry_l1": float(abs(bias).sum()),
        "industry_denominator": "A_share_equity" if equity_normalized else "fund_NAV",
        "stock_absolute_bias": float(abs(w @ (frame.stock_weight-frame.stock_true))),
        "hk_absolute_bias": float(abs(w @ (frame.hk-frame.hk_true))),
        "signed_industry_bias": dict(zip(SW_CODES, bias.tolist())),
    }


def summarize(require_complete=True):
    plan = json.loads((OUT / "inventory.json").read_text(encoding="utf8"))
    all_batches = [OUT / "batches" / f"{i:03d}" for i in range(len(plan["batches"]))]
    complete = [d for d in all_batches if (d / "done.json").exists()]
    if require_complete and len(complete) != len(all_batches):
        raise RuntimeError(f"Only {len(complete)}/{len(all_batches)} batches completed")
    if not complete:
        raise RuntimeError("No completed batches")
    for directory in complete:
        done = json.loads((directory / "done.json").read_text(encoding="utf8"))
        if done["code_hash"] != plan["code_hash"] or done["protocol_hash"] != plan["protocol_hash"]:
            raise RuntimeError("Mixed implementation or protocol versions")
        for name, expected in done["files"].items():
            if file_hash(directory / name) != expected:
                raise RuntimeError(f"Changed result file: {directory.name}/{name}")
    def collect(filename):
        frames = [pd.read_parquet(d / filename) for d in complete]
        return pd.concat([f for f in frames if not f.empty], ignore_index=True)
    universe = pd.read_parquet(OUT / "universe.parquet")
    labels = collect("industry_labels.parquet")
    stocks = collect("stock_labels.parquet")
    exclusions = collect("label_exclusions.parquet")
    if labels.duplicated(KEYS).any() or stocks.duplicated(KEYS).any():
        raise ValueError("Duplicate label rows across batches")
    industry_universe = universe.loc[pd.to_datetime(universe.report_date).dt.month.isin([6, 12])]
    summary = {
        "status": "completed_conditional_diagnostic" if len(complete) == len(all_batches) else "running",
        "formal_acceptance": "NOT_PASSED", "final_holdout_opened": False,
        "completed_batches": len(complete), "expected_batches": len(all_batches),
        "universe_products": plan["unique_products"], "quarterly_universe_pairs": len(universe),
        "industry_universe_pairs": len(industry_universe), "industry_labels": len(labels),
        "stock_labels": len(stocks),
        "label_exclusions": exclusions.groupby(["label_kind", "reason"]).size().reset_index(name="count").to_dict("records"),
        "code_hash": plan["code_hash"], "protocol_hash": plan["protocol_hash"],
        "modes": {}, "checks": {},
    }
    checks, all_errors, all_stock_errors, coverage_rows, failure_rows = [], [], [], [], []
    group_rows, period_rows = [], []
    turnover = pd.read_parquet(OUT / "turnover_proxy.parquet")
    for mode in ("recorded_ann_date", "nav_date_assumption"):
        predictions = {m: collect(f"{mode}_{m}.parquet") for m in ("index", "personalized")}
        for model, p in predictions.items():
            if p.duplicated(KEYS).any():
                raise ValueError("Duplicate predictions across batches")
            valid = p.loc[p.status.isin(ACCEPTED)]
            bad = p.loc[p.status == "unavailable"]
            max_error = float(valid.constraint_error.max())
            checks.append({"mode": mode, "model": model, "max_constraint_error": max_error,
                           "all_34_weights_finite": bool(np.isfinite(valid[list(ASSETS)].to_numpy(dtype=float)).all()),
                           "all_34_weights_sum_to_one": bool(np.allclose(valid[list(ASSETS)].sum(axis=1),1,atol=1e-6,rtol=0)),
                           "unavailable_weights_all_missing": bool(bad[list(ASSETS)].isna().all().all()),
                           "all_windows_60": bool((valid.window_observations == 60).all()),
                           "no_false_window_end": bool((valid.window_end == valid.valuation_date).all()),
                           "row_count": len(p)})
            if (max_error > 1e-6 or not all(checks[-1][k] for k in (
                    "all_34_weights_finite", "all_34_weights_sum_to_one", "unavailable_weights_all_missing",
                    "all_windows_60", "no_false_window_end"))):
                raise AssertionError(checks[-1])
            if require_complete and set(zip(p.fund_code, p.valuation_date)) != set(zip(universe.fund_code, universe.valuation_date)):
                raise AssertionError("Not every expected product/report has a result status")
            failures = p.loc[p.status == "unavailable"].copy()
            failures["input_mode"], failures["model"] = mode, model
            failure_rows.append(failures)
            atomic_parquet(OUT / f"{mode}_{model}_predictions.parquet", p)
        common = set(zip(labels.fund_code, labels.valuation_date))
        for p in predictions.values():
            valid = p.loc[p.status.isin(ACCEPTED)]
            common &= set(zip(valid.fund_code, valid.valuation_date))
        common_labels = labels.loc[[k in common for k in zip(labels.fund_code, labels.valuation_date)]]
        mode_result = {"common_industry_samples": len(common_labels), "models": {}}
        for model, p in predictions.items():
            errors = matched_errors(p, common_labels)
            errors = attach_turnover_proxy(errors, turnover)
            errors["input_mode"], errors["model"] = mode, model
            all_errors.append(errors)
            mode_result["models"][model] = statistics(errors)
            individual = matched_errors(p, labels)
            mode_result["models"][model]["individual_label_match_count"] = len(individual)
            valid = p.loc[p.status.isin(ACCEPTED)]
            # Same stock-label sample for both baselines, independent from industry-label completeness.
            stock_common = stocks.copy()
            for other in predictions.values():
                ok = other.loc[other.status.isin(ACCEPTED), KEYS]
                stock_common = stock_common.merge(ok, on=KEYS, validate="one_to_one")
            stock_errors = stock_common.merge(valid, on=KEYS, validate="one_to_one")
            stock_errors["stock_error"] = abs(stock_errors.stock_weight-stock_errors.stock_true)
            stock_errors["input_mode"], stock_errors["model"] = mode, model
            all_stock_errors.append(stock_errors)
            mode_result["models"][model]["quarterly_stock_count"] = len(stock_errors)
            mode_result["models"][model]["quarterly_stock_mae"] = float(stock_errors.stock_error.mean())
            for day, f in errors.groupby("valuation_date"):
                period_rows.append({"input_mode": mode, "model": model, "valuation_date": day, **statistics(f)})
                for method, col in (("equal", None), ("known_aum", "aum"), ("report_aum_expost", "label_aum")):
                    group_rows.append({"input_mode": mode, "model": model, "valuation_date": day,
                                       "dimension": "portfolio_bias", "group": method, **grouped_bias(f, col)})
                f = f.copy()
                f["actual_A_stock_value"] = f.label_aum * f.a_stock_true
                for scope, subset in (("all", f), ("zero_disclosed_HK", f.loc[f.hk_true == 0])):
                    for method, col in (("equal", None), ("actual_stock_value_expost", "actual_A_stock_value")):
                        group_rows.append({"input_mode": mode, "model": model, "valuation_date": day,
                                           "dimension": "reference_equity_normalized_group",
                                           "group": f"{scope}:{method}", **grouped_bias(subset, col, True)})
            errors["size_group"] = pd.cut(errors.aum, [0,1e9,5e9,np.inf], labels=["10亿以下","10至50亿","50亿以上"]).astype(object)
            errors["holdings_age_group"] = pd.cut(errors.get("holdings_age_days", pd.Series(np.nan,index=errors.index)),
                                                     [0,200,300,np.inf], labels=["200天以内","201至300天","300天以上"]).astype(object)
            for dim in ("category", "size_group", "holdings_age_group", "turnover_proxy_group"):
                for name, f in errors.groupby(dim, dropna=False):
                    group_rows.append({"input_mode": mode,"model": model,"dimension": dim,
                                       "group": str(name) if pd.notna(name) else "未知", **statistics(f)})
            for day, u in universe.groupby("valuation_date"):
                pday = p.loc[p.valuation_date == day]
                ready = pday.status.isin(ACCEPTED)
                labels_day = labels.loc[labels.valuation_date == day]
                e = errors.loc[errors.valuation_date == day]
                sizes = pday.aum
                known = sizes.notna() & (sizes > 0)
                common_size = pday.fund_code.isin(e.fund_code) & known
                coverage_rows.append({
                    "input_mode": mode, "model": model, "valuation_date": day,
                    "universe_count": len(u), "estimated_count": int(ready.sum()),
                    "strict_ok_count": int((pday.status == "ok").sum()),
                    "estimated_count_coverage": float(ready.sum()/len(u)),
                    "industry_labels": len(labels_day), "common_industry_count": len(e),
                    "common_industry_count_coverage": float(len(e)/len(u)) if len(labels_day) else None,
                    "known_aum_count": int(known.sum()),
                    "known_aum_fraction": float(known.sum()/len(u)),
                    "estimated_aum_coverage_within_known": float(sizes[ready & known].sum()/sizes[known].sum()) if known.any() else None,
                    "industry_aum_coverage_within_known": float(sizes[common_size].sum()/sizes[known].sum()) if len(labels_day) and known.any() else None,
                    "aum_coverage_is_full_denominator": bool(known.sum() == len(u)),
                })
        summary["modes"][mode] = mode_result
    # Quantify sensitivity: earlier released NAVs can legitimately change a valid window.
    unchanged = []
    for model in ("index", "personalized"):
        strict = pd.read_parquet(OUT / f"recorded_ann_date_{model}_predictions.parquet")
        assumed = pd.read_parquet(OUT / f"nav_date_assumption_{model}_predictions.parquet")
        paired = strict.loc[strict.status.isin(ACCEPTED)].merge(
            assumed.loc[assumed.status.isin(ACCEPTED)], on=KEYS, suffixes=("_recorded", "_assumed"))
        diff = np.max(np.abs(paired[[f"{a}_recorded" for a in ASSETS]].to_numpy()
                            - paired[[f"{a}_assumed" for a in ASSETS]].to_numpy()), axis=1)
        unchanged.append({"model": model, "paired_count": len(paired),
                          "max_weight_difference": float(diff.max()),
                          "note": "Earlier quarterly NAV gaps may change regression intervals even when the target NAV was available"})
    summary["checks"] = {"numeric": checks, "nav_timing_sensitivity": unchanged,
                         "turnover_dimension": "Earlier report annualized proxy, 400-day maximum age; ex-post descriptive only, announcement dates unverified"}
    atomic_parquet(OUT / "industry_labels.parquet", labels)
    atomic_parquet(OUT / "stock_labels.parquet", stocks)
    atomic_parquet(OUT / "label_exclusions.parquet", exclusions)
    atomic_parquet(OUT / "industry_errors.parquet", pd.concat(all_errors, ignore_index=True))
    atomic_parquet(OUT / "quarterly_stock_errors.parquet", pd.concat(all_stock_errors, ignore_index=True))
    atomic_parquet(OUT / "coverage.parquet", pd.DataFrame(coverage_rows))
    atomic_parquet(OUT / "failures.parquet", pd.concat(failure_rows, ignore_index=True))
    atomic_parquet(OUT / "period_metrics.parquet", pd.DataFrame(period_rows))
    atomic_json(OUT / "group_metrics.json", group_rows)
    summary["failure_counts"] = pd.concat(failure_rows).groupby(["input_mode","model","reason"]).size().reset_index(name="count").to_dict("records")
    atomic_json(OUT / "summary.json", summary)
    print({k:v for k,v in summary.items() if k not in ("failure_counts", "checks", "label_exclusions")}, flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--partial", action="store_true")
    args = p.parse_args()
    summarize(require_complete=not args.partial)
