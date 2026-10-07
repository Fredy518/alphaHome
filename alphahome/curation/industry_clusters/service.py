"""Compute snapshots and export strategy-independent cluster artifacts."""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import numpy as np
import pandas as pd

from .data import daily_returns, snapshot_features
from .adaptive import attach_selection_groups
from .engine import ClusterConfig, fit_minimax, fit_stock_overlap
from .maintenance import advance_snapshot, membership_similarity, stable_hash


def save_json(path: Path, value):
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
    if path.suffix == ".gz":
        with gzip.open(path, "wt", encoding="utf-8") as stream:
            stream.write(text + "\n")
    else:
        path.write_text(text + "\n", encoding="utf-8")


def run_history(inputs, universe, start, end, config: ClusterConfig, output: Path,
                universe_id: str, source_description: str, previous=None) -> dict:
    output.mkdir(parents=True, exist_ok=True)
    returns, price_audit = daily_returns(inputs)
    dates = pd.date_range(pd.Timestamp(start), pd.Timestamp(end), freq="ME")
    if not len(dates) or dates[-1] != pd.Timestamp(end):
        raise ValueError("end date must be a calendar month end")
    state = previous["state"] if previous else None
    old_stock, old_unbuffered, old_selection = None, None, None
    snapshots, summaries, coverage, exclusions, control_members, structure_sources = [], [], [], [], [], []
    for date in dates:
        features, rejected, source_detail = snapshot_features(inputs, returns, universe, date, config)
        active = features.subset([c for c in features.codes if features.multiview_ready(c)]) if config.isolate_unready_members else features
        snapshot = advance_snapshot(active, state, universe.index_code.tolist())
        stock_groups = fit_stock_overlap(features)
        unbuffered = fit_minimax(active)
        if config.selection_data_policy == "stock_fallback":
            attach_selection_groups(snapshot, features, stock_groups, rejected)
        summary = {"asof_date": str(date.date()), "universe_indices": len(universe),
                   "structure_ready_indices": len(features.codes),
                   "price_ready_indices": sum(features.ready(c) for c in features.codes),
                   "maintained_clusters": len(snapshot["groups"]),
                   "confirmed_clusters": sum(r["status"] == "confirmed" for r in snapshot["groups"]),
                   "price_pending_clusters": sum(r["status"] == "price_pending" for r in snapshot["groups"]),
                   "degraded_pending_clusters": sum(r["status"] == "degraded_pending" for r in snapshot["groups"]),
                   "indices_using_prior_structure": int(source_detail.used_prior_snapshot.sum()),
                   "stock60_clusters": len(stock_groups), "unbuffered_minimax_clusters": len(unbuffered),
                   "representative_changes": sum(r["event"] == "representative_changed" for r in snapshot["events"]),
                   **snapshot["metrics"]}
        if "selection_groups" in snapshot:
            selection = [g["indices"].split("|") for g in snapshot["selection_groups"]]
            summary.update({
                "selection_groups": len(selection),
                "multiview_selection_groups": sum(g["selection_method"] == "multiview" for g in snapshot["selection_groups"]),
                "stock_fallback_selection_groups": sum(g["selection_method"] == "stock_fallback" for g in snapshot["selection_groups"]),
                "unresolved_selection_groups": sum(g["selection_method"] == "unresolved_singleton" for g in snapshot["selection_groups"]),
                "selection_eligible_indices": sum(r["selection_eligible"] for r in snapshot["members"]),
                "partial_classification_indices": sum(
                    r["known_l2_weight"] is not None and r["known_l2_weight"] < 1 - 1e-6 for r in snapshot["members"]),
            })
            if old_selection is not None:
                summary.update({"selection_" + k: v for k, v in membership_similarity(old_selection, selection).items()})
            old_selection = selection
        for name, groups, prior in (("stock60", stock_groups, old_stock), ("unbuffered_minimax", unbuffered, old_unbuffered)):
            if prior is not None:
                summary.update({name + "_" + k: v for k, v in membership_similarity(prior, groups).items()})
            control_members.extend({"asof_date": str(date.date()), "variant": name, "index_code": c, "cluster_id": g[0]}
                                   for g in groups for c in g)
        snapshot["metrics"] = summary
        snapshots.append(snapshot)
        summaries.append(summary)
        coverage.append(features.coverage)
        exclusions.append(rejected)
        structure_sources.append(source_detail)
        state, old_stock, old_unbuffered = snapshot["state"], stock_groups, unbuffered
        print(json.dumps(summary, ensure_ascii=False), flush=True)
    latest = snapshots[-1]
    names = universe.set_index("index_code").index_name.to_dict()
    member_frame = pd.DataFrame([r for snapshot in snapshots for r in snapshot["members"]])
    member_frame["index_name"] = member_frame.index_code.map(names)
    member_frame["representative_name"] = member_frame.representative_index_code.map(names)
    group_records = [r for snapshot in snapshots for r in snapshot["groups"]]
    group_frame = pd.DataFrame(group_records) if group_records else pd.DataFrame(columns=[
        "asof_date", "cluster_id", "representative_index_code", "index_count", "status", "constraint_mode", "indices"])
    group_frame["representative_name"] = group_frame.representative_index_code.map(names)
    group_frame["index_names"] = group_frame.indices.map(lambda s: "|".join(names.get(c, c) for c in s.split("|")))
    member_frame.to_csv(output / "membership_history.csv.gz", index=False, compression="gzip")
    group_frame.to_csv(output / "group_history.csv.gz", index=False, compression="gzip")
    member_frame.loc[member_frame.asof_date.eq(latest["state"]["asof"])].to_csv(output / "latest_members.csv", index=False, encoding="utf-8-sig")
    group_frame.loc[group_frame.asof_date.eq(latest["state"]["asof"])].to_csv(output / "latest_groups.csv", index=False, encoding="utf-8-sig")
    if config.selection_data_policy == "stock_fallback":
        selection_frame = pd.DataFrame([r for s in snapshots for r in s["selection_groups"]])
        selection_frame["index_names"] = selection_frame.indices.map(lambda s: "|".join(names.get(c, c) for c in s.split("|")))
        selection_frame.to_csv(output / "selection_groups_history.csv.gz", index=False, compression="gzip")
        selection_frame.loc[selection_frame.asof_date.eq(latest["state"]["asof"])].to_csv(
            output / "latest_selection_groups.csv", index=False, encoding="utf-8-sig")
    pd.concat(coverage, ignore_index=True).to_csv(output / "price_coverage.csv.gz", index=False, compression="gzip")
    pd.concat(exclusions, ignore_index=True).to_csv(output / "structure_exclusions.csv", index=False, encoding="utf-8-sig")
    pd.concat(structure_sources, ignore_index=True).to_csv(output / "structure_sources.csv.gz", index=False, compression="gzip")
    pd.DataFrame(summaries).to_csv(output / "monthly_summary.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(control_members).to_csv(output / "control_memberships.csv.gz", index=False, compression="gzip")
    save_json(output / "events.json", [{"asof_date": s["state"]["asof"], **e} for s in snapshots for e in s["events"]])
    save_json(output / "price_calculation.json", price_audit)
    save_json(output / "config.json", config.to_dict())
    universe_rows = universe.where(pd.notna(universe), None).to_dict("records")
    # Hash actual source frames, rather than query time or path names.
    input_hash = inputs["content_hash"]
    payload = {"universe_id": universe_id, "universe": universe_rows,
               "source_description": source_description, "input_hash": input_hash,
               "record_kind": "historical_reconstruction", "input_receipt": inputs["receipt"],
               "input_cache_dir": inputs["cache_dir"],
               "previous_batch_id": previous["batch_id"] if previous else None, "snapshots": snapshots}
    save_json(output / "series.json.gz", payload)
    save_json(output / "latest_state.json", latest["state"])
    quality = group_frame.loc[group_frame.asof_date.eq(latest["state"]["asof"]) & group_frame.status.eq("confirmed")]
    summary = {"universe_id": universe_id, "asof_date": latest["state"]["asof"],
               "months": len(snapshots), "latest": summaries[-1], "price_calculation": price_audit,
               "input_hash": input_hash, "config_hash": stable_hash(config.to_dict()),
               "all_confirmed_representatives_rechecked": True,
               f"worst_confirmed_member_residual_corr_{max(config.windows)}":
                   float(quality[f"worst_member_residual_corr_{max(config.windows)}"].min()) if len(quality) else None}
    save_json(output / "summary.json", summary)
    return summary


def select_projection(candidates: pd.DataFrame, membership: pd.DataFrame, slots: int, *,
                      policy: str = "auto", require_representative: bool = True) -> pd.DataFrame:
    """Strategy adapter: fixed slot budget, one admissible representative per cluster."""
    if slots < 1 or candidates.index_code.duplicated().any() or membership.index_code.duplicated().any():
        raise ValueError("positive slots and unique index rows required")
    required = {"index_code", "used_score", "etf_code"}
    if not required.issubset(candidates):
        raise ValueError("candidates require index_code, used_score and etf_code")
    if policy == "auto":
        policy = "adaptive" if "selection_group_id" in membership and membership.selection_group_id.notna().all() else "strict"
    if policy == "adaptive":
        fields = ["index_code", "selection_group_id", "selection_method", "selection_confidence",
                  "selection_eligible", "can_represent_selection_group", "status"]
        if not set(fields).issubset(membership):
            raise ValueError("adaptive selection requires V2 membership")
        joined = candidates.drop(columns=[c for c in fields if c != "index_code"], errors="ignore").merge(
            membership[fields], on="index_code", how="left", validate="one_to_one")
        if joined.selection_group_id.isna().any():
            raise ValueError("candidate index is outside the cluster mother library")
        joined = joined.loc[joined.selection_eligible.eq(True) & np.isfinite(joined.used_score) & joined.etf_code.notna()]
        if require_representative:
            joined = joined.loc[joined.can_represent_selection_group.eq(True)]
        selected = joined.sort_values(["used_score", "index_code"], ascending=[False, True]).drop_duplicates("selection_group_id").head(slots).copy()
        selected["target_weight"] = 1 / slots
        return selected
    if policy != "strict":
        raise ValueError("selection policy must be auto, adaptive or strict")
    joined = candidates.drop(columns=["cluster_id", "cluster_status", "can_represent_cluster"], errors="ignore").merge(
        membership[["index_code", "cluster_id", "status", "can_represent_cluster"]].rename(columns={"status": "cluster_status"}),
        on="index_code", how="inner", validate="one_to_one")
    joined = joined.loc[joined.cluster_status.eq("confirmed") & joined.can_represent_cluster.eq(True)
                        & joined.used_score.notna() & joined.etf_code.notna()]
    selected = joined.sort_values(["used_score", "index_code"], ascending=[False, True]).drop_duplicates("cluster_id").head(slots).copy()
    selected["target_weight"] = 1 / slots
    # Any unfilled slot stays cash; no hidden increase to the remaining weights.
    return selected
