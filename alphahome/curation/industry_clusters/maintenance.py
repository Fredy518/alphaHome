"""Quarterly merging, monthly anomaly checks and persistent cluster identities."""

from __future__ import annotations

from hashlib import sha256
import json

import pandas as pd

from .engine import ClusterConfig, FeatureSet, fit_minimax


def stable_hash(value) -> str:
    return sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                             separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _new_id(asof, members, version):
    return "IC_" + stable_hash([version, asof, sorted(members)])[:16]


def _signature(members):
    return "|".join(sorted(members))


def _pairs(groups, common):
    return {tuple(sorted((a, b))) for g in groups for i, a in enumerate(sorted(set(g) & common))
            for b in sorted(set(g) & common)[:i]}


def membership_similarity(before, after) -> dict:
    left = set(c for g in before for c in g)
    right = set(c for g in after for c in g)
    common = left & right
    a, b = _pairs(before, common), _pairs(after, common)
    return {"common_indices": len(common), "changed_same_cluster_pairs": len(a ^ b),
            "same_cluster_pair_jaccard": len(a & b) / len(a | b) if a | b else 1.0,
            "before_same_cluster_pairs": len(a), "after_same_cluster_pairs": len(b)}


def advance_snapshot(features: FeatureSet, previous: dict | None = None,
                     universe: list[str] | None = None, *, maintain: bool = True) -> dict:
    """Advance once per calendar month. A repeated month cannot accrue confirmations."""
    cfg, asof = features.config, features.asof
    current_codes = set(features.codes)
    universe = sorted(universe or features.codes)
    if not current_codes.issubset(universe):
        raise ValueError("feature codes outside the persistent universe")
    if previous:
        if stable_hash(ClusterConfig.from_dict(previous["config"]).to_dict()) != stable_hash(cfg.to_dict()):
            raise ValueError("configuration change requires a new series/bootstrap")
        if pd.Period(previous["asof"], freq="M") >= pd.Period(asof, freq="M"):
            raise ValueError("snapshot must advance to a later month")
    consecutive = bool(previous and pd.Period(previous["asof"], freq="M") + 1 == pd.Period(asof, freq="M"))
    old = previous["clusters"] if previous else {}
    old_active = {key: tuple(c for c in row["members"] if c in current_codes) for key, row in old.items()}
    old_active = {key: g for key, g in old_active.items() if g}
    old_assignment = {c: key for key, row in old.items() for c in row["members"]}
    events, failures = [], {}
    prior_failures = previous.get("failures", {}) if previous and consecutive else {}
    prior_merges = previous.get("merge_confirmations", {}) if previous and consecutive else {}
    review = pd.Timestamp(asof).month in cfg.review_months
    merge_confirmations = {}

    if not previous or not maintain:
        groups = fit_minimax(features)
    else:
        groups = []
        for key, group in sorted(old_active.items()):
            result = features.evaluate(group, "retention", old[key]["representative"])
            old_weights = previous.get("weights", {})
            major = [c for c in group if c in old_weights and
                     sum(min(w, old_weights[c].get(s, 0)) for s, w in features.weights[c].items()) < cfg.major_constituent_overlap]
            fail_count = 0 if result["valid"] or not result["known"] else prior_failures.get(key, 0) + 1
            if fail_count:
                failures[key] = fail_count
            split = not result["semantic_ok"] or bool(major) or fail_count >= cfg.confirmation_months
            if split:
                pieces = fit_minimax(features.subset(group))
                groups.extend(pieces)
                events.append({"event": "reassess_group", "previous_cluster": key,
                               "reason": "economic_boundary" if not result["semantic_ok"] else "major_constituent_change" if major else "confirmed_similarity_breach",
                               "affected_indices": list(group), "major_change_indices": major,
                               "resulting_groups": [list(p) for p in pieces]})
                failures.pop(key, None)
            else:
                groups.append(group)
                if fail_count:
                    events.append({"event": "pending_similarity_breach", "previous_cluster": key,
                                   "confirmation_count": fail_count, "affected_indices": list(group)})
        accounted = set(c for g in groups for c in g)
        groups.extend((c,) for c in sorted(current_codes - accounted))
        proposals = fit_minimax(features, groups)
        committed = []
        current_groups = set(groups)
        for proposal in proposals:
            if proposal in current_groups:
                committed.append(proposal)
                continue
            signature = _signature(proposal)
            count = prior_merges.get(signature, 0) + 1
            merge_confirmations[signature] = count
            if review and count >= cfg.confirmation_months:
                committed.append(proposal)
                events.append({"event": "confirmed_merge", "affected_indices": list(proposal), "confirmation_count": count})
            else:
                parts = [g for g in groups if set(g).issubset(proposal)]
                committed.extend(parts)
                events.append({"event": "pending_merge", "affected_indices": list(proposal),
                               "confirmation_count": count, "quarterly_review": review})
        groups = sorted(committed)

    # One previous identity can follow only one new group. Largest overlap wins.
    candidates = []
    for i, group in enumerate(groups):
        for key, prior_group in old_active.items():
            intersection = len(set(group) & set(prior_group))
            if intersection:
                candidates.append((-intersection, -intersection / len(set(group) | set(prior_group)),
                                   -int(old[key]["representative"] in group), key, i))
    assigned, used = {}, set()
    for _, _, _, key, i in sorted(candidates):
        if i not in assigned and key not in used:
            assigned[i] = key
            used.add(key)
    clusters, member_rows, group_rows = {}, [], []
    for i, group in enumerate(groups):
        key = assigned.get(i, _new_id(asof, group, cfg.version))
        parents = sorted({old_assignment[c] for c in group if c in old_assignment})
        prior_rep = old.get(key, {}).get("representative")
        unchanged_group = key in old_active and tuple(sorted(old_active[key])) == tuple(group)
        constraint_mode = "retention" if previous and unchanged_group else "entry"
        assessment = features.evaluate(group, constraint_mode)
        best = assessment["representative"]
        preferred = features.evaluate(group, constraint_mode, prior_rep)
        representative = best
        if prior_rep in assessment["feasible_representatives"]:
            # Preserve a feasible representative until a material improvement at review.
            old_radius = preferred["radius"]
            if not review or best is None or old_radius - assessment["radius"] < cfg.representative_improvement:
                representative = prior_rep
        if representative is None:
            representative = prior_rep if prior_rep in group else group[0]
        status = "confirmed" if assessment["valid"] else "price_pending" if not assessment["known"] else "degraded_pending"
        if prior_rep and representative != prior_rep:
            events.append({"event": "representative_changed", "cluster_id": key,
                           "before": prior_rep, "after": representative})
        quality = features.quality(group, representative)
        clusters[key] = {"members": list(group), "representative": representative,
                         "status": status, "parents": parents}
        group_rows.append({"asof_date": asof, "cluster_id": key, "representative_index_code": representative,
                           "index_count": len(group), "status": status, "constraint_mode": constraint_mode,
                           "indices": "|".join(group),
                           **quality})
        for code in group:
            member_rows.append({"asof_date": asof, "index_code": code, "cluster_id": key,
                                "representative_index_code": representative, "status": status,
                                "price_ready": features.ready(code),
                                "can_represent_cluster": bool(status == "confirmed" and code in assessment["feasible_representatives"]),
                                "is_central_representative": code == representative})
    active_assignment = {c: key for key, row in clusters.items() for c in row["members"]}
    for code in sorted(set(universe) - current_codes):
        old_key = old_assignment.get(code)
        # An inactive member follows the identity carrying most of its old group.
        key = old_key if old_key in clusters else None
        if not key and old_key:
            destinations = [active_assignment[c] for c in old[old_key]["members"] if c in active_assignment]
            if destinations:
                key = min(set(destinations), key=lambda x: (-destinations.count(x), x))
        key = key or old_key or _new_id(asof, [code], cfg.version)
        if key not in clusters:
            clusters[key] = {"members": [], "representative": code, "status": "structure_pending", "parents": []}
        clusters[key]["members"].append(code)
        member_rows.append({"asof_date": asof, "index_code": code, "cluster_id": key,
                            "representative_index_code": clusters[key]["representative"],
                            "status": "structure_pending", "price_ready": False,
                            "can_represent_cluster": False, "is_central_representative": False})
    for key, row in clusters.items():
        row["members"] = sorted(row["members"])
    prior_active = set(previous.get("active_codes", old_assignment)) if previous else set()
    metrics = membership_similarity(
        [tuple(c for c in row["members"] if c in prior_active) for row in old.values()], groups
    ) if previous else {}
    state = {"asof": asof, "config": cfg.to_dict(), "clusters": clusters,
             "weights": {**(previous.get("weights", {}) if previous else {}), **features.weights},
             "failures": failures, "merge_confirmations": merge_confirmations,
             "active_codes": sorted(current_codes), "maintain": maintain}
    if len(member_rows) != len(universe) or len({r["index_code"] for r in member_rows}) != len(universe):
        raise ValueError("membership must cover the persistent universe exactly once")
    for row in group_rows:
        if row["status"] == "confirmed":
            members = row["indices"].split("|")
            if not features.evaluate(members, row["constraint_mode"], row["representative_index_code"])["valid"]:
                raise ValueError("published representative failed retention constraints")
    return {"state": state, "members": member_rows, "groups": group_rows,
            "events": events, "metrics": metrics}
