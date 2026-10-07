"""Select using confirmed views where possible and dated stock groups elsewhere."""
from __future__ import annotations

from .engine import FeatureSet
from .maintenance import stable_hash


def attach_selection_groups(snapshot: dict, features: FeatureSet, stock_groups, exclusions):
    """Create a disjoint selection partition without deleting data-poor members.

    Confirmed groups are indivisible. Only old stock components containing an
    unconfirmed member trigger fallback unions. Their transitive closure with
    confirmed blocks prevents an index/cluster from occupying multiple slots.
    A fallback union is explicitly not a multi-view representative guarantee.
    """
    rows = {r["index_code"]: r for r in snapshot["members"]}
    reasons = exclusions.set_index("index_code").reason.to_dict()
    parent = {code: code for code in rows}

    def root(code):
        while parent[code] != code:
            parent[code] = parent[parent[code]]
            code = parent[code]
        return code

    def union(group):
        if not group:
            return
        leader = min(root(c) for c in group)
        for code in group:
            parent[root(code)] = leader

    for code, row in rows.items():
        structural = code in features.position
        if structural:
            i = features.position[code]
            row.update(structure_ready=True, price_ready=features.ready(code),
                       known_l1_weight=float(features.l1_overlap[i, i]),
                       known_l2_weight=float(features.l2_overlap[i, i]))
            if not features.ready(code):
                row["status"] = "price_pending"
            elif not features.multiview_ready(code):
                row["status"] = "classification_pending"
        else:
            row.update(structure_ready=False, known_l1_weight=None, known_l2_weight=None)
        row["data_reason"] = reasons.get(code, "")
        # These are object/time inconsistencies, not simply missing features.
        row["selection_eligible"] = not any(
            reason in reasons.get(code, "").split("|")
            for reason in ("not_yet_launched", "non_a_share_constituents", "future_source_date")
        )
    trusted = {}
    for code, row in rows.items():
        if row["status"] == "confirmed":
            trusted.setdefault(row["cluster_id"], []).append(code)
    for group in trusted.values():
        union(group)
    fallback_codes = set()
    for group in stock_groups:
        if any(rows[c]["status"] != "confirmed" for c in group):
            union(group)
            fallback_codes.update(group)
    components = {}
    for code in sorted(rows):
        components.setdefault(root(code), []).append(code)
    selection_groups = []
    for group in components.values():
        fallback = bool(set(group) & fallback_codes)
        if fallback:
            mode = "stock_fallback"
        elif all(rows[c]["status"] == "confirmed" for c in group):
            mode = "multiview"
        else:
            mode = "unresolved_singleton"
        group_id = "SG_" + stable_hash([features.config.version, mode, group])[:16]
        for code in group:
            row = rows[code]
            row.update(selection_group_id=group_id, selection_method=mode,
                       can_represent_selection_group=bool(row["selection_eligible"] and
                           (mode != "multiview" or row["can_represent_cluster"])),
                       selection_confidence="confirmed" if mode == "multiview" else "fallback")
        selection_groups.append({
            "asof_date": features.asof, "selection_group_id": group_id,
            "selection_method": mode, "index_count": len(group), "indices": "|".join(group),
            "price_ready_indices": sum(rows[c]["price_ready"] for c in group),
            "qualified_representatives": sum(rows[c]["can_represent_selection_group"] for c in group),
            "partial_classification_indices": sum(
                rows[c]["known_l2_weight"] is not None and rows[c]["known_l2_weight"] < 1 - 1e-6 for c in group),
        })
    assert all(r["selection_method"] != "multiview" or r["status"] == "confirmed" for r in rows.values())
    assert sum(g["index_count"] for g in selection_groups) == len(rows)
    snapshot["selection_groups"] = selection_groups
    # The maintained state's active groups refer only to sufficient-data members;
    # member-level status below records why inactive members need fallback.
    snapshot["state"]["member_data_status"] = {c: r["status"] for c, r in rows.items()}
    return snapshot
