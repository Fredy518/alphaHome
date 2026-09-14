from __future__ import annotations

import pandas as pd

from .constants import SW_CODES
from .data import DataBundle
from .errors import DataUnavailable
from .pit import available, dates, map_membership, require_unique


def enhanced_index_prerequisites(
    bundle: DataBundle, fund_codes: set[str], start, end, *, allow_conditional=False
) -> list[dict]:
    """Audit input evidence only; do not touch validation truth labels."""
    history = dates(bundle["tracking_index_history"], ("effective_date", "ann_date"))
    members = dates(bundle["index_membership"], ("as_of_date", "ann_date"))
    if history.empty:
        return [
            {
                "code": "NO_HISTORICAL_TRACKING_INDEX",
                "required_funds": len(fund_codes),
                "covered_funds": 0,
            },
            {"code": "NO_HISTORICAL_INDEX_COMPONENTS", "covered_indices": 0},
        ]
    start = pd.Timestamp(start)
    end = pd.Timestamp(end)
    usable = history.loc[
        history.fund_code.isin(fund_codes)
        & history.ann_date.notna()
        & history.effective_date.notna()
        & history.effective_date.le(end)
        & history.ann_date.le(end)
    ]
    covered = set(usable.fund_code)
    gaps = []
    if covered != fund_codes:
        gaps.append(
            {
                "code": "INCOMPLETE_HISTORICAL_TRACKING_INDEX",
                "required_funds": len(fund_codes),
                "covered_funds": len(covered),
            }
        )
    strict = usable.get("strict_pit", pd.Series(True, index=usable.index)).fillna(False)
    strict_covered = set(usable.loc[strict, "fund_code"])
    if strict_covered != fund_codes:
        gaps.append(
            {
                "code": "CONDITIONAL_HISTORICAL_TRACKING_INDEX",
                "required_funds": len(fund_codes),
                "strict_funds": len(strict_covered),
                "conditional_allowed": bool(allow_conditional),
            }
        )
    needed_indices = set(usable.index_code)
    component_indices = (
        set(
            members.loc[
                members.index_code.isin(needed_indices)
                & members.as_of_date.between(start, end)
                & members.ann_date.notna()
                & members.ann_date.le(end),
                "index_code",
            ]
        )
        if not members.empty
        else set()
    )
    if component_indices != needed_indices:
        gaps.append(
            {
                "code": "INCOMPLETE_HISTORICAL_INDEX_COMPONENTS",
                "required_indices": len(needed_indices),
                "covered_indices": len(component_indices),
            }
        )
    return gaps


def tracking_index_industry_prior(
    bundle: DataBundle, fund_code: str, valuation_date, information_cutoff
) -> tuple[pd.Series, dict]:
    """Build a dated benchmark-industry comparator without using today's index."""
    target = pd.Timestamp(valuation_date)
    cutoff = pd.Timestamp(information_cutoff)
    history = dates(bundle["tracking_index_history"], ("effective_date", "ann_date"))
    required_history = {"fund_code", "index_code", "effective_date", "ann_date"}
    if history.empty or not required_history.issubset(history):
        raise DataUnavailable(
            "NO_HISTORICAL_TRACKING_INDEX",
            "Current fund metadata cannot be backfilled into history",
        )
    history = available(history.loc[history.fund_code.eq(fund_code)], cutoff)
    history = history.loc[history.effective_date.le(target)]
    if history.empty:
        raise DataUnavailable("NO_HISTORICAL_TRACKING_INDEX", fund_code)
    mapping = history.sort_values(["effective_date", "ann_date"]).iloc[-1]

    members = dates(bundle["index_membership"], ("as_of_date", "ann_date"))
    required_members = {"index_code", "security_code", "weight", "as_of_date", "ann_date"}
    if members.empty or not required_members.issubset(members):
        raise DataUnavailable("NO_HISTORICAL_INDEX_COMPONENTS", str(mapping.index_code))
    members = available(members.loc[members.index_code.eq(mapping.index_code)], cutoff)
    members = members.loc[members.as_of_date.le(target)]
    if members.empty:
        raise DataUnavailable("NO_HISTORICAL_INDEX_COMPONENTS", str(mapping.index_code))
    component_date = members.as_of_date.max()
    members = members.loc[members.as_of_date.eq(component_date)].copy()
    require_unique(members, ["security_code"], "historical index components")
    if members.weight.isna().any() or members.weight.lt(0).any():
        raise DataUnavailable("INVALID_INDEX_COMPONENTS", "Weights must be nonnegative")
    total = float(members.weight.sum())
    if not 0.99 <= total <= 1.01:
        raise DataUnavailable(
            "INVALID_INDEX_COMPONENTS", f"Decimal component weights sum to {total}"
        )
    members["weight"] = members.weight / total
    classified = map_membership(
        members[["security_code", "weight"]],
        bundle["membership"],
        target,
        fallback=bundle["membership_fallback"],
    )
    industry = classified.groupby("industry").weight.sum().reindex(SW_CODES, fill_value=0.0)
    return industry, {
        "tracking_index_code": str(mapping.index_code),
        "tracking_index_effective_date": str(mapping.effective_date.date()),
        "tracking_component_date": str(component_date.date()),
        "tracking_index_source": mapping.get("source"),
        "tracking_index_evidence_status": mapping.get("evidence_status", "strict_dated"),
        "tracking_index_strict_pit": bool(mapping.get("strict_pit", True)),
        "tracking_component_source": members.source.iloc[0] if "source" in members else None,
        "tracking_index_prior": industry.to_dict(),
    }


def evaluate_tracking_index_prior(
    predictions: pd.DataFrame, labels: pd.DataFrame
) -> tuple[dict, pd.DataFrame]:
    """Compare benchmark and disclosed A-share industry mixes on equity-normalized weights."""
    empty = pd.DataFrame(
        columns=[
            "fund_code",
            "valuation_date",
            "tracking_index_code",
            "equity_normalized_industry_l1",
        ]
    )
    if predictions.empty or labels.empty or "tracking_index_prior" not in predictions:
        return {"status": "blocked_data", "count": 0}, empty
    usable = predictions.loc[
        predictions.tracking_index_status.isin(
            ["available_point_in_time", "available_conditional"]
        )
        & predictions.tracking_index_prior.notna()
    ]
    joined = labels.merge(
        usable[["fund_code", "valuation_date", "tracking_index_code", "tracking_index_prior"]],
        on=["fund_code", "valuation_date"],
        validate="one_to_one",
    )
    errors = []
    for row in joined.to_dict("records"):
        actual = pd.Series({code: row[code] for code in SW_CODES}, dtype=float)
        if actual.sum() <= 0:
            continue
        actual /= actual.sum()
        prior_value = row["tracking_index_prior"]
        if isinstance(prior_value, str):
            import json

            prior_value = json.loads(prior_value)
        prior = pd.Series(prior_value, dtype=float).reindex(SW_CODES, fill_value=0.0)
        errors.append(
            {
                "fund_code": row["fund_code"],
                "valuation_date": row["valuation_date"],
                "tracking_index_code": row["tracking_index_code"],
                "equity_normalized_industry_l1": float(abs(actual - prior).sum()),
            }
        )
    frame = pd.DataFrame(errors)
    if frame.empty:
        return {"status": "blocked_data", "count": 0}, empty
    return {
        "status": "evaluated",
        "count": len(frame),
        "strict_count": int(
            usable.tracking_index_status.eq("available_point_in_time").sum()
        ),
        "conditional_count": int(
            usable.tracking_index_status.eq("available_conditional").sum()
        ),
        "equity_normalized_industry_l1_mean": float(
            frame.equity_normalized_industry_l1.mean()
        ),
        "equity_normalized_industry_l1_p90": float(
            frame.equity_normalized_industry_l1.quantile(.9)
        ),
    }, frame
