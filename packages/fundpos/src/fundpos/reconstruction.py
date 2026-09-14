from __future__ import annotations

import numpy as np
import pandas as pd

from .errors import DataUnavailable
from .pit import require_unique


def reconstruct_portfolio(
    previous: pd.DataFrame,
    heavy: pd.DataFrame,
    allocation: pd.DataFrame,
    proxy_bridge: pd.DataFrame,
    tolerance: float = 1e-6,
) -> pd.DataFrame:
    """Reconcile each disclosed group. All weights are fractions of fund NAV.

    Frames: previous/heavy [security_code, group, industry, weight, issuer_code];
    allocation [group, weight]; bridge [group, industry, weight] summing to one/group.
    The caller must select only rows public at its cutoff and reclassify prior stocks.
    """
    require_unique(heavy, ["security_code"], "current heavy holdings")
    require_unique(previous, ["security_code"], "prior holdings")
    require_unique(allocation, ["group"], "disclosed allocation")
    for frame in (previous, heavy, allocation, proxy_bridge):
        if not frame.empty and (frame.weight.isna().any() or (frame.weight < -tolerance).any()):
            raise DataUnavailable("INVALID_HOLDING_WEIGHT", "Negative/missing weight")
    if set(heavy.group) - set(allocation.group):
        raise DataUnavailable("MISSING_DISCLOSED_GROUP", "Heavy holdings have no group total")
    issuer = heavy.get("issuer_code", heavy.security_code).fillna(heavy.security_code)
    # A+H shares of the same issuer must be summed before the tenth-issuer cap.
    issuer_weights = heavy.assign(issuer_code=issuer).groupby("issuer_code").weight.sum()
    cap = float(issuer_weights.min()) if len(issuer_weights) >= 10 else None
    result = heavy.copy().assign(is_proxy=False)
    rows = result.to_dict("records")
    for group, total in allocation[["group", "weight"]].itertuples(index=False, name=None):
        current = heavy.loc[heavy.group == group]
        residual = float(total - current.weight.sum())
        if residual < -tolerance:
            raise DataUnavailable("HEAVY_EXCEEDS_DISCLOSURE", str(group))
        residual = max(0.0, residual)
        nonheavy = previous.loc[
            (previous.group == group) & ~previous.security_code.isin(heavy.security_code)
        ].copy()
        if "issuer_code" in nonheavy:
            nonheavy = nonheavy.loc[~nonheavy.issuer_code.isin(issuer_weights.index)]
        proxy_residual = residual
        if residual > tolerance and not nonheavy.empty and nonheavy.weight.sum() > 0:
            scaled = nonheavy.weight / nonheavy.weight.sum() * residual
            # If fewer than ten issuers were disclosed, no tenth-position cap is known.
            if cap is None:
                allocated = scaled
            else:
                prior_issuer = nonheavy.get("issuer_code", nonheavy.security_code).fillna(
                    nonheavy.security_code
                )
                issuer_total = scaled.groupby(prior_issuer).transform("sum")
                allocated = scaled * (cap / issuer_total).clip(upper=1)
            for (_, record), weight in zip(nonheavy.iterrows(), allocated, strict=True):
                if weight <= tolerance:
                    continue
                item = record.to_dict()
                item.update(weight=float(weight), is_proxy=bool(record.get("is_proxy", False)))
                rows.append(item)
            proxy_residual -= float(allocated.sum())
        if proxy_residual > tolerance:
            bridge = proxy_bridge.loc[proxy_bridge.group == group]
            if bridge.empty or not np.isclose(bridge.weight.sum(), 1.0, atol=tolerance):
                raise DataUnavailable(
                    "NO_PROXY_BRIDGE", f"Unallocated NAV weight {proxy_residual:.6f} in {group}"
                )
            for industry, share in bridge[["industry", "weight"]].itertuples(
                index=False, name=None
            ):
                rows.append(
                    {
                        "security_code": f"PROXY:{group}:{industry}",
                        "group": group,
                        "industry": industry,
                        "weight": float(proxy_residual * share),
                        "is_proxy": True,
                    }
                )
    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=["security_code", "group", "industry", "weight", "is_proxy"])
    check = out.groupby("group").weight.sum().reindex(allocation.group, fill_value=0).to_numpy()
    if not np.allclose(check, allocation.weight.to_numpy(), atol=tolerance, rtol=0):
        raise DataUnavailable("RECONSTRUCTION_NOT_CONSERVED", "Group totals changed")
    if out.industry.isna().any():
        raise DataUnavailable("UNMAPPED_PROXY", "A reconstructed asset has no SW/HK exposure")
    return out.reset_index(drop=True)


def market_cap_bridge(stock_groups: pd.DataFrame) -> pd.DataFrame:
    """Rows must use as-of membership and same-date free-float market value."""
    require_unique(stock_groups, ["security_code"], "industry crosswalk constituents")
    if (
        stock_groups[["group", "industry", "float_mv"]].isna().any().any()
        or (stock_groups.float_mv <= 0).any()
    ):
        raise DataUnavailable(
            "INCOMPLETE_MARKET_CAP_BRIDGE",
            "Declared proxy universe contains missing classifications or nonpositive float market values",
        )
    valid = stock_groups
    weights = valid.groupby(["group", "industry"], as_index=False).float_mv.sum()
    weights["weight"] = weights.float_mv / weights.groupby("group").float_mv.transform("sum")
    return weights[["group", "industry", "weight"]]
