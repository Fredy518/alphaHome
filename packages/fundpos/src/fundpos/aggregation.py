from __future__ import annotations

import pandas as pd

from .constants import ASSETS, SW_CODES
from .errors import DataUnavailable


def aggregate_estimates(
    results: pd.DataFrame, *, count_threshold=0.95, aum_threshold=0.95
) -> pd.DataFrame:
    if results.empty:
        return pd.DataFrame()
    if results.duplicated(["master_code", "valuation_date"]).any():
        raise DataUnavailable("DUPLICATE_MASTER", "Share classes would be double counted")
    rows = []
    for valuation_date, date_frame in results.groupby("valuation_date"):
        groups = [("全部主动权益", date_frame), *list(date_frame.groupby("category"))]
        for category, frame in groups:
            valid = frame.loc[frame.status == "ok"]
            known_aum = pd.to_numeric(frame.aum, errors="coerce")
            aum_known_count = int((known_aum > 0).sum())
            denominator = float(known_aum.where(known_aum > 0, 0).sum())
            valid_aum = pd.to_numeric(valid.aum, errors="coerce").where(lambda x: x > 0, 0)
            count_cov = len(valid) / len(frame)
            # An unknown denominator must not look like 100% AUM coverage.
            aum_cov = (
                float(valid_aum.sum() / denominator)
                if denominator > 0 and aum_known_count == len(frame)
                else None
            )
            complete = (
                count_cov >= count_threshold and aum_cov is not None and aum_cov >= aum_threshold
            )
            for method in ("equal", "aum"):
                row = {
                    "valuation_date": valuation_date,
                    "category": category,
                    "weighting": method,
                    "status": "complete" if complete else "partial",
                    "universe_count": len(frame),
                    "valid_count": len(valid),
                    "count_coverage": count_cov,
                    "aum_coverage": aum_cov,
                    "aum_known_count": aum_known_count,
                    "known_aum": denominator,
                }
                if valid.empty or (method == "aum" and valid_aum.sum() <= 0):
                    row.update({asset: None for asset in ASSETS})
                else:
                    w = (
                        pd.Series(1 / len(valid), index=valid.index)
                        if method == "equal"
                        else valid_aum / valid_aum.sum()
                    )
                    row.update(
                        {asset: float((pd.to_numeric(valid[asset]) * w).sum()) for asset in ASSETS}
                    )
                rows.append(row)
    return pd.DataFrame(rows)


def aggregate_exposures(
    results: pd.DataFrame,
    assets: tuple[str, ...],
    *,
    all_label="全部产品",
    count_threshold=0.95,
    aum_threshold=0.95,
) -> pd.DataFrame:
    """Aggregate any explicit exposure schema without treating unavailable values as zero."""
    if results.empty:
        return pd.DataFrame()
    if results.duplicated(["master_code", "valuation_date"]).any():
        raise DataUnavailable("DUPLICATE_MASTER", "Share classes would be double counted")
    rows = []
    for day, date_frame in results.groupby("valuation_date"):
        for category, frame in [(all_label, date_frame), *list(date_frame.groupby("category"))]:
            known = pd.to_numeric(frame.aum, errors="coerce")
            complete_aum = known.notna() & known.gt(0)
            denominator = float(known.loc[complete_aum].sum())
            usable_by_asset = {}
            for asset in assets:
                mask = frame[asset].notna() & frame.status.ne("unavailable")
                quality_column = (
                    "stock_quality"
                    if asset in {"hk", *SW_CODES}
                    else (
                        "cbond_quality"
                        if asset == "convertible_bond"
                        else (
                            "ordinary_bond_quality"
                            if asset in {"cash", "ordinary_bond", "financing"}
                            else None
                        )
                    )
                )
                if quality_column and quality_column in frame:
                    mask &= frame[quality_column].isin(["estimated", "ok"])
                else:
                    mask &= frame.status.eq("ok")
                usable_by_asset[asset] = frame.loc[mask]
            overall = pd.Series(True, index=frame.index)
            for asset in assets:
                overall &= frame.index.isin(usable_by_asset[asset].index)
            valid = frame.loc[overall]
            count_coverage = len(valid) / len(frame)
            aum_coverage = (
                float(pd.to_numeric(valid.aum, errors="coerce").sum() / denominator)
                if complete_aum.all() and denominator > 0
                else None
            )
            complete = (
                count_coverage >= count_threshold
                and aum_coverage is not None
                and aum_coverage >= aum_threshold
            )
            for method in ("equal", "aum"):
                row = {"valuation_date": day, "category": category, "weighting": method,
                       "status": "complete" if complete else "partial", "universe_count": len(frame),
                       "valid_count": len(valid), "count_coverage": count_coverage,
                       "aum_coverage": aum_coverage, "aum_known_count": int(complete_aum.sum()),
                       "known_aum": denominator}
                for asset in assets:
                    usable = usable_by_asset[asset]
                    component_count_coverage = len(usable) / len(frame)
                    component_aum_coverage = (
                        float(pd.to_numeric(usable.aum, errors="coerce").sum() / denominator)
                        if complete_aum.all() and denominator > 0
                        else None
                    )
                    component_complete = (
                        component_count_coverage >= count_threshold
                        and component_aum_coverage is not None
                        and component_aum_coverage >= aum_threshold
                    )
                    row[f"{asset}_valid_count"] = len(usable)
                    row[f"{asset}_count_coverage"] = component_count_coverage
                    row[f"{asset}_aum_coverage"] = component_aum_coverage
                    row[f"{asset}_status"] = "complete" if component_complete else "partial"
                    if usable.empty or (
                        method == "aum"
                        and pd.to_numeric(usable.aum, errors="coerce").sum() <= 0
                    ):
                        row[asset] = None
                    else:
                        weight = (
                            pd.Series(1 / len(usable), index=usable.index)
                            if method == "equal"
                            else pd.to_numeric(usable.aum, errors="coerce")
                            / pd.to_numeric(usable.aum, errors="coerce").sum()
                        )
                        row[asset] = float((usable[asset] * weight).sum())
                rows.append(row)
    return pd.DataFrame(rows)


def custom_portfolio(
    results: pd.DataFrame,
    allocation: pd.DataFrame,
    valuation_date,
    *,
    assets: tuple[str, ...] = ASSETS,
) -> pd.DataFrame:
    if (
        allocation.duplicated("master_code").any()
        or allocation.weight.isna().any()
        or (allocation.weight < 0).any()
    ):
        raise DataUnavailable(
            "INVALID_CUSTOM_PORTFOLIO", "Unique nonnegative master weights required"
        )
    if abs(allocation.weight.sum() - 1) > 1e-6:
        raise DataUnavailable("INVALID_CUSTOM_PORTFOLIO", "Weights must sum to 1")
    subset = results.loc[results.valuation_date == str(pd.Timestamp(valuation_date).date())]
    merged = allocation.merge(subset, on="master_code", how="left", validate="one_to_one")
    if merged.status.isna().any() or not (merged.status == "ok").all():
        raise DataUnavailable(
            "CUSTOM_PORTFOLIO_INCOMPLETE",
            "All components need same-date valid estimates; weights are not renormalized",
        )
    return pd.DataFrame(
        [
            {
                "valuation_date": str(pd.Timestamp(valuation_date).date()),
                **{a: float((merged[a] * merged.weight).sum()) for a in assets},
            }
        ]
    )
