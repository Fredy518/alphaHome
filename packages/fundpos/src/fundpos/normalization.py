"""Normalize disclosed amounts and keep field-specific availability intact."""

from __future__ import annotations

import numpy as np
import pandas as pd

from .constants import SW_INDUSTRIES
from .errors import DataUnavailable
from .pit import dates, require_unique

REPORT_KEYS = ["fund_code", "report_date"]


def normalize_holdings(holdings: pd.DataFrame, assets: pd.DataFrame) -> pd.DataFrame:
    """Use exact disclosed money, never renormalize a missing position to a total.

    The stock amount is an independent control. Displayed individual percentages
    remain available for audit and are not added up to certify completeness.
    """
    h = dates(holdings, ("report_date", "ann_date"))
    if h.empty:
        return h.assign(full_report_verified=pd.Series(dtype=bool))
    a = dates(assets, ("report_date", "ann_date"))
    required = REPORT_KEYS + ["aum", "stock_market_value", "stock_weight"]
    if not set(required).issubset(a):
        raise DataUnavailable("NO_MONETARY_ASSET_CONTROL", ", ".join(required))
    require_unique(a, REPORT_KEYS, "monetary asset report")
    if "weight_disclosed" not in h:
        h["weight_disclosed"] = h["weight"]
    h = h.drop(columns=["full_report_verified", "report_net_asset", "control_stock_value",
                        "control_stock_weight", "weight_basis", "monetary_control_gap",
                        "monetary_control_pass", "weight_control_gap"], errors="ignore")
    control = a[required].rename(columns={"aum": "report_net_asset",
        "stock_market_value": "control_stock_value", "stock_weight": "control_stock_weight"})
    h = h.merge(control, on=REPORT_KEYS, how="left", validate="many_to_one")
    h["weight"] = h.market_value / h.report_net_asset.where(h.report_net_asset > 0)
    h["weight_basis"] = "security_market_value_over_report_product_NAV"
    invalid = (~np.isfinite(h.weight) | h.weight.lt(0) | h.weight_disclosed.isna()
               | h.weight_disclosed.lt(0) | h.security_code.isna()
               | h.duplicated(REPORT_KEYS + ["security_code"], keep=False))
    h["_invalid"] = invalid
    totals = h.groupby(REPORT_KEYS, as_index=False).agg(
        amount=("market_value", "sum"), exact_weight=("weight", "sum"),
        invalid=("_invalid", "any"), control_amount=("control_stock_value", "first"),
        control_weight=("control_stock_weight", "first"))
    totals["monetary_control_gap"] = totals.amount - totals.control_amount
    totals["weight_control_gap"] = totals.exact_weight - totals.control_weight
    totals["monetary_control_pass"] = (
        ~totals.invalid & totals.control_amount.notna() & totals.control_weight.notna()
        & totals.monetary_control_gap.abs().le(np.maximum(1., totals.control_amount.abs() * 1e-8))
        & totals.weight_control_gap.abs().le(1e-6))
    totals["full_report_verified"] = (totals.monetary_control_pass
                                          & totals.report_date.dt.month.isin([6, 12]))
    return h.drop(columns="_invalid").merge(totals[REPORT_KEYS + ["monetary_control_gap",
        "weight_control_gap", "monetary_control_pass", "full_report_verified"]],
        on=REPORT_KEYS, how="left", validate="many_to_one")


def separate_nav_availability(nav: pd.DataFrame) -> pd.DataFrame:
    n = dates(nav, ("date", "ann_date", "nav_available_at", "aum_available_at"))
    if "nav_available_at" not in n:
        n["nav_available_at"] = n.ann_date
        n["nav_availability_source"] = "vendor_ann_date_unverified_daily_semantics"
    if "aum_available_at" not in n:
        n["aum_available_at"] = n.ann_date
    return n


def conditional_next_day_nav_availability(
    nav: pd.DataFrame, *, suspicious_delay_days: int = 7
) -> pd.DataFrame:
    """Create an explicitly conditional daily-NAV availability scenario.

    Tushare's ``ann_date`` can switch to the periodic-report announcement on a
    quarter end even though ``adj_nav`` is a daily series.  Strict history keeps
    that vendor date.  Conditional development/selection may instead assume
    next-calendar-day availability only for those conspicuously delayed rows;
    the original announcement and AUM availability are preserved.
    """
    n = separate_nav_availability(nav).copy()
    delayed = (
        n.ann_date.notna()
        & n.date.notna()
        & (n.ann_date - n.date).dt.days.gt(suspicious_delay_days)
    )
    n.loc[delayed, "nav_available_at"] = n.loc[delayed, "date"] + pd.Timedelta(days=1)
    n.loc[delayed, "nav_availability_source"] = (
        "conditional_next_calendar_day_for_vendor_periodic_ann_date"
    )
    n["nav_availability_conditional"] = delayed
    return n


def historical_membership_fallback(source: pd.DataFrame) -> pd.DataFrame:
    """Normalize dated SW change events. Never extrapolate a future first entry.

    Only rows whose industry belongs to SW2021 are usable, starting at the
    locally evidenced SW2021 transition boundary. Primary membership takes
    precedence in map_membership; this table only fills uncovered intervals.
    """
    columns = ["security_code", "industry", "in_date", "out_date", "source", "source_in_date"]
    if source.empty:
        return pd.DataFrame(columns=columns)
    f = dates(source, ("in_date",)).copy()
    if "industry_source" in f:
        f = f.loc[f.industry_source.eq("SWHY")]
    names = {v: k for k, v in SW_INDUSTRIES.items()}
    f["industry"] = f.industry_l1.str.removeprefix("申万").map(names)
    f = f.loc[f.industry.notna() & f.in_date.notna()].copy()
    f = f[["security_code", "industry", "in_date"]].drop_duplicates()
    require_unique(f, ["security_code", "in_date"], "historical SW change event")
    f = f.sort_values(["security_code", "in_date"])
    f["out_date"] = f.groupby("security_code").in_date.shift(-1)
    f["source_in_date"] = f.in_date
    boundary = pd.Timestamp("2021-12-13")
    f["in_date"] = f.in_date.clip(lower=boundary)
    f = f.loc[f.out_date.isna() | f.in_date.lt(f.out_date)].copy()
    f["source"] = "rawdata.stock_industry_versioned:SWHY_change_event"
    return f[columns].reset_index(drop=True)


def aum_at_date(nav, financial, share_codes, valuation_date, cutoff, max_age=200):
    """Sum complete share families at one common report date, using later availability."""
    parts = []
    if not nav.empty and "net_asset" in nav:
        n = separate_nav_availability(nav)
        n = n.loc[n.net_asset.gt(0), ["fund_code", "date", "aum_available_at", "net_asset"]]
        n = n.rename(columns={"date": "report_date", "aum_available_at": "available_at"})
        n["source"] = "fund_nav"
        if not n.empty:
            parts.append(n)
    if not financial.empty:
        f = dates(financial, ("report_date", "ann_date"))
        f = f.loc[f.net_asset.gt(0), REPORT_KEYS + ["ann_date", "net_asset"]]
        f = f.rename(columns={"ann_date": "available_at"})
        f["source"] = "fund_financial_quarterly_ext"
        if not f.empty:
            parts.append(f)
    empty = {"aum": None, "aum_date": None, "aum_available_at": None,
             "aum_source": None, "aum_reason": "NO_COMPLETE_SHARE_AUM"}
    if not parts:
        return empty
    combined = pd.concat(parts, ignore_index=True)
    at, cut = pd.Timestamp(valuation_date), pd.Timestamp(cutoff)
    # Filter availability BEFORE resolving versions, so future rows cannot alter history.
    combined = combined.loc[combined.fund_code.isin(share_codes) & combined.available_at.notna()
        & combined.available_at.le(cut) & combined.report_date.le(at)
        & combined.report_date.ge(at - pd.Timedelta(days=max_age))]
    if combined.empty:
        return empty
    grouped = combined.groupby(REPORT_KEYS, as_index=False).agg(
        low=("net_asset", "min"), high=("net_asset", "max"),
        available_at=("available_at", "max"),
        source=("source", lambda s: "+".join(sorted(set(s)))))
    grouped = grouped.loc[np.isclose(grouped.low, grouped.high, atol=1., rtol=1e-8)]
    counts = grouped.groupby("report_date").fund_code.nunique()
    complete = counts[counts == len(set(share_codes))]
    if complete.empty:
        return empty
    day = complete.index.max()
    selected = grouped.loc[grouped.report_date == day]
    return {"aum": float(selected.high.sum()), "aum_date": str(day.date()),
            "aum_available_at": str(selected.available_at.max().date()),
            "aum_source": ";".join(sorted(set(selected.source))), "aum_reason": None}
