from __future__ import annotations

import re

import numpy as np
import pandas as pd

from .constants import CATEGORIES, DISCLOSURE_GROUPS
from .errors import DataUnavailable


def dates(frame: pd.DataFrame, columns: tuple[str, ...]) -> pd.DataFrame:
    out = frame.copy()
    for name in columns:
        if name in out:
            out[name] = pd.to_datetime(out[name], errors="coerce").dt.normalize()
    return out


def require_unique(frame: pd.DataFrame, keys: list[str], context: str) -> None:
    if frame.duplicated(keys).any():
        raise DataUnavailable("DUPLICATE_KEYS", f"{context}: {keys}")


def security_code(value):
    """Normalize explicit exchange identifiers only; never infer an exchange from a name."""
    if pd.isna(value):
        return None
    text = str(value).strip().upper()
    if re.fullmatch(r"HK\d{1,5}", text):
        return text[2:].zfill(5) + ".HK"
    if re.fullmatch(r"\d{1,5}\.HK", text):
        return text[:-3].zfill(5) + ".HK"
    if re.fullmatch(r"(SH|SZ|BJ)\d{6}", text):
        return text[2:] + "." + text[:2]
    return text


def available(frame: pd.DataFrame, cutoff, column: str = "ann_date") -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    if column not in frame:
        return frame.iloc[:0].copy()
    values = pd.to_datetime(frame[column], errors="coerce")
    return frame.loc[values.notna() & (values <= pd.Timestamp(cutoff))].copy()


def available_nav(frame: pd.DataFrame, cutoff) -> pd.DataFrame:
    return available(frame, cutoff, "nav_available_at" if "nav_available_at" in frame else "ann_date")


def recover_holding_announcements(holdings: pd.DataFrame, disclosed: pd.DataFrame) -> pd.DataFrame:
    """Recover dates per security and matching value, never at the report-group minimum."""
    out = holdings.copy().reset_index(drop=True)
    if "security_code_raw" in out:
        out["security_code"] = out.security_code.fillna(out.security_code_raw)
    out["security_code"] = out.security_code.map(security_code)
    if "ann_date" not in out:
        out["ann_date"] = pd.NaT
    out = dates(out, ("ann_date", "report_date"))
    out["announcement_source"] = np.where(out.ann_date.notna(), "source", "unverified")
    if out.empty or disclosed.empty:
        return out
    keys = ["fund_code", "report_date", "security_code"]
    right = dates(disclosed, ("ann_date", "report_date"))
    right["security_code"] = right.security_code.map(security_code)
    matched = out.reset_index(names="row_id").merge(right, on=keys, suffixes=("", "_reference"))
    if matched.empty:
        return out
    matching_value = np.isclose(
        pd.to_numeric(matched.market_value, errors="coerce"),
        pd.to_numeric(matched.market_value_reference, errors="coerce"),
        rtol=1e-5,
        atol=1.0,
        equal_nan=False,
    )
    valid = matched.loc[matching_value & matched.ann_date_reference.notna()]
    recovered = valid.groupby("row_id").ann_date_reference.min()
    needs_date = out.ann_date.isna() & out.index.isin(recovered.index)
    out.loc[needs_date, "ann_date"] = out.index.to_series().map(recovered)[needs_date]
    out.loc[needs_date, "announcement_source"] = "fund_portfolio_security_value_match"
    return out


def map_membership(holdings: pd.DataFrame, membership: pd.DataFrame, at_date,
                   *, fallback: pd.DataFrame | None = None, allow_unknown=False) -> pd.DataFrame:
    """An out_date is the first excluded date in the normalized contract."""
    if holdings.empty:
        return holdings.assign(industry=pd.Series(dtype=str))
    m = dates(membership, ("in_date", "out_date"))
    m = m.loc[m.security_code.isin(holdings.security_code)].copy()
    at = pd.Timestamp(at_date)
    m = m.loc[m.in_date.notna() & (m.in_date <= at) & (m.out_date.isna() | (at < m.out_date))]
    m = m[["security_code", "industry"]].drop_duplicates()
    require_unique(m, ["security_code"], "industry membership at cutoff")
    out = holdings.drop(columns=["industry"], errors="ignore").merge(
        m, on="security_code", how="left", validate="many_to_one"
    )
    out["industry_source"] = "primary_membership"
    if fallback is not None and not fallback.empty and out.industry.isna().any():
        f = dates(fallback, ("in_date", "out_date"))
        f = f.loc[f.in_date.notna() & (f.in_date <= at)
                  & (f.out_date.isna() | (at < f.out_date))]
        f = f[["security_code", "industry"]].drop_duplicates()
        require_unique(f, ["security_code"], "fallback SW membership at cutoff")
        values = out.security_code.map(f.set_index("security_code").industry)
        restored = out.industry.isna() & values.notna()
        out.loc[restored, "industry"] = values[restored]
        out.loc[restored, "industry_source"] = "historical_SWHY_gap_fill"
    out.loc[out.security_code.str.endswith(".HK"), "industry"] = "hk"
    if allow_unknown:
        a_share = out.security_code.str.fullmatch(r"\d{6}\.(SH|SZ|BJ)", na=False)
        unknown = out.industry.isna() & a_share
        out.loc[unknown, "industry"] = "unknown_a"
        out.loc[unknown, "industry_source"] = "unclassified_A_equity"
    if out.industry.isna().any():
        missing = out.loc[out.industry.isna(), "security_code"].unique()
        raise DataUnavailable("UNMAPPED_HOLDINGS", ", ".join(missing[:8]))
    return out


def eligible_universe(
    funds: pd.DataFrame, classification: pd.DataFrame, at_date, min_age=180, *, categories=CATEGORIES
) -> pd.DataFrame:
    at = pd.Timestamp(at_date)
    f = dates(funds, ("found_date", "liquidation_date"))
    f = f.loc[
        f.found_date.notna()
        & (f.found_date <= at)
        & (f.liquidation_date.isna() | (at < f.liquidation_date))
    ].copy()
    m = dates(classification, ("in_date", "out_date"))
    if not m.empty:
        m = m.loc[
            m.category.isin(categories)
            & m.in_date.notna()
            & (m.in_date <= at)
            & (m.out_date.isna() | (at < m.out_date))
        ]
        m = m[["fund_code", "category"]].drop_duplicates()
        conflicts = m.groupby("fund_code").category.nunique()
        if (conflicts > 1).any():
            raise DataUnavailable(
                "AMBIGUOUS_FUND_CLASSIFICATION", str(conflicts[conflicts > 1].index[:5].tolist())
            )
        f = f.drop(columns=["category"], errors="ignore").merge(
            m, on="fund_code", how="inner", validate="one_to_one"
        )
    else:
        raise DataUnavailable(
            "NO_HISTORICAL_UNIVERSE", "Historical classification membership is required"
        )
    f["master_code"] = f.master_code.fillna(f.fund_code)
    f["family_metadata_missing"] = ~f.master_code.isin(funds.fund_code)
    old_enough = f.groupby("master_code").found_date.transform("min") <= at - pd.Timedelta(
        days=min_age
    )
    f = f.loc[old_enough].copy()
    # A master is chosen by a verified share-family mapping, never a guessed name match.
    f["is_master"] = f.fund_code == f.master_code
    f = f.sort_values(
        ["master_code", "is_master", "found_date", "fund_code"], ascending=[True, False, True, True]
    )
    siblings = f.groupby("master_code").fund_code.agg(list).to_dict()
    f = f.drop_duplicates("master_code").copy()
    f["share_codes"] = f.master_code.map(siblings)
    return f.drop(columns="is_master").reset_index(drop=True)


def disclosure_group(name: str) -> str | None:
    text = re.sub(r"\s+", "", str(name))
    if text in DISCLOSURE_GROUPS:
        return DISCLOSURE_GROUPS[text]
    if re.fullmatch(r"[A-S]", text):
        return text
    return None


def last_full_holdings(holdings: pd.DataFrame, fund_code: str, cutoff) -> pd.DataFrame:
    all_fund = dates(holdings.loc[holdings.fund_code == fund_code], ("report_date", "ann_date"))
    for report_date, report in sorted(all_fund.groupby("report_date"), reverse=True):
        if report_date.month not in (6, 12):
            continue
        # Completeness is a source/control statement, not inferred from "more than 10 rows".
        if (
            "full_report_verified" not in report
            or not report.full_report_verified.fillna(False).all()
        ):
            continue
        if report.ann_date.isna().any() or report.ann_date.max() > pd.Timestamp(cutoff):
            continue
        require_unique(report, ["security_code"], "full holdings")
        return report.copy()
    raise DataUnavailable(
        "NO_VERIFIED_FULL_HOLDINGS", f"{fund_code}: no complete published holdings at {cutoff}"
    )
