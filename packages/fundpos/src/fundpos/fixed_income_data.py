from __future__ import annotations

import numpy as np
import pandas as pd

from .pit import dates, require_unique


def recover_report_announcements(target: pd.DataFrame, evidence: pd.DataFrame) -> pd.DataFrame:
    """Recover dates only from the same document or an exact holding match.

    A fund/report-level date alone is deliberately insufficient: it could be
    the publication date of a top-ten equity table rather than the bond table.
    """
    out = dates(target, ("report_date", "ann_date"))
    if out.empty:
        return out
    if "ann_date" not in out:
        out["ann_date"] = pd.NaT
    out["announcement_source"] = np.where(out.ann_date.notna(), "source", "unverified")
    e = dates(evidence, ("report_date", "ann_date"))
    e = e.loc[e.ann_date.notna()].copy()
    if e.empty:
        return out
    keys = ["fund_code", "report_date"]
    source_label = None
    document_key = next(
        (key for key in ("document_hash", "document_id") if key in out and key in e),
        None,
    )
    if document_key:
        keys.append(document_key)
        source_label = "same_formal_document"
    elif {"security_code", "market_value"}.issubset(out) and {
        "security_code",
        "market_value",
    }.issubset(e):
        keys.append("security_code")
        source_label = "same_security_report_and_amount"
    else:
        return out
    merged = out.reset_index(names="_row").merge(
        e[[*keys, "market_value", "ann_date"]]
        if "market_value" in out and "market_value" in e
        else e[[*keys, "ann_date"]],
        on=keys,
        how="left",
        suffixes=("", "_evidence"),
    )
    if source_label == "same_security_report_and_amount":
        scale = merged[["market_value", "market_value_evidence"]].abs().max(axis=1).clip(lower=1.0)
        merged = merged.loc[
            (merged.market_value - merged.market_value_evidence).abs().le(scale * 1e-8)
        ]
    unique = merged.groupby("_row").ann_date_evidence.agg(["min", "max"])
    unique = unique.loc[unique["min"] == unique["max"], ["min"]].rename(
        columns={"min": "report_ann_date"}
    )
    merged = out.reset_index(names="_row").merge(unique, on="_row", how="left")
    fill = merged.ann_date.isna() & merged.report_ann_date.notna()
    merged.loc[fill, "ann_date"] = merged.loc[fill, "report_ann_date"]
    merged.loc[fill, "announcement_source"] = source_label
    return merged.sort_values("_row").drop(columns=["_row", "report_ann_date"]).reset_index(drop=True)


def recover_asset_control_announcements(
    target: pd.DataFrame, financial_reports: pd.DataFrame
) -> pd.DataFrame:
    """Attach the same-period quarterly financial-report announcement to asset control.

    AlphaHome independently verified that the quarterly financial announcement and
    the first portfolio announcement agree for the repaired 2021-09--2024-12
    interval.  This cross-table recovery is retained as conditional evidence; a
    formal report hash, when available, supersedes it later.
    """
    out = dates(target, ("report_date", "ann_date"))
    evidence = dates(financial_reports, ("report_date", "ann_date"))
    if out.empty or evidence.empty:
        return out
    required = {"fund_code", "report_date", "ann_date"}
    if not required.issubset(out) or not required.issubset(evidence):
        return out
    evidence = evidence.loc[evidence.ann_date.notna(), list(required)].copy()
    conflicts = evidence.groupby(["fund_code", "report_date"]).ann_date.nunique()
    if (conflicts > 1).any():
        raise ValueError("Conflicting quarterly financial announcement dates")
    evidence = evidence.drop_duplicates(["fund_code", "report_date"]).rename(
        columns={"ann_date": "financial_report_ann_date"}
    )
    result = out.merge(
        evidence,
        on=["fund_code", "report_date"],
        how="left",
        validate="many_to_one",
    )
    if "announcement_source" not in result:
        result["announcement_source"] = np.where(
            result.ann_date.notna(), "source", "unverified"
        )
    fill = result.ann_date.isna() & result.financial_report_ann_date.notna()
    result.loc[fill, "ann_date"] = result.loc[fill, "financial_report_ann_date"]
    result.loc[fill, "announcement_source"] = (
        "financial_quarterly_same_period_conditional"
    )
    result["announcement_evidence_status"] = np.where(
        result.announcement_source.eq("financial_quarterly_same_period_conditional"),
        "cross_table_verified_not_document_hash",
        "source_or_formal",
    )
    return result


def classify_bond_disclosures(
    assets: pd.DataFrame, bond_allocations: pd.DataFrame
) -> pd.DataFrame:
    """Reconcile bond categories and expose convertible weight without double counting."""
    a = dates(assets, ("report_date", "ann_date"))
    if a.empty:
        return a
    require_unique(a, ["fund_code", "report_date"], "asset allocation control")
    b = dates(bond_allocations, ("report_date", "ann_date"))
    if b.empty:
        return a.assign(convertible_bond_weight=np.nan, ordinary_bond_weight=np.nan,
                        bond_allocation_complete=False, bond_allocation_gap=np.nan)
    invalid = b.weight.isna() | b.weight.lt(0) | b.bond_category.isna()
    b = b.assign(_invalid=invalid,
                 _cbond=np.where(b.bond_category.astype(str).str.contains("可转换|可交换"), b.weight, 0.0))
    totals = b.groupby(["fund_code", "report_date"], as_index=False).agg(
        allocation_bond_weight=("weight", "sum"), convertible_bond_weight=("_cbond", "sum"),
        allocation_invalid=("_invalid", "any"))
    result = a.merge(totals, on=["fund_code", "report_date"], how="left", validate="one_to_one")
    result["bond_allocation_gap"] = result.allocation_bond_weight - result.bond_weight
    result["bond_allocation_complete"] = (
        result.allocation_invalid.eq(False)
        & result.bond_weight.notna()
        & result.bond_allocation_gap.abs().le(0.001)
    )
    result.loc[~result.bond_allocation_complete, "convertible_bond_weight"] = np.nan
    result["ordinary_bond_weight"] = result.bond_weight - result.convertible_bond_weight
    result.loc[result.ordinary_bond_weight.lt(-1e-9), "bond_allocation_complete"] = False
    result.loc[~result.bond_allocation_complete, "ordinary_bond_weight"] = np.nan
    return result.drop(columns=["allocation_invalid"])


def disclosed_financing(asset_reports: pd.DataFrame, fund_code: str, at_date, cutoff, max_age=200):
    a = dates(asset_reports, ("report_date", "ann_date"))
    needed = {"fund_code", "report_date", "ann_date"}
    if a.empty or not needed.issubset(a):
        return None, "NO_ASSET_LIABILITY_CONTROL"
    a = a.loc[(a.fund_code == fund_code) & a.ann_date.notna()
              & a.ann_date.le(pd.Timestamp(cutoff)) & a.report_date.le(pd.Timestamp(at_date))]
    a = a.loc[a.report_date.ge(pd.Timestamp(at_date) - pd.Timedelta(days=max_age))]
    has_ratio = pd.Series(False, index=a.index)
    if "repo_sold_weight" in a:
        has_ratio = a.repo_sold_weight.gt(0)
    has_amount = pd.Series(False, index=a.index)
    if {"repo_sold_value", "aum"}.issubset(a):
        has_amount = a.repo_sold_value.gt(0) & a.aum.gt(0)
    a = a.loc[has_ratio | has_amount]
    if a.empty:
        return None, "NO_EXPLICIT_REPO_FINANCING"
    row = a.sort_values(["report_date", "ann_date"]).iloc[-1]
    value = (
        float(row.repo_sold_weight)
        if pd.notna(row.get("repo_sold_weight")) and row.repo_sold_weight > 0
        else float(row.repo_sold_value / row.aum)
    )
    return value, None


def disclosed_balance_sheet_leverage(
    asset_reports: pd.DataFrame,
    fund_code: str,
    at_date,
    cutoff,
    *,
    max_age=200,
    tolerance=0.005,
):
    """Return last public total-assets/NAV leverage without calling it repo.

    This is an explicit scenario input for assets whose NAV weight can exceed
    one.  It controls the balance-sheet scale, but does not prove the liability
    type or funding rate and therefore cannot make ordinary-bond output formal.
    """
    reports = dates(asset_reports, ("report_date", "ann_date"))
    required = {"fund_code", "report_date", "ann_date", "total_asset_value", "aum"}
    if reports.empty or not required.issubset(reports):
        return None, "NO_TOTAL_ASSET_CONTROL"
    reports = reports.loc[
        reports.fund_code.eq(fund_code)
        & reports.ann_date.notna()
        & reports.ann_date.le(pd.Timestamp(cutoff))
        & reports.report_date.le(pd.Timestamp(at_date))
        & reports.report_date.ge(pd.Timestamp(at_date) - pd.Timedelta(days=max_age))
        & reports.total_asset_value.notna()
        & reports.aum.gt(0)
    ]
    if reports.empty:
        return None, "NO_TOTAL_ASSET_CONTROL"
    report = reports.sort_values(["report_date", "ann_date"]).iloc[-1]
    leverage = float(report.total_asset_value / report.aum - 1)
    if leverage < -tolerance:
        return None, "TOTAL_ASSETS_BELOW_NAV"
    return max(0.0, leverage), None


def apply_report_evidence(frames: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Attach a verified formal-report date to tables from that same report."""
    evidence = dates(frames.get("report_evidence", pd.DataFrame()), ("report_date", "ann_date"))
    if evidence.empty:
        return frames
    required = {"fund_code", "report_date", "ann_date", "document_sha256", "verified"}
    if not required.issubset(evidence):
        raise ValueError(f"report_evidence missing {sorted(required - set(evidence))}")
    evidence = evidence.loc[
        evidence.verified.fillna(False) & evidence.ann_date.notna()
    ].copy()
    if evidence.empty:
        return frames
    conflicts = evidence.groupby(["fund_code", "report_date"]).ann_date.nunique()
    if (conflicts > 1).any():
        raise ValueError("Conflicting first publication dates in report evidence")
    evidence = (
        evidence.sort_values(["fund_code", "report_date", "ann_date"])
        .drop_duplicates(["fund_code", "report_date"], keep="first")
        [["fund_code", "report_date", "ann_date", "document_sha256", "document_path"]]
        .rename(
            columns={
                "ann_date": "formal_report_ann_date",
                "document_sha256": "formal_report_sha256",
                "document_path": "formal_report_path",
            }
        )
    )
    for name in (
        "asset_reports",
        "bond_allocations",
        "holdings",
        "bond_holdings",
        "cbond_holdings",
    ):
        frame = dates(frames.get(name, pd.DataFrame()), ("report_date", "ann_date"))
        if frame.empty or not {"fund_code", "report_date"}.issubset(frame):
            continue
        frame = frame.merge(evidence, on=["fund_code", "report_date"], how="left")
        if "announcement_source" not in frame:
            frame["announcement_source"] = np.where(
                frame.ann_date.notna(), "source", "unverified"
            )
        frame["announcement_conflict"] = (
            frame.ann_date.notna()
            & frame.formal_report_ann_date.notna()
            & frame.ann_date.ne(frame.formal_report_ann_date)
        )
        matches = (
            frame.ann_date.notna()
            & frame.formal_report_ann_date.notna()
            & frame.ann_date.eq(frame.formal_report_ann_date)
        )
        frame.loc[matches, "announcement_source"] = "formal_report_document_hash"
        if "announcement_evidence_status" in frame:
            frame.loc[matches, "announcement_evidence_status"] = "formal_document_hash"
        fill = frame.ann_date.isna() & frame.formal_report_ann_date.notna()
        frame.loc[fill, "ann_date"] = frame.loc[fill, "formal_report_ann_date"]
        frame.loc[fill, "announcement_source"] = "formal_report_document_hash"
        if "announcement_evidence_status" in frame:
            frame.loc[fill, "announcement_evidence_status"] = "formal_document_hash"
        frames[name] = frame
    return frames
