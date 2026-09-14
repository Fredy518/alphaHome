from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from .constants import FIXED_INCOME_PLUS_CATEGORIES
from .pit import available, dates, eligible_universe, require_unique
from .storage import file_hash


def load_frozen_pilot(root: Path, universe_config: dict) -> pd.DataFrame:
    path = root / universe_config["pilot_file"]
    expected = universe_config["pilot_sha256"]
    if not path.exists() or file_hash(path) != expected:
        raise ValueError("Frozen v3 pilot file is missing or changed")
    pilot = pd.read_parquet(path)
    if len(pilot) != 110 or not pilot.master_code.is_unique:
        raise ValueError("Frozen v3 pilot must contain 110 unique products")
    return pilot


def _latest_contract(constraints: pd.DataFrame, fund_code: str, at, cutoff):
    if constraints.empty:
        return None
    c = dates(constraints, ("ann_date", "effective_date"))
    c = available(c.loc[c.fund_code == fund_code], cutoff)
    c = c.loc[c.effective_date.notna() & c.effective_date.le(pd.Timestamp(at))]
    if c.empty:
        return None
    return c.sort_values(["effective_date", "ann_date"]).iloc[-1]


def fixed_income_plus_universe(
    funds: pd.DataFrame,
    classification: pd.DataFrame,
    asset_reports: pd.DataFrame,
    constraints: pd.DataFrame,
    at_date,
    cutoff,
    *,
    min_age=180,
    max_report_age=200,
    max_stock_each=0.40,
    max_stock_median=0.30,
    min_bond_median=0.50,
) -> pd.DataFrame:
    """Build separate contract/style pools using only information public by cutoff."""
    universe = eligible_universe(
        funds,
        classification,
        at_date,
        min_age,
        # Enhanced-index funds share the v3 audit/pilot inventory but remain in
        # the equity model family.  Keeping them here makes the scope audit
        # complete without allowing their style data to route them to FI+.
        categories=(*FIXED_INCOME_PLUS_CATEGORIES, "增强指数型"),
    )
    if universe.empty:
        return universe
    reports = dates(asset_reports, ("report_date", "ann_date"))
    if not reports.empty:
        require_unique(reports, ["fund_code", "report_date"], "fixed-income asset reports")
        reports = reports.loc[reports.report_date.le(pd.Timestamp(at_date))]
    observed_reports = reports.copy()
    reports = available(reports, cutoff) if not reports.empty else reports
    rows = []
    for fund in universe.itertuples(index=False):
        c = _latest_contract(constraints, fund.fund_code, at_date, cutoff)
        is_fixed_income_candidate = fund.category in FIXED_INCOME_PLUS_CATEGORIES
        contract_pool = bool(
            is_fixed_income_candidate
            and
            c is not None
            and bool(c.get("verified", False))
            and bool(c.get("fixed_income_primary", False))
            and bool(c.get("allows_equity_or_cbond", False))
            and c.get("stock_denominator") == "fund_nav"
            and pd.notna(c.get("stock_upper"))
            and float(c.stock_upper) <= max_stock_each
        )
        f = reports.loc[reports.fund_code == fund.fund_code].sort_values("report_date").tail(4)
        usable = (
            len(f) >= 2
            and {"stock_weight", "bond_weight"}.issubset(f)
            and f[["stock_weight", "bond_weight"]].notna().all().all()
        )
        style_pool = bool(
            is_fixed_income_candidate
            and
            usable
            and f.stock_weight.le(max_stock_each + 1e-12).all()
            and f.stock_weight.median() <= max_stock_median + 1e-12
            and f.bond_weight.median() >= min_bond_median - 1e-12
            and (pd.Timestamp(at_date) - f.report_date.max()).days <= max_report_age
        )
        observed = observed_reports.loc[
            observed_reports.fund_code == fund.fund_code
        ].sort_values("report_date").tail(4)
        observed_usable = (
            len(observed) >= 2
            and {"stock_weight", "bond_weight"}.issubset(observed)
            and observed[["stock_weight", "bond_weight"]].notna().all().all()
        )
        observed_style = bool(
            is_fixed_income_candidate
            and observed_usable
            and observed.stock_weight.le(max_stock_each + 1e-12).all()
            and observed.stock_weight.median() <= max_stock_median + 1e-12
            and observed.bond_weight.median() >= min_bond_median - 1e-12
            and (pd.Timestamp(at_date) - observed.report_date.max()).days <= max_report_age
        )
        cbond_ratio = np.nan
        if usable and "convertible_bond_weight" in f and f.bond_weight.median() > 0:
            cbond_ratio = float(f.convertible_bond_weight.fillna(0).median() / f.bond_weight.median())
        convertible = is_fixed_income_candidate and (
            fund.category == "可转债债基"
            or (pd.notna(cbond_ratio) and cbond_ratio >= 0.50)
        )
        pure_bond = bool(
            fund.category == "普通债基"
            and usable
            and f.stock_weight.fillna(0).max() <= 1e-6
            and ("convertible_bond_weight" not in f or f.convertible_bond_weight.fillna(0).max() <= 1e-6)
        )
        observed_pure_bond = bool(
            fund.category == "普通债基"
            and observed_usable
            and observed.stock_weight.fillna(0).max() <= 1e-6
            and (
                "convertible_bond_weight" not in observed
                or observed.convertible_bond_weight.fillna(0).max() <= 1e-6
            )
        )
        conflicts = []
        if c is None:
            conflicts.append("CONTRACT_MISSING")
        elif not contract_pool:
            conflicts.append("CONTRACT_NOT_ELIGIBLE_OR_UNVERIFIED")
        if not usable:
            conflicts.append("INSUFFICIENT_PUBLIC_REPORTS")
        elif not style_pool:
            conflicts.append("STYLE_RULE_NOT_MET")
        if getattr(fund, "family_metadata_missing", False):
            conflicts.append("SHARE_FAMILY_INCOMPLETE")
        row = fund._asdict()
        row.update(
            contract_pool=contract_pool,
            style_pool=style_pool,
            in_fixed_income_plus=bool(contract_pool or style_pool),
            comparison_group="convertible_dominant" if convertible else ("pure_bond_control" if pure_bond else None),
            style_report_count=int(len(f)),
            style_latest_report=str(f.report_date.max().date()) if len(f) else None,
            style_stock_median=float(f.stock_weight.median()) if usable else None,
            style_bond_median=float(f.bond_weight.median()) if usable else None,
            style_cbond_to_bond_median=cbond_ratio if pd.notna(cbond_ratio) else None,
            observed_style_rule_only=observed_style,
            observed_pure_bond_control=observed_pure_bond,
            observed_report_count=int(len(observed)),
            observed_information_status=(
                "not_point_in_time_until_announcement_verified"
                if observed_usable and not usable
                else "point_in_time"
            ),
            eligibility_reasons=json.dumps(conflicts, ensure_ascii=False),
        )
        rows.append(row)
    return pd.DataFrame(rows)


def select_fixed_count(frame: pd.DataFrame, quotas: dict[str, int]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Deterministic input-readiness selection. Never ranks on model errors."""
    chosen, gaps, used = [], [], set()
    for group, count in quotas.items():
        if group in {"convertible_dominant", "pure_bond_control"}:
            candidates = frame.loc[frame.comparison_group == group]
            if group == "pure_bond_control" and "observed_pure_bond_control" in frame:
                candidates = pd.concat(
                    [candidates, frame.loc[frame.observed_pure_bond_control]],
                    ignore_index=True,
                ).drop_duplicates("master_code")
        elif group == "other_style":
            candidates = frame.loc[frame.style_pool & ~frame.contract_pool]
            if "observed_style_rule_only" in frame:
                candidates = pd.concat(
                    [
                        candidates,
                        frame.loc[
                            frame.observed_style_rule_only
                            & ~frame.contract_pool
                            & frame.comparison_group.isna()
                            & ~frame.observed_pure_bond_control
                        ],
                    ],
                    ignore_index=True,
                ).drop_duplicates("master_code")
        else:
            candidates = frame.loc[frame.category == group]
        candidates = candidates.loc[
            ~candidates.family_metadata_missing & ~candidates.master_code.isin(used)
        ].sort_values("master_code")
        candidates = candidates.drop_duplicates("master_code").head(count).copy()
        candidates["selection_group"] = group
        candidates["selection_evidence_status"] = np.select(
            [
                candidates.in_fixed_income_plus
                | candidates.comparison_group.eq("pure_bond_control"),
                candidates.category.eq("增强指数型"),
                candidates.comparison_group.eq("convertible_dominant"),
            ],
            [
                "strict_point_in_time",
                "category_history_verified_model_input_pending",
                "category_history_verified_dominance_pending",
            ],
            default="pending_announcement_verification",
        )
        used.update(candidates.master_code)
        chosen.append(candidates)
        gaps.append({"selection_group": group, "requested": count, "selected": len(candidates),
                     "shortfall": max(0, count - len(candidates))})
    selected = pd.concat(chosen, ignore_index=True) if chosen else frame.iloc[:0].copy()
    if selected.duplicated("master_code").any():
        raise RuntimeError("Pilot selection must be product-unique")
    return selected, pd.DataFrame(gaps)
