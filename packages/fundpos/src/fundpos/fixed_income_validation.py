from __future__ import annotations

import json

import numpy as np
import pandas as pd
from filelock import FileLock

from .config import Settings
from .constants import FIXED_INCOME_PLUS_CATEGORIES, SW_CODES
from .data import DataBundle
from .errors import ProtocolError
from .fixed_income_pipeline import compute_fixed_income_date, fixed_income_panel
from .normalization import conditional_next_day_nav_availability
from .pit import dates, require_unique
from .storage import atomic_json, atomic_parquet, code_fingerprint, file_hash, frame_hash
from .universe import fixed_income_plus_universe, load_frozen_pilot
from .validation import disclosed_labels

MAIN_VALIDATION_GROUP = "fixed_income_plus_main"


def fixed_income_validation_group(
    *, contract_pool: bool, style_pool: bool, comparison_group
) -> str:
    """Keep comparison cohorts out of the stable fixed-income-plus gate."""
    if pd.notna(comparison_group):
        return str(comparison_group)
    if contract_pool or style_pool:
        return MAIN_VALIDATION_GROUP
    return "ineligible"


def factor_registry_gaps(
    settings: Settings,
    required: list[str],
    start,
    end,
    *,
    allow_conditional=False,
) -> list[dict]:
    path = settings.root / "config/fixed_income_factor_registry.csv"
    if not path.exists():
        return [{"asset": asset, "reason": "registry_missing"} for asset in required]
    registry = pd.read_csv(path, keep_default_na=False).set_index("asset")
    # Each actual estimate still proves its own 60-observation alignment.  The
    # registry only needs to reach the phase boundary; otherwise a conservative
    # calendar-day bound can incorrectly reject a valid 2021-09 warm-up for the
    # first 2022 quarter-end label.
    needed_start = pd.Timestamp(start)
    needed_end = pd.Timestamp(end)
    gaps = []
    for asset in required:
        if asset not in registry.index:
            gaps.append({"asset": asset, "reason": "unregistered"})
            continue
        row = registry.loc[asset]
        reasons = []
        accepted_statuses = {"ready", "conditional_ready"} if allow_conditional else {"ready"}
        if row.status not in accepted_statuses:
            reasons.append(str(row.status or "status_missing"))
        if not row.source_code or not row.source_table:
            reasons.append("source_not_frozen")
        start_value = pd.to_datetime(row.local_start, errors="coerce")
        end_value = pd.to_datetime(row.local_end, errors="coerce")
        if pd.isna(start_value) or start_value > needed_start:
            reasons.append("history_start_incomplete")
        if pd.isna(end_value) or end_value < needed_end:
            reasons.append("history_end_incomplete")
        if reasons:
            gaps.append({"asset": asset, "reason": ";".join(reasons)})
    return gaps


def fixed_income_labels(
    settings: Settings,
    bundle: DataBundle,
    start,
    end,
    *,
    selected_products: set[str] | None = None,
):
    reports = dates(bundle["asset_reports"], ("report_date", "ann_date"))
    reports = reports.loc[reports.report_date.between(pd.Timestamp(start), pd.Timestamp(end))
                          & reports.ann_date.notna() & reports.stock_weight.notna()
                          & reports.bond_allocation_complete.fillna(False)
                          & reports.convertible_bond_weight.notna()]
    calendar = pd.DatetimeIndex(pd.to_datetime(bundle["calendar"].date)).sort_values()
    rows = []
    universes = {}
    for report in reports.itertuples(index=False):
        prior = calendar[calendar <= report.report_date]
        if len(prior):
            target = prior[-1]
            if target not in universes:
                config = settings.values["fixed_income"]
                candidates = fixed_income_plus_universe(
                    bundle["funds"],
                    bundle["classification"],
                    bundle["asset_reports"],
                    bundle["constraints"],
                    target,
                    target + pd.Timedelta(days=1),
                    min_age=settings.values["universe"]["min_age_days"],
                    max_report_age=config["max_report_age_days"],
                    max_stock_each=config["stock_upper"],
                    max_stock_median=config["style_stock_median"],
                    min_bond_median=config["style_bond_median"],
                )
                universes[target] = candidates.loc[
                    candidates.in_fixed_income_plus
                    | candidates.comparison_group.notna()
                ]
            representatives = universes[target]
            eligible = representatives.loc[
                representatives.fund_code.eq(report.fund_code)
            ]
            if eligible.empty:
                continue
            if selected_products is not None and not eligible.master_code.isin(
                selected_products
            ).any():
                continue
            member = eligible.iloc[0]
            rows.append({"fund_code": report.fund_code, "valuation_date": str(prior[-1].date()),
                "stock_weight": float(report.stock_weight),
                "convertible_bond": float(report.convertible_bond_weight),
                "ordinary_bond": float(report.ordinary_bond_weight),
                "label_ann_date": str(report.ann_date.date()),
                "contract_pool": bool(member.contract_pool),
                "style_pool": bool(member.style_pool),
                "comparison_group": member.comparison_group,
                "validation_group": fixed_income_validation_group(
                    contract_pool=bool(member.contract_pool),
                    style_pool=bool(member.style_pool),
                    comparison_group=member.comparison_group,
                )})
    labels = pd.DataFrame(rows)
    if not labels.empty:
        require_unique(labels, ["fund_code", "valuation_date"], "fixed-income labels")
    return labels


def evaluate_fixed_income(
    predictions, asset_labels, industry_labels, *, include_diagnostic=False
):
    stable = predictions.loc[predictions.status.isin(["ok", "degraded"])].copy()
    assets = asset_labels.merge(
        stable,
        on=["fund_code", "valuation_date"],
        suffixes=("_true", "_estimated"),
    )
    if assets.empty:
        return {"status": "blocked_data", "coverage": 0.0}, assets
    stock_quality = assets.get(
        "stock_quality", pd.Series("estimated", index=assets.index)
    )
    cbond_quality = assets.get(
        "cbond_quality", pd.Series("estimated", index=assets.index)
    )
    stock_valid = assets.stock_weight_estimated.notna() & (
        stock_quality.ne("diagnostic") | include_diagnostic
    )
    cbond_valid = assets.convertible_bond_estimated.notna() & (
        cbond_quality.ne("diagnostic") | include_diagnostic
    )
    assets["stock_error"] = (
        abs(assets.stock_weight_true - assets.stock_weight_estimated).where(stock_valid)
    )
    assets["cbond_error"] = (
        abs(assets.convertible_bond_true - assets.convertible_bond_estimated).where(cbond_valid)
    )
    metrics = {
        "status": "evaluated" if stock_valid.any() and cbond_valid.any() else "blocked_data",
        "scope": "conditional_diagnostic" if include_diagnostic else "strict",
        "label_count": len(asset_labels),
        "stock_evaluated_count": int(stock_valid.sum()),
        "cbond_evaluated_count": int(cbond_valid.sum()),
        "stock_coverage": float(stock_valid.sum() / len(asset_labels)),
        "cbond_coverage": float(cbond_valid.sum() / len(asset_labels)),
        "coverage": float(min(stock_valid.sum(), cbond_valid.sum()) / len(asset_labels)),
        "stock_mae": float(assets.stock_error.mean()),
        "cbond_mae": float(assets.cbond_error.mean()),
        "stock_p90": float(assets.stock_error.quantile(.9)),
        "cbond_p90": float(assets.cbond_error.quantile(.9)),
    }
    stable_stock_quality = stable.get(
        "stock_quality", pd.Series("estimated", index=stable.index)
    )
    industry_valid = stable.loc[
        stable_stock_quality.ne("diagnostic") | include_diagnostic
    ]
    industries = industry_labels.merge(
        industry_valid,
        on=["fund_code", "valuation_date"],
        suffixes=("_true", "_estimated"),
    )
    if not industries.empty:
        actual = industries[[f"{s}_true" for s in SW_CODES]].to_numpy(float)
        estimated = industries[[f"{s}_estimated" for s in SW_CODES]].to_numpy(float)
        error = np.abs(actual-estimated).sum(axis=1)
        metrics.update(industry_label_count=len(industry_labels), industry_count=len(industries),
                       industry_l1_mean=float(error.mean()), industry_l1_p90=float(np.quantile(error, .9)),
                       hk_mae=float(abs(industries.hk_true-industries.hk_estimated).mean()))
    return metrics, assets


def fixed_income_selection_assessment(metrics: dict, acceptance: dict) -> dict:
    candidates = [name for name in metrics if name not in {"index", "personalized"}]
    selected = min(
        candidates,
        key=lambda name: (metrics[name].get("industry_l1_mean", float("inf")), name),
    )
    baseline = metrics["personalized"]
    candidate = metrics[selected]
    improvement = 1 - candidate["industry_l1_mean"] / baseline["industry_l1_mean"]
    tests = {
        "stock_mae_absolute": candidate["stock_mae"] <= acceptance["stock_mae_max"],
        "cbond_mae_absolute": candidate["cbond_mae"]
        <= acceptance["convertible_bond_mae_max"],
        "industry_improvement": improvement
        >= acceptance["industry_l1_relative_improvement"],
        "stock_mae_not_worse": candidate["stock_mae"] <= baseline["stock_mae"],
        "cbond_mae_not_worse": candidate["cbond_mae"] <= baseline["cbond_mae"],
        "stock_tail_not_worse": candidate["stock_p90"] <= baseline["stock_p90"],
        "cbond_tail_not_worse": candidate["cbond_p90"] <= baseline["cbond_p90"],
        "industry_tail_not_worse": candidate["industry_l1_p90"]
        <= baseline["industry_l1_p90"],
        "hk_not_worse": candidate["hk_mae"] <= baseline["hk_mae"],
        "coverage_not_worse": candidate["full_asset_label_coverage"]
        >= baseline["full_asset_label_coverage"],
    }
    return {
        "candidate": selected,
        "industry_improvement": improvement,
        "tests": tests,
        "all_selection_gates_pass": all(tests.values()),
        "conditional_ready_for_final_holdout": False,
    }


class FixedIncomeValidationState:
    def __init__(self, settings: Settings, *, conditional=False):
        self.settings = settings
        self.conditional = conditional
        self.directory = settings.path("data_dir") / (
            "validation_fixed_income_conditional" if conditional else "validation_fixed_income"
        )
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = self.directory / "state.json"
        self.protocol_path = settings.root / "config/v3_validation_protocol.json"
        self.protocol = json.loads(self.protocol_path.read_text(encoding="utf8"))
        self.protocol_hash = file_hash(self.protocol_path)

    def read(self):
        return json.loads(self.path.read_text(encoding="utf8")) if self.path.exists() else {}

    def assert_open_allowed(self, phase):
        if self.conditional and phase == "final":
            raise ProtocolError("Conditional evidence can never open the final holdout")
        state = self.read()
        if state.get("final_opened_at"):
            raise ProtocolError("Fixed-income final holdout was already opened")
        if state and state.get("protocol_hash") != self.protocol_hash:
            raise ProtocolError("Fixed-income protocol fingerprint changed")
        if phase == "selection" and not state.get("development_complete"):
            raise ProtocolError("Complete fixed-income development first")
        if phase == "final":
            if (
                not state.get("selection_frozen")
                or state.get("code_hash") != code_fingerprint(self.settings.root)
            ):
                raise ProtocolError(
                    "Freeze fixed-income selection and implementation before final"
                )
            if not state.get("selection_gates", {}).get(
                "all_selection_gates_pass", False
            ):
                raise ProtocolError(
                    "Fixed-income selection gates must all pass before final"
                )


def run_fixed_income_validation(
    settings: Settings,
    bundle: DataBundle,
    phase: str,
    progress=print,
    *,
    allow_conditional_evidence=False,
):
    state = FixedIncomeValidationState(settings, conditional=allow_conditional_evidence)
    state.assert_open_allowed(phase)
    start, end = state.protocol[phase]
    directory = settings.path("output_dir") / (
        "validation_fixed_income_conditional"
        if allow_conditional_evidence
        else "validation_fixed_income"
    ) / phase
    directory.mkdir(parents=True, exist_ok=True)
    with FileLock(str(state.directory / ".lock"), timeout=0):
        source_input_hash = bundle.fingerprint
        if allow_conditional_evidence:
            frames = dict(bundle.frames)
            frames["nav"] = conditional_next_day_nav_availability(bundle["nav"])
            provenance = dict(bundle.provenance)
            provenance["conditional_nav_availability"] = (
                "next calendar day only for vendor ann_date delays over 7 days"
            )
            bundle = DataBundle(frames, provenance)
        prerequisites = []
        selected_products = set(
            load_frozen_pilot(settings.root, settings.values["universe"]).master_code
        )
        registry_gaps = factor_registry_gaps(
            settings,
            state.protocol["required_total_return_factors"],
            start,
            end,
            allow_conditional=allow_conditional_evidence,
        )
        if registry_gaps:
            prerequisites.append(
                {"code": "FACTOR_REGISTRY_NOT_READY", "factors": registry_gaps}
            )
        try:
            fixed_income_panel(bundle, end, pd.DatetimeIndex(bundle["calendar"].date))
        except Exception as exc:
            prerequisites.append({"code": getattr(exc, "code", "FACTOR_PREFLIGHT_FAILED"),
                                  "detail": getattr(exc, "detail", type(exc).__name__)})
        verified_contracts = (not bundle["constraints"].empty and "verified" in bundle["constraints"]
                              and bundle["constraints"].verified.fillna(False).any())
        evidence_warnings = []
        if not verified_contracts:
            item = {"code": "NO_VERIFIED_HISTORICAL_CONTRACTS"}
            (evidence_warnings if allow_conditional_evidence else prerequisites).append(item)
        if allow_conditional_evidence and bundle["nav"].get(
            "nav_availability_conditional", pd.Series(dtype=bool)
        ).any():
            evidence_warnings.append(
                {"code": "CONDITIONAL_NEXT_DAY_NAV_AVAILABILITY"}
            )
        asset_labels = fixed_income_labels(
            settings, bundle, start, end, selected_products=selected_products
        )
        industry_labels = disclosed_labels(bundle, start, end, categories=FIXED_INCOME_PLUS_CATEGORIES)
        if not industry_labels.empty:
            valid_keys = set(
                zip(asset_labels.fund_code, asset_labels.valuation_date, strict=False)
            )
            industry_labels = industry_labels.loc[
                [
                    key in valid_keys
                    for key in zip(
                        industry_labels.fund_code,
                        industry_labels.valuation_date,
                        strict=False,
                    )
                ]
            ].reset_index(drop=True)
        main_asset_labels = asset_labels.loc[
            asset_labels.validation_group.eq(MAIN_VALIDATION_GROUP)
        ].reset_index(drop=True)
        main_keys = set(zip(
            main_asset_labels.fund_code,
            main_asset_labels.valuation_date,
            strict=False,
        ))
        main_industry_labels = industry_labels.loc[
            [
                key in main_keys
                for key in zip(
                    industry_labels.fund_code,
                    industry_labels.valuation_date,
                    strict=False,
                )
            ]
        ].reset_index(drop=True)
        if asset_labels.empty:
            prerequisites.append({"code": "NO_COMPLETE_STOCK_CBOND_LABELS"})
        if prerequisites:
            result = {"status": "blocked_data", "phase": phase, "prerequisites": prerequisites,
                "input_hash": bundle.fingerprint, "source_input_hash": source_input_hash,
                "protocol_hash": state.protocol_hash,
                "asset_label_count": len(asset_labels),
                "asset_label_products": int(asset_labels.fund_code.nunique()),
                "industry_label_count": len(industry_labels),
                "industry_label_products": int(industry_labels.fund_code.nunique()),
                "evidence_scope": "conditional" if allow_conditional_evidence else "strict",
                "evidence_warnings": evidence_warnings,
                "final_holdout_opened": False}
            atomic_json(directory / "summary.json", result)
            return directory
        if phase == "final":
            saved = state.read()
            saved.update(final_opened_at=pd.Timestamp.now(tz="Asia/Shanghai").isoformat(),
                         final_input_hash=bundle.fingerprint)
            atomic_json(state.path, saved)
        configurations = [{"name": "index", "prior_penalty": 0.0, "smooth_penalty": 0.0},
            {"name": "personalized", "prior_penalty": 0.0, "smooth_penalty": 0.0},
            *state.protocol["candidates"]]
        if phase == "final":
            selected = state.read()["selection_frozen"]
            configurations = [c for c in configurations if c["name"] in {"personalized", selected}]
        label_dates = sorted(set(asset_labels.valuation_date) | set(industry_labels.valuation_date))
        metrics, predictions = {}, {}
        prepared_factor_cache = {}
        for config in configurations:
            model_settings = settings.with_model(**config, window=state.protocol["window"],
                                                 weighting=state.protocol["weighting"])
            records, previous = [], None
            # Validation truth exists only on disclosure report dates.  Use the
            # prior report-date estimate for the smoothing experiment and test
            # daily recursive operation separately during P6.  This cadence is
            # frozen before selection and avoids changing the candidate budget.
            calculation = pd.to_datetime(label_dates)
            for day in calculation:
                result, _ = compute_fixed_income_date(
                    model_settings,
                    bundle,
                    day,
                    day + pd.Timedelta(days=1),
                    previous,
                    selected_products=selected_products,
                    prepared_factor_cache=prepared_factor_cache,
                )
                valid = result.loc[result.status.isin(["ok", "degraded"])]
                if not valid.empty:
                    previous = valid
                if str(day.date()) in label_dates:
                    records.append(result)
            if records:
                columns = list(dict.fromkeys(column for frame in records for column in frame))
                prediction = pd.concat(
                    [frame.dropna(axis=1, how="all") for frame in records],
                    ignore_index=True,
                ).reindex(columns=columns)
            else:
                prediction = pd.DataFrame()
            predictions[config["name"]] = prediction
            metrics[config["name"]], errors = evaluate_fixed_income(
                prediction,
                main_asset_labels,
                main_industry_labels,
                include_diagnostic=allow_conditional_evidence,
            )
            atomic_parquet(directory / f"{config['name']}_predictions.parquet", prediction)
            atomic_parquet(directory / f"{config['name']}_asset_errors.parquet", errors)
            progress(f"fixed-income validation {phase} {config['name']}: {metrics[config['name']]['status']}", flush=True)
        individual_metrics = metrics
        diagnostic_group_metrics = {}
        for group_name, group_labels in asset_labels.groupby(
            "validation_group", dropna=False
        ):
            if group_name == MAIN_VALIDATION_GROUP:
                continue
            group_keys = set(zip(
                group_labels.fund_code,
                group_labels.valuation_date,
                strict=False,
            ))
            group_industry = industry_labels.loc[
                [
                    key in group_keys
                    for key in zip(
                        industry_labels.fund_code,
                        industry_labels.valuation_date,
                        strict=False,
                    )
                ]
            ].reset_index(drop=True)
            diagnostic_group_metrics[str(group_name)] = {
                name: evaluate_fixed_income(
                    prediction,
                    group_labels.reset_index(drop=True),
                    group_industry,
                    include_diagnostic=allow_conditional_evidence,
                )[0]
                for name, prediction in predictions.items()
            }
        common = None
        common_industry = None
        for value in predictions.values():
            stock_quality = value.get(
                "stock_quality", pd.Series("estimated", index=value.index)
            )
            cbond_quality = value.get(
                "cbond_quality", pd.Series("estimated", index=value.index)
            )
            stable = value.loc[
                value.status.isin(["ok", "degraded"])
                & value.stock_weight.notna()
                & value.convertible_bond.notna()
                & (stock_quality.ne("diagnostic") | allow_conditional_evidence)
                & (cbond_quality.ne("diagnostic") | allow_conditional_evidence)
            ]
            keys = set(zip(stable.fund_code, stable.valuation_date))
            common = keys if common is None else common & keys
            industry_rows = stable.loc[stable[list(SW_CODES)].notna().all(axis=1)]
            industry_keys = set(zip(industry_rows.fund_code, industry_rows.valuation_date))
            common_industry = (
                industry_keys
                if common_industry is None
                else common_industry & industry_keys
            )
        common_asset_labels = main_asset_labels.loc[
            [key in (common or set()) for key in zip(
                main_asset_labels.fund_code, main_asset_labels.valuation_date
            )]
        ].reset_index(drop=True)
        common_industry_labels = main_industry_labels.loc[
            [key in (common_industry or set()) for key in zip(
                main_industry_labels.fund_code, main_industry_labels.valuation_date
            )]
        ].reset_index(drop=True)
        metrics = {}
        for name, prediction in predictions.items():
            metrics[name], _ = evaluate_fixed_income(
                prediction,
                common_asset_labels,
                common_industry_labels,
                include_diagnostic=allow_conditional_evidence,
            )
            metrics[name]["full_asset_label_coverage"] = individual_metrics[name].get(
                "coverage", 0.0
            )
        enough = bool(
            len(common_asset_labels) >= 10
            and common_asset_labels.valuation_date.nunique() >= 2
            and len(common_industry_labels) >= 10
            and common_industry_labels.valuation_date.nunique() >= 2
        )
        result = {"status": (
                "conditional_complete" if enough and allow_conditional_evidence
                else ("complete" if enough else "blocked_data")
            ), "phase": phase,
            "protocol_hash": state.protocol_hash, "input_hash": bundle.fingerprint,
            "source_input_hash": source_input_hash,
            "asset_label_hash": frame_hash(asset_labels), "industry_label_hash": frame_hash(industry_labels),
            "metrics": metrics,
            "individual_coverage_metrics": individual_metrics,
            "diagnostic_group_metrics": diagnostic_group_metrics,
            "all_asset_label_count": len(asset_labels),
            "all_industry_label_count": len(industry_labels),
            "common_label_count": len(common_asset_labels),
            "common_industry_label_count": len(common_industry_labels),
            "evidence_scope": "conditional" if allow_conditional_evidence else "strict",
            "evidence_warnings": evidence_warnings,
            "eligible_for_formal_promotion": not allow_conditional_evidence,
            "smoothing_validation_cadence": "prior_truth_label_date",
            "production_smoothing_cadence": "daily_separate_P6_acceptance",
            "final_holdout_opened": phase == "final"}
        if enough:
            saved = state.read() | {"protocol_hash": state.protocol_hash, "code_hash": code_fingerprint(settings.root)}
            if phase == "development":
                saved["development_complete"] = True
            elif phase == "selection":
                assessment = fixed_income_selection_assessment(
                    metrics, state.protocol["acceptance"]
                )
                selected = assessment["candidate"]
                saved["selection_frozen"] = selected
                saved["selection_gates"] = assessment
                saved["selection_evidence_scope"] = (
                    "conditional" if allow_conditional_evidence else "strict"
                )
                result["selected_candidate"] = selected
                result["selection_assessment"] = assessment
            else:
                selected = saved["selection_frozen"]
                base, candidate = metrics["personalized"], metrics[selected]
                improvement = 1-candidate["industry_l1_mean"]/base["industry_l1_mean"]
                tests = {"stock_mae_absolute": candidate["stock_mae"] <= .05,
                    "cbond_mae_absolute": candidate["cbond_mae"] <= .05,
                    "industry_improvement": improvement >= .10,
                    "stock_not_worse": candidate["stock_mae"] <= base["stock_mae"],
                    "cbond_not_worse": candidate["cbond_mae"] <= base["cbond_mae"],
                    "tail_not_worse": candidate["industry_l1_p90"] <= base["industry_l1_p90"],
                    "coverage_not_worse": candidate["coverage"] >= base["coverage"]}
                result["promotion"] = {"promoted": all(tests.values()), "tests": tests,
                                       "industry_improvement": improvement}
                saved["final_decision"] = result["promotion"]
            atomic_json(state.path, saved)
        atomic_json(directory / "summary.json", result)
        return directory
