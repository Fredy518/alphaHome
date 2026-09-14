from __future__ import annotations

import json

import numpy as np
import pandas as pd
from filelock import FileLock

from .config import Settings
from .constants import FIXED_INCOME_PLUS_CATEGORIES, SW_CODES
from .convertible_dominant import CONVERTIBLE_DOMINANT_GROUP
from .data import DataBundle
from .errors import ProtocolError
from .fixed_income_pipeline import compute_fixed_income_date, fixed_income_panel
from .fixed_income_validation import (
    evaluate_fixed_income,
    factor_registry_gaps,
    fixed_income_labels,
)
from .normalization import conditional_next_day_nav_availability
from .pit import dates
from .storage import atomic_json, atomic_parquet, code_fingerprint, file_hash, frame_hash
from .universe import load_frozen_pilot
from .validation import disclosed_labels


def historical_contract_sample_gaps(
    constraints: pd.DataFrame,
    samples: pd.DataFrame,
    *,
    allow_conditional: bool = False,
) -> pd.DataFrame:
    """Return fund/date pairs without a PIT, solver-usable contract row."""
    required = samples[["fund_code", "valuation_date"]].drop_duplicates().copy()
    required["valuation_date"] = pd.to_datetime(required.valuation_date).dt.normalize()
    if required.empty:
        return required
    if constraints.empty:
        return required
    contracts = dates(constraints, ("ann_date", "effective_date"))
    verified = contracts.get(
        "verified", pd.Series(False, index=contracts.index)
    ).fillna(False)
    if not allow_conditional:
        verified &= contracts.get(
            "formal_verified", verified
        ).fillna(False)
    contracts = contracts.loc[
        verified
        & contracts.ann_date.notna()
        & contracts.effective_date.notna()
    ]
    covered = []
    for row in required.itertuples(index=False):
        # Validation estimates use the next calendar day as information cutoff.
        cutoff = row.valuation_date + pd.Timedelta(days=1)
        eligible = contracts.loc[
            contracts.fund_code.eq(row.fund_code)
            & contracts.ann_date.le(cutoff)
            & contracts.effective_date.le(row.valuation_date)
        ]
        covered.append(not eligible.empty)
    return required.loc[[not value for value in covered]].reset_index(drop=True)


def convertible_dominant_assessment(
    metrics: dict,
    acceptance: dict,
    *,
    candidate_names: list[str] | None = None,
    baseline_names: list[str] | None = None,
) -> dict:
    candidate_names = candidate_names or [name for name in metrics if name != "index"]
    baseline_names = baseline_names or ["index"]
    eligible = {
        name: metrics[name]
        for name in candidate_names
        if name in metrics and metrics[name].get("status") == "evaluated"
    }
    baselines = {
        name: metrics[name]
        for name in baseline_names
        if name in metrics and metrics[name].get("status") == "evaluated"
    }
    if not eligible or not baselines:
        return {
            "candidate": None,
            "tests": {"metrics_available": False},
            "all_development_gates_pass": False,
        }
    selected = min(
        eligible,
        key=lambda name: (
            eligible[name].get("cbond_mae", float("inf")),
            eligible[name].get("stock_mae", float("inf")),
            eligible[name].get("industry_l1_mean", float("inf")),
            name,
        ),
    )
    candidate = metrics[selected]
    # The formal comparison stays tied to the corrected personalized baseline.
    # Disclosure carry/mark-to-market are dedicated CB diagnostics and must not
    # silently replace the baseline used by the v3 promotion contract.
    formal_baseline_name = (
        "personalized"
        if "personalized" in baselines
        else min(
            baselines,
            key=lambda name: (
                baselines[name].get("industry_l1_mean", float("inf")),
                name,
            ),
        )
    )
    formal_baseline = baselines[formal_baseline_name]
    dedicated_baseline_name = min(
        baselines,
        key=lambda name: (baselines[name].get("cbond_mae", float("inf")), name),
    )
    dedicated_baseline = baselines[dedicated_baseline_name]
    industry_improvement = 1 - (
        candidate.get("industry_l1_mean", float("inf"))
        / formal_baseline.get("industry_l1_mean", 0.0)
    ) if formal_baseline.get("industry_l1_mean", 0.0) > 0 else float("-inf")
    dedicated_improvement = 1 - (
        candidate["cbond_mae"] / dedicated_baseline["cbond_mae"]
    )
    formal_tests = {
        "metrics_available": True,
        "stock_mae_absolute": candidate["stock_mae"] <= acceptance["stock_mae_max"],
        "cbond_mae_absolute": candidate["cbond_mae"]
        <= acceptance["convertible_bond_mae_max"],
        "industry_improvement": industry_improvement
        >= acceptance.get("industry_relative_improvement", 0.10),
        "stock_mae_not_worse": candidate["stock_mae"]
        <= formal_baseline["stock_mae"],
        "cbond_mae_not_worse": candidate["cbond_mae"]
        <= formal_baseline["cbond_mae"],
        "stock_tail_not_worse": candidate["stock_p90"]
        <= formal_baseline["stock_p90"],
        "cbond_tail_not_worse": candidate["cbond_p90"]
        <= formal_baseline["cbond_p90"],
        "industry_tail_not_worse": candidate.get("industry_l1_p90", float("inf"))
        <= formal_baseline.get("industry_l1_p90", float("inf")),
        "hk_not_worse": candidate.get("hk_mae", float("inf"))
        <= formal_baseline.get("hk_mae", float("inf")),
        "coverage": candidate["full_asset_label_coverage"]
        >= acceptance["coverage_min"],
        "coverage_not_worse": candidate["full_asset_label_coverage"]
        >= formal_baseline["full_asset_label_coverage"],
    }
    dedicated_tests = {
        "cbond_improvement_vs_strongest_reference": dedicated_improvement
        >= acceptance["dedicated_cbond_relative_improvement"],
        "cbond_tail_not_worse_than_strongest_reference": candidate["cbond_p90"]
        <= dedicated_baseline["cbond_p90"],
    }
    reference_limit = acceptance.get(
        "group_industry_l1_reference",
        acceptance.get("industry_l1_reference", 0.20),
    )
    reference = {
        "single_fund_industry_l1_mean": candidate.get("industry_l1_mean"),
        "group_industry_l1_reference": reference_limit,
        "single_fund_mean_below_reference": (
            candidate.get("industry_l1_mean", float("inf")) <= reference_limit
        ),
        "is_acceptance_gate": False,
        "reason": "20pp is a group-reporting reference, not a universal single-fund gate",
    }
    return {
        "candidate": selected,
        "comparison_baseline": formal_baseline_name,
        "dedicated_comparison_baseline": dedicated_baseline_name,
        "industry_improvement_vs_baseline": industry_improvement,
        "cbond_improvement_vs_dedicated_baseline": dedicated_improvement,
        "formal_tests": formal_tests,
        "dedicated_research_tests": dedicated_tests,
        "references": reference,
        # Keep this compatibility field for state/database consumers. Its
        # contents now contain only gates, never the 20pp observation reference.
        "tests": formal_tests | dedicated_tests,
        "all_formal_gates_pass": all(formal_tests.values()),
        "all_dedicated_research_gates_pass": all(dedicated_tests.values()),
        "all_development_gates_pass": (
            all(formal_tests.values()) and all(dedicated_tests.values())
        ),
        "eligible_to_open_final": False,
    }


class ConvertibleDominantValidationState:
    def __init__(self, settings: Settings, *, conditional=False):
        self.settings = settings
        self.conditional = conditional
        protocol_filename = settings.values.get("convertible_dominant", {}).get(
            "protocol_file", "v3_convertible_dominant_protocol.json"
        )
        self.protocol_path = settings.root / "config" / protocol_filename
        self.protocol = json.loads(self.protocol_path.read_text(encoding="utf8"))
        self.protocol_hash = file_hash(self.protocol_path)
        namespace = self.protocol.get(
            "state_namespace", "validation_convertible_dominant"
        )
        self.directory = settings.path("data_dir") / (
            f"{namespace}_conditional" if conditional else namespace
        )
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = self.directory / "state.json"

    def read(self):
        return json.loads(self.path.read_text(encoding="utf8")) if self.path.exists() else {}

    def assert_open_allowed(self, phase):
        if phase == "selection":
            raise ProtocolError(
                "Convertible-dominant has no clean selection window; 2022-2024 is development"
            )
        if phase not in {"development", "final"}:
            raise ProtocolError(f"Unsupported convertible-dominant phase: {phase}")
        if self.conditional and phase == "final":
            raise ProtocolError("Conditional evidence can never open the final holdout")
        state = self.read()
        if state.get("final_opened_at"):
            raise ProtocolError("Convertible-dominant final holdout was already opened")
        if state and state.get("protocol_hash") != self.protocol_hash:
            raise ProtocolError("Convertible-dominant protocol fingerprint changed")
        if phase == "final":
            if (
                not state.get("algorithm_frozen")
                or state.get("code_hash") != code_fingerprint(self.settings.root)
            ):
                raise ProtocolError(
                    "Freeze the strict convertible-dominant algorithm before final"
                )
            if not state.get("development_gates", {}).get(
                "all_development_gates_pass", False
            ):
                raise ProtocolError(
                    "Convertible-dominant development gates must pass before final"
                )


def _common_labels(predictions: dict, asset_labels, industry_labels, include_diagnostic):
    common_assets = None
    common_industries = None
    for prediction in predictions.values():
        stock_quality = prediction.get(
            "stock_quality", pd.Series("estimated", index=prediction.index)
        )
        cbond_quality = prediction.get(
            "cbond_quality", pd.Series("estimated", index=prediction.index)
        )
        stable = prediction.loc[
            prediction.status.isin(["ok", "degraded"])
            & prediction.stock_weight.notna()
            & prediction.convertible_bond.notna()
            & (stock_quality.ne("diagnostic") | include_diagnostic)
            & (cbond_quality.ne("diagnostic") | include_diagnostic)
        ]
        keys = set(zip(stable.fund_code, stable.valuation_date, strict=False))
        common_assets = keys if common_assets is None else common_assets & keys
        industry_rows = stable.loc[stable[list(SW_CODES)].notna().all(axis=1)]
        keys = set(
            zip(industry_rows.fund_code, industry_rows.valuation_date, strict=False)
        )
        common_industries = (
            keys if common_industries is None else common_industries & keys
        )
    common_asset_labels = asset_labels.loc[
        [
            key in (common_assets or set())
            for key in zip(
                asset_labels.fund_code, asset_labels.valuation_date, strict=False
            )
        ]
    ].reset_index(drop=True)
    common_industry_labels = industry_labels.loc[
        [
            key in (common_industries or set())
            for key in zip(
                industry_labels.fund_code,
                industry_labels.valuation_date,
                strict=False,
            )
        ]
    ].reset_index(drop=True)
    return common_asset_labels, common_industry_labels


def run_convertible_dominant_validation(
    settings: Settings,
    bundle: DataBundle,
    phase: str,
    progress=print,
    *,
    allow_conditional_evidence=False,
):
    state = ConvertibleDominantValidationState(
        settings, conditional=allow_conditional_evidence
    )
    state.assert_open_allowed(phase)
    start, end = state.protocol[phase]
    directory = settings.path("output_dir") / state.directory.name / phase
    directory.mkdir(parents=True, exist_ok=True)
    with FileLock(str(state.directory / ".lock"), timeout=0):
        source_input_hash = bundle.fingerprint
        if allow_conditional_evidence:
            frames = dict(bundle.frames)
            frames["nav"] = conditional_next_day_nav_availability(bundle["nav"])
            bundle = DataBundle(
                frames,
                dict(bundle.provenance)
                | {
                    "conditional_nav_availability": (
                        "next calendar day only for vendor ann_date delays over 7 days"
                    )
                },
            )
        pilot = load_frozen_pilot(settings.root, settings.values["universe"])
        pilot = pilot.loc[pilot.selection_group.eq(CONVERTIBLE_DOMINANT_GROUP)]
        selected_products = set(pilot.master_code)
        prerequisites = []
        warnings = []
        gaps = factor_registry_gaps(
            settings,
            state.protocol["required_total_return_factors"],
            start,
            end,
            allow_conditional=allow_conditional_evidence,
        )
        if gaps:
            prerequisites.append({"code": "FACTOR_REGISTRY_NOT_READY", "factors": gaps})
        try:
            fixed_income_panel(
                bundle, end, pd.DatetimeIndex(pd.to_datetime(bundle["calendar"].date))
            )
        except Exception as exc:
            prerequisites.append(
                {
                    "code": getattr(exc, "code", "FACTOR_PREFLIGHT_FAILED"),
                    "detail": getattr(exc, "detail", type(exc).__name__),
                }
            )
        constituent_status = set(
            bundle["cbond_prices"].get("evidence_status", pd.Series(dtype=str)).dropna()
        )
        conditional_constituent_status = sorted(
            value
            for value in constituent_status
            if str(value).startswith("conditional_")
        )
        if conditional_constituent_status:
            item = {
                "code": "CONDITIONAL_CBOND_CONSTITUENT_TOTAL_RETURN",
                "evidence_status": conditional_constituent_status,
            }
            (warnings if allow_conditional_evidence else prerequisites).append(item)
        elif bundle["cbond_prices"].empty:
            prerequisites.append({"code": "NO_CBOND_CONSTITUENT_RETURNS"})
        asset_labels = fixed_income_labels(
            settings,
            bundle,
            start,
            end,
            selected_products=selected_products,
        )
        asset_labels = asset_labels.loc[
            asset_labels.validation_group.eq(CONVERTIBLE_DOMINANT_GROUP)
        ].reset_index(drop=True)
        contract_gaps = historical_contract_sample_gaps(
            bundle["constraints"],
            asset_labels,
            allow_conditional=allow_conditional_evidence,
        )
        if not contract_gaps.empty:
            missing_funds = sorted(contract_gaps.fund_code.unique())
            item = {
                "code": "NO_VERIFIED_HISTORICAL_CONTRACTS",
                "missing_sample_count": len(contract_gaps),
                "missing_fund_count": len(missing_funds),
                "missing_funds": missing_funds,
            }
            (warnings if allow_conditional_evidence else prerequisites).append(item)
        industry_labels = disclosed_labels(
            bundle, start, end, categories=FIXED_INCOME_PLUS_CATEGORIES
        )
        keys = set(
            zip(asset_labels.fund_code, asset_labels.valuation_date, strict=False)
        )
        industry_labels = industry_labels.loc[
            [
                key in keys
                for key in zip(
                    industry_labels.fund_code,
                    industry_labels.valuation_date,
                    strict=False,
                )
            ]
        ].reset_index(drop=True)
        if asset_labels.empty:
            prerequisites.append({"code": "NO_CONVERTIBLE_DOMINANT_LABELS"})
        if prerequisites:
            result = {
                "status": "blocked_data",
                "phase": phase,
                "prerequisites": prerequisites,
                "evidence_warnings": warnings,
                "input_hash": bundle.fingerprint,
                "source_input_hash": source_input_hash,
                "protocol_hash": state.protocol_hash,
                "asset_label_count": len(asset_labels),
                "industry_label_count": len(industry_labels),
                "final_holdout_opened": False,
            }
            atomic_json(directory / "summary.json", result)
            return directory
        if phase == "final":
            saved = state.read()
            saved.update(
                final_opened_at=pd.Timestamp.now(tz="Asia/Shanghai").isoformat(),
                final_input_hash=bundle.fingerprint,
            )
            atomic_json(state.path, saved)
        configurations = [
            {"name": "index", "prior_penalty": 0.0, "smooth_penalty": 0.0},
            {"name": "personalized", "prior_penalty": 0.0, "smooth_penalty": 0.0},
            *state.protocol["candidates"],
            *state.protocol.get("structural_candidates", []),
        ]
        if phase == "final":
            selected = state.read()["algorithm_frozen"]
            configurations = [
                config
                for config in configurations
                if config["name"] in {"index", "personalized", selected}
            ]
        label_dates = sorted(
            set(asset_labels.valuation_date) | set(industry_labels.valuation_date)
        )
        predictions = {}
        individual_metrics = {}
        prepared_factor_cache = {}
        for config in configurations:
            model_settings = settings.with_model(
                **config,
                window=state.protocol["window"],
                weighting=state.protocol["weighting"],
            )
            records = []
            previous = None
            for day in pd.to_datetime(label_dates):
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
                records.append(result)
            prediction = pd.concat(records, ignore_index=True) if records else pd.DataFrame()
            predictions[config["name"]] = prediction
            individual_metrics[config["name"]], errors = evaluate_fixed_income(
                prediction,
                asset_labels,
                industry_labels,
                include_diagnostic=allow_conditional_evidence,
            )
            atomic_parquet(
                directory / f"{config['name']}_predictions.parquet", prediction
            )
            atomic_parquet(directory / f"{config['name']}_asset_errors.parquet", errors)
            progress(
                f"convertible-dominant {phase} {config['name']}: "
                f"{individual_metrics[config['name']]['status']}",
                flush=True,
            )
        reference_source = predictions.get("personalized")
        reference_columns = {
            "disclosure_carry": "cbond_control_weight",
            "marked_to_market": "cbond_mark_to_market_weight",
        }
        if reference_source is not None:
            for reference_name, column in reference_columns.items():
                if column not in reference_source:
                    continue
                reference = reference_source.copy()
                reference["convertible_bond"] = reference[column]
                reference["model"] = reference_name
                predictions[reference_name] = reference
                individual_metrics[reference_name], errors = evaluate_fixed_income(
                    reference,
                    asset_labels,
                    industry_labels,
                    include_diagnostic=allow_conditional_evidence,
                )
                atomic_parquet(
                    directory / f"{reference_name}_predictions.parquet", reference
                )
                atomic_parquet(
                    directory / f"{reference_name}_asset_errors.parquet", errors
                )
        common_assets, common_industries = _common_labels(
            predictions,
            asset_labels,
            industry_labels,
            allow_conditional_evidence,
        )
        metrics = {}
        for name, prediction in predictions.items():
            metrics[name], _ = evaluate_fixed_income(
                prediction,
                common_assets,
                common_industries,
                include_diagnostic=allow_conditional_evidence,
            )
            metrics[name]["full_asset_label_coverage"] = individual_metrics[name].get(
                "coverage", 0.0
            )
        minimum = state.protocol["minimum_sample"]
        enough = bool(
            len(common_assets) >= minimum["asset_labels"]
            and common_assets.fund_code.nunique() >= minimum["products"]
            and common_assets.valuation_date.nunique() >= minimum["valuation_dates"]
            and len(common_industries) >= minimum["industry_labels"]
        )
        candidate_names = [
            config["name"]
            for config in [
                *state.protocol["candidates"],
                *state.protocol.get("structural_candidates", []),
            ]
        ]
        assessment = (
            convertible_dominant_assessment(
                metrics,
                state.protocol["acceptance"],
                candidate_names=candidate_names,
                baseline_names=[
                    "personalized",
                    "disclosure_carry",
                    "marked_to_market",
                ],
            )
            if enough
            else {
                "candidate": None,
                "tests": {"minimum_sample": False},
                "all_development_gates_pass": False,
            }
        )
        selected_candidate = assessment.get("candidate")
        agreement_metrics = None
        if selected_candidate in predictions:
            selected_prediction = predictions[selected_candidate]
            if "cbond_agreement_qualified" in selected_prediction:
                agreement_values = selected_prediction[
                    "cbond_agreement_qualified"
                ].to_numpy()
                agreement_qualified = np.fromiter(
                    (
                        False if pd.isna(value) else bool(value)
                        for value in agreement_values
                    ),
                    dtype=bool,
                    count=len(agreement_values),
                )
                agreement_prediction = selected_prediction.loc[
                    agreement_qualified
                ]
                agreement_metrics, _ = evaluate_fixed_income(
                    agreement_prediction,
                    common_assets,
                    common_industries,
                    include_diagnostic=allow_conditional_evidence,
                )
        validation_code_hash = code_fingerprint(settings.root)
        result = {
            "status": (
                "conditional_complete"
                if enough and allow_conditional_evidence
                else ("complete" if enough else "blocked_data")
            ),
            "phase": phase,
            "protocol_hash": state.protocol_hash,
            "input_hash": bundle.fingerprint,
            "source_input_hash": source_input_hash,
            "asset_label_hash": frame_hash(asset_labels),
            "industry_label_hash": frame_hash(industry_labels),
            "metrics": metrics,
            "individual_coverage_metrics": individual_metrics,
            "all_asset_label_count": len(asset_labels),
            "all_industry_label_count": len(industry_labels),
            "common_label_count": len(common_assets),
            "common_industry_label_count": len(common_industries),
            "selected_candidate": selected_candidate,
            "development_assessment": assessment,
            "agreement_qualified_metrics": agreement_metrics,
            "evidence_scope": "conditional" if allow_conditional_evidence else "strict",
            "evidence_warnings": warnings,
            "history_role": "reused_model_development_2022_2024",
            "eligible_for_formal_freeze": bool(
                not allow_conditional_evidence
                and assessment.get("all_development_gates_pass", False)
            ),
            "eligible_for_formal_promotion": False,
            "final_holdout_opened": phase == "final",
            "model_family": "convertible_dominant",
            "model_version": (
                state.protocol["protocol_version"] + ":" + validation_code_hash[:16]
            ),
            "scope_version": "fundpos_v3_convertible_dominant_pilot_10",
            "truth_version": frame_hash(
                pd.concat(
                    [
                        asset_labels.assign(_truth_kind="asset"),
                        industry_labels.assign(_truth_kind="industry"),
                    ],
                    ignore_index=True,
                    sort=False,
                )
            ),
            "recalculation_code_hash": validation_code_hash,
        }
        if enough:
            saved = state.read() | {
                "protocol_hash": state.protocol_hash,
                "code_hash": validation_code_hash,
            }
            if phase == "development":
                saved["research_candidate"] = assessment.get("candidate")
                saved["development_gates"] = assessment
                saved["evidence_scope"] = (
                    "conditional" if allow_conditional_evidence else "strict"
                )
                if not allow_conditional_evidence and assessment.get(
                    "all_development_gates_pass"
                ):
                    saved["algorithm_frozen"] = assessment["candidate"]
            else:
                saved["final_decision"] = assessment
            atomic_json(state.path, saved)
        atomic_json(directory / "summary.json", result)
        return directory
