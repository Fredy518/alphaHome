from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from filelock import FileLock

from .config import Settings
from .constants import ASSETS, CATEGORIES, SW_CODES
from .data import DataBundle
from .errors import DataUnavailable, ProtocolError
from .factors import factor_panel
from .normalization import conditional_next_day_nav_availability
from .pipeline import advance_state, compute_date
from .pit import dates, eligible_universe, map_membership, require_unique
from .storage import atomic_json, atomic_parquet, code_fingerprint, file_hash, frame_hash


def choose_dates(calendar: pd.DatetimeIndex, start, end, frequency="daily") -> pd.DatetimeIndex:
    c = pd.DatetimeIndex(calendar).normalize().unique().sort_values()
    c = c[(c >= pd.Timestamp(start)) & (c <= pd.Timestamp(end))]
    if frequency == "daily":
        return c
    frame = pd.DataFrame({"date": c})
    periods = frame.date.dt.to_period("W-FRI" if frequency == "weekly" else "Q")
    return pd.DatetimeIndex(frame.groupby(periods).date.max())


def validation_calculation_dates(
    calendar: pd.DatetimeIndex,
    start,
    end,
    label_dates,
    *,
    cadence="daily",
) -> pd.DatetimeIndex:
    """Return the frozen validation cadence without fabricating observations.

    Candidate smoothing in development and selection may advance only on truth
    label dates.  Production observation remains daily and is validated in P6.
    """
    if cadence == "daily":
        return choose_dates(calendar, pd.Timestamp(start) - pd.Timedelta(days=90), end)
    if cadence != "prior_truth_label_date":
        raise ProtocolError(f"Unknown validation smoothing cadence: {cadence}")
    available_dates = set(pd.DatetimeIndex(calendar).normalize())
    selected = sorted(
        {
            pd.Timestamp(value).normalize()
            for value in label_dates
            if pd.Timestamp(value).normalize() in available_dates
        }
    )
    return pd.DatetimeIndex(selected)


def disclosed_labels(bundle: DataBundle, start, end, *, categories=None) -> pd.DataFrame:
    """Only called for an explicitly allowed validation interval.

    Later announcements are allowed in labels, never in prediction inputs. Complete
    stock NAV totals must already have passed the independent asset-allocation control.
    """
    h = dates(bundle["holdings"], ("report_date", "ann_date"))
    h = h.loc[
        (h.report_date >= pd.Timestamp(start))
        & (h.report_date <= pd.Timestamp(end))
        & h.report_date.dt.month.isin([6, 12])
    ]
    calendar = pd.DatetimeIndex(pd.to_datetime(bundle["calendar"].date)).sort_values()
    rows = []
    for (fund_code, report_date), group in h.groupby(["fund_code", "report_date"]):
        if not group.full_report_verified.fillna(False).all() or group.ann_date.isna().any():
            continue
        require_unique(group, ["security_code"], "validation holdings labels")
        try:
            mapped = map_membership(group, bundle["membership"], report_date,
                                    fallback=bundle["membership_fallback"])
        except DataUnavailable:
            continue
        trading = calendar[calendar <= report_date]
        if not len(trading):
            continue
        kwargs = {"categories": categories} if categories is not None else {}
        representatives = eligible_universe(
            bundle["funds"], bundle["classification"], trading[-1], **kwargs
        )
        if fund_code not in representatives.fund_code.values:
            continue
        weights = mapped.groupby("industry").weight.sum().reindex(ASSETS[2:], fill_value=0)
        rows.append(
            {
                "fund_code": fund_code,
                "valuation_date": str(trading[-1].date()),
                "report_date": str(report_date.date()),
                "label_ann_date": str(group.ann_date.max().date()),
                **weights.to_dict(),
                "stock_weight": float(weights.sum()),
                "label_source": "complete_holdings_reconciled_to_asset_report",
            }
        )
    return pd.DataFrame(rows)


def partial_industry_errors(predictions: pd.DataFrame, labels: pd.DataFrame) -> pd.DataFrame:
    """Conservative NAV L1 bounds; partial labels never become point truth."""
    keys = ["fund_code", "valuation_date"]
    require_unique(predictions, keys, "partial-label predictions")
    require_unique(labels, keys, "partial industry labels")
    if labels.empty:
        return pd.DataFrame()
    fields = [*SW_CODES, "unclassified_weight"]
    if (not np.isfinite(labels[fields].to_numpy(dtype=float)).all()
            or (labels[fields] < 0).any().any()):
        raise DataUnavailable("INVALID_PARTIAL_TRUTH", "Finite nonnegative known and unknown weights required")
    valid = predictions.loc[predictions.status.isin(["ok", "degraded"])]
    result = labels.merge(valid, on=keys, suffixes=("_true", "_estimated"), validate="one_to_one")
    if result.empty:
        return result
    actual = result[[f"{s}_true" for s in SW_CODES]].to_numpy(dtype=float)
    estimated = result[[f"{s}_estimated" for s in SW_CODES]].to_numpy(dtype=float)
    if not np.isfinite(estimated).all():
        raise DataUnavailable("INVALID_PARTIAL_PREDICTION", "Missing estimates cannot be zero-filled")
    distance_to_known = np.abs(estimated - actual).sum(axis=1)
    unknown = result.unclassified_weight.to_numpy(dtype=float)
    result["industry_l1_lower"] = np.maximum(0., distance_to_known - unknown)
    result["industry_l1_upper"] = distance_to_known + unknown
    result["meets_20pp_guaranteed"] = result.industry_l1_upper <= .2 + 1e-12
    result["fails_20pp_guaranteed"] = result.industry_l1_lower > .2 + 1e-12
    return result


def evaluate_predictions(
    predictions: pd.DataFrame, labels: pd.DataFrame, *, include_degraded=False
) -> tuple[dict, pd.DataFrame]:
    if labels.empty:
        return {
            "status": "blocked_data",
            "reason": "NO_COMPLETE_LABELS",
            "label_count": 0,
        }, pd.DataFrame()
    require_unique(labels, ["fund_code", "valuation_date"], "label universe")
    if labels[list(ASSETS[2:]) + ["stock_weight"]].isna().any().any():
        raise DataUnavailable(
            "INCOMPLETE_TRUTH", "Missing industry labels must be excluded, never zero-filled"
        )
    accepted = ("ok", "degraded") if include_degraded else ("ok",)
    valid = (
        predictions.loc[predictions.status.isin(accepted)].copy()
        if not predictions.empty
        else predictions
    )
    if valid.empty:
        return {
            "status": "blocked_data",
            "reason": "NO_VALID_PREDICTIONS",
            "label_count": len(labels),
            "coverage": 0.0,
        }, pd.DataFrame()
    require_unique(valid, ["fund_code", "valuation_date"], "valid estimates")
    matched = labels.merge(
        valid, on=["fund_code", "valuation_date"], how="inner", suffixes=("_true", "_estimated")
    )
    if matched.empty:
        return {
            "status": "blocked_data",
            "reason": "NO_VALID_PREDICTIONS",
            "label_count": len(labels),
            "coverage": 0.0,
        }, matched
    actual = matched[[f"{s}_true" for s in SW_CODES]].to_numpy(dtype=float)
    estimated = matched[[f"{s}_estimated" for s in SW_CODES]].to_numpy(dtype=float)
    matched["industry_l1"] = np.abs(actual - estimated).sum(axis=1)
    matched["stock_error"] = np.abs(matched.stock_weight_estimated - matched.stock_weight_true)
    matched["hk_error"] = np.abs(matched.hk_estimated - matched.hk_true)
    metrics = {
        "status": "evaluated",
        "scope": "conditional_diagnostic" if include_degraded else "strict",
        "label_count": len(labels),
        "evaluated_count": len(matched),
        "coverage": len(matched) / len(labels),
        "industry_l1_mean": float(matched.industry_l1.mean()),
        "industry_l1_p90": float(matched.industry_l1.quantile(0.9)),
        "stock_mae": float(matched.stock_error.mean()),
        "hk_mae": float(matched.hk_error.mean()),
    }
    return metrics, matched


def quarterly_labels(
    bundle: DataBundle,
    start,
    end,
    *,
    categories=None,
    selected_products: set[str] | None = None,
) -> pd.DataFrame:
    reports = dates(bundle["asset_reports"], ("report_date", "ann_date"))
    if reports.empty:
        return pd.DataFrame()
    reports = reports.loc[
        reports.ann_date.notna()
        & reports.stock_weight.notna()
        & reports.report_date.between(pd.Timestamp(start), pd.Timestamp(end))
    ].copy()
    calendar = pd.DatetimeIndex(bundle["calendar"].date).sort_values()
    rows = []
    for row in reports.itertuples(index=False):
        prior = calendar[calendar <= row.report_date]
        if len(prior):
            kwargs = {"categories": categories} if categories is not None else {}
            representatives = eligible_universe(
                bundle["funds"], bundle["classification"], prior[-1], **kwargs
            )
            allowed = row.fund_code in representatives.fund_code.values
            if selected_products is not None and allowed:
                allowed = bool(
                    representatives.loc[
                        representatives.fund_code.eq(row.fund_code), "master_code"
                    ].isin(selected_products).any()
                )
            if allowed:
                rows.append(
                    {
                        "fund_code": row.fund_code,
                        "valuation_date": str(prior[-1].date()),
                        "stock_weight": row.stock_weight,
                        "label_ann_date": str(row.ann_date.date()),
                    }
                )
    out = pd.DataFrame(rows)
    if not out.empty:
        require_unique(out, ["fund_code", "valuation_date"], "quarterly asset labels")
    return out


def quarterly_metrics(predictions, labels, *, include_degraded=False):
    if labels.empty or predictions.empty:
        return {"status": "blocked_data", "reason": "NO_QUARTERLY_COMPARISON"}
    accepted = ("ok", "degraded") if include_degraded else ("ok",)
    valid = predictions.loc[predictions.status.isin(accepted)]
    joined = labels.merge(
        valid, on=["fund_code", "valuation_date"], suffixes=("_true", "_estimated")
    )
    if joined.empty:
        return {"status": "blocked_data", "label_count": len(labels), "coverage": 0.0}
    error = abs(joined.stock_weight_true - joined.stock_weight_estimated)
    return {
        "status": "evaluated",
        "stock_mae": float(error.mean()),
        "stock_p90": float(error.quantile(0.9)),
        "count": len(joined),
        "label_count": len(labels),
        "coverage": len(joined) / len(labels),
    }


def grouped_metrics(errors, bundle: DataBundle):
    if errors.empty:
        return []
    data = errors.copy()
    data["size_group"] = pd.cut(
        data.aum,
        [-float("inf"), 1e9, 5e9, float("inf")],
        labels=["10亿以下", "10至50亿", "50亿以上"],
    ).astype(object)
    data["turnover_group"] = None
    turnover = dates(bundle["turnover"], ("report_date", "ann_date"))
    if not turnover.empty:
        for idx, row in data.iterrows():
            t = turnover.loc[
                (turnover.fund_code == row.fund_code)
                & (turnover.ann_date <= pd.Timestamp(row.information_cutoff))
                & (turnover.report_date < pd.Timestamp(row.valuation_date))
            ]
            if not t.empty:
                value = t.sort_values(["report_date", "ann_date"]).iloc[-1].turnover
                data.loc[idx, "turnover_group"] = (
                    "低于1倍" if value < 1 else ("1至3倍" if value < 3 else "3倍以上")
                )
    rows = []
    for dimension in ("category", "size_group", "turnover_group"):
        for name, frame in data.groupby(dimension, dropna=False):
            rows.append(
                {
                    "dimension": dimension,
                    "group": str(name) if pd.notna(name) else "未知",
                    "count": len(frame),
                    "industry_l1_mean": float(frame.industry_l1.mean()),
                    "industry_l1_p90": float(frame.industry_l1.quantile(0.9)),
                    "stock_mae": float(frame.stock_error.mean()),
                    "hk_mae": float(frame.hk_error.mean()),
                }
            )
    for day, frame in data.groupby("valuation_date"):
        for method in ("equal", "aum"):
            if method == "aum" and (frame.aum.isna().any() or frame.aum.sum() <= 0):
                continue
            weights = (
                pd.Series(1 / len(frame), index=frame.index)
                if method == "equal"
                else frame.aum / frame.aum.sum()
            )
            differences = [
                float(((frame[f"{asset}_estimated"] - frame[f"{asset}_true"]) * weights).sum())
                for asset in SW_CODES
            ]
            rows.append(
                {
                    "dimension": "group_bias",
                    "group": f"{day}:{method}",
                    "count": len(frame),
                    "industry_l1": sum(abs(v) for v in differences),
                    "signed_industry_bias": dict(zip(SW_CODES, differences)),
                }
            )
    return rows


def promotion_decision(baseline: dict, candidate: dict, threshold=0.1) -> dict:
    if any(metric.get("scope", "strict") != "strict" for metric in (baseline, candidate)):
        return {"decision": "INSUFFICIENT_EVIDENCE", "promoted": False}
    if baseline.get("status") != "evaluated" or candidate.get("status") != "evaluated":
        return {"decision": "INSUFFICIENT_EVIDENCE", "promoted": False}
    old = baseline["industry_l1_mean"]
    improvement = (old - candidate["industry_l1_mean"]) / old if old > 0 else 0.0
    tests = {
        "industry_improvement": improvement >= threshold,
        "stock_mae": candidate["stock_mae"] <= baseline["stock_mae"],
        "hk_mae": candidate["hk_mae"] <= baseline["hk_mae"],
        "industry_p90": candidate["industry_l1_p90"] <= baseline["industry_l1_p90"],
        "coverage": candidate["coverage"] >= baseline["coverage"],
    }
    return {
        "decision": "PROMOTE" if all(tests.values()) else "KEEP_BASELINE",
        "promoted": all(tests.values()),
        "industry_improvement": improvement,
        "tests": tests,
    }


class ValidationState:
    def __init__(
        self,
        settings: Settings,
        *,
        protocol_filename="validation_protocol.json",
        state_subdir="validation",
    ):
        self.settings = settings
        self.directory = settings.path("data_dir") / state_subdir
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = self.directory / "state.json"
        self.protocol_path = settings.root / "config" / protocol_filename
        self.protocol = json.loads(self.protocol_path.read_text(encoding="utf-8"))
        self.protocol_hash = file_hash(self.protocol_path)

    def read(self):
        return json.loads(self.path.read_text(encoding="utf-8")) if self.path.exists() else {}

    def assert_open_allowed(self, phase: str):
        state = self.read()
        if state.get("final_opened_at"):
            raise ProtocolError(
                "Final holdout was already opened; this protocol is closed to further tuning/reopening"
            )
        if state and state.get("protocol_hash") != self.protocol_hash:
            raise ProtocolError("Protocol fingerprint changed")
        if phase == "selection" and not state.get("development_complete"):
            raise ProtocolError("Complete development data validation first")
        if phase == "final":
            if not state.get("selection_frozen"):
                raise ProtocolError("Freeze selection before opening the final holdout")
            if state.get("code_hash") != code_fingerprint(self.settings.root):
                raise ProtocolError("Implementation changed after selection freeze")
            if not state.get("selection_assessment", {}).get(
                "all_selection_gates_pass", False
            ):
                raise ProtocolError("Selection gates must all pass before final")

    def mark_final_opened(self, input_hash: str):
        self.assert_open_allowed("final")
        state = self.read()
        state.update(
            final_opened_at=pd.Timestamp.now(tz="Asia/Shanghai").isoformat(),
            final_input_hash=input_hash,
        )
        atomic_json(self.path, state)


def run_validation(
    settings: Settings,
    bundle: DataBundle,
    phase: str,
    progress=print,
    *,
    state: ValidationState | None = None,
    categories=None,
    selected_products: set[str] | None = None,
    output_subdir="validation",
    require_full_universe=True,
    extra_prerequisites: list[dict] | None = None,
    allow_conditional_evidence=False,
) -> Path:
    state = state or ValidationState(settings)
    protocol = state.protocol
    directory = settings.path("output_dir") / output_subdir / phase
    directory.mkdir(parents=True, exist_ok=True)
    with FileLock(str(state.directory / ".lock"), timeout=0):
        state.assert_open_allowed(phase)
        source_input_hash = bundle.fingerprint
        conditional_nav_rows = 0
        if allow_conditional_evidence:
            frames = {name: bundle[name] for name in bundle.frames}
            frames["nav"] = conditional_next_day_nav_availability(bundle["nav"])
            conditional_nav_rows = int(
                frames["nav"].get(
                    "nav_availability_conditional",
                    pd.Series(False, index=frames["nav"].index),
                ).fillna(False).sum()
            )
            bundle = DataBundle(
                frames,
                {
                    **bundle.provenance,
                    "conditional_nav_availability": (
                        "next_calendar_day_only_for_vendor_periodic_ann_date"
                    ),
                },
            )
        start, end = protocol[phase]
        if bundle.provenance.get("synthetic"):
            raise ProtocolError("Synthetic fixtures cannot enter the formal research protocol")
        calendar = pd.DatetimeIndex(pd.to_datetime(bundle["calendar"].date)).sort_values()
        panel = factor_panel(bundle["factors"], end, calendar)
        # Inspect only input coverage before any holdout label access.
        interval = panel.loc[
            panel.index.to_series().between(pd.Timestamp(start), pd.Timestamp(end))
        ]
        missing = interval.columns[interval.notna().sum() < protocol["window"]].tolist()
        prerequisites = list(extra_prerequisites or [])
        evidence_warnings = []
        if allow_conditional_evidence:
            conditional_codes = {
                "CONDITIONAL_HISTORICAL_TRACKING_INDEX",
                "NO_VERIFIED_HISTORICAL_CONTRACTS",
            }
            evidence_warnings = [
                item for item in prerequisites if item.get("code") in conditional_codes
            ]
            prerequisites = [
                item for item in prerequisites if item.get("code") not in conditional_codes
            ]
            if conditional_nav_rows:
                evidence_warnings.append(
                    {
                        "code": "CONDITIONAL_NAV_AVAILABILITY",
                        "affected_rows": conditional_nav_rows,
                        "rule": "next_calendar_day_for_vendor_periodic_ann_date",
                    }
                )
        if missing:
            prerequisites.append({"code": "MISSING_FACTOR_HISTORY", "assets": missing})
        verified_contracts = not bundle["constraints"].empty
        if verified_contracts and "verified" in bundle["constraints"]:
            verified_contracts = bundle["constraints"].verified.fillna(False).any()
        if not verified_contracts:
            item = {"code": "NO_VERIFIED_HISTORICAL_CONTRACTS"}
            (evidence_warnings if allow_conditional_evidence else prerequisites).append(item)
        if prerequisites:
            atomic_json(
                directory / "summary.json",
                {
                    "status": "blocked_data",
                    "phase": phase,
                    "prerequisites": prerequisites,
                    "final_holdout_opened": False,
                    "input_hash": bundle.fingerprint,
                    "source_input_hash": source_input_hash,
                    "protocol_hash": state.protocol_hash,
                    "evidence_scope": (
                        "conditional" if allow_conditional_evidence else "strict"
                    ),
                    "evidence_warnings": evidence_warnings,
                },
            )
            return directory
        if phase == "final":
            # Atomically mark the final sample used BEFORE reading its labels or metrics.
            state.mark_final_opened(bundle.fingerprint)
        labels = disclosed_labels(bundle, start, end, categories=categories)
        if selected_products is not None and not labels.empty:
            masters = bundle["funds"].set_index("fund_code").master_code
            labels = labels.loc[
                labels.fund_code.map(masters).isin(selected_products)
            ].reset_index(drop=True)
        stock_labels = quarterly_labels(
            bundle,
            start,
            end,
            categories=categories,
            selected_products=selected_products,
        )
        if labels.empty:
            atomic_json(
                directory / "summary.json",
                {"status": "blocked_data", "reason": "NO_COMPLETE_LABELS", "phase": phase},
            )
            return directory
        configurations = [
            {"name": "index", "prior_penalty": 0.0, "smooth_penalty": 0.0},
            {"name": "personalized", "prior_penalty": 0.0, "smooth_penalty": 0.0},
            *protocol["candidates"],
        ]
        if phase == "final":
            selected = state.read()["selection_frozen"]
            configurations = [c for c in configurations if c["name"] in ("personalized", selected)]
        label_dates = set(labels.valuation_date) | (
            set(stock_labels.valuation_date) if not stock_labels.empty else set()
        )
        smoothing_cadence = protocol.get("validation_smoothing_cadence", "daily")
        calculation_dates = validation_calculation_dates(
            calendar,
            start,
            end,
            label_dates,
            cadence=smoothing_cadence,
        )
        prediction_sets = {}
        individual_metrics = {}
        quarterly_by_model = {}
        group_by_model = {}
        all_flags = []
        tracking_by_model = {}
        for config in configurations:
            name = config["name"]
            model_settings = settings.with_model(
                **config, window=protocol["window"], weighting=protocol["weighting"]
            )
            previous = None
            records = []
            for day in calculation_dates:
                if not config["smooth_penalty"] and str(day.date()) not in label_dates:
                    continue
                result = compute_date(
                    model_settings,
                    bundle,
                    day,
                    day + pd.Timedelta(days=1),
                    previous,
                    categories=categories if categories is not None else CATEGORIES,
                    selected_products=selected_products,
                )
                if not result.empty:
                    previous = advance_state(previous, result)
                    if str(day.date()) in label_dates:
                        records.append(result)
            predictions = pd.concat(records, ignore_index=True) if records else pd.DataFrame()
            prediction_sets[name] = predictions
            metrics, errors = evaluate_predictions(
                predictions, labels, include_degraded=allow_conditional_evidence
            )
            individual_metrics[name] = metrics
            quarterly_by_model[name] = quarterly_metrics(
                predictions,
                stock_labels,
                include_degraded=allow_conditional_evidence,
            )
            group_by_model[name] = grouped_metrics(errors, bundle)
            if categories == ("增强指数型",):
                from .enhanced_index import evaluate_tracking_index_prior

                tracking_by_model[name], tracking_errors = evaluate_tracking_index_prior(
                    predictions, labels
                )
                atomic_parquet(
                    directory / f"{name}_tracking_index_errors.parquet", tracking_errors
                )
            atomic_parquet(directory / f"{name}_predictions.parquet", predictions)
            atomic_parquet(directory / f"{name}_errors.parquet", errors)
            if not predictions.empty:
                all_flags.extend(
                    predictions.loc[
                        predictions.status != "ok", ["fund_code", "valuation_date", "reason"]
                    ]
                    .assign(model=name)
                    .to_dict("records")
                )
            progress(f"validation {phase} {name}: {metrics['status']}", flush=True)
        # Compare on the exact same fund/date labels; do not reward candidate sample attrition.
        common_keys = None
        for predictions in prediction_sets.values():
            accepted = ("ok", "degraded") if allow_conditional_evidence else ("ok",)
            valid = (
                predictions.loc[predictions.status.isin(accepted)]
                if not predictions.empty
                else predictions
            )
            keys = set(zip(valid.fund_code, valid.valuation_date)) if not valid.empty else set()
            common_keys = keys if common_keys is None else common_keys.intersection(keys)
        common_labels = labels.loc[
            [key in common_keys for key in zip(labels.fund_code, labels.valuation_date)]
        ]
        metrics_by_model = {
            name: evaluate_predictions(
                preds, common_labels, include_degraded=allow_conditional_evidence
            )[0]
            for name, preds in prediction_sets.items()
        }
        for name, metrics in metrics_by_model.items():
            # Error metrics use common labels; coverage keeps the original label denominator.
            metrics["coverage"] = individual_metrics[name].get("coverage", 0.0)
        enough = bool(
            len(common_labels) >= 10
            and common_labels.valuation_date.nunique() >= 2
            and all(q.get("status") == "evaluated" for q in quarterly_by_model.values())
            and (
                not require_full_universe
                or bundle.provenance.get("universe_scope") == "all_active_equity_metadata"
            )
        )
        result = {
            "phase": phase,
            "status": (
                "conditional_complete" if enough and allow_conditional_evidence
                else ("complete" if enough else "blocked_data")
            ),
            "protocol_hash": state.protocol_hash,
            "input_hash": bundle.fingerprint,
            "source_input_hash": source_input_hash,
            "label_hash": frame_hash(labels),
            "metrics": metrics_by_model,
            "individual_coverage_metrics": individual_metrics,
            "common_label_count": len(common_labels),
            "quarterly_stock_metrics": quarterly_by_model,
            "grouped_metrics": group_by_model,
            "tracking_index_comparison": tracking_by_model,
            "minimum_comparable_labels": 10,
            "minimum_report_dates": 2,
            "unavailable": all_flags,
            "missing_group_dimensions": ["turnover"] if bundle["turnover"].empty else [],
            "evidence_scope": "conditional" if allow_conditional_evidence else "strict",
            "evidence_warnings": evidence_warnings,
            "eligible_for_formal_promotion": not allow_conditional_evidence,
            "smoothing_validation_cadence": smoothing_cadence,
            "final_holdout_opened": phase == "final",
        }
        validation_code_hash = code_fingerprint(settings.root)
        result.update(
            model_family="enhanced_index_equity",
            model_version=(
                "fundpos_v3_enhanced_index_candidates_"
                + validation_code_hash[:16]
            ),
            scope_version="fundpos_v3_enhanced_index_pilot_20",
            truth_version=frame_hash(
                pd.concat(
                    [
                        labels.assign(_truth_kind="industry"),
                        stock_labels.assign(_truth_kind="quarterly_stock"),
                    ],
                    ignore_index=True,
                    sort=False,
                )
            ),
            recalculation_code_hash=validation_code_hash,
        )
        saved = state.read()
        if enough:
            saved.update(
                protocol_hash=state.protocol_hash, code_hash=validation_code_hash
            )
            if phase == "development":
                saved["development_complete"] = True
            elif phase == "selection":
                candidates = [c["name"] for c in protocol["candidates"]]
                selected = min(
                    candidates, key=lambda name: (metrics_by_model[name]["industry_l1_mean"], name)
                )
                saved["selection_frozen"] = selected
                saved["selection_input_hash"] = bundle.fingerprint
                saved["selection_metrics"] = metrics_by_model
                result["selected_candidate"] = selected
                baseline = metrics_by_model["personalized"]
                candidate = metrics_by_model[selected]
                old_error = baseline["industry_l1_mean"]
                improvement = (
                    (old_error - candidate["industry_l1_mean"]) / old_error
                    if old_error > 0
                    else 0.0
                )
                tests = {
                    "industry_improvement": improvement
                    >= protocol.get("acceptance", {}).get(
                        "industry_l1_relative_improvement", 0.10
                    ),
                    "stock_mae_not_worse": candidate["stock_mae"]
                    <= baseline["stock_mae"],
                    "hk_mae_not_worse": candidate["hk_mae"] <= baseline["hk_mae"],
                    "industry_tail_not_worse": candidate["industry_l1_p90"]
                    <= baseline["industry_l1_p90"],
                    "coverage_not_worse": candidate["coverage"] >= baseline["coverage"],
                }
                result["selection_assessment"] = {
                    "selected_candidate": selected,
                    "industry_improvement": improvement,
                    "tests": tests,
                    "all_selection_gates_pass": all(tests.values()),
                    "conditional_ready_for_final_holdout": False,
                }
                saved["selection_assessment"] = result["selection_assessment"]
                saved["selection_evidence_scope"] = (
                    "conditional" if allow_conditional_evidence else "strict"
                )
            else:
                selected = saved["selection_frozen"]
                decision = promotion_decision(
                    metrics_by_model["personalized"],
                    metrics_by_model[selected],
                    protocol.get("promotion", protocol.get("acceptance", {}))[
                        "industry_l1_relative_improvement"
                    ],
                )
                quarter_base, quarter_candidate = (
                    quarterly_by_model["personalized"],
                    quarterly_by_model[selected],
                )
                quarterly_pass = (
                    quarter_candidate["stock_mae"] <= quarter_base["stock_mae"]
                    and quarter_candidate["coverage"] >= quarter_base["coverage"]
                )
                decision["tests"]["quarterly_stock_mae_and_coverage"] = quarterly_pass
                if not quarterly_pass:
                    decision.update(promoted=False, decision="KEEP_BASELINE")
                result["promotion"] = decision
                saved["final_decision"] = decision
            atomic_json(state.path, saved)
        atomic_json(directory / "summary.json", result)
        return directory
