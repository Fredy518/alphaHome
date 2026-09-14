from __future__ import annotations

import json
from dataclasses import fields
from pathlib import Path

import pandas as pd
from filelock import FileLock

from .aggregation import aggregate_estimates
from .config import Settings
from .constants import ASSETS, CATEGORIES, CATEGORY_BOUNDS
from .data import DataBundle
from .enhanced_index import tracking_index_industry_prior
from .errors import DataUnavailable
from .estimation import InvestmentConstraints, estimate_weights
from .factors import align_window, factor_panel, personalized_panel
from .normalization import aum_at_date
from .pit import (
    available,
    available_nav,
    dates,
    eligible_universe,
    last_full_holdings,
    map_membership,
)
from .reconstruction import market_cap_bridge, reconstruct_portfolio
from .storage import atomic_json, atomic_parquet, code_fingerprint, frame_hash, git_revision


def select_constraints(
    bundle: DataBundle, fund, cutoff, valuation_date=None
) -> InvestmentConstraints:
    constraints = bundle["constraints"]
    if not constraints.empty:
        constraints = dates(constraints, ("ann_date", "effective_date"))
        constraints = available(constraints.loc[constraints.fund_code == fund.fund_code], cutoff)
        if "verified" in constraints:
            constraints = constraints.loc[constraints.verified.fillna(False)]
        constraints = constraints.loc[
            constraints.effective_date <= pd.Timestamp(valuation_date or cutoff)
        ]
        if not constraints.empty:
            latest = constraints.sort_values(["effective_date", "ann_date"]).iloc[-1]
            required = ("stock_lower", "stock_upper", "source")
            if any(key not in latest or pd.isna(latest[key]) for key in required) or not any(
                key in latest and pd.notna(latest[key])
                for key in ("hk_upper_nav", "hk_upper_equity")
            ):
                raise DataUnavailable(
                    "INCOMPLETE_CONTRACT",
                    "Verified bounds require stock limits, HK denominator and source",
                )
            values = {
                f.name: latest[f.name]
                for f in fields(InvestmentConstraints)
                if f.name in latest and pd.notna(latest[f.name])
            }
            return InvestmentConstraints(**values)
    lower, upper = CATEGORY_BOUNDS[fund.category]
    # Category bounds are declared approximations, never represented as parsed fund contracts.
    return InvestmentConstraints(
        stock_lower=lower, stock_upper=upper, source="category_default_unverified_contract"
    )


def asof_aum(bundle: DataBundle, fund, valuation_date, cutoff, max_age=200):
    if getattr(fund, "family_metadata_missing", False):
        return None, None
    result = aum_at_date(bundle["nav"], bundle["financial_reports"], list(fund.share_codes),
                         valuation_date, cutoff, max_age)
    return result["aum"], result["aum_date"]


def build_holdings(
    bundle: DataBundle, fund_code: str, cutoff, valuation_date=None, *, max_unknown_weight=0.0
) -> tuple[pd.DataFrame, dict]:
    full = last_full_holdings(bundle["holdings"], fund_code, cutoff)
    report_date = full.report_date.iloc[0]
    mapped = map_membership(full, bundle["membership"], valuation_date or report_date,
                            fallback=bundle["membership_fallback"], allow_unknown=True)
    unknown_weight = float(mapped.loc[mapped.industry == "unknown_a", "weight"].sum())
    if unknown_weight > max_unknown_weight + 1e-12:
        raise DataUnavailable("UNMAPPED_HOLDINGS", f"unclassified_A_NAV_weight={unknown_weight:.8f}")
    mapped["is_proxy"] = False
    metadata = {
        "holdings_report_date": str(report_date.date()),
        "holdings_ann_date": str(full.ann_date.max().date()),
        "holdings_mode": "last_verified_full_report",
        "unclassified_holdings_weight": unknown_weight,
        "membership_gap_fill_weight": float(mapped.loc[
            mapped.industry_source == "historical_SWHY_gap_fill", "weight"].sum()),
        "holdings_weight_basis": str(full.weight_basis.iloc[0]) if "weight_basis" in full else "source_declared",
    }
    allocations = available(dates(bundle["allocations"], ("report_date", "ann_date")), cutoff)
    if allocations.empty:
        return mapped, metadata
    allocations = allocations.loc[
        (allocations.fund_code == fund_code) & (allocations.report_date > report_date)
    ]
    if allocations.empty:
        return mapped, metadata
    latest = allocations.report_date.max()
    alloc = allocations.loc[allocations.report_date == latest]
    if (
        "allocation_complete_verified" not in alloc
        or not alloc.allocation_complete_verified.fillna(False).all()
    ):
        control = bundle["asset_reports"]
        control = dates(control, ("report_date",))
        if control.empty:
            raise DataUnavailable(
                "INCOMPLETE_INDUSTRY_DISCLOSURE", "No control for the complete industry report"
            )
        control = control.loc[(control.fund_code == fund_code) & (control.report_date == latest)]
        if (
            len(control) != 1
            or pd.isna(control.stock_weight.iloc[0])
            or abs(alloc.weight.sum() - control.stock_weight.iloc[0]) > 0.001
        ):
            raise DataUnavailable(
                "INCOMPLETE_INDUSTRY_DISCLOSURE",
                "Industry totals must reconcile to all equity, including HK",
            )
    stock_groups = dates(bundle["stock_groups"], ("date", "ann_date"))
    if stock_groups.empty or "group" not in alloc:
        metadata["quarterly_update_unavailable"] = "No verified same-date disclosure-to-SW bridge"
        return mapped, metadata
    crosswalk = stock_groups.loc[stock_groups.date == latest]
    if "ann_date" in crosswalk:
        crosswalk = available(crosswalk, cutoff)
    if crosswalk.empty:
        metadata["quarterly_update_unavailable"] = "No same-date crosswalk"
        return mapped, metadata
    current = available(dates(bundle["holdings"], ("report_date", "ann_date")), cutoff)
    current = current.loc[
        (current.fund_code == fund_code) & (current.report_date == latest) & (current.rank_no <= 10)
    ]
    if current.empty:
        metadata["quarterly_update_unavailable"] = "No published top holdings"
        return mapped, metadata
    if crosswalk.duplicated("security_code").any():
        raise DataUnavailable("AMBIGUOUS_CROSSWALK", "Stock group mapping is not unique")
    previous = mapped.drop(columns=["industry", "group"], errors="ignore").merge(
        crosswalk[["security_code", "group", "industry"]],
        on="security_code",
        how="left",
        validate="one_to_one",
    )
    current = current.merge(
        crosswalk[["security_code", "group", "industry"]],
        on="security_code",
        how="left",
        validate="one_to_one",
    )
    if previous.group.isna().any() or current.group.isna().any():
        raise DataUnavailable(
            "CROSSWALK_GAPS", "Prior/current holdings cannot be mapped at the quarter date"
        )
    reconstructed = reconstruct_portfolio(
        previous, current, alloc[["group", "weight"]], market_cap_bridge(crosswalk)
    )
    if valuation_date is not None:
        actual = reconstructed.loc[~reconstructed.is_proxy]
        mapped_actual = map_membership(actual, bundle["membership"], valuation_date,
                                       fallback=bundle["membership_fallback"])
        reconstructed = pd.concat(
            [mapped_actual, reconstructed.loc[reconstructed.is_proxy]], ignore_index=True
        )
    metadata.update(
        holdings_report_date=str(latest.date()),
        holdings_ann_date=str(max(alloc.ann_date.max(), current.ann_date.max()).date()),
        holdings_mode="quarterly_reconstruction",
    )
    return reconstructed, metadata


def compute_date(
    settings: Settings,
    bundle: DataBundle,
    valuation_date,
    cutoff,
    previous_results: pd.DataFrame | None = None,
    *,
    categories=CATEGORIES,
    selected_products: set[str] | None = None,
) -> pd.DataFrame:
    target = pd.Timestamp(valuation_date).normalize()
    cutoff = pd.Timestamp(cutoff).normalize()
    if cutoff < target:
        raise DataUnavailable("INVALID_CUTOFF", "Information cutoff precedes NAV date")
    model = settings.values["model"]
    universe = eligible_universe(
        bundle["funds"],
        bundle["classification"],
        target,
        settings.values["universe"]["min_age_days"],
        categories=categories,
    )
    if selected_products is not None:
        universe = universe.loc[universe.master_code.isin(selected_products)]
    calendar = pd.DatetimeIndex(pd.to_datetime(bundle["calendar"].date)).sort_values().unique()
    base = factor_panel(bundle["factors"], cutoff, calendar)
    # Build lookups once; never copy an entire market NAV/price panel for every fund.
    grouped = {
        name: {
            code: frame
            for code, frame in bundle.frames.get(name, pd.DataFrame()).groupby(
                "fund_code", sort=False
            )
        }
        for name in ("nav", "holdings", "allocations", "constraints", "financial_reports")
        if "fund_code" in bundle.frames.get(name, pd.DataFrame())
    }
    prices = bundle.frames.get("prices", pd.DataFrame())
    # Repeated intersection on a non-unique market index scans millions of quotes
    # for every fund. Group once, and never copy stock prices for the index model.
    price_groups = (
        {code: frame for code, frame in prices.groupby("security_code", sort=False)}
        if model["name"] != "index" and "security_code" in prices
        else {}
    )
    results = []
    for fund in universe.itertuples(index=False):
        local = dict(bundle.frames)
        for name, lookup in grouped.items():
            codes = fund.share_codes if name in ("nav", "financial_reports") else [fund.fund_code]
            parts = [lookup[code] for code in codes if code in lookup]
            local[name] = (
                pd.concat(parts, ignore_index=True) if parts else bundle.frames[name].iloc[:0]
            )
        held = local.get("holdings", pd.DataFrame())
        if not held.empty and price_groups:
            pieces = [price_groups[code] for code in held.security_code.unique()
                      if code in price_groups]
            local["prices"] = pd.concat(pieces, ignore_index=True) if pieces else prices.iloc[:0]
        fund_bundle = DataBundle(local, bundle.provenance)
        row = {
            "fund_code": fund.fund_code,
            "master_code": fund.master_code,
            "fund_name": fund.fund_name,
            "category": fund.category,
            "valuation_date": str(target.date()),
            "information_cutoff": str(cutoff.date()),
            "model": model["name"],
            "model_family": "equity",
            "status": "unavailable",
            "reason": None,
            **{a: None for a in ASSETS},
        }
        if fund.category == "增强指数型":
            try:
                _, tracking_meta = tracking_index_industry_prior(
                    fund_bundle, fund.fund_code, target, cutoff
                )
                status = (
                    "available_point_in_time"
                    if tracking_meta.get("tracking_index_strict_pit", True)
                    else "available_conditional"
                )
                row.update(tracking_meta, tracking_index_status=status)
            except DataUnavailable as exc:
                row.update(
                    tracking_index_status="unavailable",
                    tracking_index_reason=exc.code,
                )
        if getattr(fund, "family_metadata_missing", False):
            row.update(aum=None, aum_date=None, aum_reason="MISSING_MASTER_METADATA")
        else:
            row.update(aum_at_date(fund_bundle["nav"], fund_bundle["financial_reports"],
                list(fund.share_codes), target, cutoff,
                settings.values["publication"]["max_aum_age_days"]))
        try:
            bounds = select_constraints(fund_bundle, fund, cutoff, target)
            inactive = ("hk",) if bounds.hk_upper_nav == 0 or bounds.hk_upper_equity == 0 else ()
            nav = fund_bundle["nav"]
            nav = nav.loc[nav.fund_code == fund.fund_code]
            # Check freshness/alignment before spending time constructing personal factors.
            align_window(
                nav,
                base,
                calendar,
                target,
                cutoff,
                window=model["window"],
                max_calendar_days=model["max_window_calendar_days"],
                inactive_assets=inactive,
            )
            panel = base
            prior = None
            extras = {"proxy_ratio": 1.0, "holdings_mode": "index_only"}
            if model["name"] != "index":
                portfolio, holding_meta = build_holdings(
                    fund_bundle, fund.fund_code, cutoff, target,
                    max_unknown_weight=model.get("max_unclassified_nav_weight", 0.001)
                )
                age = (target - pd.Timestamp(holding_meta["holdings_report_date"])).days
                if age > model["max_holdings_age_days"]:
                    raise DataUnavailable("STALE_HOLDINGS", f"report_age_days={age}")
                n = available_nav(dates(nav, ("date", "ann_date")), cutoff)
                first_date = (
                    n.loc[n.date <= target].sort_values("date").tail(model["window"] + 1).date.min()
                )
                needed = base.loc[(base.index >= first_date) & (base.index <= target)]
                panel, extras, prior = personalized_panel(
                    needed,
                    portfolio,
                    fund_bundle["prices"],
                    holding_meta["holdings_report_date"],
                    cutoff=cutoff,
                )
                extras.update(holding_meta, holdings_age_days=age)
            x, y, timing = align_window(
                nav,
                panel,
                calendar,
                target,
                cutoff,
                window=model["window"],
                max_calendar_days=model["max_window_calendar_days"],
                inactive_assets=inactive,
            )
            previous = None
            if previous_results is not None and not previous_results.empty:
                old = previous_results.loc[
                    (previous_results.master_code == fund.master_code)
                    & (previous_results.valuation_date < str(target.date()))
                    & (previous_results.information_cutoff <= str(cutoff.date()))
                    & (previous_results.model == model["name"])
                    & previous_results.status.isin(["ok", "degraded"])
                ]
                if not old.empty:
                    previous = (
                        old.sort_values("valuation_date").iloc[-1].reindex(ASSETS).astype(float)
                    )
            smoothing = model["smooth_penalty"]
            # Explicit candidate initialization: prior on the first date; no hidden future state.
            if smoothing and previous is None and prior is not None:
                previous = prior
                extras["smoothing_initialization"] = "published_equity_prior_cash_residual"
            weights, diagnostics = estimate_weights(
                x,
                y,
                bounds,
                prior=prior,
                previous=previous,
                prior_penalty=model["prior_penalty"],
                smooth_penalty=smoothing,
                weighting=model["weighting"],
                tolerance=model["constraint_tolerance"],
            )
            flags = []
            if bounds.source == "category_default_unverified_contract":
                flags.append("CONTRACT_NOT_VERIFIED")
            if fund.category == "增强指数型":
                if row.get("tracking_index_status") == "available_conditional":
                    flags.append("TRACKING_INDEX_CONDITIONAL_EVIDENCE")
                elif row.get("tracking_index_status") != "available_point_in_time":
                    flags.append("HISTORICAL_TRACKING_INDEX_UNAVAILABLE")
            if getattr(fund, "family_metadata_missing", False):
                flags.append("MISSING_MASTER_METADATA")
            if model["name"] != "index" and extras["proxy_ratio"] > model["max_proxy_weight"]:
                flags.append("HIGH_PROXY_RATIO")
            if extras.get("quarterly_update_unavailable"):
                flags.append("QUARTERLY_UPDATE_UNAVAILABLE")
            if extras.get("unclassified_holdings_weight", 0) > 0:
                flags.append("UNCLASSIFIED_SMALL_A_HOLDINGS")
            if "nav_announcement_assumed" in nav:
                used_nav = available_nav(nav, cutoff)
                used_nav = used_nav.loc[(used_nav.date >= pd.Timestamp(timing["window_start"]))
                                         & (used_nav.date <= target)]
                if used_nav.nav_announcement_assumed.any():
                    flags.append("NAV_AVAILABILITY_ASSUMED")
            row.update(
                weights.to_dict(),
                **timing,
                **diagnostics,
                **extras,
                status="degraded" if flags else "ok",
                reason=";".join(flags) or None,
                stock_weight=float(weights.iloc[2:].sum()),
                a_stock_weight=float(weights.iloc[3:].sum()),
                non_equity=float(weights.iloc[:2].sum()),
                method_used=model["name"],
                weight_change_l1=float(abs(weights - previous).sum())
                if previous is not None
                else None,
            )
            row["proxy_industries"] = json.dumps(
                extras.get("proxy_industries", []), ensure_ascii=False
            )
        except DataUnavailable as exc:
            row.update(reason=exc.code, error_detail=exc.detail)
        except Exception as exc:
            # Unexpected errors remain visible and are distinguished from ordinary gaps.
            row.update(reason="INTERNAL_ERROR", error_detail=f"{type(exc).__name__}: {exc}")
        results.append(row)
    return pd.DataFrame(results)


def advance_state(previous: pd.DataFrame | None, result: pd.DataFrame) -> pd.DataFrame:
    """Keep the latest actually estimated state per fund across failed days; never fill outputs."""
    valid = result.loc[result.status.isin(["ok", "degraded"])].copy()
    if previous is None or previous.empty:
        return valid
    if valid.empty:
        return previous
    old = previous.loc[~previous.master_code.isin(valid.master_code)]
    return valid if old.empty else pd.concat([old, valid], ignore_index=True)


def run_estimate(
    settings: Settings,
    bundle: DataBundle,
    valuation_date,
    cutoff,
    previous_results: pd.DataFrame | None = None,
    *,
    publish=True,
    categories=CATEGORIES,
    selected_products: set[str] | None = None,
    scope_version="configured",
) -> Path:
    output = settings.path("output_dir")
    output.mkdir(parents=True, exist_ok=True)
    digest = bundle.fingerprint
    code = code_fingerprint(settings.root)
    model = settings.values["model"]
    day = str(pd.Timestamp(valuation_date).date())
    import hashlib

    state_hash = frame_hash(previous_results) if previous_results is not None else "none"
    identity = hashlib.sha256(
        f"{settings.fingerprint}|{digest}|{code}|{cutoff}|{state_hash}|"
        f"{tuple(categories)}|{sorted(selected_products or [])}|{scope_version}".encode()
    ).hexdigest()
    name = f"{day}_{model['name']}_{identity[:24]}"
    directory = output / "runs" / name
    with FileLock(str(output / ".run.lock"), timeout=0):
        done = directory / "manifest.json"
        if done.exists():
            return directory
        directory.mkdir(parents=True, exist_ok=True)
        results = compute_date(
            settings,
            bundle,
            valuation_date,
            cutoff,
            previous_results,
            categories=categories,
            selected_products=selected_products,
        )
        if results.empty:
            raise DataUnavailable("EMPTY_UNIVERSE", "No eligible funds at the requested date")
        aggregates = aggregate_estimates(
            results,
            count_threshold=settings.values["publication"]["count_coverage"],
            aum_threshold=settings.values["publication"]["aum_coverage"],
        )
        if bundle.provenance.get("universe_scope") != "all_active_equity_metadata":
            aggregates["category"] = aggregates.category.replace({"全部主动权益": "指定样本合计"})
        atomic_parquet(directory / "estimates.parquet", results)
        atomic_parquet(directory / "aggregates.parquet", aggregates)
        source_dates = {}
        for table, frame in bundle.frames.items():
            for col in ("date", "report_date", "ann_date"):
                if col in frame:
                    dt = pd.to_datetime(frame[col], errors="coerce")
                    source_dates[f"{table}.{col}"] = (
                        str(dt.max().date()) if dt.notna().any() else None
                    )
        status_counts = {str(k): int(v) for k, v in results.status.value_counts().items()}
        reason_counts = {str(k): int(v) for k, v in results.reason.dropna().value_counts().items()}
        manifest = {
            "run_id": name,
            "model_family": "equity",
            "scope_version": scope_version,
            "created_at": pd.Timestamp.now(tz="Asia/Shanghai").isoformat(),
            "valuation_date": day,
            "information_cutoff": str(pd.Timestamp(cutoff).date()),
            "configuration": settings.values,
            "config_hash": settings.fingerprint,
            "input_hash": digest,
            "code_hash": code,
            "git_revision": git_revision(settings.root),
            "previous_state_hash": state_hash,
            "source_dates": source_dates,
            "provenance": bundle.provenance,
            "status_counts": status_counts,
            "reason_counts": reason_counts,
            "complete": bool((aggregates.status == "complete").all()),
            "history_kind": "announcement_date_reconstruction",
        }
        manifest["formal_publication"] = bool(
            manifest["complete"]
            and bundle.provenance.get("universe_scope") == "all_active_equity_metadata"
            and not bundle.provenance.get("synthetic")
        )
        atomic_json(done, manifest)
        if publish:
            publication_dir = output
            if bundle.provenance.get(
                "universe_scope"
            ) != "all_active_equity_metadata" and not bundle.provenance.get("synthetic"):
                scope = hashlib.sha256("|".join(sorted(results.master_code)).encode()).hexdigest()[
                    :12
                ]
                publication_dir = output / "scopes" / scope
            for pointer in ("latest_attempt", "latest_complete"):
                if pointer == "latest_complete" and not manifest["complete"]:
                    continue
                path = publication_dir / f"{pointer}.json"
                old = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
                if old.get("valuation_date", "") <= day:
                    atomic_json(path, {"run_dir": str(directory.resolve()), **manifest})
        return directory
