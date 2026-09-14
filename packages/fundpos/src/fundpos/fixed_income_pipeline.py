from __future__ import annotations

import hashlib

import numpy as np
import pandas as pd
from filelock import FileLock

from .aggregation import aggregate_exposures
from .config import Settings
from .constants import (
    ASSETS,
    FIXED_INCOME_ASSETS,
    FIXED_INCOME_FACTOR_COLUMNS,
    FIXED_INCOME_OUTPUTS,
    SW_CODES,
)
from .convertible_dominant import (
    CBOND_STYLE_COLUMNS,
    CONVERTIBLE_DOMINANT_GROUP,
    convertible_dominant_constraints,
    convertible_rotation_mix,
    convertible_style_factor_panel,
    disclosed_convertible_style_prior,
    disclosure_anchored_stock_constraints,
    estimate_convertible_endpoint,
    estimate_convertible_nav_drift_sparse_trade,
    estimate_convertible_state_space,
    estimate_convertible_style_endpoint,
    latest_published_convertible_holdings,
    mark_to_market_convertible_weight,
    personalized_convertible_panel,
)
from .data import DataBundle
from .errors import DataUnavailable
from .factors import align_window, factor_panel, personalized_panel
from .fixed_income import FixedIncomeConstraints, estimate_financing_scenarios
from .fixed_income_data import disclosed_balance_sheet_leverage, disclosed_financing
from .normalization import aum_at_date
from .pipeline import build_holdings
from .pit import available, available_nav, dates
from .storage import atomic_json, atomic_parquet, code_fingerprint, frame_hash, git_revision
from .universe import fixed_income_plus_universe


def fixed_income_constraints_from_row(row: pd.Series) -> FixedIncomeConstraints:
    """Translate documented denominators into linear solver constraints."""
    exception_days = pd.to_numeric(
        row.get("portfolio_ratio_exception_days"), errors="coerce"
    )
    enforcement_mode = row.get("constraint_enforcement_mode")
    enforcement_mode = None if pd.isna(enforcement_mode) else str(enforcement_mode)
    if pd.notna(exception_days) and enforcement_mode not in {
        "point_in_time_verified",
        "unconditional_diagnostic",
    }:
        raise DataUnavailable(
            "CONTRACT_COMPLIANCE_STATE_UNKNOWN",
            f"passive breach grace period={int(exception_days)} trading days",
        )
    values = {}
    direct_fields = {
        "financing_lower",
        "financing_upper",
        "gross_assets_upper",
        "hk_upper_equity",
        "source",
        "stock_gross_lower",
        "stock_gross_upper",
        "fixed_income_nav_lower",
        "fixed_income_gross_lower",
        "cbond_gross_lower",
        "cbond_non_cash_lower",
        "cbond_non_cash_nav_lower",
        "cbond_fixed_income_lower",
    }
    for name in direct_fields:
        if name in row and pd.notna(row[name]):
            values[name] = row[name]

    interpretation = row.get("denominator_interpretation")
    interpretation = None if pd.isna(interpretation) else str(interpretation)

    stock_denominator = row.get("stock_denominator")
    stock_denominator = (
        None if pd.isna(stock_denominator) else str(stock_denominator)
    )
    if pd.notna(row.get("stock_lower")):
        if stock_denominator in {None, "fund_nav"} or (
            stock_denominator == "fund_assets"
            and interpretation == "nav_investment_ratio"
        ):
            values["stock_lower"] = float(row.stock_lower)
        elif (
            stock_denominator == "fund_assets"
            and interpretation == "gross_balance_sheet"
        ):
            values["stock_gross_lower"] = float(row.stock_lower)
        else:
            raise DataUnavailable(
                "UNSUPPORTED_CONSTRAINT_DENOMINATOR",
                f"stock={stock_denominator}",
            )
    if pd.notna(row.get("stock_upper")):
        if stock_denominator in {None, "fund_nav"} or (
            stock_denominator == "fund_assets"
            and interpretation == "nav_investment_ratio"
        ):
            values["stock_upper"] = float(row.stock_upper)
        elif (
            stock_denominator == "fund_assets"
            and interpretation == "gross_balance_sheet"
        ):
            values["stock_gross_upper"] = float(row.stock_upper)
        else:
            raise DataUnavailable(
                "UNSUPPORTED_CONSTRAINT_DENOMINATOR",
                f"stock={stock_denominator}",
            )

    bond_denominator = row.get("bond_denominator")
    bond_denominator = None if pd.isna(bond_denominator) else str(bond_denominator)
    if pd.notna(row.get("bond_lower")):
        if bond_denominator == "fund_nav" or (
            bond_denominator == "fund_assets"
            and interpretation == "nav_investment_ratio"
        ):
            values["fixed_income_nav_lower"] = float(row.bond_lower)
        elif (
            bond_denominator == "fund_assets"
            and interpretation == "gross_balance_sheet"
        ):
            values["fixed_income_gross_lower"] = float(row.bond_lower)
        else:
            raise DataUnavailable(
                "UNSUPPORTED_CONSTRAINT_DENOMINATOR",
                f"bond={bond_denominator}",
            )

    cbond_denominator = row.get("cbond_denominator")
    cbond_denominator = (
        None if pd.isna(cbond_denominator) else str(cbond_denominator)
    )
    if pd.notna(row.get("cbond_lower")):
        field = {
            None: "cbond_lower",
            "fund_nav": "cbond_lower",
            "fixed_income_assets": "cbond_fixed_income_lower",
            "bond_assets": "cbond_fixed_income_lower",
        }.get(cbond_denominator)
        if cbond_denominator == "fund_assets":
            field = {
                "gross_balance_sheet": "cbond_gross_lower",
                "nav_investment_ratio": "cbond_lower",
            }.get(interpretation)
        elif cbond_denominator == "non_cash_fund_assets":
            field = {
                "gross_balance_sheet": "cbond_non_cash_lower",
                "nav_investment_ratio": "cbond_non_cash_nav_lower",
            }.get(interpretation)
        if field is None:
            raise DataUnavailable(
                "UNSUPPORTED_CONSTRAINT_DENOMINATOR",
                f"cbond={cbond_denominator}",
            )
        values[field] = float(row.cbond_lower)
    if pd.notna(row.get("cbond_upper")):
        if cbond_denominator not in {None, "fund_nav"}:
            raise DataUnavailable(
                "UNSUPPORTED_CONSTRAINT_DENOMINATOR",
                f"cbond_upper={cbond_denominator}",
            )
        values["cbond_upper"] = float(row.cbond_upper)
    return FixedIncomeConstraints(**values)


def select_fixed_income_constraints(bundle: DataBundle, fund, at_date, cutoff, settings):
    c = dates(bundle["constraints"], ("ann_date", "effective_date"))
    if not c.empty:
        c = available(c.loc[c.fund_code == fund.fund_code], cutoff)
        verified = c.get("verified", pd.Series(False, index=c.index)).fillna(False)
        c = c.loc[
            c.effective_date.le(pd.Timestamp(at_date)) & verified
        ]
        if not c.empty:
            row = c.sort_values(["effective_date", "ann_date"]).iloc[-1]
            return fixed_income_constraints_from_row(row)
    config = settings.values["fixed_income"]
    return FixedIncomeConstraints(stock_upper=config["stock_upper"],
        financing_upper=config["financing_upper"], gross_assets_upper=config["gross_assets_upper"],
        source="unverified_category_diagnostic")


def fixed_income_panel(bundle: DataBundle, cutoff, calendar):
    equity = factor_panel(bundle["factors"], cutoff, calendar)
    required = tuple(a for a in FIXED_INCOME_FACTOR_COLUMNS if a not in {"cash", "hk", *SW_CODES})
    fixed = factor_panel(bundle["fixed_income_factors"], cutoff, calendar, assets=required)
    panel = pd.DataFrame(index=equity.index, columns=FIXED_INCOME_FACTOR_COLUMNS, dtype=float)
    panel[["cash", "hk", *SW_CODES]] = equity[["cash", "hk", *SW_CODES]]
    panel[list(required)] = fixed.reindex(panel.index)[list(required)]
    # Development/selection cash uses the same prior-known DR007 accrual as the
    # financing-cost leg when the legacy FR007/GC007 cash proxy is unavailable.
    # This preserves the economic spread interpretation without splicing rates.
    panel["cash"] = panel.cash.fillna(panel.financing_cost)
    missing = panel.columns[panel.notna().sum() < 61].tolist()
    if missing:
        raise DataUnavailable("MISSING_FIXED_INCOME_FACTORS", ", ".join(missing))
    return panel


def _prior_from_disclosure(bundle, fund_code, equity_prior, at_date, cutoff):
    # Unknown disclosure components stay unknown.  A missing ordinary-bond
    # reconciliation must not erase an otherwise usable stock or convertible-
    # bond prior, and it must never be represented as a zero position.
    prior = pd.Series(
        np.nan, index=[*FIXED_INCOME_FACTOR_COLUMNS[:-1], "financing"], dtype=float
    )
    prior[["hk", *SW_CODES]] = equity_prior[["hk", *SW_CODES]]
    a = dates(bundle["asset_reports"], ("report_date", "ann_date"))
    a = available(a.loc[(a.fund_code == fund_code) & a.report_date.le(pd.Timestamp(at_date))], cutoff)
    if a.empty:
        raise DataUnavailable("NO_FIXED_INCOME_PRIOR", "No public asset allocation")
    row = a.sort_values(["report_date", "ann_date"]).iloc[-1]
    if pd.notna(row.get("convertible_bond_weight")):
        prior.convertible_bond = float(row.convertible_bond_weight)
    if pd.notna(row.get("ordinary_bond_weight")):
        for asset in ("rate_short", "rate_long", "credit_short", "credit_long"):
            prior[asset] = float(row.ordinary_bond_weight) / 4
    financing, _ = disclosed_financing(
        bundle["asset_reports"], fund_code, at_date, cutoff
    )
    prior.financing = financing
    asset_components = prior.drop(["cash", "financing"])
    if financing is None or asset_components.isna().any():
        prior.cash = None
    else:
        prior.cash = 1 + financing - asset_components.sum()
        if prior.cash < -1e-6:
            raise DataUnavailable(
                "INVALID_FIXED_INCOME_PRIOR",
                "Disclosed assets exceed controlled gross assets",
            )
        prior.cash = max(0.0, prior.cash)
    return prior


def _market_cap_equity_proxy(bundle, fund_code, at_date, cutoff):
    """Build the declared no-holdings proxy from historical SW float market caps."""
    reports = dates(bundle["asset_reports"], ("report_date", "ann_date"))
    reports = available(
        reports.loc[
            (reports.fund_code == fund_code)
            & reports.report_date.le(pd.Timestamp(at_date))
        ],
        cutoff,
    )
    if reports.empty or reports.stock_weight.dropna().empty:
        raise DataUnavailable("NO_EQUITY_PROXY_CONTROL", "No public stock allocation")
    report = reports.sort_values(["report_date", "ann_date"]).iloc[-1]
    stock_weight = float(report.stock_weight)
    if stock_weight < -1e-8 or stock_weight > 1 + 1e-8:
        raise DataUnavailable("INVALID_EQUITY_PROXY_CONTROL", f"stock={stock_weight}")
    prior = pd.Series(np.nan, index=ASSETS, dtype=float)
    prior["cash"] = max(0.0, 1.0 - stock_weight)
    prior["hk"] = 0.0
    if stock_weight <= 1e-8:
        prior[list(SW_CODES)] = 0.0
        proxy_date = report.report_date
    else:
        market = dates(bundle["industry_market_weights"], ("date",))
        market = market.loc[market.date.le(report.report_date)]
        if market.empty:
            raise DataUnavailable(
                "NO_INDUSTRY_MARKET_PROXY", "No historical SW float-market-cap weights"
            )
        proxy_date = market.date.max()
        weights = market.loc[market.date.eq(proxy_date)].set_index("asset").weight
        weights = weights.reindex(SW_CODES)
        if weights.isna().any() or float(weights.sum()) <= 0:
            raise DataUnavailable(
                "INCOMPLETE_INDUSTRY_MARKET_PROXY", str(proxy_date.date())
            )
        prior[list(SW_CODES)] = weights / weights.sum() * stock_weight
    metadata = {
        "proxy_ratio": stock_weight,
        "holdings_mode": "sw_float_market_cap_proxy_no_verified_full_holdings",
        "holdings_proxy_used": True,
        "holdings_report_date": str(report.report_date.date()),
        "holdings_ann_date": str(report.ann_date.date()),
        "holdings_age_days": int((pd.Timestamp(at_date) - report.report_date).days),
        "industry_proxy_date": str(pd.Timestamp(proxy_date).date()),
        "hk_proxy_assumption": "zero_without_verified_hk_split",
    }
    return prior, metadata


def compute_fixed_income_date(settings: Settings, bundle: DataBundle, valuation_date, cutoff,
                              previous_results: pd.DataFrame | None = None,
                              selected_products: set[str] | None = None,
                              prepared_factor_cache: dict | None = None):
    target, cutoff = pd.Timestamp(valuation_date).normalize(), pd.Timestamp(cutoff).normalize()
    if cutoff < target:
        raise DataUnavailable("INVALID_CUTOFF", "Information cutoff precedes NAV date")
    universe = fixed_income_plus_universe(bundle["funds"], bundle["classification"],
        bundle["asset_reports"], bundle["constraints"], target, cutoff,
        min_age=settings.values["universe"]["min_age_days"],
        max_report_age=settings.values["fixed_income"]["max_report_age_days"],
        max_stock_each=settings.values["fixed_income"]["stock_upper"],
        max_stock_median=settings.values["fixed_income"]["style_stock_median"],
        min_bond_median=settings.values["fixed_income"]["style_bond_median"])
    if selected_products is not None:
        universe = universe.loc[
            universe.master_code.isin(selected_products)
            & universe.category.ne("增强指数型")
        ]
    else:
        universe = universe.loc[
            universe.in_fixed_income_plus | universe.comparison_group.notna()
        ]
    calendar = pd.DatetimeIndex(pd.to_datetime(bundle["calendar"].date)).sort_values().unique()
    try:
        base = fixed_income_panel(bundle, cutoff, calendar)
        factor_error = None
    except DataUnavailable as exc:
        base, factor_error = None, exc
    results, scenarios = [], []
    model = settings.values["model"]
    for fund in universe.itertuples(index=False):
        is_convertible_dominant = fund.comparison_group == CONVERTIBLE_DOMINANT_GROUP
        row = {"fund_code": fund.fund_code, "master_code": fund.master_code,
               "fund_name": fund.fund_name, "category": fund.category,
               "valuation_date": str(target.date()), "information_cutoff": str(cutoff.date()),
               "model": model["name"], "model_family": (
                   "convertible_dominant" if is_convertible_dominant else "fixed_income_plus"
               ),
               "contract_pool": fund.contract_pool, "style_pool": fund.style_pool,
               "comparison_group": fund.comparison_group, "status": "unavailable", "reason": None,
               **dict.fromkeys(FIXED_INCOME_OUTPUTS, None)}
        aum = aum_at_date(bundle["nav"], bundle["financial_reports"], list(fund.share_codes),
                          target, cutoff, settings.values["publication"]["max_aum_age_days"])
        row.update(aum)
        try:
            if not fund.in_fixed_income_plus and pd.isna(fund.comparison_group):
                raise DataUnavailable(
                    "UNIVERSE_RULES_NOT_MET", fund.eligibility_reasons
                )
            if factor_error:
                raise factor_error
            nav = bundle["nav"].loc[bundle["nav"].fund_code == fund.fund_code]
            bounds = select_fixed_income_constraints(bundle, fund, target, cutoff, settings)
            constraint_extras = {}
            if is_convertible_dominant:
                bounds = convertible_dominant_constraints(
                    bounds,
                    settings.values["convertible_dominant"]["cbond_upper"],
                    settings.values["convertible_dominant"]["stock_upper"],
                )
                try:
                    bounds, constraint_extras = disclosure_anchored_stock_constraints(
                        bounds,
                        bundle["asset_reports"],
                        fund.fund_code,
                        target,
                        cutoff,
                    )
                except DataUnavailable as exc:
                    constraint_extras = {
                        "stock_total_method": "return_regression_fallback",
                        "stock_anchor_error": exc.code,
                    }
            panel = base.copy()
            prior = None
            cbond_style_prior = None
            extras = {
                "proxy_ratio": 1.0,
                "holdings_mode": "index_only",
                **constraint_extras,
            }
            if model["name"] != "index":
                cache_key = (
                    fund.fund_code,
                    str(target.date()),
                    str(cutoff.date()),
                    int(model["window"]),
                    int(model["max_holdings_age_days"]),
                )
                cached = (
                    prepared_factor_cache.get(cache_key)
                    if prepared_factor_cache is not None
                    else None
                )
                if cached is None:
                    try:
                        holdings, meta = build_holdings(
                            bundle,
                            fund.fund_code,
                            cutoff,
                            target,
                            max_unknown_weight=model.get("max_unclassified_nav_weight", 0.001),
                        )
                        holdings_age = (
                            target - pd.Timestamp(meta["holdings_report_date"])
                        ).days
                        if holdings_age > model["max_holdings_age_days"]:
                            raise DataUnavailable("STALE_HOLDINGS", f"age={holdings_age}")
                        equity = factor_panel(bundle["factors"], cutoff, calendar)
                        n = available_nav(nav, cutoff).loc[lambda x: x.date.le(target)]
                        first = n.sort_values("date").tail(model["window"] + 1).date.min()
                        personalized, factor_extras, equity_prior = personalized_panel(
                            equity.loc[equity.index.to_series().between(first, target)],
                            holdings,
                            bundle["prices"],
                            meta["holdings_report_date"],
                            cutoff=cutoff,
                        )
                        factor_extras.update(meta)
                        factor_extras["holdings_age_days"] = holdings_age
                    except DataUnavailable as exc:
                        if exc.code not in {"NO_VERIFIED_FULL_HOLDINGS", "STALE_HOLDINGS"}:
                            raise
                        equity_prior, factor_extras = _market_cap_equity_proxy(
                            bundle, fund.fund_code, target, cutoff
                        )
                        factor_extras["holdings_proxy_reason"] = exc.code
                        personalized = None
                    cached = (personalized, dict(factor_extras), equity_prior)
                    if prepared_factor_cache is not None:
                        prepared_factor_cache[cache_key] = cached
                personalized, factor_extras, equity_prior = cached
                extras.update(factor_extras)
                if personalized is not None:
                    panel.loc[
                        personalized.index, ["cash", "hk", *SW_CODES]
                    ] = personalized[["cash", "hk", *SW_CODES]]
                if is_convertible_dominant:
                    try:
                        cbond_cache_key = (
                            "cbond_factor",
                            fund.fund_code,
                            str(target.date()),
                            str(cutoff.date()),
                            int(
                                settings.values["convertible_dominant"][
                                    "max_price_age_days"
                                ]
                            ),
                        )
                        cbond_cached = (
                            prepared_factor_cache.get(cbond_cache_key)
                            if prepared_factor_cache is not None
                            else None
                        )
                        if cbond_cached is None:
                            cbond_holdings, cbond_meta = (
                                latest_published_convertible_holdings(
                                    bundle["cbond_holdings"],
                                    bundle["asset_reports"],
                                    fund.fund_code,
                                    target,
                                    cutoff,
                                )
                            )
                            panel, cbond_factor_meta = personalized_convertible_panel(
                                panel,
                                cbond_holdings,
                                bundle["cbond_prices"],
                                cbond_meta["cbond_holdings_report_date"],
                                cbond_meta["cbond_control_weight"],
                                cutoff,
                                max_price_age_days=settings.values[
                                    "convertible_dominant"
                                ]["max_price_age_days"],
                                endpoint_date=target,
                            )
                            cbond_extras = dict(cbond_meta) | dict(cbond_factor_meta)
                            try:
                                _, mark_meta = mark_to_market_convertible_weight(
                                    nav,
                                    control_weight=cbond_meta["cbond_control_weight"],
                                    anchor_date=cbond_factor_meta[
                                        "cbond_factor_anchor_date"
                                    ],
                                    endpoint_date=cbond_factor_meta[
                                        "cbond_factor_endpoint_date"
                                    ],
                                    factor_growth=cbond_factor_meta[
                                        "cbond_factor_growth_since_anchor"
                                    ],
                                    cutoff=cutoff,
                                )
                                cbond_extras.update(mark_meta)
                            except DataUnavailable as exc:
                                cbond_extras["cbond_mark_error"] = exc.code
                            cbond_cached = (
                                panel["convertible_bond"].copy(),
                                cbond_extras,
                            )
                            if prepared_factor_cache is not None:
                                prepared_factor_cache[cbond_cache_key] = cbond_cached
                        cbond_returns, cbond_extras = cbond_cached
                        panel.loc[cbond_returns.index, "convertible_bond"] = cbond_returns
                        extras.update(cbond_extras)
                    except DataUnavailable as exc:
                        extras.update(
                            cbond_factor_mode="official_broad_fallback",
                            cbond_factor_error=exc.code,
                            cbond_proxy_ratio=1.0,
                            cbond_priced_coverage=0.0,
                        )
                    if model.get("cbond_exposure_mode") == (
                        "style_decomposed_linear_endpoint"
                    ):
                        style_key = (
                            "cbond_style_market",
                            str(target.date()),
                            str(cutoff.date()),
                            float(model["cbond_style_equity_premium_max"]),
                            float(model["cbond_style_bond_premium_min"]),
                            int(model["cbond_style_minimum_constituents"]),
                        )
                        style_cached = (
                            prepared_factor_cache.get(style_key)
                            if prepared_factor_cache is not None
                            else None
                        )
                        if style_cached is None:
                            styled, style_meta = convertible_style_factor_panel(
                                base,
                                bundle["cbond_prices"],
                                cutoff,
                                equity_premium_max=model[
                                    "cbond_style_equity_premium_max"
                                ],
                                bond_premium_min=model[
                                    "cbond_style_bond_premium_min"
                                ],
                                minimum_constituents=model[
                                    "cbond_style_minimum_constituents"
                                ],
                            )
                            style_cached = (
                                styled[list(CBOND_STYLE_COLUMNS)].copy(),
                                style_meta,
                            )
                            if prepared_factor_cache is not None:
                                prepared_factor_cache[style_key] = style_cached
                        style_returns, style_meta = style_cached
                        panel.loc[style_returns.index, list(CBOND_STYLE_COLUMNS)] = (
                            style_returns
                        )
                        style_holdings, style_control = (
                            latest_published_convertible_holdings(
                                bundle["cbond_holdings"],
                                bundle["asset_reports"],
                                fund.fund_code,
                                target,
                                cutoff,
                            )
                        )
                        cbond_style_prior, style_prior_meta = (
                            disclosed_convertible_style_prior(
                                style_holdings,
                                bundle["cbond_prices"],
                                style_control["cbond_holdings_report_date"],
                                style_control["cbond_control_weight"],
                                cutoff,
                                equity_premium_max=model[
                                    "cbond_style_equity_premium_max"
                                ],
                                bond_premium_min=model[
                                    "cbond_style_bond_premium_min"
                                ],
                                max_price_age_days=settings.values[
                                    "convertible_dominant"
                                ]["max_price_age_days"],
                            )
                        )
                        extras.update(style_meta)
                        extras.update(style_prior_meta)
                cbond_exposure_mode = model.get("cbond_exposure_mode", "static")
                needs_cbond_anchor = is_convertible_dominant and cbond_exposure_mode in {
                    "last_public_anchor_linear_endpoint",
                    "bounded_state_space_endpoint",
                    "nav_drift_sparse_trade_endpoint",
                    "style_decomposed_linear_endpoint",
                }
                if model["prior_penalty"] or needs_cbond_anchor:
                    prior = _prior_from_disclosure(
                        bundle, fund.fund_code, equity_prior, target, cutoff
                    )
            alignment_assets = (
                [*FIXED_INCOME_FACTOR_COLUMNS, *CBOND_STYLE_COLUMNS]
                if model.get("cbond_exposure_mode")
                == "style_decomposed_linear_endpoint"
                else FIXED_INCOME_FACTOR_COLUMNS
            )
            x, y, timing = align_window(
                nav,
                panel,
                calendar,
                target,
                cutoff,
                window=model["window"],
                max_calendar_days=model["max_window_calendar_days"],
                assets=alignment_assets,
            )
            previous = None
            if previous_results is not None and not previous_results.empty:
                old = previous_results.loc[(previous_results.master_code == fund.master_code)
                    & previous_results.status.isin(["ok", "degraded"])].sort_values("valuation_date")
                if not old.empty:
                    previous = old.iloc[-1].copy()
                    for asset in (*FIXED_INCOME_ASSETS, "financing"):
                        internal = f"model_{asset}"
                        if internal in previous and pd.notna(previous[internal]):
                            previous[asset] = previous[internal]
            if model["smooth_penalty"] and previous is None and prior is not None:
                previous = prior
                extras["smoothing_initialization"] = (
                    "published_fixed_income_prior_known_components"
                )
            disclosed, leverage_reason = disclosed_financing(bundle["asset_reports"], fund.fund_code,
                                                               target, cutoff)
            balance_leverage, balance_leverage_reason = disclosed_balance_sheet_leverage(
                bundle["asset_reports"], fund.fund_code, target, cutoff
            )
            estimator = None
            estimator_kwargs = {}
            if is_convertible_dominant and model.get("anchor_equity_vector", False):
                if extras.get("stock_anchor_error"):
                    raise DataUnavailable(
                        "NO_EQUITY_VECTOR_TOTAL_CONTROL",
                        str(extras["stock_anchor_error"]),
                    )
                if prior is None:
                    raise DataUnavailable(
                        "NO_EQUITY_VECTOR_ANCHOR",
                        "Equity-vector anchor requires published holdings",
                    )
                fixed_weights = pd.Series(np.nan, index=FIXED_INCOME_ASSETS)
                equity_assets = ["hk", *SW_CODES]
                equity_anchor = prior[equity_assets].astype(float).clip(lower=0.0)
                disclosed_total = float(bounds.stock_lower)
                reconstructed_total = float(equity_anchor.sum())
                if disclosed_total <= 1e-12:
                    equity_anchor[:] = 0.0
                elif reconstructed_total <= 1e-12:
                    raise DataUnavailable(
                        "EMPTY_EQUITY_VECTOR_ANCHOR",
                        f"disclosed stock total={disclosed_total:.6f}",
                    )
                else:
                    equity_anchor *= disclosed_total / reconstructed_total
                fixed_weights[equity_assets] = equity_anchor
                estimator_kwargs["fixed_weights"] = fixed_weights
                extras["stock_industry_method"] = (
                    "last_public_holdings_vector_anchor"
                )
                extras["stock_vector_reconciliation_gap"] = (
                    reconstructed_total - disclosed_total
                )
            exposure_mode = model.get("cbond_exposure_mode", "static")
            if is_convertible_dominant and exposure_mode in {
                "last_public_anchor_linear_endpoint",
                "bounded_state_space_endpoint",
                "nav_drift_sparse_trade_endpoint",
                "style_decomposed_linear_endpoint",
            }:
                if prior is None:
                    raise DataUnavailable(
                        "NO_CBOND_ENDPOINT_PRIOR",
                        "Endpoint exposure requires the latest public allocation",
                    )
                if not extras.get("cbond_holdings_report_date"):
                    raise DataUnavailable(
                        "NO_CBOND_ENDPOINT_ANCHOR",
                        "Endpoint exposure requires dated convertible holdings",
                    )
                replacement, replacement_meta = convertible_rotation_mix(
                    prior,
                    balance_sheet_leverage=balance_leverage,
                )
                extras.update(replacement_meta)
                estimator = {
                    "last_public_anchor_linear_endpoint": estimate_convertible_endpoint,
                    "bounded_state_space_endpoint": estimate_convertible_state_space,
                    "nav_drift_sparse_trade_endpoint": (
                        estimate_convertible_nav_drift_sparse_trade
                    ),
                    "style_decomposed_linear_endpoint": (
                        estimate_convertible_style_endpoint
                    ),
                }[exposure_mode]
                estimator_kwargs.update({
                    "cbond_anchor": float(prior.convertible_bond),
                    "anchor_date": extras["cbond_holdings_report_date"],
                    "replacement_weights": replacement,
                })
                if exposure_mode == "bounded_state_space_endpoint":
                    estimator_kwargs["state_penalty"] = float(
                        model["cbond_state_penalty"]
                    )
                elif exposure_mode == "nav_drift_sparse_trade_endpoint":
                    estimator_kwargs.update(
                        trade_penalty=float(model["cbond_trade_penalty"]),
                        anchor_penalty=float(model.get("cbond_anchor_penalty", 1.0)),
                    )
                elif exposure_mode == "style_decomposed_linear_endpoint":
                    if cbond_style_prior is None:
                        raise DataUnavailable(
                            "NO_CBOND_STYLE_PRIOR", fund.fund_code
                        )
                    estimator_kwargs["cbond_style_prior"] = cbond_style_prior
            weights, diagnostics, comparison = estimate_financing_scenarios(
                x,
                y,
                bounds,
                disclosed_financing=disclosed,
                balance_sheet_leverage=balance_leverage,
                contract_financing_upper=bounds.financing_upper,
                sensitivity_limit=settings.values["fixed_income"]["sensitivity_limit"],
                include_free_financing=is_convertible_dominant,
                prefer_free_financing=is_convertible_dominant,
                prefer_balance_sheet_leverage=is_convertible_dominant,
                prior=prior,
                previous=previous,
                prior_penalty=model["prior_penalty"],
                smooth_penalty=model["smooth_penalty"],
                weighting=model["weighting"],
                tolerance=model["constraint_tolerance"],
                **({"estimator": estimator} if estimator is not None else {}),
                **estimator_kwargs,
            )
            flags = []
            if timing["window_calendar_days"] > 150:
                flags.append("LOW_FREQUENCY_NAV_WINDOW")
            if bounds.source.startswith("unverified_category_diagnostic"):
                flags.append("CONTRACT_NOT_VERIFIED")
                diagnostics["stock_quality"] = "diagnostic"
                diagnostics["cbond_quality"] = "diagnostic"
            if leverage_reason:
                flags.append(leverage_reason)
            if balance_leverage_reason:
                flags.append(balance_leverage_reason)
            elif diagnostics["primary_scenario"] == "last_disclosed_leverage":
                flags.append("TOTAL_LIABILITY_FINANCING_PROXY")
            if diagnostics["financing_input_status"] == "outside_verified_constraint":
                flags.append("DISCLOSED_FINANCING_OUTSIDE_CONSTRAINT")
            financing_sensitive = bool(
                diagnostics.get("stock_sensitivity", 0.0)
                > settings.values["fixed_income"]["sensitivity_limit"]
                or diagnostics.get("cbond_sensitivity", 0.0)
                > settings.values["fixed_income"]["sensitivity_limit"]
            )
            diagnostics["financing_sensitivity_qualified"] = not financing_sensitive
            if financing_sensitive:
                flags.append("FINANCING_SENSITIVE")
            if (
                model["name"] != "index"
                and extras.get("proxy_ratio", 0) > model["max_proxy_weight"]
            ):
                flags.append("HIGH_PROXY_RATIO")
                diagnostics["stock_quality"] = "diagnostic"
            if extras.get("holdings_proxy_used"):
                flags.append("HOLDINGS_PROXY_USED")
            if is_convertible_dominant:
                control_weight = extras.get("cbond_control_weight")
                mark_weight = extras.get("cbond_mark_to_market_weight")
                agreement_threshold = float(
                    settings.values["convertible_dominant"][
                        "agreement_threshold"
                    ]
                )
                if control_weight is not None:
                    diagnostics["cbond_disclosure_gap"] = abs(
                        float(weights.convertible_bond) - float(control_weight)
                    )
                if mark_weight is not None:
                    diagnostics["cbond_mark_to_market_gap"] = abs(
                        float(weights.convertible_bond) - float(mark_weight)
                    )
                agreement_gap = diagnostics.get("cbond_disclosure_gap")
                agreement_qualified = bool(
                    agreement_gap is not None
                    and agreement_gap <= agreement_threshold
                    and extras.get("cbond_proxy_ratio", 1.0)
                    <= settings.values["convertible_dominant"][
                        "max_broad_proxy_weight"
                    ]
                )
                diagnostics["cbond_agreement_threshold"] = agreement_threshold
                diagnostics["cbond_agreement_qualified"] = agreement_qualified
                diagnostics["cbond_disclosure_proxy_qualified"] = agreement_qualified
                diagnostics["cbond_reliability"] = (
                    "disclosure_consistent_proxy_qualified"
                    if agreement_qualified
                    else "diagnostic_model_disagreement"
                )
                method_points = [float(weights.convertible_bond)]
                method_points.extend(
                    float(value)
                    for value in (control_weight, mark_weight)
                    if value is not None and np.isfinite(value)
                )
                diagnostics["cbond_method_envelope_lower"] = min(method_points)
                diagnostics["cbond_method_envelope_upper"] = max(method_points)
                financing_lower = float(comparison.convertible_bond.min())
                financing_upper = float(comparison.convertible_bond.max())
                diagnostics["cbond_financing_scenario_lower"] = financing_lower
                diagnostics["cbond_financing_scenario_upper"] = financing_upper
                # Compatibility aliases now match their report label. Older
                # frozen runs retain the previous three-method-envelope meaning.
                diagnostics["cbond_sensitivity_lower"] = financing_lower
                diagnostics["cbond_sensitivity_upper"] = financing_upper
                if extras.get("stock_anchor_error"):
                    flags.append(str(extras["stock_anchor_error"]))
                    diagnostics["stock_quality"] = "diagnostic"
                cbond_proxy_limit = settings.values["convertible_dominant"][
                    "max_broad_proxy_weight"
                ]
                if extras.get("cbond_proxy_ratio", 1.0) > cbond_proxy_limit:
                    flags.append("HIGH_CBOND_PROXY_RATIO")
                    diagnostics["cbond_quality"] = "diagnostic"
                if extras.get("cbond_factor_error"):
                    flags.append(str(extras["cbond_factor_error"]))
                if not agreement_qualified:
                    flags.append("CBOND_MODEL_DISAGREEMENT")
            internal_state = {
                f"model_{asset}": float(weights[asset])
                for asset in (*FIXED_INCOME_ASSETS, "financing")
            }
            row.update(weights.to_dict(), **internal_state, **timing, **diagnostics, **extras,
                       status="degraded" if flags else "ok", reason=";".join(flags) or None)
            if diagnostics["ordinary_bond_quality"] == "estimated":
                row["ordinary_bond"] = weights.ordinary_bond
            else:
                # Net non-equity remains identifiable from stock, but cash,
                # financing and gross ordinary-bond totals are scenario-specific.
                row["ordinary_bond"] = None
                row["cash"] = None
                row["financing"] = None
            for record in comparison.to_dict("records"):
                scenarios.append({"fund_code": fund.fund_code, "master_code": fund.master_code,
                                  "valuation_date": str(target.date()), **record})
        except DataUnavailable as exc:
            row.update(reason=exc.code, error_detail=exc.detail)
        except Exception as exc:
            row.update(reason="INTERNAL_ERROR", error_detail=f"{type(exc).__name__}: {exc}")
        results.append(row)
    return pd.DataFrame(results), pd.DataFrame(scenarios)


def run_fixed_income_estimate(settings: Settings, bundle: DataBundle, valuation_date, cutoff,
                              previous_results=None, *, selected_products=None,
                              model_family="fixed_income_plus"):
    output = settings.path("output_dir")
    output.mkdir(parents=True, exist_ok=True)
    state_hash = frame_hash(previous_results) if previous_results is not None else "none"
    identity = hashlib.sha256(f"v3|{settings.fingerprint}|{bundle.fingerprint}|{code_fingerprint(settings.root)}|{cutoff}|{state_hash}|{sorted(selected_products or [])}".encode()).hexdigest()
    directory = output / "runs" / (
        f"{pd.Timestamp(valuation_date):%Y-%m-%d}_{model_family}_{identity[:24]}"
    )
    with FileLock(str(output / ".run.lock"), timeout=0):
        if (directory / "manifest.json").exists():
            return directory
        directory.mkdir(parents=True, exist_ok=True)
        estimates, scenarios = compute_fixed_income_date(settings, bundle, valuation_date, cutoff,
                                                           previous_results, selected_products)
        if estimates.empty:
            raise DataUnavailable("EMPTY_UNIVERSE", "No fixed-income-plus products at date")
        all_label = (
            "全部转债主导" if model_family == "convertible_dominant" else "全部固收+"
        )
        aggregates = aggregate_exposures(estimates, FIXED_INCOME_OUTPUTS, all_label=all_label,
            count_threshold=settings.values["publication"]["count_coverage"],
            aum_threshold=settings.values["publication"]["aum_coverage"])
        atomic_parquet(directory / "estimates.parquet", estimates)
        atomic_parquet(directory / "scenarios.parquet", scenarios)
        atomic_parquet(directory / "aggregates.parquet", aggregates)
        status_counts = {str(k): int(v) for k, v in estimates.status.value_counts().items()}
        reason_counts = {str(k): int(v) for k, v in estimates.reason.dropna().value_counts().items()}
        manifest = {"run_id": directory.name, "model_family": model_family,
            "created_at": pd.Timestamp.now(tz="Asia/Shanghai").isoformat(),
            "valuation_date": str(pd.Timestamp(valuation_date).date()),
            "information_cutoff": str(pd.Timestamp(cutoff).date()), "configuration": settings.values,
            "config_hash": settings.fingerprint, "input_hash": bundle.fingerprint,
            "code_hash": code_fingerprint(settings.root), "git_revision": git_revision(settings.root),
            "previous_state_hash": state_hash, "status_counts": status_counts,
            "reason_counts": reason_counts,
            "complete": bool(not aggregates.empty and aggregates.status.eq("complete").all()),
            "formal_publication": bool(not aggregates.empty and aggregates.status.eq("complete").all()
                                       and estimates.status.eq("ok").any()),
            "final_holdout_opened": False,
            "scope_version": (
                "fundpos_v3_convertible_dominant_20260913"
                if model_family == "convertible_dominant"
                else "fundpos_v3_fixed_income_plus_20260912"
            ),
            "provenance": bundle.provenance}
        atomic_json(directory / "manifest.json", manifest)
    return directory
