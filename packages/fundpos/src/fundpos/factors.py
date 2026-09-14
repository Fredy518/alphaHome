from __future__ import annotations

import numpy as np
import pandas as pd

from .constants import ASSETS
from .errors import DataUnavailable
from .pit import available, available_nav, dates, require_unique


def returns_from_prices(prices: pd.DataFrame, *, code="asset", price="close") -> pd.DataFrame:
    """No pad/backfill. A price row is never silently replaced by a future row."""
    require_unique(prices, ["date", code], "prices")
    data = dates(prices, ("date",)).sort_values([code, "date"]).copy()
    data[price] = pd.to_numeric(data[price], errors="coerce")
    data["return"] = data.groupby(code)[price].pct_change(fill_method=None)
    data["start_date"] = data.groupby(code).date.shift(1)
    return data


def returns_from_prices_on_calendar(
    prices: pd.DataFrame,
    calendar: pd.DatetimeIndex,
    *,
    code="asset",
    price="close",
    max_price_age_days=14,
) -> pd.DataFrame:
    """Convert a cross-market price series to the fund valuation calendar.

    Missing target-calendar observations use only the last observed close.  A
    source market holiday therefore has zero return, while its next open date
    carries the full change since the prior observed close.  No future value is
    backfilled and stale prices fail downstream completeness checks.
    """
    require_unique(prices, ["date", code], "cross-calendar prices")
    data = dates(prices, ("date",)).sort_values([code, "date"]).copy()
    target = pd.DatetimeIndex(calendar).normalize().sort_values().unique()
    frames = []
    for asset, group in data.groupby(code, sort=False):
        observed = group.set_index("date")[price].sort_index()
        joined = observed.index.union(target).sort_values()
        carried = observed.reindex(joined).ffill().reindex(target)
        observed_dates = pd.Series(observed.index, index=observed.index)
        source_dates = observed_dates.reindex(joined).ffill().reindex(target)
        age = pd.Series(target, index=target) - source_dates
        carried = carried.where(age.dt.days.le(max_price_age_days))
        frames.append(
            pd.DataFrame({"date": target, code: asset, price: carried.to_numpy()})
        )
    expanded = pd.concat(frames, ignore_index=True) if frames else data.iloc[0:0]
    return returns_from_prices(expanded, code=code, price=price)


def cash_returns(
    rates: pd.DataFrame, calendar: pd.DatetimeIndex, max_rate_age_days=14
) -> pd.DataFrame:
    """Use the prior known annual percent rate and actual elapsed calendar days."""
    rates = dates(rates, ("date",))
    require_unique(rates, ["date"], "cash proxy rates")
    r = rates.copy().set_index("date")["annual_rate_pct"].sort_index()
    previous_dates = pd.Series(calendar, index=calendar).shift(1)
    known = r.reindex(r.index.union(calendar)).sort_index().ffill().reindex(calendar).shift(1)
    rate_dates = pd.Series(r.index, index=r.index).where(r.notna())
    known_dates = (
        rate_dates.reindex(r.index.union(calendar)).sort_index().ffill().reindex(calendar).shift(1)
    )
    known = known.where(
        (pd.Series(calendar, index=calendar) - known_dates).dt.days <= max_rate_age_days
    )
    elapsed = (pd.Series(calendar, index=calendar) - previous_dates).dt.days
    result = (1 + known / 100 / 365) ** elapsed - 1
    return pd.DataFrame(
        {
            "date": calendar,
            "start_date": previous_dates.to_numpy(),
            "asset": "cash",
            "return": result.to_numpy(),
            "source": "prior_known_FR007_ACT365",
            "return_basis": "interest_proxy_CNY",
        }
    )


def factor_panel(factors: pd.DataFrame, cutoff, calendar=None, *, assets=ASSETS) -> pd.DataFrame:
    f = dates(factors, ("date", "start_date", "ann_date"))
    f = f.loc[f.date <= pd.Timestamp(cutoff)].copy()
    if "ann_date" in f:
        f = available(f, cutoff)
    if calendar is not None:
        c = pd.DatetimeIndex(calendar).sort_values().unique()
        preceding = pd.Series(c, index=c).shift(1)
        if "start_date" not in f:
            raise DataUnavailable("FACTOR_INTERVAL_UNKNOWN", "Factor returns require start_date")
        # A two-day price change must never masquerade as one day's return.
        f.loc[f.start_date != f.date.map(preceding), "return"] = np.nan
    require_unique(f, ["date", "asset"], "factor returns")
    return (
        f.pivot(index="date", columns="asset", values="return").reindex(columns=assets).sort_index()
    )


def personalized_panel(
    base: pd.DataFrame, holdings: pd.DataFrame, prices: pd.DataFrame, report_date, *, cutoff
) -> tuple[pd.DataFrame, dict, pd.Series]:
    """Buy-and-hold baskets with portfolio weights anchored to the disclosed report date.

    This is an as-of interpretation factor, not a trading return backtest. Missing
    constituent prices replace the entire industry factor with its declared index.
    HK prices must already be adjusted and converted to CNY by the price adapter.
    """
    panel = base.copy()
    prior = pd.Series(0.0, index=ASSETS)
    if holdings.empty or holdings.weight.isna().any() or (holdings.weight < 0).any():
        raise DataUnavailable("INVALID_HOLDINGS", "Positive complete NAV weights required")
    total = float(holdings.weight.sum())
    if total > 1 + 1e-6:
        raise DataUnavailable("HOLDINGS_OVER_100", f"NAV weights sum={total}")
    prior.loc["cash"] = max(0.0, 1 - total)
    p = dates(prices, ("date", "ann_date"))
    p = p.loc[p.date <= pd.Timestamp(cutoff)]
    if "ann_date" in p:
        p = available(p, cutoff)
    require_unique(p, ["date", "security_code"], "adjusted stock prices")
    # Only the regression dates, its preceding quote and the report-date anchor
    # can enter the basket. Preserve the original market date index, including
    # dates with no quotes for this portfolio, so missing-price fallbacks do not
    # change when unrelated historical securities are projected away.
    quote_dates = pd.DatetimeIndex(p.date.unique()).sort_values()
    anchors = quote_dates[quote_dates <= pd.Timestamp(report_date)][-1:]
    before = quote_dates[quote_dates < panel.index[0]][-1:]
    used_dates = quote_dates.intersection(panel.index).union(anchors).union(before).sort_values()
    p = p.loc[p.date.isin(used_dates) & p.security_code.isin(holdings.security_code)]
    full_prices = p.pivot(
        index="date", columns="security_code", values="adjusted_close"
    ).reindex(index=used_dates)
    proxy_nav_weight = 0.0
    replacements = []
    unknown = float(holdings.loc[holdings.industry == "unknown_a", "weight"].sum())
    proxy_nav_weight += unknown
    for industry, basket in holdings.groupby("industry"):
        if industry == "unknown_a":
            continue
        if industry not in ASSETS[2:]:
            raise DataUnavailable("UNKNOWN_INDUSTRY", str(industry))
        weight = float(basket.weight.sum())
        prior.loc[industry] = weight
        if weight <= 0:
            continue
        if basket.get("is_proxy", pd.Series(False, index=basket.index)).any():
            proxy_nav_weight += weight
            replacements.append({"industry": industry, "reason": "reconstruction_proxy"})
            continue
        securities = basket.security_code.tolist()
        # Denominators must be known at/before the report date, never future-filled.
        quote = full_prices.reindex(columns=securities)
        anchor = quote.loc[quote.index <= pd.Timestamp(report_date)].tail(1)
        if anchor.empty or anchor.isna().any().any() or (anchor <= 0).any().any():
            proxy_nav_weight += weight
            replacements.append({"industry": industry, "reason": "missing_report_prices"})
            continue
        needed = quote.reindex(panel.index)
        values = needed.to_numpy(dtype=float)
        units = basket.weight.to_numpy(dtype=float) / weight / anchor.iloc[0].to_numpy(dtype=float)
        if not np.isfinite(values).all() or (values <= 0).any():
            proxy_nav_weight += weight
            replacements.append({"industry": industry, "reason": "missing_constituent_prices"})
            continue
        nav = pd.Series(values @ units, index=panel.index)
        returns = nav.pct_change(fill_method=None)
        # Obtain the first day's denominator from the actual preceding quote day.
        before = quote.loc[quote.index < panel.index[0]].tail(1)
        if before.empty or before.isna().any().any():
            proxy_nav_weight += weight
            replacements.append({"industry": industry, "reason": "missing_initial_prices"})
            continue
        returns.iloc[0] = nav.iloc[0] / float(before.to_numpy()[0] @ units) - 1
        panel.loc[:, industry] = returns
    personalized_count = int((prior.iloc[2:] > 0).sum() - len(replacements))
    # Keep the unknown equity out of cash. This prior allocation is explicitly
    # a proxy; it does not assert a historical industry label for those stocks.
    if unknown:
        a = prior.iloc[3:].copy()
        prior.iloc[3:] += unknown * (a / a.sum() if a.sum() else np.full(len(a), 1 / len(a)))
    return (
        panel,
        {
            "proxy_ratio": proxy_nav_weight / total if total else 0.0,
            "proxy_industries": replacements,
            "personalized_factor_count": personalized_count,
            "unknown_prior_allocation": "known_A_proportion_or_equal" if unknown else None,
        },
        prior,
    )


def align_window(
    nav: pd.DataFrame,
    panel: pd.DataFrame,
    calendar: pd.DatetimeIndex,
    valuation_date,
    information_cutoff,
    *,
    window=60,
    max_calendar_days=150,
    inactive_assets: tuple[str, ...] = (),
    assets=ASSETS,
) -> tuple[pd.DataFrame, pd.Series, dict]:
    target = pd.Timestamp(valuation_date).normalize()
    calendar = pd.DatetimeIndex(calendar).normalize().sort_values().unique()
    if target not in calendar:
        raise DataUnavailable("NOT_TRADING_DATE", str(target.date()))
    n = available_nav(dates(nav, ("date", "ann_date")), information_cutoff).sort_values("date")
    n = n.loc[n.date <= target]
    require_unique(n, ["date"], "fund NAV")
    if n.empty or n.date.max() != target:
        end = None if n.empty else n.date.max().date()
        raise DataUnavailable("STALE_NAV", f"requested={target.date()}, available={end}")
    n = n.tail(window + 1)
    if len(n) < window + 1:
        raise DataUnavailable("INSUFFICIENT_NAV", f"Need {window + 1} NAV observations")
    if (target - n.date.min()).days > max_calendar_days:
        raise DataUnavailable("WINDOW_TOO_LONG", "NAV gaps exceed configured window span")
    if not np.isfinite(n.adj_nav.to_numpy(dtype=float)).all() or (n.adj_nav <= 0).any():
        raise DataUnavailable("INVALID_NAV", "NAV must be finite and positive")
    if not set(n.date).issubset(set(calendar)):
        raise DataUnavailable(
            "NAV_CALENDAR_MISMATCH", "NAV dates must be on the fund valuation calendar"
        )
    needed = calendar[(calendar > n.date.min()) & (calendar <= target)]
    f = panel.reindex(needed).reindex(columns=assets).copy()
    for asset in inactive_assets:
        f[asset] = 0.0  # A constrained-to-zero coefficient is removed economically.
    if f.isna().any().any():
        missing = f.columns[f.isna().any()].tolist()
        raise DataUnavailable("MISSING_FACTORS", ", ".join(missing))
    if not np.isfinite(f.to_numpy(dtype=float)).all() or (f <= -1).any().any():
        raise DataUnavailable("INVALID_FACTOR_RETURN", "Invalid total-return factor")
    rows = []
    ys = []
    ends = []
    records = list(n[["date", "adj_nav"]].itertuples(index=False, name=None))
    for (start, previous), (end, value) in zip(records, records[1:]):
        segment = f.loc[(f.index > start) & (f.index <= end)]
        rows.append(np.prod(1 + segment.to_numpy(dtype=float), axis=0) - 1)
        ys.append(float(value / previous - 1))
        ends.append(end)
    idx = pd.DatetimeIndex(ends)
    return (
        pd.DataFrame(rows, index=idx, columns=assets),
        pd.Series(ys, index=idx),
        {
            "valuation_date": target.strftime("%Y-%m-%d"),
            "information_cutoff": str(pd.Timestamp(information_cutoff).date()),
            "data_complete_through": target.strftime("%Y-%m-%d"),
            "window_start": str(n.date.min().date()),
            "window_end": str(target.date()),
            "window_calendar_days": (target - n.date.min()).days,
            "aggregated_nav_intervals": int(len(needed) - window),
        },
    )
