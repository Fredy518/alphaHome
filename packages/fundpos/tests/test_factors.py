import numpy as np
import pandas as pd
import pytest

from fundpos.constants import ASSETS
from fundpos.errors import DataUnavailable
from fundpos.factors import (
    align_window,
    cash_returns,
    factor_panel,
    personalized_panel,
    returns_from_prices,
    returns_from_prices_on_calendar,
)


def test_first_specialized_return_uses_decimal_units():
    idx = pd.to_datetime(["2023-06-30", "2023-07-03", "2023-07-04"])
    prices = pd.DataFrame(
        {"date": idx, "security_code": ["A"] * 3, "adjusted_close": [100, 102, 104.04]}
    )
    holdings = pd.DataFrame({"security_code": ["A"], "industry": ["801030.SI"], "weight": [0.8]})
    base = pd.DataFrame(0.0, index=idx[1:], columns=ASSETS)
    out, meta, _ = personalized_panel(base, holdings, prices, idx[0], cutoff=idx[-1])
    assert out["801030.SI"].tolist() == pytest.approx([0.02, 0.02])
    assert meta["proxy_ratio"] == 0


def test_dividend_adjustment_and_no_bidirectional_fill():
    raw = pd.DataFrame(
        {
            "date": pd.date_range("2023-01-01", periods=3),
            "asset": ["A"] * 3,
            "close": [10, np.nan, 9],
            "factor": [1, np.nan, 10 / 9],
        }
    )
    raw["adjusted"] = raw.close * raw.factor
    out = returns_from_prices(raw, price="adjusted")
    assert out["return"].isna().all()
    raw = raw.dropna()
    out = returns_from_prices(raw, price="adjusted")
    assert out["return"].iloc[-1] == pytest.approx(0)
    out["asset"] = "cash"
    panel = factor_panel(out, "2023-01-03", pd.date_range("2023-01-01", periods=3))
    assert pd.isna(panel.at[pd.Timestamp("2023-01-03"), "cash"])  # interval length mismatch


def test_cross_market_calendar_carries_only_the_last_observed_close():
    calendar = pd.to_datetime(["2023-01-02", "2023-01-03", "2023-01-04", "2023-01-05"])
    prices = pd.DataFrame(
        {
            "date": pd.to_datetime(["2023-01-02", "2023-01-04", "2023-01-05"]),
            "asset": ["hk"] * 3,
            "close": [100.0, 102.0, 103.02],
        }
    )
    out = returns_from_prices_on_calendar(prices, calendar)
    assert out["return"].tolist()[1:] == pytest.approx([0.0, 0.02, 0.01])
    assert out["start_date"].tolist()[1:] == list(calendar[:-1])


def test_cross_market_calendar_rejects_stale_carried_close():
    calendar = pd.date_range("2023-01-01", "2023-01-20")
    prices = pd.DataFrame(
        {"date": [calendar[0]], "asset": ["hk"], "close": [100.0]}
    )
    out = returns_from_prices_on_calendar(prices, calendar, max_price_age_days=5)
    assert out.loc[out.date.eq(calendar[5]), "return"].notna().all()
    assert out.loc[out.date.gt(calendar[6]), "return"].isna().all()


def test_nav_gap_compounds_matching_factor_interval(bundle):
    nav = bundle["nav"].query("fund_code == 'DEMO001.OF'")
    nav = nav.drop(nav.tail(15).index[0])
    calendar = pd.DatetimeIndex(bundle["calendar"].date)
    panel = factor_panel(bundle["factors"], "2023-09-30", calendar)
    x, y, meta = align_window(nav, panel, calendar, "2023-09-29", "2023-09-30")
    assert len(y) == 60 and meta["aggregated_nav_intervals"] == 1
    dates = nav.tail(61).date.to_list()
    for left, right in zip(dates, dates[1:]):
        expected = (1 + panel.loc[(panel.index > left) & (panel.index <= right)]).prod() - 1
        np.testing.assert_allclose(x.loc[right], expected, atol=1e-14)


def test_stale_inputs_cannot_be_relabelled(bundle):
    nav = bundle["nav"].query("fund_code == 'DEMO001.OF'")
    cal = pd.DatetimeIndex(bundle["calendar"].date)
    panel = factor_panel(bundle["factors"], "2023-09-30", cal)
    with pytest.raises(DataUnavailable, match="STALE_NAV"):
        align_window(nav.iloc[:-1], panel, cal, "2023-09-29", "2023-09-30")
    with pytest.raises(DataUnavailable, match="MISSING_FACTORS"):
        align_window(nav, panel.iloc[:-1], cal, "2023-09-29", "2023-09-30")


def test_cash_proxy_stops_when_rate_is_stale():
    cal = pd.bdate_range("2023-01-02", "2023-02-01")
    rates = pd.DataFrame({"date": [cal[0]], "annual_rate_pct": [2.0]})
    out = cash_returns(rates, cal)
    assert np.isfinite(out["return"].iloc[1])
    assert pd.isna(out["return"].iloc[-1])


@pytest.mark.parametrize("security", ["A.SZ", "A.HK"])
def test_suspension_or_hk_holiday_uses_declared_whole_factor(security):
    idx = pd.bdate_range("2023-06-30", periods=4)
    industry = "hk" if security.endswith("HK") else "801030.SI"
    prices = pd.DataFrame(
        {"date": idx.delete(2), "security_code": [security] * 3, "adjusted_close": [100, 101, 102]}
    )
    h = pd.DataFrame({"security_code": [security], "industry": [industry], "weight": [0.2]})
    base = pd.DataFrame(0.001, index=idx[1:], columns=ASSETS)
    out, meta, _ = personalized_panel(base, h, prices, idx[0], cutoff=idx[-1])
    pd.testing.assert_frame_equal(out, base)
    assert meta["proxy_ratio"] == 1
    assert meta["proxy_industries"][0]["reason"] == "missing_constituent_prices"
