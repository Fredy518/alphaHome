"""Deterministic fictional funds; never a substitute for real-data validation."""

from __future__ import annotations

import numpy as np
import pandas as pd

from .constants import ASSETS, CATEGORIES, SW_CODES
from .data import DataBundle


def synthetic_bundle(start="2021-01-01", end="2023-12-29", n_funds=12, seed=7351):
    rng = np.random.default_rng(seed)
    calendar = pd.bdate_range(start, end)
    n = len(calendar)
    market = rng.normal(0.0002, 0.006, n)
    returns = rng.normal(0, 0.008, (n, len(ASSETS))) + market[:, None]
    returns[:, 0] = 0.00005 + rng.normal(0, 0.00001, n)
    returns[:, 1] = rng.normal(0.00006, 0.001, n)
    x = pd.DataFrame(returns, index=calendar, columns=ASSETS)
    levels = (1 + x).cumprod() * 100
    stocks = {code: f"SYN{i:03d}.SZ" for i, code in enumerate(SW_CODES)}
    stocks["hk"] = "SYN000.HK"
    factors = (
        x.rename_axis("date")
        .reset_index()
        .melt(id_vars="date", var_name="asset", value_name="return")
    )
    preceding = pd.Series(calendar, index=calendar).shift(1)
    factors["start_date"] = factors.date.map(preceding)
    factors["ann_date"] = factors.date
    factors["source"] = "SYNTHETIC_SEEDED_RETURNS"
    factors["return_basis"] = "synthetic_CNY_total_return"
    membership = pd.DataFrame(
        [
            {
                "security_code": stock,
                "industry": industry,
                "in_date": pd.Timestamp("2020-01-01"),
                "out_date": pd.NaT,
            }
            for industry, stock in stocks.items()
            if industry != "hk"
        ]
    )
    prices = pd.concat(
        [
            pd.DataFrame(
                {
                    "date": calendar,
                    "security_code": stock,
                    "adjusted_close": levels[industry].to_numpy(),
                }
            )
            for industry, stock in stocks.items()
        ],
        ignore_index=True,
    )
    funds, classifications, nav, holdings, constraints, assets, truths = [], [], [], [], [], [], []
    report_dates = pd.date_range(pd.Timestamp(start).normalize(), end, freq="QE")
    for i in range(n_funds):
        fund_code = f"DEMO{i + 1:03d}.OF"
        category = CATEGORIES[i % len(CATEGORIES)]
        equity = [0.88, 0.78, 0.58][i % 3]
        w = pd.Series(0.0, index=ASSETS)
        w.iloc[2:] = rng.dirichlet(np.ones(32) * 0.8) * equity
        w.iloc[:2] = (1 - equity) * np.array([0.6, 0.4])
        truths.append({"fund_code": fund_code, **w.to_dict()})
        codes = [fund_code] + ([f"DEMOC{i + 1:03d}.OF"] if i == 0 else [])
        fund_nav = (1 + x.to_numpy() @ w.to_numpy()).cumprod()
        for j, code in enumerate(codes):
            funds.append(
                {
                    "fund_code": code,
                    "master_code": fund_code,
                    "fund_name": f"合成示例基金{i + 1:02d}{'C' if j else 'A'}",
                    "category": category,
                    "found_date": pd.Timestamp("2018-01-01"),
                    "liquidation_date": pd.NaT,
                    "share_class": "C类份额" if j else "A类份额",
                }
            )
            classifications.append(
                {
                    "fund_code": code,
                    "category": category,
                    "in_date": pd.Timestamp("2018-01-01"),
                    "out_date": pd.NaT,
                }
            )
            nav.extend(
                pd.DataFrame(
                    {
                        "fund_code": code,
                        "date": calendar,
                        "ann_date": calendar + pd.Timedelta(days=1),
                        "adj_nav": fund_nav,
                        "net_asset": (1 + i) * 1e8 * (0.25 if j else 1),
                        "total_netasset": None,
                    }
                ).to_dict("records")
            )
        constraints.append(
            {
                "fund_code": fund_code,
                "ann_date": pd.Timestamp("2018-01-01"),
                "effective_date": pd.Timestamp("2018-01-01"),
                "stock_lower": max(0, equity - 0.1),
                "stock_upper": min(1, equity + 0.1),
                "hk_upper_equity": 0.5,
                "source": "synthetic_known_contract",
            }
        )
        for report_date in report_dates:
            ann = report_date + pd.Timedelta(days=60 if report_date.month in (6, 12) else 25)
            assets.append(
                {
                    "fund_code": fund_code,
                    "report_date": report_date,
                    "ann_date": ann,
                    "stock_weight": equity,
                    "aum": (1 + i) * 1e8,
                }
            )
            if report_date.month not in (6, 12):
                continue
            ordered = w.iloc[2:].sort_values(ascending=False)
            for rank, (asset, weight) in enumerate(ordered.items(), start=1):
                holdings.append(
                    {
                        "fund_code": fund_code,
                        "report_date": report_date,
                        "ann_date": ann,
                        "security_code": stocks[asset],
                        "weight": weight,
                        "rank_no": rank,
                        "market_value": weight * (1 + i) * 1e8,
                        "full_report_verified": True,
                        "source": "synthetic_known_holdings",
                    }
                )
    frames = {
        "funds": pd.DataFrame(funds),
        "classification": pd.DataFrame(classifications),
        "nav": pd.DataFrame(nav),
        "calendar": pd.DataFrame({"date": calendar}),
        "factors": factors,
        "holdings": pd.DataFrame(holdings),
        "membership": membership,
        "prices": prices,
        "constraints": pd.DataFrame(constraints),
        "asset_reports": pd.DataFrame(assets),
        "allocations": pd.DataFrame(
            columns=["fund_code", "report_date", "ann_date", "group", "weight"]
        ),
        "stock_groups": pd.DataFrame(),
        "turnover": pd.DataFrame(),
    }
    return DataBundle(
        frames,
        {
            "provider": "SYNTHETIC_DEMO_NOT_MARKET_DATA",
            "universe_scope": "synthetic_12_funds",
            "seed": seed,
            "start": start,
            "end": end,
            "synthetic": True,
            "calendar": "weekday_fixture_not_exchange_calendar",
            "history_kind": "synthetic",
        },
    ), pd.DataFrame(truths)
