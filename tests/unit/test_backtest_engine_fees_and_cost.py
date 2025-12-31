import pandas as pd
from decimal import Decimal

from alphahome.fund_backtest import BacktestEngine, MemoryDataProvider, PortfolioConfig
from alphahome.fund_backtest.core.portfolio import Portfolio


def test_portfolio_redeem_keeps_average_cost_unchanged():
    portfolio = Portfolio(portfolio_id="p1", portfolio_name="P1", cash=Decimal("1000"))

    # Buy 100 units at nav=1.0, no fee
    portfolio.freeze_cash(Decimal("100"))
    portfolio.execute_purchase(
        fund_id="F1",
        amount=Decimal("100"),
        nav=Decimal("1"),
        fee=Decimal("0"),
        fund_name="Fund1",
        rebalance_id=1,
    )

    pos = portfolio.get_position("F1")
    assert pos is not None
    assert float(pos.units) == 100.0
    assert float(pos.cost) == 1.0

    # Redeem 50 units at a higher nav; average cost should remain 1.0
    assert portfolio.freeze_units("F1", Decimal("50"))
    portfolio.execute_redeem(
        fund_id="F1",
        units=Decimal("50"),
        nav=Decimal("1.2"),
        fee=Decimal("0"),
    )

    pos = portfolio.get_position("F1")
    assert pos is not None
    assert float(pos.total_units) == 50.0
    assert float(pos.cost) == 1.0


def test_engine_management_fee_is_deducted_daily():
    # Two business days
    dates = pd.date_range("2023-01-02", "2023-01-03", freq="B")

    # Empty nav panel but with index, so engine sees nav_today (empty Series)
    nav_panel = pd.DataFrame(index=dates)

    data_provider = MemoryDataProvider(nav_panel=nav_panel, rebalance_records={})
    engine = BacktestEngine(data_provider)

    # Set management_fee so that daily fee is exactly 1.0 on initial cash 1000:
    # daily_fee = 1000 * 0.365 / 365 = 1
    cfg = PortfolioConfig(
        portfolio_id="p1",
        portfolio_name="P1",
        initial_cash=1000.0,
        setup_date="2023-01-02",
        management_fee=0.365,
    )
    engine.add_portfolio(cfg)

    result = engine.run("2023-01-02", "2023-01-03")["p1"]

    cash_rows = result.holdings_history[result.holdings_history["fund_id"] == "cash"]
    assert not cash_rows.empty

    last_cash = float(cash_rows.iloc[-1]["units"])
    assert last_cash == 999.0


def test_engine_management_fee_does_not_double_accrue():
    # Three business days
    dates = pd.date_range("2023-01-02", "2023-01-04", freq="B")
    nav_panel = pd.DataFrame(index=dates)

    data_provider = MemoryDataProvider(nav_panel=nav_panel, rebalance_records={})
    engine = BacktestEngine(data_provider)

    cfg = PortfolioConfig(
        portfolio_id="p1",
        portfolio_name="P1",
        initial_cash=1000.0,
        setup_date="2023-01-02",
        management_fee=0.365,
    )
    engine.add_portfolio(cfg)

    result = engine.run("2023-01-02", "2023-01-04")["p1"]
    cash_rows = result.holdings_history[result.holdings_history["fund_id"] == "cash"]
    assert not cash_rows.empty

    # Day1: no fee; Day2 fee is 1000*0.365/365=1 => 999
    # Day3 fee is 999*0.365/365=0.999 => 998.001
    last_cash = float(cash_rows.iloc[-1]["units"])
    assert abs(last_cash - 998.001) < 1e-9
