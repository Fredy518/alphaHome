#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, List, Optional

import pandas as pd

from fund_analysis import PerformanceAnalyzer

from .core.portfolio import Portfolio


@dataclass
class PortfolioConfig:
    portfolio_id: str
    portfolio_name: str
    initial_cash: float
    setup_date: str
    rebalance_delay: int = 0
    purchase_fee_rate: float = 0.0
    redeem_fee_rate: float = 0.0
    management_fee: float = 0.0
    rebalance_effective_delay: int = 0
    redeem_settle_delay: int = 0


@dataclass
class BacktestResult:
    portfolio_id: str
    portfolio_name: str
    nav_series: pd.Series
    returns: pd.Series
    holdings_history: pd.DataFrame
    trades: pd.DataFrame
    metrics: Dict[str, float] = field(default_factory=dict)


class MemoryDataProvider:
    def __init__(
        self,
        nav_panel: pd.DataFrame,
        rebalance_records: Optional[Dict[str, pd.DataFrame]] = None,
    ) -> None:
        self.nav_panel = nav_panel.copy()
        if not self.nav_panel.empty:
            self.nav_panel.index = pd.to_datetime(self.nav_panel.index)
            self.nav_panel = self.nav_panel.sort_index().ffill(limit=5)
        self.rebalance_records: Dict[str, pd.DataFrame] = {}
        for portfolio_id, records in (rebalance_records or {}).items():
            self.set_rebalance_records(portfolio_id, records)

    def set_rebalance_records(self, portfolio_id: str, records: pd.DataFrame) -> None:
        frame = records.copy()
        if "rebalance_date" in frame.columns:
            frame["rebalance_date"] = pd.to_datetime(frame["rebalance_date"]).dt.normalize()
        self.rebalance_records[portfolio_id] = frame

    def get_rebalance_records(self, portfolio_id: str) -> pd.DataFrame:
        return self.rebalance_records.get(portfolio_id, pd.DataFrame()).copy()

    def get_calendar(self, start_date: str, end_date: str) -> pd.DatetimeIndex:
        start = pd.Timestamp(start_date).normalize()
        end = pd.Timestamp(end_date).normalize()
        if self.nav_panel.index.empty:
            return pd.date_range(start, end, freq="B")
        calendar = self.nav_panel.loc[(self.nav_panel.index >= start) & (self.nav_panel.index <= end)].index
        if len(calendar) == 0:
            return pd.date_range(start, end, freq="B")
        return pd.DatetimeIndex(calendar)

    def get_nav_row(self, date: pd.Timestamp) -> pd.Series:
        if self.nav_panel.empty or date not in self.nav_panel.index:
            return pd.Series(dtype=float)
        return self.nav_panel.loc[date].dropna()


class BacktestEngine:
    def __init__(self, data_provider: MemoryDataProvider) -> None:
        self.data_provider = data_provider
        self.portfolios: Dict[str, PortfolioConfig] = {}
        self.analyzer = PerformanceAnalyzer()

    def add_portfolio(self, config: PortfolioConfig) -> None:
        self.portfolios[config.portfolio_id] = config

    def run(self, start_date: str, end_date: str, use_adj_nav: bool = False) -> Dict[str, BacktestResult]:
        del use_adj_nav
        results: Dict[str, BacktestResult] = {}
        for portfolio_id, config in self.portfolios.items():
            results[portfolio_id] = self._run_single(config, start_date, end_date)
        return results

    def _run_single(self, config: PortfolioConfig, start_date: str, end_date: str) -> BacktestResult:
        calendar = self.data_provider.get_calendar(start_date, end_date)
        records = self.data_provider.get_rebalance_records(config.portfolio_id)
        portfolio = Portfolio(
            portfolio_id=config.portfolio_id,
            portfolio_name=config.portfolio_name,
            cash=Decimal(str(config.initial_cash)),
        )

        nav_points: Dict[pd.Timestamp, float] = {}
        holdings_rows: List[Dict[str, object]] = []
        trade_rows: List[Dict[str, object]] = []

        for idx, trade_date in enumerate(calendar):
            nav_row = self.data_provider.get_nav_row(trade_date)
            if idx > 0 and config.management_fee > 0:
                portfolio_value = self._portfolio_value(portfolio, nav_row)
                fee = Decimal(str(portfolio_value * config.management_fee / 365.0))
                portfolio.cash -= fee

            day_records = records[records["rebalance_date"] == trade_date.normalize()] if not records.empty else pd.DataFrame()
            if not day_records.empty:
                trade_rows.extend(self._rebalance(portfolio, day_records, nav_row, config, trade_date))

            portfolio_value = self._portfolio_value(portfolio, nav_row)
            nav_points[trade_date] = portfolio_value / config.initial_cash if config.initial_cash else 0.0
            holdings_rows.extend(self._snapshot_holdings(portfolio, nav_row, trade_date, portfolio_value))

        nav_series = pd.Series(nav_points, dtype=float)
        returns = nav_series.pct_change().dropna()
        trades_df = pd.DataFrame(
            trade_rows,
            columns=["update", "fund_id", "fund_name", "side", "amount", "units", "fee"],
        )
        holdings_df = pd.DataFrame(
            holdings_rows,
            columns=["update", "fund_id", "fund_name", "units", "weight", "nav"],
        )
        metrics = self.analyzer.calculate_metrics(returns, nav_series)
        return BacktestResult(
            portfolio_id=config.portfolio_id,
            portfolio_name=config.portfolio_name,
            nav_series=nav_series,
            returns=returns,
            holdings_history=holdings_df,
            trades=trades_df,
            metrics=metrics,
        )

    def _portfolio_value(self, portfolio: Portfolio, nav_row: pd.Series) -> float:
        total = float(portfolio.cash + portfolio.frozen_cash)
        for fund_id, position in portfolio.positions.items():
            nav = float(nav_row.get(fund_id, 0.0))
            total += float(position.total_units) * nav
        return total

    def _rebalance(
        self,
        portfolio: Portfolio,
        records: pd.DataFrame,
        nav_row: pd.Series,
        config: PortfolioConfig,
        trade_date: pd.Timestamp,
    ) -> List[Dict[str, object]]:
        trades: List[Dict[str, object]] = []
        if nav_row.empty:
            return trades

        records = records.copy()
        weight_sum = records["target_weight"].sum()
        if weight_sum > 0:
            records["target_weight"] = records["target_weight"] / weight_sum

        total_value = self._portfolio_value(portfolio, nav_row)
        desired_values = {
            row["fund_id"]: float(total_value * row["target_weight"])
            for _, row in records.iterrows()
        }
        fund_names = {
            row["fund_id"]: row.get("fund_name", row["fund_id"])
            for _, row in records.iterrows()
        }

        current_values = {
            fund_id: float(position.total_units) * float(nav_row.get(fund_id, 0.0))
            for fund_id, position in portfolio.positions.items()
        }

        for fund_id, position in list(portfolio.positions.items()):
            current_value = current_values.get(fund_id, 0.0)
            target_value = desired_values.get(fund_id, 0.0)
            excess = current_value - target_value
            nav = float(nav_row.get(fund_id, 0.0))
            if excess <= 0 or nav <= 0:
                continue
            units = min(float(position.total_units), excess / nav)
            if units <= 0:
                continue
            fee = excess * config.redeem_fee_rate
            portfolio.execute_redeem(
                fund_id=fund_id,
                units=Decimal(str(units)),
                nav=Decimal(str(nav)),
                fee=Decimal(str(fee)),
            )
            trades.append(
                {
                    "update": trade_date,
                    "fund_id": fund_id,
                    "fund_name": position.fund_name,
                    "side": -1,
                    "amount": float(units * nav),
                    "units": float(units),
                    "fee": float(fee),
                }
            )

        current_values = {
            fund_id: float(position.total_units) * float(nav_row.get(fund_id, 0.0))
            for fund_id, position in portfolio.positions.items()
        }

        buy_plan: List[Dict[str, float]] = []
        required_cash = 0.0
        for _, row in records.iterrows():
            fund_id = row["fund_id"]
            nav = float(nav_row.get(fund_id, 0.0))
            if nav <= 0:
                continue
            current_value = current_values.get(fund_id, 0.0)
            target_value = desired_values.get(fund_id, 0.0)
            amount = target_value - current_value
            if amount <= 0:
                continue
            gross_cash = amount * (1.0 + config.purchase_fee_rate)
            required_cash += gross_cash
            buy_plan.append({"fund_id": fund_id, "amount": amount, "nav": nav})

        cash_available = float(portfolio.cash + portfolio.frozen_cash)
        scale = min(1.0, cash_available / required_cash) if required_cash > 0 else 1.0

        for plan in buy_plan:
            amount = plan["amount"] * scale
            if amount <= 0:
                continue
            fee = amount * config.purchase_fee_rate
            fund_id = plan["fund_id"]
            portfolio.execute_purchase(
                fund_id=fund_id,
                amount=Decimal(str(amount)),
                nav=Decimal(str(plan["nav"])),
                fee=Decimal(str(fee)),
                fund_name=fund_names[fund_id],
            )
            trades.append(
                {
                    "update": trade_date,
                    "fund_id": fund_id,
                    "fund_name": fund_names[fund_id],
                    "side": 1,
                    "amount": float(amount),
                    "units": float(amount / plan["nav"]),
                    "fee": float(fee),
                }
            )

        return trades

    def _snapshot_holdings(
        self,
        portfolio: Portfolio,
        nav_row: pd.Series,
        trade_date: pd.Timestamp,
        portfolio_value: float,
    ) -> List[Dict[str, object]]:
        rows: List[Dict[str, object]] = []
        if portfolio_value <= 0:
            portfolio_value = 1.0

        cash_value = float(portfolio.cash + portfolio.frozen_cash)
        rows.append(
            {
                "update": trade_date,
                "fund_id": "cash",
                "fund_name": "cash",
                "units": cash_value,
                "weight": cash_value / portfolio_value,
                "nav": 1.0,
            }
        )
        for fund_id, position in portfolio.positions.items():
            nav = float(nav_row.get(fund_id, 0.0))
            market_value = float(position.total_units) * nav
            rows.append(
                {
                    "update": trade_date,
                    "fund_id": fund_id,
                    "fund_name": position.fund_name,
                    "units": float(position.total_units),
                    "weight": market_value / portfolio_value if portfolio_value else 0.0,
                    "nav": nav,
                }
            )
        return rows


__all__ = [
    "BacktestEngine",
    "BacktestResult",
    "MemoryDataProvider",
    "PortfolioConfig",
]
