#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, Optional


@dataclass
class Position:
    fund_id: str
    fund_name: str
    total_units: Decimal = Decimal("0")
    available_units: Decimal = Decimal("0")
    frozen_units: Decimal = Decimal("0")
    cost: Decimal = Decimal("0")

    @property
    def units(self) -> Decimal:
        return self.total_units


class Portfolio:
    def __init__(self, portfolio_id: str, portfolio_name: str, cash: Decimal) -> None:
        self.portfolio_id = portfolio_id
        self.portfolio_name = portfolio_name
        self.cash = Decimal(cash)
        self.frozen_cash = Decimal("0")
        self.positions: Dict[str, Position] = {}

    def freeze_cash(self, amount: Decimal) -> bool:
        amount = Decimal(amount)
        if amount < 0 or self.cash < amount:
            return False
        self.cash -= amount
        self.frozen_cash += amount
        return True

    def freeze_units(self, fund_id: str, units: Decimal) -> bool:
        position = self.positions.get(fund_id)
        units = Decimal(units)
        if position is None or position.available_units < units or units < 0:
            return False
        position.available_units -= units
        position.frozen_units += units
        return True

    def get_position(self, fund_id: str) -> Optional[Position]:
        return self.positions.get(fund_id)

    def execute_purchase(
        self,
        fund_id: str,
        amount: Decimal,
        nav: Decimal,
        fee: Decimal,
        fund_name: str,
        rebalance_id: Optional[int] = None,
    ) -> None:
        del rebalance_id
        amount = Decimal(amount)
        nav = Decimal(nav)
        fee = Decimal(fee)
        if nav <= 0 or amount < 0 or fee < 0:
            raise ValueError("invalid purchase arguments")

        funded_from_frozen = min(self.frozen_cash, amount)
        self.frozen_cash -= funded_from_frozen
        remaining_amount = amount - funded_from_frozen
        if remaining_amount > 0:
            self.cash -= remaining_amount
        self.cash -= fee

        units = amount / nav if nav > 0 else Decimal("0")
        position = self.positions.get(fund_id)
        if position is None:
            position = Position(fund_id=fund_id, fund_name=fund_name)
            self.positions[fund_id] = position

        existing_cost_value = position.total_units * position.cost
        new_total_units = position.total_units + units
        if new_total_units > 0:
            position.cost = (existing_cost_value + amount) / new_total_units
        position.total_units = new_total_units
        position.available_units += units

    def execute_redeem(
        self,
        fund_id: str,
        units: Decimal,
        nav: Decimal,
        fee: Decimal,
    ) -> None:
        position = self.positions.get(fund_id)
        if position is None:
            raise ValueError(f"position not found: {fund_id}")

        units = Decimal(units)
        nav = Decimal(nav)
        fee = Decimal(fee)
        if units < 0 or nav <= 0 or fee < 0:
            raise ValueError("invalid redeem arguments")

        frozen_to_use = min(position.frozen_units, units)
        position.frozen_units -= frozen_to_use
        remaining_units = units - frozen_to_use
        if remaining_units > 0:
            if position.available_units < remaining_units:
                raise ValueError("insufficient units")
            position.available_units -= remaining_units

        position.total_units -= units
        proceeds = units * nav - fee
        self.cash += proceeds

        if position.total_units <= 0:
            self.positions.pop(fund_id, None)


__all__ = ["Portfolio", "Position"]
