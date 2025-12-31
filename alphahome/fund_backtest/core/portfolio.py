"""组合与持仓管理"""

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, Optional, List
import pandas as pd

from .order import Order, OrderSide, OrderStatus


@dataclass
class Position:
    """单个持仓"""
    fund_id: str
    fund_name: str = ""
    units: Decimal = field(default_factory=lambda: Decimal(0))
    frozen_units: Decimal = field(default_factory=lambda: Decimal(0))
    nav: Decimal = field(default_factory=lambda: Decimal(1))
    cost: Decimal = field(default_factory=lambda: Decimal(0))
    last_update: Optional[date] = None
    rebalance_id: int = 0
    
    @property
    def market_value(self) -> Decimal:
        return (self.units + self.frozen_units) * self.nav
    
    @property
    def total_units(self) -> Decimal:
        return self.units + self.frozen_units
    
    @property
    def pnl(self) -> Decimal:
        if self.cost == 0:
            return Decimal(0)
        return self.market_value - self.total_units * self.cost
    
    def to_dict(self) -> dict:
        return {
            'fund_id': self.fund_id,
            'fund_name': self.fund_name,
            'units': float(self.units),
            'frozen_units': float(self.frozen_units),
            'nav': float(self.nav),
            'cost': float(self.cost),
            'market_value': float(self.market_value),
            'last_update': self.last_update,
            'rebalance_id': self.rebalance_id,
        }


@dataclass
class Portfolio:
    """组合状态管理"""
    portfolio_id: str
    portfolio_name: str = ""
    cash: Decimal = field(default_factory=lambda: Decimal(0))
    frozen_cash: Decimal = field(default_factory=lambda: Decimal(0))
    pending_redeem_cash: List[dict] = field(default_factory=list)  # 待到账赎回资金 [{amount, settle_date}]
    positions: Dict[str, Position] = field(default_factory=dict)
    last_update: Optional[date] = None
    _precision: int = 2
    
    @property
    def pending_cash_total(self) -> Decimal:
        """待到账赎回资金总额（参与估值）"""
        return sum(Decimal(str(item['amount'])) for item in self.pending_redeem_cash)
    
    @property
    def market_value(self) -> Decimal:
        pos_value = sum(p.market_value for p in self.positions.values())
        return self.cash + self.frozen_cash + self.pending_cash_total + pos_value
    
    def settle_pending_redeem(self, current_date: date) -> Decimal:
        """结算到期的待到账赎回资金，返回到账金额"""
        settled_amount = Decimal(0)
        remaining = []
        for item in self.pending_redeem_cash:
            if item['settle_date'] <= current_date:
                settled_amount += Decimal(str(item['amount']))
            else:
                remaining.append(item)
        self.pending_redeem_cash = remaining
        self.cash += settled_amount
        return settled_amount
    
    def add_pending_redeem(self, amount: Decimal, settle_date: date) -> None:
        """添加待到账赎回资金"""
        self.pending_redeem_cash.append({
            'amount': float(amount),
            'settle_date': settle_date
        })
    
    def get_weights(self) -> pd.Series:
        mv = self.market_value
        if mv == 0:
            return pd.Series(dtype=float)
        weights = {fid: float(p.market_value / mv) for fid, p in self.positions.items()}
        weights['cash'] = float((self.cash + self.frozen_cash) / mv)
        return pd.Series(weights)
    
    def get_position(self, fund_id: str) -> Optional[Position]:
        return self.positions.get(fund_id)
    
    def update_nav(self, nav_series: pd.Series, dt: date) -> None:
        """更新所有持仓的净值"""
        for fund_id, pos in self.positions.items():
            if fund_id in nav_series.index:
                pos.nav = Decimal(str(nav_series[fund_id]))
                pos.last_update = dt
        self.last_update = dt
    
    def freeze_cash(self, amount: Decimal) -> bool:
        """冻结现金用于申购
        
        增加 0.10 元容差处理浮点精度问题：
        当 amount 略大于 cash（差额 <= 0.10）时，自动调整为 cash
        """
        amount = self._round(amount)
        tolerance = Decimal('0.10')
        if amount > self.cash:
            # 容差处理：差额在容差范围内时，调整 amount 为 cash
            if amount - self.cash <= tolerance:
                amount = self.cash
            else:
                return False
        self.cash -= amount
        self.frozen_cash += amount
        return True
    
    def unfreeze_cash(self, amount: Decimal) -> None:
        """解冻现金"""
        amount = self._round(amount)
        self.frozen_cash -= amount
        self.cash += amount
    
    def freeze_units(self, fund_id: str, units: Decimal) -> bool:
        """冻结份额用于赎回"""
        units = self._round(units)
        pos = self.positions.get(fund_id)
        if not pos or units > pos.units:
            return False
        pos.units -= units
        pos.frozen_units += units
        return True

    def unfreeze_units(self, fund_id: str, units: Decimal) -> None:
        """解冻份额（撤销赎回冻结）"""
        units = self._round(units)
        pos = self.positions.get(fund_id)
        if not pos:
            return
        pos.frozen_units -= units
        pos.units += units
    
    def execute_purchase(self, fund_id: str, amount: Decimal, nav: Decimal, 
                         fee: Decimal, fund_name: str = "", rebalance_id: int = 0) -> Decimal:
        """执行申购，返回获得的份额"""
        net_amount = amount - fee
        units = self._round(net_amount / nav)
        
        if fund_id not in self.positions:
            self.positions[fund_id] = Position(fund_id=fund_id, fund_name=fund_name)
        
        pos = self.positions[fund_id]
        # 更新平均成本
        if pos.units > 0:
            total_cost = pos.cost * pos.units + net_amount
            pos.cost = self._round(total_cost / (pos.units + units), 4)
        else:
            pos.cost = nav
        
        pos.units += units
        pos.nav = nav
        pos.rebalance_id = rebalance_id
        self.frozen_cash -= amount
        
        return units
    
    def execute_redeem(self, fund_id: str, units: Decimal, nav: Decimal, 
                       fee: Decimal, add_to_cash: bool = True) -> Decimal:
        """执行赎回，返回获得的现金
        
        Args:
            fund_id: 基金代码
            units: 赎回份额
            nav: 净值
            fee: 赎回费
            add_to_cash: 是否直接加到现金（False时由调用方处理待到账逻辑）
        """
        gross_amount = units * nav
        net_amount = self._round(gross_amount - fee)
        
        pos = self.positions.get(fund_id)
        if pos:
            pos.frozen_units -= units
            # 平均成本法：赎回不改变剩余份额的单位成本（pos.cost）
            
            # 清理空仓
            if pos.total_units <= 0:
                del self.positions[fund_id]
        
        if add_to_cash:
            self.cash += net_amount
        return net_amount
    
    def deduct_fee(self, amount: Decimal, fee_fund_id: Optional[str] = None) -> bool:
        """扣除管理费"""
        amount = self._round(amount)
        if fee_fund_id and fee_fund_id in self.positions:
            pos = self.positions[fee_fund_id]
            units_to_deduct = self._round(amount / pos.nav)
            if units_to_deduct <= pos.units:
                pos.units -= units_to_deduct
                return True
        # 从现金扣除
        if amount <= self.cash:
            self.cash -= amount
            return True
        return False
    
    def _round(self, value: Decimal, precision: Optional[int] = None) -> Decimal:
        p = precision if precision is not None else self._precision
        return Decimal(str(value)).quantize(Decimal(10) ** -p, rounding=ROUND_HALF_UP)
    
    def to_dataframe(self) -> pd.DataFrame:
        """导出为 DataFrame"""
        records = [{'fund_id': 'cash', 'fund_name': '现金', 'units': float(self.cash),
                    'frozen_units': float(self.frozen_cash), 'nav': 1.0, 'cost': 1.0,
                    'market_value': float(self.cash + self.frozen_cash)}]
        records.extend([p.to_dict() for p in self.positions.values()])
        df = pd.DataFrame(records)
        df['weight'] = df['market_value'] / df['market_value'].sum()
        df['update'] = self.last_update
        df['portfolio_id'] = self.portfolio_id
        return df
