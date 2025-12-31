"""交易指令定义"""

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from enum import Enum
from typing import Optional
import uuid


class OrderSide(Enum):
    """交易方向"""
    BUY = 1      # 申购
    SELL = -1    # 赎回
    FEE = 0      # 管理费扣除


class OrderStatus(Enum):
    """订单状态"""
    PENDING = "pending"      # 待执行
    FROZEN = "frozen"        # 已冻结
    FILLED = "filled"        # 已成交
    CANCELLED = "cancelled"  # 已取消


@dataclass
class Order:
    """交易指令"""
    portfolio_id: str
    fund_id: str
    side: OrderSide
    order_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    amount: Optional[Decimal] = None   # 申购金额
    units: Optional[Decimal] = None    # 赎回份额
    nav: Optional[Decimal] = None      # 执行净值
    fee: Decimal = field(default_factory=lambda: Decimal(0))
    status: OrderStatus = OrderStatus.PENDING
    create_date: Optional[date] = None
    settle_date: Optional[date] = None
    rebalance_id: Optional[int] = None
    fund_name: str = ""
    
    def to_dict(self) -> dict:
        return {
            'order_id': self.order_id,
            'portfolio_id': self.portfolio_id,
            'fund_id': self.fund_id,
            'fund_name': self.fund_name,
            'side': self.side.value,
            'amount': float(self.amount) if self.amount else None,
            'units': float(self.units) if self.units else None,
            'nav': float(self.nav) if self.nav else None,
            'fee': float(self.fee),
            'status': self.status.value,
            'create_date': self.create_date,
            'settle_date': self.settle_date,
            'rebalance_id': self.rebalance_id,
        }
