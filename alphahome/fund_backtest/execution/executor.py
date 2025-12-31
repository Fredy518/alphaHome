"""交易执行器"""

from typing import List, Optional
from datetime import date
import pandas as pd

from ..core.order import Order, OrderSide, OrderStatus
from ..core.portfolio import Portfolio


class TradeExecutor:
    """
    交易执行器
    
    职责：
    - 生成交易订单
    - 执行订单冻结/解冻
    - 处理交易结算
    """
    
    def __init__(self):
        pass
    
    def generate_rebalance_orders(
        self,
        portfolio: Portfolio,
        target_weights: pd.Series,
        nav_series: pd.Series,
        dt: date,
        rebalance_id: int
    ) -> List[Order]:
        """
        生成调仓订单
        
        Args:
            portfolio: 当前组合状态
            target_weights: 目标权重 (index=fund_id, values=weight)
            nav_series: 当日净值 (index=fund_id, values=nav)
            dt: 当前日期
            rebalance_id: 调仓编号
        
        Returns:
            订单列表
        """
        orders = []
        total_value = float(portfolio.market_value)
        
        if total_value <= 0:
            return orders
        
        current_weights = portfolio.get_weights()
        
        # 计算需要调整的基金
        all_funds = set(target_weights.index) | set(current_weights.index)
        all_funds.discard('cash')
        
        for fund_id in all_funds:
            target_w = target_weights.get(fund_id, 0)
            current_w = current_weights.get(fund_id, 0)
            delta_w = target_w - current_w
            
            if abs(delta_w) < 0.001:  # 忽略微小差异
                continue
            
            nav = nav_series.get(fund_id, 1.0)
            
            if delta_w < 0:
                # 需要赎回
                redeem_value = abs(delta_w) * total_value
                units = redeem_value / nav
                pos = portfolio.get_position(fund_id)
                if pos:
                    units = min(units, float(pos.units))
                
                if units > 0:
                    orders.append(Order(
                        portfolio_id=portfolio.portfolio_id,
                        fund_id=fund_id,
                        side=OrderSide.SELL,
                        units=units,
                        create_date=dt,
                        rebalance_id=rebalance_id
                    ))
            else:
                # 需要申购
                purchase_value = delta_w * total_value
                if purchase_value > 0:
                    orders.append(Order(
                        portfolio_id=portfolio.portfolio_id,
                        fund_id=fund_id,
                        side=OrderSide.BUY,
                        amount=purchase_value,
                        create_date=dt,
                        rebalance_id=rebalance_id
                    ))
        
        return orders
