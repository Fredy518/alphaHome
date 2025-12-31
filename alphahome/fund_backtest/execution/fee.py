"""费用计算器"""

from typing import Dict, Optional
import pandas as pd


class FeeCalculator:
    """
    费用计算器
    
    支持：
    - 申购费率
    - 赎回费率
    - 管理费率
    """
    
    def __init__(self):
        self._purchase_fees: Dict[str, float] = {}
        self._redeem_fees: Dict[str, float] = {}
        self._management_fees: Dict[str, float] = {}
        
        # 默认费率
        self._default_purchase_fee = 0.015  # 1.5%
        self._default_redeem_fee = 0.005    # 0.5%
        self._default_management_fee = 0.0
    
    def load_fees(self, fee_df: pd.DataFrame) -> None:
        """
        加载费率数据
        
        Args:
            fee_df: DataFrame with columns [fund_id, purchase_fee, redeem_fee]
        """
        if fee_df.empty:
            return
        
        for _, row in fee_df.iterrows():
            fund_id = row.get('fund_id')
            if fund_id:
                if 'purchase_fee' in row:
                    self._purchase_fees[fund_id] = float(row['purchase_fee'])
                if 'redeem_fee' in row:
                    self._redeem_fees[fund_id] = float(row['redeem_fee'])
                if 'management_fee' in row:
                    self._management_fees[fund_id] = float(row['management_fee'])
    
    def set_fee(self, fund_id: str, purchase_fee: Optional[float] = None,
                redeem_fee: Optional[float] = None, management_fee: Optional[float] = None) -> None:
        """设置单个基金的费率"""
        if purchase_fee is not None:
            self._purchase_fees[fund_id] = purchase_fee
        if redeem_fee is not None:
            self._redeem_fees[fund_id] = redeem_fee
        if management_fee is not None:
            self._management_fees[fund_id] = management_fee
    
    def get_purchase_fee(self, fund_id: str) -> float:
        """获取申购费率"""
        return self._purchase_fees.get(fund_id, self._default_purchase_fee)
    
    def get_redeem_fee(self, fund_id: str) -> float:
        """获取赎回费率"""
        return self._redeem_fees.get(fund_id, self._default_redeem_fee)
    
    def get_management_fee(self, fund_id: str) -> float:
        """获取管理费率"""
        return self._management_fees.get(fund_id, self._default_management_fee)
    
    def calc_purchase_fee(self, amount: float, fund_id: str, discount: float = 1.0) -> float:
        """计算申购费用"""
        rate = self.get_purchase_fee(fund_id) * discount
        return round(amount * rate, 2)
    
    def calc_redeem_fee(self, amount: float, fund_id: str, discount: float = 1.0) -> float:
        """计算赎回费用"""
        rate = self.get_redeem_fee(fund_id) * discount
        return round(amount * rate, 2)
