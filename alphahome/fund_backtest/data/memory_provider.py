"""内存数据提供者 - 用于测试和示例"""

from typing import Optional, List, Dict
import pandas as pd

from .provider import DataProvider


class MemoryDataProvider(DataProvider):
    """
    内存数据提供者
    
    用于测试和示例，数据直接从 DataFrame 加载。
    """
    
    def __init__(
        self,
        nav_panel: Optional[pd.DataFrame] = None,
        rebalance_records: Optional[Dict[str, pd.DataFrame]] = None,
        fee_df: Optional[pd.DataFrame] = None,
        portfolio_configs: Optional[Dict[str, Dict]] = None
    ):
        """
        Args:
            nav_panel: 净值面板 (index=date, columns=fund_id)
            rebalance_records: 调仓记录 {portfolio_id: DataFrame}
            fee_df: 费率表 (index=fund_id, columns=[purchase_fee, redeem_fee])
            portfolio_configs: 组合配置 {portfolio_id: config_dict}
        """
        self._nav_panel = nav_panel if nav_panel is not None else pd.DataFrame()
        self._rebalance_records = rebalance_records if rebalance_records is not None else {}
        self._fee_df = fee_df if fee_df is not None else pd.DataFrame()
        self._portfolio_configs = portfolio_configs if portfolio_configs is not None else {}
    
    def set_nav_panel(self, nav_panel: pd.DataFrame) -> None:
        """设置净值面板"""
        self._nav_panel = nav_panel
    
    def set_rebalance_records(self, portfolio_id: str, records: pd.DataFrame) -> None:
        """设置调仓记录"""
        self._rebalance_records[portfolio_id] = records
    
    def set_fee_df(self, fee_df: pd.DataFrame) -> None:
        """设置费率表"""
        self._fee_df = fee_df
    
    def set_portfolio_config(self, portfolio_id: str, config: Dict) -> None:
        """设置组合配置"""
        self._portfolio_configs[portfolio_id] = config
    
    def get_fund_nav(
        self,
        fund_ids: List[str],
        start_date: str,
        end_date: str,
        nav_type: str = 'unit_nav'
    ) -> pd.DataFrame:
        if self._nav_panel.empty:
            return pd.DataFrame()

        start_ts = pd.to_datetime(start_date)
        end_ts = pd.to_datetime(end_date)
        
        # 筛选日期范围
        mask = (self._nav_panel.index >= start_ts) & (self._nav_panel.index <= end_ts)
        df = self._nav_panel.loc[mask]
        
        # 筛选基金
        available_funds = [f for f in fund_ids if f in df.columns]
        return df[available_funds] if available_funds else pd.DataFrame()
    
    def get_rebalance_records(
        self,
        portfolio_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        records = self._rebalance_records.get(portfolio_id, pd.DataFrame())
        
        if records.empty:
            return records

        records = records.copy()
        if 'rebalance_date' in records.columns:
            records['rebalance_date'] = pd.to_datetime(records['rebalance_date'])

        start_ts = pd.to_datetime(start_date) if start_date else None
        end_ts = pd.to_datetime(end_date) if end_date else None
        
        if start_ts is not None:
            records = records[records['rebalance_date'] >= start_ts]
        if end_ts is not None:
            records = records[records['rebalance_date'] <= end_ts]
        
        return records
    
    def get_fund_fee(self, fund_ids: List[str]) -> pd.DataFrame:
        if self._fee_df.empty:
            # 返回默认费率
            return pd.DataFrame({
                'fund_id': fund_ids,
                'purchase_fee': [0.015] * len(fund_ids),  # 默认1.5%申购费
                'redeem_fee': [0.005] * len(fund_ids),    # 默认0.5%赎回费
            })
        
        available = [f for f in fund_ids if f in self._fee_df.index]
        if not available:
            return pd.DataFrame({
                'fund_id': fund_ids,
                'purchase_fee': [0.015] * len(fund_ids),
                'redeem_fee': [0.005] * len(fund_ids),
            })
        
        result = self._fee_df.loc[available].reset_index()
        result.columns = ['fund_id', 'purchase_fee', 'redeem_fee']
        return result
    
    def get_calendar(
        self,
        start_date: str,
        end_date: str,
        calendar_type: str = 'trade',
        exchange: str = 'SSE'
    ) -> pd.DatetimeIndex:
        if self._nav_panel.empty:
            return pd.date_range(start_date, end_date, freq='B')  # 工作日

        start_ts = pd.to_datetime(start_date)
        end_ts = pd.to_datetime(end_date)
        mask = (self._nav_panel.index >= start_ts) & (self._nav_panel.index <= end_ts)
        return self._nav_panel.index[mask]
    
    def get_portfolio_config(self, portfolio_id: str) -> Optional[Dict]:
        return self._portfolio_configs.get(portfolio_id)
