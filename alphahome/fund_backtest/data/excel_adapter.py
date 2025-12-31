"""
Excel 数据适配器

用于从 Excel 文件导入数据到 alphadb 或用于调试。
仅作为迁移/调试工具，生产环境应使用 AlphaDBDataProvider。
"""

from typing import Optional, List, Dict
import pandas as pd
import logging

from .provider import DataProvider

logger = logging.getLogger(__name__)


class ExcelAdapter(DataProvider):
    """
    Excel 数据适配器
    
    从 xlsm/xlsx 文件读取数据，兼容原有的 Excel 格式。
    """
    
    def __init__(self, excel_path: str):
        """
        Args:
            excel_path: Excel 文件路径
        """
        self.excel_path = excel_path
        self._nav_df: Optional[pd.DataFrame] = None
        self._trade_df: Optional[pd.DataFrame] = None
        self._fee_df: Optional[pd.DataFrame] = None
        self._strat_df: Optional[pd.DataFrame] = None
        self._channel_df: Optional[pd.DataFrame] = None
        self._loaded = False
    
    def load(self) -> None:
        """加载 Excel 数据"""
        try:
            import xlwings as xw
            
            wb = xw.Book(self.excel_path)
            
            # 读取净值数据
            nav_sht = wb.sheets['Nav']
            self._nav_df = nav_sht['A1'].options(pd.DataFrame, expand='table').value
            self._nav_df.index = pd.to_datetime(self._nav_df.index)
            
            # 读取调仓记录
            trade_sht = wb.sheets['调仓记录']
            self._trade_df = trade_sht['A1'].options(pd.DataFrame, expand='table', index=False).value
            
            # 读取费率
            fee_sht = wb.sheets['费率']
            self._fee_df = fee_sht['A2:C2'].options(pd.DataFrame, expand='down').value
            
            # 读取策略配置
            strat_sht = wb.sheets['组合']
            self._strat_df = strat_sht['A1'].options(pd.DataFrame, expand='table', index=True).value
            
            # 读取渠道配置
            channel_sht = wb.sheets['渠道']
            self._channel_df = channel_sht['A1'].options(pd.DataFrame, expand='table', index=True).value
            
            wb.close()
            self._loaded = True
            logger.info(f"Excel 数据加载完成: {self.excel_path}")
            
        except ImportError:
            logger.error("xlwings 未安装，请运行: pip install xlwings")
            raise
        except Exception as e:
            logger.error(f"加载 Excel 失败: {e}")
            raise
    
    def _ensure_loaded(self) -> None:
        if not self._loaded:
            self.load()
    
    def get_fund_nav(
        self,
        fund_ids: List[str],
        start_date: str,
        end_date: str,
        nav_type: str = 'unit_nav'
    ) -> pd.DataFrame:
        self._ensure_loaded()
        
        if self._nav_df is None:
            return pd.DataFrame()

        start_ts = pd.to_datetime(start_date)
        end_ts = pd.to_datetime(end_date)
        
        # 筛选日期
        mask = (self._nav_df.index >= start_ts) & (self._nav_df.index <= end_ts)
        df = self._nav_df.loc[mask]
        
        # 筛选基金
        available = [f for f in fund_ids if f in df.columns]
        return df[available] if available else pd.DataFrame()
    
    def get_rebalance_records(
        self,
        portfolio_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        self._ensure_loaded()
        
        if self._trade_df is None:
            return pd.DataFrame()
        
        # 筛选组合
        df = self._trade_df[self._trade_df['组合'] == portfolio_id].copy()
        
        if df.empty:
            return pd.DataFrame()
        
        # 转换列名
        df = df.rename(columns={
            '代码': 'fund_id',
            '证券名称': 'fund_name',
            '持仓权重': 'target_weight',
            '调仓时间': 'rebalance_date',
        })
        
        df['rebalance_date'] = pd.to_datetime(df['rebalance_date'])

        start_ts = pd.to_datetime(start_date) if start_date else None
        end_ts = pd.to_datetime(end_date) if end_date else None
        
        if start_ts is not None:
            df = df[df['rebalance_date'] >= start_ts]
        if end_ts is not None:
            df = df[df['rebalance_date'] <= end_ts]
        
        return df[['rebalance_date', 'fund_id', 'fund_name', 'target_weight']]
    
    def get_fund_fee(self, fund_ids: List[str]) -> pd.DataFrame:
        self._ensure_loaded()
        
        if self._fee_df is None or self._fee_df.empty:
            return pd.DataFrame({
                'fund_id': fund_ids,
                'purchase_fee': [0.015] * len(fund_ids),
                'redeem_fee': [0.005] * len(fund_ids),
            })
        
        # 转换列名
        fee_df = self._fee_df.copy()
        fee_df = fee_df.reset_index()
        fee_df.columns = ['fund_id', 'purchase_fee', 'redeem_fee']
        
        # 转换费率为小数
        fee_df['purchase_fee'] = pd.to_numeric(fee_df['purchase_fee'], errors='coerce') / 100
        fee_df['redeem_fee'] = pd.to_numeric(fee_df['redeem_fee'], errors='coerce') / 100
        
        available = fee_df[fee_df['fund_id'].isin(fund_ids)]
        return available
    
    def get_calendar(
        self,
        start_date: str,
        end_date: str,
        calendar_type: str = 'trade',
        exchange: str = 'SSE'
    ) -> pd.DatetimeIndex:
        self._ensure_loaded()
        
        if self._nav_df is None:
            return pd.date_range(start_date, end_date, freq='B')

        start_ts = pd.to_datetime(start_date)
        end_ts = pd.to_datetime(end_date)
        mask = (self._nav_df.index >= start_ts) & (self._nav_df.index <= end_ts)
        return self._nav_df.index[mask]
    
    def get_portfolio_config(self, portfolio_id: str) -> Optional[Dict]:
        self._ensure_loaded()
        
        if self._strat_df is None or self._channel_df is None:
            return None
        
        # 合并策略和渠道配置
        merged = pd.merge(
            self._strat_df, self._channel_df,
            left_on='渠道', right_index=True
        ).T
        
        if portfolio_id not in merged.columns:
            return None
        
        config = merged[portfolio_id].to_dict()
        
        return {
            'portfolio_id': portfolio_id,
            'portfolio_name': config.get('组合名称', portfolio_id),
            'initial_cash': float(config.get('起投金额', 1000)),
            'setup_date': str(config.get('上线日期', '')),
            'rebalance_delay': int(config.get('调仓效率', 2)),
            'purchase_fee_rate': float(config.get('申购折扣', 0.1)) * 0.015,  # 折扣*标准费率
            'redeem_fee_rate': float(config.get('赎回折扣', 0.0)) * 0.005,   # 折扣*标准费率
            'management_fee': float(config.get('固定费率', 0)) / 100,
            'rebalance_effective_delay': 1,  # 默认T+1生效
        }
    
    def get_portfolio_list(self) -> List[str]:
        """获取所有组合ID列表"""
        self._ensure_loaded()
        
        if self._trade_df is None:
            return []
        
        return list(self._trade_df['组合'].unique())
