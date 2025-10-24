# -*- coding: utf-8 -*-

"""
Akshare API 封装类

借鉴tushare和pytdx的实现，为akshare数据源提供统一的API接口封装。
akshare是一个免费的开源金融数据接口库，提供多种数据源的访问。
"""

import logging
from typing import Any, Dict, List, Optional

import pandas as pd

from ....common.logging_utils import get_logger

logger = get_logger(__name__)


class AkshareAPI:
    """Akshare API 封装类"""

    def __init__(self):
        """
        初始化Akshare API

        akshare是一个纯Python库，通常不需要复杂的连接管理，
        但我们保留这个结构以保持与其他数据源的一致性。
        """
        self.logger = logger
        self.logger.info("AkshareAPI 已初始化")

    async def get_stock_info_sh(self) -> List[Dict[str, Any]]:
        """
        获取上海市场股票信息
        对标 hikyuu 的 get_stk_code_name_list 函数逻辑

        Returns:
            上海市场股票信息列表
        """
        try:
            import akshare as ak
            # 对标 hikyuu 的实现，使用多个分类获取完整的股票列表
            ind_list = ["主板A股", "主板B股", "科创板"]
            df = None

            for ind in ind_list:
                try:
                    tmp_df = ak.stock_info_sh_name_code(ind)
                    if not tmp_df.empty:
                        # 对标 hikyuu 的列名重命名逻辑
                        tmp_df.rename(columns={'证券代码': 'code', '证券简称': 'name'}, inplace=True)
                        df = pd.concat([df, tmp_df], ignore_index=True) if df is not None else tmp_df
                        self.logger.debug(f"获取上海市场 {ind} 分类: {len(tmp_df)} 只股票")
                except Exception as e:
                    self.logger.warning(f"获取上海市场 {ind} 分类失败: {e}")
                    continue

            if df is None or df.empty:
                self.logger.warning("上海市场未获取到任何股票信息")
                return []

            # 去重并转换格式
            df = df.drop_duplicates(subset=['code'])
            stocks = []
            for _, row in df.iterrows():
                stocks.append({
                    'code': str(row['code']),
                    'name': str(row['name']),
                    'market': 'SH',
                    'exchange': '上海证券交易所'
                })

            return stocks

        except Exception as e:
            self.logger.error(f"获取上海市场股票信息失败: {e}")
            return []

    async def get_stock_info_sz(self) -> List[Dict[str, Any]]:
        """
        获取深圳市场股票信息
        对标 hikyuu 的 get_stk_code_name_list 函数逻辑

        Returns:
            深圳市场股票信息列表
        """
        try:
            import akshare as ak
            # 对标 hikyuu 的实现，使用多个分类获取完整的股票列表
            ind_list = ["A股列表", "B股列表"]
            df = None

            for ind in ind_list:
                try:
                    tmp_df = ak.stock_info_sz_name_code(ind)
                    if not tmp_df.empty:
                        # 对标 hikyuu 的列名重命名逻辑
                        if 'A股代码' in tmp_df.columns:
                            tmp_df.rename(columns={'A股代码': 'code', 'A股简称': 'name'}, inplace=True)
                        elif '证券代码' in tmp_df.columns:
                            tmp_df.rename(columns={'证券代码': 'code', '证券简称': 'name'}, inplace=True)
                        else:
                            # 备用方案：使用位置索引
                            tmp_df = tmp_df.iloc[:, [1, 2]].copy()
                            tmp_df.columns = ['code', 'name']

                        df = pd.concat([df, tmp_df], ignore_index=True) if df is not None else tmp_df
                        self.logger.debug(f"获取深圳市场 {ind} 分类: {len(tmp_df)} 只股票")
                except Exception as e:
                    self.logger.warning(f"获取深圳市场 {ind} 分类失败: {e}")
                    continue

            if df is None or df.empty:
                self.logger.warning("深圳市场未获取到任何股票信息")
                return []

            # 去重并转换格式
            df = df.drop_duplicates(subset=['code'])
            stocks = []
            for _, row in df.iterrows():
                stocks.append({
                    'code': str(row['code']),
                    'name': str(row['name']),
                    'market': 'SZ',
                    'exchange': '深圳证券交易所'
                })

            return stocks

        except Exception as e:
            self.logger.error(f"获取深圳市场股票信息失败: {e}")
            return []

    async def get_stock_info_bj(self) -> List[Dict[str, Any]]:
        """
        获取北京市场股票信息

        Returns:
            北京市场股票信息列表
        """
        try:
            import akshare as ak
            df = ak.stock_info_bj_name_code()
            if df.empty:
                return []

            # 转换DataFrame为字典列表
            stocks = []
            for _, row in df.iterrows():
                stocks.append({
                    'code': str(row['证券代码']),
                    'name': str(row['证券简称']),
                    'market': 'BJ',
                    'exchange': '北京证券交易所'
                })
            return stocks

        except Exception as e:
            self.logger.error(f"获取北京市场股票信息失败: {e}")
            return []

    async def get_all_stocks_info(self, market_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        获取所有市场的股票基本信息

        Args:
            market_filter: 市场过滤器 ('SH', 'SZ', 'BJ')

        Returns:
            所有股票基本信息列表
        """
        all_stocks = []

        # 上海市场
        if not market_filter or market_filter.upper() == 'SH':
            self.logger.info("获取上海市场股票信息")
            sh_stocks = await self.get_stock_info_sh()
            all_stocks.extend(sh_stocks)
            self.logger.info(f"上海市场: {len(sh_stocks)} 只股票")

        # 深圳市场
        if not market_filter or market_filter.upper() == 'SZ':
            self.logger.info("获取深圳市场股票信息")
            sz_stocks = await self.get_stock_info_sz()
            all_stocks.extend(sz_stocks)
            self.logger.info(f"深圳市场: {len(sz_stocks)} 只股票")

        # 北京市场
        if not market_filter or market_filter.upper() == 'BJ':
            self.logger.info("获取北京市场股票信息")
            bj_stocks = await self.get_stock_info_bj()
            all_stocks.extend(bj_stocks)
            self.logger.info(f"北京市场: {len(bj_stocks)} 只股票")

        if not all_stocks:
            self.logger.warning("未获取到任何股票信息")

        return all_stocks

    async def close(self):
        """清理资源"""
        # akshare通常不需要特殊的清理操作
        pass

    async def __aenter__(self):
        """异步上下文管理器入口"""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口"""
        await self.close()
