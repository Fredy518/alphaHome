# -*- coding: utf-8 -*-

"""
基于 Akshare API 的数据任务基类

借鉴tushare和pytdx的实现，为所有akshare任务提供通用功能。
akshare是一个免费的开源金融数据接口库。
"""

import abc
import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import pandas as pd

from alphahome.fetchers.base.fetcher_task import FetcherTask
from .akshare_api import AkshareAPI


class AkshareTask(FetcherTask, abc.ABC):
    """
    一个针对 Akshare API 的抽象任务基类。

    此类继承自 FetcherTask，并实现了 Akshare 特有的技术逻辑：
    - 在 `fetch_batch` 中统一调用 Akshare API 并进行标准数据转换。
    - 提供一个通用的 `prepare_params` 实现。

    它将 `get_batch_list` 继续声明为抽象方法，因为每个具体的 Akshare 接口
    都有其独特的批处理要求。
    """

    data_source = "akshare"

    # Akshare 特有配置
    default_market_filter: Optional[str] = None  # 默认市场过滤器

    def __init__(self, db_connection, api=None, **kwargs):
        """
        初始化 AkshareTask。

        Args:
            db_connection: 数据库连接。
            api (AkshareAPI, optional): 已初始化的 AkshareAPI 实例。
            **kwargs: 传递给 FetcherTask 的参数。
        """
        super().__init__(db_connection, **kwargs)

        # 初始化API实例
        self.api = api or AkshareAPI()

    def _apply_config(self, task_config: Dict):
        """合并代码默认值和配置文件设置。"""
        super()._apply_config(task_config)

    async def prepare_params(self, batch_params: Dict) -> Dict[str, Any]:
        """
        准备 Akshare API 请求的参数。

        Args:
            batch_params: 批次参数字典。

        Returns:
            准备好的参数字典。
        """
        prepared_params = {}

        # 复制所有批次参数
        prepared_params.update(batch_params)

        # 确保必要的参数存在
        if 'market' not in prepared_params and self.default_market_filter:
            prepared_params['market'] = self.default_market_filter

        return prepared_params

    async def fetch_batch(self, params: Dict[str, Any], stop_event: Optional[asyncio.Event] = None) -> Optional[pd.DataFrame]:
        """
        从 Akshare API 获取一批数据。

        Args:
            params: 请求参数。
            stop_event: 取消事件。

        Returns:
            获取到的数据 DataFrame。
        """
        try:
            # 检查是否被取消
            if stop_event and stop_event.is_set():
                self.logger.warning("任务被取消")
                raise asyncio.CancelledError("任务在fetch_batch中被取消")

            # 调用具体的获取方法（由子类实现）
            raw_data = await self._fetch_raw_data(params)

            if raw_data is None:
                return pd.DataFrame()

            # 转换数据格式
            df = self._transform_data(raw_data, params)

            self.logger.debug(f"成功获取 {len(df)} 条数据记录")
            return df

        except Exception as e:
            self.logger.error(f"获取批次数据时出错: {e}", exc_info=True)
            return pd.DataFrame()

    @abc.abstractmethod
    async def _fetch_raw_data(self, params: Dict) -> Optional[List[Dict]]:
        """
        从Akshare API获取原始数据（由子类实现）

        Args:
            params: 准备好的参数

        Returns:
            原始数据列表
        """
        pass

    @abc.abstractmethod
    def _transform_data(self, raw_data: List[Dict], params: Dict) -> pd.DataFrame:
        """
        转换原始数据为标准格式（由子类实现）

        Args:
            raw_data: 原始数据
            params: 参数信息

        Returns:
            标准格式的DataFrame
        """
        pass

    async def cleanup(self):
        """清理资源"""
        if self.api:
            await self.api.close()
        await super().cleanup()


class StockTask(AkshareTask, abc.ABC):
    """
    股票相关任务的基类

    为所有股票数据获取任务提供通用的股票处理逻辑，包括：
    - 退市股票过滤
    - 股票类型判断
    - 市场ID映射
    - 股票类型映射
    """

    # 市场ID映射 (对标hikyuu market表)
    MARKET_ID_MAPPING = {
        'SH': 1,  # 上海证券交易所
        'SZ': 2,  # 深圳证券交易所
        'BJ': 3   # 北京证券交易所
    }

    # 股票类型映射 (对标hikyuu stocktypeinfo表)
    STOCK_TYPE_MAPPING = {
        'SH': {
            '60': 1,   # 上证A股
            '90': 3,   # 上证B股
            '68': 1,   # 科创板A股 (归类为A股)
            '50': 4,   # 上证基金
            '51': 5,   # 上证ETF
        },
        'SZ': {
            '00': 1,   # 深证A股
            '20': 3,   # 深证B股
            '30': 8,   # 创业板
            '15': 4,   # 深证基金
            '16': 4,   # 深证基金
            '18': 4,   # 深证基金
            '39': 2,   # 深证指数
            '159': 5,  # 深证ETF
        },
        'BJ': {
            '8': 1,    # 北证A股
            '4': 1,    # 北证A股
        }
    }

    def _get_stock_type(self, market: str, code: str) -> int:
        """
        根据市场和股票代码确定股票类型

        对标 hikyuu 的 stocktypeinfo 表逻辑
        """
        try:
            # 获取代码前缀
            if len(code) >= 2:
                prefix = code[:2]
                market_types = self.STOCK_TYPE_MAPPING.get(market, {})
                return market_types.get(prefix, 1)  # 默认返回A股类型

            # 北京市场的特殊处理
            if market == 'BJ' and len(code) >= 1:
                prefix = code[0]
                market_types = self.STOCK_TYPE_MAPPING.get(market, {})
                return market_types.get(prefix, 1)  # 默认返回A股类型

            return 1  # 默认A股类型

        except Exception as e:
            self.logger.warning(f"获取股票类型失败 {market}.{code}: {e}")
            return 1  # 默认A股类型

    async def _get_delisted_stock_codes(self) -> Set[str]:
        """
        获取退市股票代码集合

        对标 hikyuu 的 import_stock_name 函数中获取退市股票的逻辑
        """
        try:
            import akshare as ak
            delisted_codes = set()

            # 上海市场退市股票
            try:
                sh_delist_df = ak.stock_info_sh_delist()
                if not sh_delist_df.empty and '公司代码' in sh_delist_df.columns:
                    sh_codes = sh_delist_df['公司代码'].astype(str).tolist()
                    delisted_codes.update(sh_codes)
                    self.logger.debug(f"获取上海退市股票: {len(sh_codes)} 只")
            except Exception as e:
                self.logger.warning(f"获取上海退市股票失败: {e}")

            # 深圳市场退市股票
            try:
                for status in ['暂停上市公司', '终止上市公司']:
                    sz_delist_df = ak.stock_info_sz_delist(status)
                    if not sz_delist_df.empty and '证券代码' in sz_delist_df.columns:
                        sz_codes = sz_delist_df['证券代码'].astype(str).tolist()
                        delisted_codes.update(sz_codes)
                        self.logger.debug(f"获取深圳{status}: {len(sz_codes)} 只")
            except Exception as e:
                self.logger.warning(f"获取深圳退市股票失败: {e}")

            # 北京市场退市股票（如果有API的话）
            try:
                bj_delist_df = ak.stock_info_bj_delist()
                if not bj_delist_df.empty and '证券代码' in bj_delist_df.columns:
                    bj_codes = bj_delist_df['证券代码'].astype(str).tolist()
                    delisted_codes.update(bj_codes)
                    self.logger.debug(f"获取北京退市股票: {len(bj_codes)} 只")
            except Exception as e:
                self.logger.debug(f"北京退市股票API不可用或失败: {e}")

            self.logger.info(f"获取退市股票总数: {len(delisted_codes)}")
            return delisted_codes

        except Exception as e:
            self.logger.error(f"获取退市股票列表失败: {e}")
            return set()
