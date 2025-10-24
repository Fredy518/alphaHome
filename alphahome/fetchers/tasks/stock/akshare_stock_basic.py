# -*- coding: utf-8 -*-

"""
akshare股票基本信息任务

借鉴hikyuu的实现，从akshare获取股票的基本信息，包括：
- 股票代码和名称
- 市场信息
- 行业分类
- 上市日期等

继承自FetcherTask，使用akshare数据源，对标hikyuu的做法。
"""

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd

from ...base.fetcher_task import FetcherTask
from ....common.task_system.task_decorator import task_register
from ...sources.akshare import StockTask


@task_register()
class AkshareStockBasicTask(StockTask):
    """akshare股票基本信息任务

    从akshare获取股票的基本信息，包括代码、名称、市场、行业等。
    对标hikyuu的做法，使用akshare作为数据源。
    """

    # 1.核心属性
    domain = "stock"  # 业务域标识
    name = "akshare_stock_basic"
    description = "获取股票基本信息（akshare数据源）"
    table_name = "stock_basic"
    primary_keys = ["stockid"]  # 对标hikyuu stockid主键
    date_column = None  # 全量更新任务
    default_start_date = "19901219"  # A股最早交易日

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 1  # akshare全量更新，设置为串行执行
    default_batch_size = 10000  # 全量数据，一次性处理

    # 2.市场映射
    MARKET_MAPPING = {
        'SH': '上海证券交易所',
        'SZ': '深圳证券交易所',
        'BJ': '北京证券交易所'
    }

    # 3.数据类型转换
    transformations = {
        "start_date": lambda x: int(x) if pd.notna(x) and str(x).isdigit() else 19901219,  # yyyymmdd格式
        "end_date": lambda x: 99999999,  # 统一记为99999999
        "update_time": lambda x: pd.to_datetime(x) if x else pd.Timestamp.now(),
    }

    # 4.表结构定义 (对标hikyuu stock表结构)
    schema_def = {
        "stockid": {"type": "SERIAL"},                                  # 股票ID (对标hikyuu stockid，自增主键)
        "marketid": {"type": "INTEGER"},                                # 市场ID (对标hikyuu marketid)
        "code": {"type": "VARCHAR(20)"},                                # 股票代码 (对标hikyuu code)
        "name": {"type": "VARCHAR(60)"},                                # 股票名称 (对标hikyuu name)
        "type": {"type": "INTEGER"},                                    # 股票类型 (对标hikyuu type)
        "valid": {"type": "INTEGER", "default": 1},                    # 是否有效 (对标hikyuu valid, 1=有效, 0=无效)
        "start_date": {"type": "INTEGER"},                             # 上市日期 (yyyymmdd格式，对标hikyuu startDate)
        "end_date": {"type": "INTEGER", "default": 99999999},          # 退市日期 (统一99999999，对标hikyuu endDate)
        "ts_code": {"type": "VARCHAR(15)"},                             # 兼容字段: 股票代码(带市场标识)
        "market": {"type": "VARCHAR(10)"},                              # 兼容字段: 市场简称
        "exchange": {"type": "VARCHAR(50)"},                            # 兼容字段: 交易所全称
        "update_time": {"type": "TIMESTAMP"},                           # 更新时间
    }

    # 5.验证模式配置
    validation_mode = "report"  # 报告验证结果但保留所有数据

    # 6.自定义索引 (对标hikyuu的索引需求)
    indexes = [
        {"name": "idx_akshare_stock_basic_code", "columns": "code"},           # 对标hikyuu stock表code字段索引
        {"name": "idx_akshare_stock_basic_marketid", "columns": "marketid"},   # 对标hikyuu marketid字段索引
        {"name": "idx_akshare_stock_basic_valid", "columns": "valid"},         # 对标hikyuu valid字段索引
        {"name": "idx_akshare_stock_basic_ts_code", "columns": "ts_code"},     # 兼容字段索引
        {"name": "idx_akshare_stock_basic_update_time", "columns": "update_time"},
    ]

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成全量更新的批处理参数列表。

        对标hikyuu的做法，使用akshare获取全量股票基本信息。
        由于是全量更新，只需要一个批次处理所有数据。
        """
        self.logger.info(f"任务 {self.name}: 生成全量更新批次")

        # 全量更新，只需要一个批次
        batch_params = {
            "market": kwargs.get("market"),  # 可选的市场过滤
            "update_type": "full"  # 标识为全量更新
        }

        return [batch_params]

    async def prepare_params(self, batch: Dict) -> Dict[str, Any]:
        """
        准备批次参数

        Args:
            batch: 批次参数字典

        Returns:
            处理后的参数字典
        """
        # 对于全量更新，直接返回批次参数
        return batch

    async def fetch_batch(self, batch_params: Dict, stop_event: Optional[asyncio.Event] = None) -> pd.DataFrame:
        """
        从akshare获取股票基本信息。

        对标hikyuu的做法，使用akshare获取全量股票基本信息。
        """
        try:
            # 检查是否被取消
            if stop_event and stop_event.is_set():
                self.logger.warning("任务被取消")
                raise asyncio.CancelledError("任务在fetch_batch中被取消")

            self.logger.info("开始获取股票基本信息")

            # 获取原始数据
            raw_data = await self._fetch_raw_data(batch_params)

            if not raw_data:
                return pd.DataFrame()

            # 转换数据格式
            df = self._transform_data(raw_data, batch_params)

            self.logger.info(f"成功获取 {len(df)} 条股票基本信息记录")
            return df

        except Exception as e:
            self.logger.error(f"获取股票基本信息时出错: {e}", exc_info=True)
            return pd.DataFrame()

    async def _fetch_raw_data(self, params: Dict) -> List[Dict]:
        """
        从akshare获取股票基本信息。

        对标 hikyuu 的 import_stock_name 函数逻辑：
        1. 获取退市股票列表
        2. 获取当前活跃股票列表
        3. 过滤退市股票
        4. 返回有效股票信息
        """
        try:
            market_filter = params.get("market")
            self.logger.info(f"从akshare获取股票基本信息，市场过滤: {market_filter or '全市场'}")

            # 对标 hikyuu 的逻辑：获取退市股票集合
            delisted_codes = await self._get_delisted_stock_codes()

            # 使用AkshareAPI获取数据
            stocks_info = await self.api.get_all_stocks_info(market_filter)

            if not stocks_info:
                return []

            # 对标 hikyuu 的逻辑：过滤退市股票，只保留有效股票
            valid_stocks = []
            for stock in stocks_info:
                stock_code = stock['code']
                if stock_code not in delisted_codes:
                    valid_stocks.append(stock)
                else:
                    self.logger.debug(f"过滤退市股票: {stock_code} - {stock['name']}")

            # 转换为标准格式 (对标hikyuu stock表结构)
            raw_data = []
            for stock in valid_stocks:
                # 获取市场ID和股票类型
                market = stock['market']
                code = str(stock['code'])
                marketid = self.MARKET_ID_MAPPING.get(market, 0)
                stock_type = self._get_stock_type(market, code)

                # 计算start_date (yyyymmdd格式上市日期，暂时使用默认值，实际应该从akshare获取)
                # TODO: 从akshare获取真实的上市日期，这里暂时使用默认值
                start_date_int = 19901219  # A股最早交易日作为默认值

                record = {
                    # hikyuu 对标字段 (stockid是自增主键，不在这里设置)
                    'marketid': marketid,
                    'code': code,
                    'name': str(stock['name']),
                    'type': stock_type,
                    'valid': 1,  # 有效股票
                    'start_date': start_date_int,  # 上市日期
                    'end_date': 99999999,  # 退市日期(默认长期有效)

                    # 兼容字段
                    'ts_code': f"{code}.{market}",
                    'market': market,
                    'exchange': stock['exchange'],
                    'update_time': datetime.now().isoformat()
                }
                raw_data.append(record)

            self.logger.info(f"成功从akshare获取 {len(raw_data)} 条有效股票基本信息 (过滤退市股票: {len(stocks_info) - len(raw_data)})")
            return raw_data

        except Exception as e:
            self.logger.error(f"从akshare获取股票基本信息失败: {e}")
            import traceback
            self.logger.error(f"错误详情: {traceback.format_exc()}")
            return []

    def supports_incremental_update(self) -> bool:
        """
        akshare股票基本信息不支持智能增量更新。

        由于akshare的数据是实时获取的全量数据，
        智能增量模式下会自动退回到全量更新。
        """
        return False

    def get_incremental_skip_reason(self) -> str:
        """
        返回不支持智能增量更新的原因说明。

        Returns:
            str: 跳过原因说明
        """
        return "akshare股票基本信息为全量更新任务，不支持智能增量模式"

    def _transform_data(self, raw_data: List[Dict], params: Dict) -> pd.DataFrame:
        """
        转换akshare原始数据为标准格式

        Args:
            raw_data: akshare返回的原始股票基本信息数据
            params: 参数信息

        Returns:
            标准格式的DataFrame
        """
        if not raw_data:
            return pd.DataFrame()

        try:
            # 转换为DataFrame
            df = pd.DataFrame(raw_data)

            # 数据类型转换
            for col, transform_func in self.transformations.items():
                if col in df.columns:
                    try:
                        df[col] = df[col].apply(transform_func)
                    except Exception as e:
                        self.logger.warning(f"转换列 {col} 时出错: {e}")

            # 清理和验证数据 (对标hikyuu的必填字段检查)
            df = df.dropna(subset=['code', 'name'])  # hikyuu必填字段

            # 确保valid字段有默认值
            if 'valid' not in df.columns:
                df['valid'] = 1

            self.logger.debug(f"转换得到 {len(df)} 条股票基本信息记录")
            return df

        except Exception as e:
            self.logger.error(f"转换股票基本信息数据时出错: {e}")
            return pd.DataFrame()
