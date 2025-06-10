#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
PostgreSQL Data Feeds for Backtesting Module

PostgreSQL数据源，为backtrader提供数据接口
"""

import asyncio
import logging
from datetime import date, datetime
from typing import Dict, List, Optional

import backtrader as bt
import pandas as pd

from ..utils.exceptions import BacktestError, DataError

logger = logging.getLogger(__name__)


class PostgreSQLDataFeed(bt.feeds.DataBase):
    """
    PostgreSQL数据源，直接从数据库查询OHLCV数据
    支持多种时间周期和股票代码
    """

    # 数据源参数定义
    params = (
        ("ts_code", ""),  # 股票代码
        ("table_name", "tushare_stock_daily"),  # 数据表名
        ("start_date", None),  # 开始日期
        ("end_date", None),  # 结束日期
        ("db_manager", None),  # 数据库管理器
    )

    def __init__(self, **kwargs):
        """
        初始化PostgreSQL数据源
        """
        super().__init__()

        # 设置参数
        for key, value in kwargs.items():
            if hasattr(self.params, key):
                setattr(self.params, key, value)

        # 验证必需参数
        if not self.p.ts_code:
            raise BacktestError("ts_code参数是必需的")

        if not self.p.db_manager:
            raise BacktestError("db_manager参数是必需的")

        # 数据存储
        self._data_df = None
        self._current_index = 0
        self._total_rows = 0

        # 简单缓存 (类级别缓存)
        if not hasattr(PostgreSQLDataFeed, "_cache"):
            PostgreSQLDataFeed._cache = {}

        logger.info(
            f"初始化PostgreSQL数据源: {self.p.ts_code}, 表: {self.p.table_name}"
        )

    def start(self):
        """
        启动数据源，加载数据
        """
        logger.info(f"启动数据源: {self.p.ts_code}")

        try:
            # 使用DBManager的同步方法，避免异步冲突
            self._sync_load_data()
            logger.info(f"数据源启动成功: {self.p.ts_code}, 数据量: {self._total_rows}")
        except Exception as e:
            logger.error(f"数据加载失败: {e}")
            raise DataError(
                f"数据加载失败: {e}",
                table_name=self.p.table_name,
                ts_code=self.p.ts_code,
            )

    def stop(self):
        """
        停止数据源
        """
        logger.info(f"停止数据源: {self.p.ts_code}")
        self._data_df = None
        self._current_index = 0

    def _load(self):
        """
        加载下一个数据点 (backtrader要求的方法)
        """
        if self._data_df is None or self._current_index >= self._total_rows:
            return False

        try:
            # 获取当前行数据
            row = self._data_df.iloc[self._current_index]

            # 设置backtrader数据线
            self.lines.datetime[0] = bt.date2num(row["datetime"])
            self.lines.open[0] = float(row["open"])
            self.lines.high[0] = float(row["high"])
            self.lines.low[0] = float(row["low"])
            self.lines.close[0] = float(row["close"])
            self.lines.volume[0] = float(row["volume"])
            self.lines.openinterest[0] = float(row.get("amount", 0))

            self._current_index += 1
            return True

        except Exception as e:
            logger.error(f"数据加载错误，行{self._current_index}: {e}")
            return False

    async def _async_load_data(self):
        """
        异步从数据库加载数据
        """
        # 生成缓存键
        cache_key = self._generate_cache_key()

        # 尝试从缓存获取
        if cache_key in PostgreSQLDataFeed._cache:
            logger.info(f"从缓存加载数据: {cache_key}")
            self._data_df = PostgreSQLDataFeed._cache[cache_key]
            self._total_rows = len(self._data_df)
            return

        # 从数据库查询
        logger.info(f"从数据库查询数据: {self.p.ts_code}")

        try:
            # 确保数据库连接是活跃的
            if not self.p.db_manager.pool:
                await self.p.db_manager.connect()

            # 构建SQL查询
            sql = f"""
            SELECT trade_date, open, high, low, close, volume, amount
            FROM {self.p.table_name} 
            WHERE ts_code = $1
            """
            params = [self.p.ts_code]

            if self.p.start_date:
                sql += " AND trade_date >= $2"
                params.append(self.p.start_date)

            if self.p.end_date:
                if len(params) == 2:
                    sql += " AND trade_date <= $3"
                else:
                    sql += " AND trade_date <= $2"
                params.append(self.p.end_date)

            sql += " ORDER BY trade_date ASC"

            # 使用重试机制的数据库查询
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    records = await self.p.db_manager.fetch(sql, *params)
                    break
                except Exception as retry_e:
                    if attempt == max_retries - 1:
                        raise retry_e
                    logger.warning(
                        f"数据库查询重试 {attempt + 1}/{max_retries}: {retry_e}"
                    )
                    # 短暂等待后重试
                    await asyncio.sleep(0.5)

            if not records:
                raise DataError(
                    f"未找到数据: {self.p.ts_code}",
                    table_name=self.p.table_name,
                    ts_code=self.p.ts_code,
                )

            # 转换为DataFrame
            data_list = []
            for record in records:
                data_list.append(
                    {
                        "trade_date": record["trade_date"],
                        "open": record["open"],
                        "high": record["high"],
                        "low": record["low"],
                        "close": record["close"],
                        "volume": record["volume"],
                        "amount": record.get("amount", 0),
                    }
                )

            self._data_df = pd.DataFrame(data_list)

            # 数据预处理
            self._preprocess_data()

            # 缓存数据
            PostgreSQLDataFeed._cache[cache_key] = self._data_df

            self._total_rows = len(self._data_df)
            logger.info(f"数据查询完成，共{self._total_rows}条记录")

        except Exception as e:
            logger.error(f"数据库查询失败: {e}")
            raise DataError(
                f"数据库查询失败: {e}",
                table_name=self.p.table_name,
                ts_code=self.p.ts_code,
            )

    def _sync_load_data(self):
        """
        同步从数据库加载数据（用于backtrader）
        """
        # 生成缓存键
        cache_key = self._generate_cache_key()

        # 尝试从缓存获取
        if cache_key in PostgreSQLDataFeed._cache:
            logger.info(f"从缓存加载数据: {cache_key}")
            self._data_df = PostgreSQLDataFeed._cache[cache_key]
            self._total_rows = len(self._data_df)
            return

        # 从数据库查询
        logger.info(f"从数据库查询数据: {self.p.ts_code}")

        try:
            # 构建SQL查询
            sql = f"""
            SELECT trade_date, open, high, low, close, volume, amount
            FROM {self.p.table_name} 
            WHERE ts_code = %s
            """
            params = [self.p.ts_code]

            if self.p.start_date:
                sql += " AND trade_date >= %s"
                params.append(self.p.start_date)

            if self.p.end_date:
                sql += " AND trade_date <= %s"
                params.append(self.p.end_date)

            sql += " ORDER BY trade_date ASC"

            # 使用同步方法查询数据库
            records = self.p.db_manager.fetch(sql, tuple(params))

            if not records:
                raise DataError(
                    f"未找到数据: {self.p.ts_code}",
                    table_name=self.p.table_name,
                    ts_code=self.p.ts_code,
                )

            # 转换为DataFrame
            data_list = []
            for record in records:
                data_list.append(
                    {
                        "trade_date": record["trade_date"],
                        "open": record["open"],
                        "high": record["high"],
                        "low": record["low"],
                        "close": record["close"],
                        "volume": record["volume"],
                        "amount": record.get("amount", 0),
                    }
                )

            self._data_df = pd.DataFrame(data_list)

            # 数据预处理
            self._preprocess_data()

            # 缓存数据
            PostgreSQLDataFeed._cache[cache_key] = self._data_df

            self._total_rows = len(self._data_df)
            logger.info(f"数据查询完成: {self.p.ts_code}, 共 {self._total_rows} 条记录")

        except Exception as e:
            logger.error(f"数据库查询失败: {e}")
            raise DataError(
                f"数据库查询失败: {e}",
                table_name=self.p.table_name,
                ts_code=self.p.ts_code,
            )

    def _preprocess_data(self):
        """
        数据预处理
        """
        # 确保日期列为datetime类型
        self._data_df["datetime"] = pd.to_datetime(self._data_df["trade_date"])

        # 确保数值列为float类型
        numeric_columns = ["open", "high", "low", "close", "volume"]
        for col in numeric_columns:
            if col in self._data_df.columns:
                self._data_df[col] = pd.to_numeric(self._data_df[col], errors="coerce")

        # 处理缺失值
        self._data_df = self._data_df.dropna(subset=numeric_columns)

        # 按日期排序
        self._data_df = self._data_df.sort_values("datetime").reset_index(drop=True)

        # 数据验证
        self._validate_data()

    def _validate_data(self):
        """
        数据验证
        """
        if self._data_df.empty:
            raise DataError("预处理后数据为空")

        # 检查OHLCV数据的合理性
        for _, row in self._data_df.iterrows():
            if not (
                row["low"] <= row["open"] <= row["high"]
                and row["low"] <= row["close"] <= row["high"]
            ):
                logger.warning(f"发现异常OHLC数据: {row['datetime']}")

            if row["volume"] < 0:
                logger.warning(f"发现负成交量: {row['datetime']}")

    def _generate_cache_key(self):
        """
        生成缓存键
        """
        return f"{self.p.table_name}_{self.p.ts_code}_{self.p.start_date}_{self.p.end_date}"

    def islive(self):
        """
        返回False，表示这是历史数据，不是实时数据
        """
        return False


class PostgreSQLDataFeedFactory:
    """
    PostgreSQL数据源工厂类
    用于创建和管理多个数据源实例
    """

    def __init__(self, db_manager):
        """
        初始化工厂

        Args:
            db_manager: 数据库管理器实例
        """
        self.db_manager = db_manager
        self._feeds = {}

    def create_feed(
        self,
        ts_code: str,
        table_name: str = "tushare_stock_daily",
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        **kwargs,
    ) -> PostgreSQLDataFeed:
        """
        创建数据源实例

        Args:
            ts_code: 股票代码
            table_name: 数据表名
            start_date: 开始日期
            end_date: 结束日期
            **kwargs: 其他参数

        Returns:
            PostgreSQLDataFeed实例
        """
        feed_key = f"{table_name}_{ts_code}_{start_date}_{end_date}"

        if feed_key in self._feeds:
            return self._feeds[feed_key]

        feed = PostgreSQLDataFeed(
            ts_code=ts_code,
            table_name=table_name,
            start_date=start_date,
            end_date=end_date,
            db_manager=self.db_manager,
            **kwargs,
        )

        self._feeds[feed_key] = feed
        return feed

    async def get_available_codes(
        self, table_name: str = "tushare_stock_daily"
    ) -> List[str]:
        """
        异步获取可用的股票代码列表

        Args:
            table_name: 数据表名

        Returns:
            股票代码列表
        """
        try:
            sql = f"SELECT DISTINCT ts_code FROM {table_name} ORDER BY ts_code"
            records = await self.db_manager.fetch(sql)
            return [record["ts_code"] for record in records]

        except Exception as e:
            logger.error(f"获取股票代码列表失败: {e}")
            return []

    def clear_cache(self):
        """
        清理所有数据源的缓存
        """
        if hasattr(PostgreSQLDataFeed, "_cache"):
            PostgreSQLDataFeed._cache.clear()
        logger.info("已清理所有数据源缓存")
