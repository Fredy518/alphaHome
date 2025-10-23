# -*- coding: utf-8 -*-

"""
Pytdx 数据转换器

负责将pytdx返回的原始数据转换为标准格式。
"""

import logging
from typing import Any, Dict, List, Optional
import pandas as pd
from datetime import datetime

from ....common.logging_utils import get_logger

logger = get_logger(__name__)


class PytdxDataTransformer:
    """Pytdx数据转换器"""

    def __init__(self, task):
        """
        初始化转换器

        Args:
            task: 关联的任务实例
        """
        self.task = task

    def transform_daily_bars(self, raw_data: List[Dict], market: str, code: str) -> pd.DataFrame:
        """
        转换日线数据

        Args:
            raw_data: pytdx返回的原始日线数据
            market: 市场代码
            code: 股票代码

        Returns:
            标准格式的DataFrame
        """
        if not raw_data:
            return pd.DataFrame()

        try:
            # 转换数据
            transformed_data = []
            for bar in raw_data:
                # 转换日期时间格式
                datetime_str = str(bar['datetime'])
                if len(datetime_str) == 8:  # 格式: 20231201
                    trade_date = datetime_str
                elif len(datetime_str) == 12:  # 格式: 202312011500
                    trade_date = datetime_str[:8]
                else:
                    # 尝试其他格式转换
                    try:
                        dt = pd.to_datetime(datetime_str)
                        trade_date = dt.strftime('%Y%m%d')
                    except:
                        logger.warning(f"无法解析日期时间: {datetime_str}")
                        continue

                transformed_data.append({
                    'ts_code': f"{code}.{market.upper()}",
                    'trade_date': trade_date,
                    'open': float(bar.get('open', 0)),
                    'high': float(bar.get('high', 0)),
                    'low': float(bar.get('low', 0)),
                    'close': float(bar.get('close', 0)),
                    'volume': float(bar.get('volume', bar.get('vol', 0))),  # pytdx使用'vol'
                    'amount': float(bar.get('amount', 0)),
                })

            df = pd.DataFrame(transformed_data)

            # 数据验证
            if not df.empty:
                self._validate_daily_data(df)

            return df

        except Exception as e:
            logger.error(f"转换日线数据时出错: {e}")
            return pd.DataFrame()

    def _validate_daily_data(self, df: pd.DataFrame) -> None:
        """
        验证日线数据的有效性

        Args:
            df: 待验证的DataFrame
        """
        try:
            # 基本验证规则
            validations = [
                (df["close"] > 0, "收盘价必须为正数"),
                (df["open"] > 0, "开盘价必须为正数"),
                (df["high"] > 0, "最高价必须为正数"),
                (df["low"] > 0, "最低价必须为正数"),
                (df["volume"] >= 0, "成交量不能为负数"),
                (df["amount"] >= 0, "成交额不能为负数"),
                (df["high"] >= df["low"], "最高价不能低于最低价"),
                (df["high"] >= df["open"], "最高价不能低于开盘价"),
                (df["high"] >= df["close"], "最高价不能低于收盘价"),
                (df["low"] <= df["open"], "最低价不能高于开盘价"),
                (df["low"] <= df["close"], "最低价不能高于收盘价"),
            ]

            invalid_count = 0
            for condition, message in validations:
                invalid_mask = ~condition
                if invalid_mask.any():
                    count = invalid_mask.sum()
                    invalid_count += count
                    logger.warning(f"{message}: 发现 {count} 条无效记录")

            if invalid_count > 0:
                logger.warning(f"数据验证完成，发现 {invalid_count} 条数据质量问题")

        except Exception as e:
            logger.error(f"数据验证过程中出错: {e}")

    def merge_data(self, existing_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
        """
        合并现有数据和新数据

        Args:
            existing_df: 现有数据
            new_df: 新数据

        Returns:
            合并后的数据
        """
        if existing_df.empty:
            return new_df
        if new_df.empty:
            return existing_df

        try:
            # 合并数据并去重
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)

            # 按照ts_code和trade_date去重，保留最新的数据
            combined_df = combined_df.drop_duplicates(
                subset=['ts_code', 'trade_date'],
                keep='last'
            )

            # 排序
            combined_df = combined_df.sort_values(['ts_code', 'trade_date'])

            return combined_df

        except Exception as e:
            logger.error(f"合并数据时出错: {e}")
            return existing_df
