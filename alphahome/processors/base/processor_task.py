#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据处理任务基类

为所有数据处理任务提供基础功能和接口。
处理任务是数据处理模块的核心组件，负责执行完整的数据处理生命周期。
"""

import abc
import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from ...common.logging_utils import get_logger


class ProcessorTask(abc.ABC):
    """数据处理任务基类

    为所有数据处理任务提供公共功能和接口。
    每个处理任务负责从数据库获取数据、处理数据并保存结果。

    子类应重写:
    - fetch_data: 从数据库获取数据
    - process: 处理数据的主逻辑

    子类可选择性重写:
    - pre_process: 预处理钩子
    - post_process: 后处理钩子
    - validate_data: 数据验证逻辑
    """

    def __init__(self, db_connection, config: Optional[Dict[str, Any]] = None):
        """初始化处理任务

        Args:
            db_connection: 数据库连接
            config: 配置参数
        """
        self.db = db_connection
        self.config = config or {}
        self.name = self.__class__.__name__
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """设置日志记录器"""
        logger = get_logger(f"processor.{self.name}")
        return logger

    async def execute(self, data=None, **kwargs):
        """执行处理任务的完整生命周期

        Args:
            data: 可选的输入数据，如果为None则从数据库获取
            **kwargs: 额外参数

        Returns:
            Dict: 执行结果
        """
        try:
            # 1. 预处理
            await self.pre_process(**kwargs)

            # 2. 如果没有传入数据，则从数据源获取
            if data is None:
                self.logger.info("从数据库获取数据...")
                data = await self.fetch_data(**kwargs)

            # 空数据检查
            if data is None or (isinstance(data, pd.DataFrame) and data.empty):
                self.logger.warning("没有数据可处理")
                return {"status": "no_data", "message": "没有数据可处理"}

            # 3. 数据验证
            self.logger.info("验证数据...")
            data = await self.validate_data(data)

            # 4. 主处理逻辑
            self.logger.info(f"处理数据 ({len(data)} 行)...")
            processed_data = await self.process(data, **kwargs)

            # 5. 处理结果保存和后处理
            self.logger.info("处理后处理...")
            result = await self.post_process(processed_data, **kwargs)

            return {
                "status": "success",
                "rows_processed": len(processed_data),
                "result": result,
            }

        except Exception as e:
            self.logger.error(f"处理任务执行失败: {str(e)}", exc_info=True)
            return {"status": "failed", "error": str(e)}

    @abc.abstractmethod
    async def fetch_data(self, **kwargs) -> pd.DataFrame:
        """从数据源获取数据

        子类必须实现此方法，通常是从数据库获取数据。

        Args:
            **kwargs: 获取数据的参数

        Returns:
            pd.DataFrame: 获取的数据框
        """
        raise NotImplementedError("子类必须实现fetch_data方法")

    @abc.abstractmethod
    async def process(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """处理数据的主逻辑

        子类必须实现此方法，定义具体的数据处理逻辑。

        Args:
            data: 要处理的数据框
            **kwargs: 额外参数

        Returns:
            pd.DataFrame: 处理后的数据框
        """
        raise NotImplementedError("子类必须实现process方法")

    async def pre_process(self, **kwargs):
        """预处理钩子

        在处理开始前执行，可用于设置环境、验证参数等。
        子类可选择性重写此方法。

        Args:
            **kwargs: 额外参数
        """
        pass

    async def post_process(self, data: pd.DataFrame, **kwargs) -> Dict[str, Any]:
        """后处理钩子

        在处理完成后执行，可用于保存结果、清理资源等。
        子类可选择性重写此方法。

        Args:
            data: 处理后的数据框
            **kwargs: 额外参数

        Returns:
            Dict[str, Any]: 后处理结果
        """
        if (
            kwargs.get("save_result", True)
            and hasattr(self, "result_table")
            and self.result_table
        ):
            return await self._save_result(data, **kwargs)
        return {"message": "数据处理完成，未保存结果"}

    async def validate_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """验证数据

        验证输入数据的有效性，子类可重写以实现特定验证逻辑。

        Args:
            data: 要验证的数据框

        Returns:
            pd.DataFrame: 验证后的数据框
        """
        return data

    async def _save_result(self, data: pd.DataFrame, **kwargs) -> Dict[str, Any]:
        """保存处理结果到数据库

        默认实现，保存处理结果到结果表。

        Args:
            data: 要保存的数据框
            **kwargs: 额外参数

        Returns:
            Dict[str, Any]: 保存结果
        """
        if not hasattr(self, "db") or not self.db:
            raise ValueError("数据库连接未初始化")

        if not hasattr(self, "result_table") or not self.result_table:
            raise ValueError("未定义结果表名")

        # 转换为记录列表
        records = data.to_dict("records")

        if not records:
            self.logger.warning("没有数据需要保存")
            return {"rows_saved": 0}

        # 清除可能存在的旧结果
        if kwargs.get("replace_existing", True):
            await self._clear_existing_results(data, **kwargs)

        try:
            # 保存数据
            if hasattr(self.db, "copy_records_to_table"):
                # 使用批量插入
                rows_saved = await self.db.copy_records_to_table(
                    self.result_table, records
                )
            else:
                # 使用逐条插入
                for record in records:
                    await self.db.insert(self.result_table, record)
                rows_saved = len(records)

            self.logger.info(f"保存了 {rows_saved} 行数据到表 {self.result_table}")
            return {"rows_saved": rows_saved}

        except Exception as e:
            self.logger.error(
                f"保存数据到表 {self.result_table} 时出错: {str(e)}", exc_info=True
            )
            raise

    async def _clear_existing_results(self, data: pd.DataFrame, **kwargs):
        """清除可能存在的旧结果

        根据数据范围清除结果表中的旧数据。

        Args:
            data: 新数据
            **kwargs: 额外参数
        """
        if not hasattr(self, "result_table") or not self.result_table:
            return

        conditions = []
        params = {}

        # 日期条件
        date_column = kwargs.get("date_column") or getattr(self, "date_column", None)
        if date_column and date_column in data.columns:
            min_date = data[date_column].min()
            max_date = data[date_column].max()
            conditions.append(f"{date_column} BETWEEN $min_date AND $max_date")
            params["min_date"] = min_date
            params["max_date"] = max_date

        # 代码条件
        code_column = kwargs.get("code_column") or getattr(self, "code_column", None)
        if code_column and code_column in data.columns:
            unique_codes = data[code_column].unique().tolist()
            conditions.append(f"{code_column} = ANY($codes)")
            params["codes"] = unique_codes

        if conditions:
            query = f"DELETE FROM {self.result_table} WHERE " + " AND ".join(conditions)
            self.logger.info(f"清除旧结果: {query}")
            await self.db.execute(query, params)

    async def get_last_processed_date(self) -> Optional[datetime.date]:
        """获取最后处理日期

        从结果表查询最大日期，用于增量处理。

        Returns:
            Optional[datetime.date]: 最后处理日期，如果没有则返回None
        """
        if not hasattr(self, "db") or not self.db:
            self.logger.warning("数据库连接未初始化，无法获取最后处理日期")
            return None

        if not hasattr(self, "result_table") or not self.result_table:
            self.logger.warning("未定义结果表，无法获取最后处理日期")
            return None

        date_column = getattr(self, "date_column", None)
        if not date_column:
            self.logger.warning("未定义日期列，无法获取最后处理日期")
            return None

        try:
            query = f"SELECT MAX({date_column}) as last_date FROM {self.result_table}"
            result = await self.db.fetch_one(query)

            if result and result["last_date"]:
                # 确保返回日期对象
                if isinstance(result["last_date"], str):
                    return datetime.strptime(result["last_date"], "%Y%m%d").date()
                return (
                    result["last_date"].date()
                    if hasattr(result["last_date"], "date")
                    else result["last_date"]
                )

            return None

        except Exception as e:
            self.logger.error(f"获取最后处理日期失败: {str(e)}")
            return None
