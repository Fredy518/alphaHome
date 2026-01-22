#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
基于 AkShare API 的数据任务基类

核心设计：
1. 继承 FetcherTask，为所有 AkShare 任务提供通用功能
2. 实现 `prepare_params` 和 `fetch_batch` 方法，统一处理 AkShare API 调用
3. 处理 akshare 特有的数据特点（中文表头、宽表格式）
4. 将 `get_batch_list` 作为抽象方法，由具体任务实现
"""

import abc
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from alphahome.fetchers.base.fetcher_task import FetcherTask
from alphahome.common.constants import UpdateTypes
from .akshare_api import AkShareAPI
from .akshare_data_transformer import AkShareDataTransformer


class AkShareTask(FetcherTask, abc.ABC):
    """
    AkShare 数据任务基类

    此类继承自 FetcherTask，实现了 AkShare 特有的逻辑：
    - 在 `fetch_batch` 中统一调用 AkShare API 并进行数据转换
    - 处理中文表头、宽表格式等 akshare 特有数据特点
    - 提供通用的 `prepare_params` 实现

    子类必须定义：
    - api_name: AkShare 函数名称（如 "bond_zh_us_rate"）

    子类可选定义：
    - column_mapping: 中文到英文的列名映射
    - melt_config: 宽表转长表的配置
    - transformations: 数据类型转换规则
    """

    data_source = "akshare"

    # AkShare 特有配置
    default_request_interval = 1.5  # 默认请求间隔（秒）
    default_max_retries = 3  # 默认最大重试次数
    default_retry_delay = 5  # 默认重试等待时间（秒）

    # 必须由具体任务定义的属性
    api_name: Optional[str] = None  # AkShare 函数名称

    # 可选属性
    column_mapping: Optional[Dict[str, str]] = None  # 中文到英文的列名映射
    melt_config: Optional[Dict[str, Any]] = None  # 宽表转长表配置
    api_params: Optional[Dict[str, Any]] = None  # 固定的 API 参数

    def __init__(self, db_connection, api: Optional[AkShareAPI] = None, **kwargs):
        """
        初始化 AkShareTask

        Args:
            db_connection: 数据库连接
            api: 可选的 AkShareAPI 实例，如果不提供则自动创建
            **kwargs: 传递给 FetcherTask 的参数
        """
        # 从 kwargs 提取 task_config
        task_config = kwargs.get("task_config", {})

        # 将 AkShare 特有配置合并到 task_config
        if "request_interval" not in task_config:
            task_config["request_interval"] = self.default_request_interval
        if "max_retries" not in task_config:
            task_config["max_retries"] = self.default_max_retries
        if "retry_delay" not in task_config:
            task_config["retry_delay"] = self.default_retry_delay

        kwargs["task_config"] = task_config

        super().__init__(db_connection, **kwargs)

        if self.api_name is None:
            raise ValueError("AkShareTask 子类必须定义 api_name 属性")

        # 初始化 API 客户端
        self.api = api or AkShareAPI(
            logger=self.logger,
            request_interval=self.request_interval,
            max_retries=self.max_retries,
            retry_delay=self.retry_delay,
        )

        # 初始化数据转换器
        self.data_transformer = AkShareDataTransformer(self)

    def _apply_config(self, task_config: Dict):
        """合并代码默认值和配置文件设置"""
        super()._apply_config(task_config)

        cls = type(self)
        self.request_interval = float(
            task_config.get("request_interval", cls.default_request_interval)
        )
        self.max_retries = int(
            task_config.get("max_retries", cls.default_max_retries)
        )
        self.retry_delay = float(
            task_config.get("retry_delay", cls.default_retry_delay)
        )

    async def prepare_params(self, batch_params: Dict) -> Dict:
        """
        准备 AkShare API 请求的参数

        默认实现将批处理参数与固定的 api_params 合并。
        子类可按需重写以进行更复杂的参数处理。

        Args:
            batch_params: 批处理参数

        Returns:
            合并后的 API 参数
        """
        params = {}

        # 先添加固定参数
        if self.api_params:
            params.update(self.api_params)

        # 再添加批处理参数（覆盖固定参数）
        params.update(batch_params)

        return params

    async def fetch_batch(
        self,
        params: Dict[str, Any],
        stop_event: Optional[asyncio.Event] = None
    ) -> Optional[pd.DataFrame]:
        """
        使用 AkShare API 获取单个批次的数据

        Args:
            params: API 参数
            stop_event: 可选的停止事件

        Returns:
            处理后的 DataFrame 或 None
        """
        try:
            assert self.api_name is not None, "api_name 必须在任务子类中定义"

            self.logger.debug(f"调用 akshare.{self.api_name}，参数: {params}")

            # 调用 AkShare API
            data = await self.api.call(
                func_name=self.api_name,
                stop_event=stop_event,
                **params
            )

            if data is None or data.empty:
                self.logger.debug(f"akshare.{self.api_name} 未返回数据，参数: {params}")
                return None

            # 使用数据转换器处理数据
            processed_data = self.data_transformer.process_data(data)

            return processed_data

        except Exception as e:
            self.logger.error(
                f"获取批次数据失败，API: {self.api_name}，参数: {params}，错误: {e}",
                exc_info=True
            )
            raise

    @abc.abstractmethod
    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """
        获取批处理任务列表

        每个具体的 AkShare 任务必须实现此方法，根据自身特点定义如何生成批次。

        常见策略：
        1. 单批次（数据量小，一次性获取全部）
        2. 按日期分批
        3. 按代码分批

        Args:
            **kwargs: 包含 start_date, end_date, update_type 等参数

        Returns:
            批处理参数列表，每个元素是传递给 API 的参数字典
        """
        raise NotImplementedError("每个 AkShare 任务子类必须实现 get_batch_list 方法")

    def process_data(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        处理从 API 获取的原始数据

        在 AkShareTask 中，主要的数据转换已在 fetch_batch 阶段由
        AkShareDataTransformer 完成，此处直接返回数据。

        子类可以重写此方法添加额外的业务逻辑处理。

        Args:
            data: 数据
            **kwargs: 额外参数

        Returns:
            处理后的数据
        """
        # 转换已在 fetch_batch 中完成，直接返回
        return data

    def supports_incremental_update(self) -> bool:
        """
        是否支持增量更新

        默认支持，子类可根据数据特点重写。
        """
        return True

    def get_incremental_skip_reason(self) -> str:
        """
        获取不支持增量更新的原因

        Returns:
            跳过原因说明
        """
        return ""

    async def _should_skip_by_recent_update_time(
        self,
        update_type: str,
        *,
        max_age_days: int = 30,
    ) -> bool:
        """
        对于无法通过 API 参数限定时间范围的任务：
        - SMART 模式下，如果数据库表最近更新在 max_age_days 内，自动跳过
        - 否则允许执行“全量/覆盖式”获取
        """
        if update_type != UpdateTypes.SMART:
            return False

        try:
            table_exists = await self.db.table_exists(self)
            if not table_exists:
                return False

            latest_update_time = await self.db.get_latest_update_time(self)
            if not latest_update_time:
                return False

            age = datetime.now() - latest_update_time
            if age <= timedelta(days=max_age_days):
                self.logger.info(
                    f"{self.name}: SMART 模式检测到表最近更新时间 {latest_update_time}，"
                    f"{max_age_days} 天内无需重复拉取，自动跳过。"
                )
                return True

            return False
        except Exception as e:
            self.logger.warning(
                f"{self.name}: SMART 模式更新时间跳过判断失败，将继续执行: {e}"
            )
            return False


class AkShareSingleBatchTask(AkShareTask):
    """
    单批次 AkShare 任务基类

    适用于数据量较小、一次性获取全部数据的任务。
    例如：bond_zh_us_rate（中美国债收益率）一次返回所有历史数据。

    子类只需定义：
    - api_name: AkShare 函数名称
    - api_params: 可选的固定 API 参数
    """

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """
        生成单批次参数列表

        对于单批次任务，只返回一个包含必要参数的字典。

        Args:
            **kwargs: 包含 start_date 等参数

        Returns:
            只包含一个元素的参数列表
        """
        # 构建单个批次的参数
        batch_params = {}

        # 如果任务需要 start_date 参数
        if "start_date" in kwargs and kwargs["start_date"]:
            batch_params["start_date"] = kwargs["start_date"]

        # 如果任务需要 end_date 参数
        if "end_date" in kwargs and kwargs["end_date"]:
            batch_params["end_date"] = kwargs["end_date"]

        # 合并固定的 API 参数
        if self.api_params:
            batch_params.update(self.api_params)

        self.logger.info(
            f"任务 {self.name}: 生成单批次参数: {batch_params}"
        )

        return [batch_params]

    def supports_incremental_update(self) -> bool:
        """
        单批次任务通常不支持真正的增量更新

        因为每次都是获取全部数据，但可以通过数据库的 upsert 实现增量效果。
        """
        return True

