#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
基于 Tushare API 的数据任务基类

核心设计：
1. 本身是一个抽象类，为所有 Tushare 任务提供通用功能。
2. 实现 FetcherTask 的 `prepare_params` 和 `fetch_batch` 方法，统一处理 Tushare API 的调用。
3. 将 `get_batch_list` 继续作为抽象方法，强制具体的 API 任务（如 TushareStockDailyTask）实现自己的批处理逻辑。
"""

import abc
import asyncio
import logging
import os
from typing import Any, Dict, List, Optional

import pandas as pd

from alphahome.fetchers.base.fetcher_task import FetcherTask
from .tushare_api import TushareAPI
from .tushare_data_transformer import TushareDataTransformer


class TushareTask(FetcherTask, abc.ABC):
    """
    一个针对 Tushare API 的抽象任务基类。

    此类继承自 FetcherTask，并实现了 Tushare 特有的技术逻辑：
    - 在 `fetch_batch` 中统一调用 Tushare API 并进行标准数据转换。
    - 提供一个通用的 `prepare_params` 实现。
    
    它将 `get_batch_list` 继续声明为抽象方法，因为每个具体的 Tushare 接口
    （如 'daily', 'stock_basic'）都有其独特的批处理要求。
    """

    data_source = "tushare"

    # Tushare 特有配置
    default_page_size = 5000
    default_rate_limit_delay = 65

    # 必须由具体任务定义的属性
    api_name: Optional[str] = None
    fields: Optional[List[str]] = None
    
    # 可选属性
    column_mapping: Optional[Dict[str, str]] = None
    single_batch = False # 这个属性可能需要重新审视或在其子类中处理

    def __init__(self, db_connection, api_token=None, api=None, **kwargs):
        """
        初始化 TushareTask。

        Args:
            db_connection: 数据库连接。
            api_token (str, optional): Tushare API 令牌。
            api (TushareAPI, optional): 已初始化的 TushareAPI 实例。
            **kwargs: 传递给 FetcherTask 的参数 (例如 start_date, end_date, update_type)。
        """
        # 从 kwargs 中提取 task_config，以便 _apply_config 能正确工作
        task_config = kwargs.get("task_config", {})
        
        # 将 Tushare 特有的配置添加到 task_config 中
        if "page_size" not in task_config:
            task_config["page_size"] = self.default_page_size
        
        kwargs["task_config"] = task_config
        
        super().__init__(db_connection, **kwargs)

        if self.api_name is None or self.fields is None:
            raise ValueError("TushareTask 子类必须定义 api_name 和 fields 属性")

        if api_token is None:
            api_token = os.environ.get("TUSHARE_TOKEN")
            if not api_token:
                raise ValueError("未提供 Tushare API 令牌，请通过参数或环境变量 TUSHARE_TOKEN 提供")

        self.api = api or TushareAPI(
            api_token, rate_limit_delay=self.rate_limit_delay
        )
        self.data_transformer = TushareDataTransformer(self)

    def _apply_config(self, task_config: Dict):
        """合并代码默认值和配置文件设置。"""
        super()._apply_config(task_config) # 调用父类的配置应用
        
        cls = type(self)
        self.rate_limit_delay = int(
            task_config.get("rate_limit_delay", cls.default_rate_limit_delay)
        )

    async def prepare_params(self, batch_params: Dict) -> Dict:
        """
        准备 Tushare API 请求的参数。
        默认实现直接返回批处理参数，因为批次生成时已包含所需参数。
        子类可按需重写以进行更复杂的转换。
        """
        return batch_params.copy()

    async def fetch_batch(self, params: Dict[str, Any], stop_event: Optional[asyncio.Event] = None) -> Optional[pd.DataFrame]:
        """
        使用 Tushare API 获取单个批次的数据。
        这是 FetcherTask 的一个实现。
        """
        try:
            # 类型检查器无法推断出 __init__ 中的检查，因此在此处添加断言以确保类型安全
            assert self.api_name is not None, "api_name must be defined in the task subclass"
            assert self.fields is not None, "fields must be defined in the task subclass"
            
            # `params` 是应该传递给 Tushare API 的具体参数
            # 避免fields参数冲突：如果params中包含fields，移除它以防重复
            clean_params = params.copy()
            if 'fields' in clean_params:
                self.logger.debug(f"批处理参数中包含fields，将使用任务定义的fields: {self.fields}")
                clean_params.pop('fields')
            
            data = await self.api.query(
                api_name=self.api_name,
                fields=self.fields,
                stop_event=stop_event,
                **clean_params  # 将清理后的批处理参数解包传递
            )

            if data is None or data.empty:
                self.logger.debug(f"API未返回任何数据，参数: {params}")
                return None

            # 数据转换
            processed_data = await self.data_transformer.process_data(data)
            return processed_data

        except Exception as e:
            self.logger.error(
                f"获取批次数据失败，参数: {params}。错误: {e}", exc_info=True
            )
            # 向上抛出异常，由 FetcherTask 的重试逻辑处理
            raise

    @abc.abstractmethod
    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """
        获取批处理任务列表。
        
        这是一个抽象方法，强制每个具体的 Tushare 任务（如 tushare_stock_daily）
        必须根据其自身特点定义如何生成批次。
        这是对 FetcherTask.get_batch_list 的重写，以强制子类实现。
        """
        raise NotImplementedError("每个 Tushare 任务子类必须实现 get_batch_list 方法")

    def _process_data(self, data, **kwargs):
        """
        Tushare 特有的数据处理。
        
        注意：在新架构下，大部分通用处理由 BaseTask._process_data 和 
        TushareDataTransformer 完成。如果具体任务（如TushareStockDailyTask）
        需要非常特殊的处理，可以重写此方法。
        
        默认实现：
        1. 调用基类（BaseTask）的通用处理逻辑。
        2. （可选）执行额外的 Tushare 特定处理。
        """
        # 首先调用基类（BaseTask）的通用处理
        data = super()._process_data(data, **kwargs)
        
        # 此处可以添加所有 TushareTask 子类都需要的通用处理逻辑
        # 例如，基于 Tushare API 返回的通用字段进行的处理
            
        return data
