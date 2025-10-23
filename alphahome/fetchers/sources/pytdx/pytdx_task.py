# -*- coding: utf-8 -*-

"""
基于 Pytdx API 的数据任务基类

借鉴alphahome的TushareTask实现，为所有Pytdx任务提供通用功能。
"""

import abc
import asyncio
import logging
import os
from typing import Any, Dict, List, Optional

import pandas as pd

from alphahome.fetchers.base.fetcher_task import FetcherTask
from .pytdx_api import PytdxAPI
from .pytdx_data_transformer import PytdxDataTransformer


class PytdxTask(FetcherTask, abc.ABC):
    """
    一个针对 Pytdx API 的抽象任务基类。

    此类继承自 FetcherTask，并实现了 Pytdx 特有的技术逻辑：
    - 在 `fetch_batch` 中统一调用 Pytdx API 并进行标准数据转换。
    - 提供一个通用的 `prepare_params` 实现。

    它将 `get_batch_list` 继续声明为抽象方法，因为每个具体的 Pytdx 接口
    都有其独特的批处理要求。
    """

    data_source = "pytdx"

    # Pytdx 特有配置
    default_host: Optional[str] = None
    default_port: int = 7709
    default_timeout: int = 10

    def __init__(self, db_connection, host=None, port=None, api=None, **kwargs):
        """
        初始化 PytdxTask。

        Args:
            db_connection: 数据库连接。
            host (str, optional): Pytdx服务器地址。
            port (int, optional): Pytdx服务器端口。
            api (PytdxAPI, optional): 已初始化的 PytdxAPI 实例。
            **kwargs: 传递给 FetcherTask 的参数。
        """
        # 从 kwargs 中提取 task_config，以便 _apply_config 能正确工作
        task_config = kwargs.get("task_config", {})

        # 将 Pytdx 特有的配置添加到 task_config 中
        if "host" not in task_config and host is None:
            task_config["host"] = self.default_host
        if "port" not in task_config and port is None:
            task_config["port"] = self.default_port
        if "timeout" not in task_config:
            task_config["timeout"] = self.default_timeout

        kwargs["task_config"] = task_config

        super().__init__(db_connection, **kwargs)

        # 初始化Pytdx API
        self.host = host or task_config.get("host")
        self.port = port or task_config.get("port", self.default_port)
        self.timeout = task_config.get("timeout", self.default_timeout)

        self.api = api or PytdxAPI(
            host=self.host,
            port=self.port,
            timeout=self.timeout
        )
        self.data_transformer = PytdxDataTransformer(self)

        # 连接状态管理
        self._connected = False

    def _apply_config(self, task_config: Dict):
        """合并代码默认值和配置文件设置。"""
        super()._apply_config(task_config)  # 调用父类的配置应用

        cls = type(self)
        self.host = task_config.get("host", cls.default_host)
        self.port = int(task_config.get("port", cls.default_port))
        self.timeout = int(task_config.get("timeout", cls.default_timeout))

    async def ensure_connection(self) -> bool:
        """确保与Pytdx服务器的连接"""
        if not self._connected:
            self._connected = await self.api.connect()
        return self._connected

    async def prepare_params(self, batch_params: Dict) -> Dict:
        """
        准备 Pytdx API 请求的参数。

        Args:
            batch_params: 批次参数字典。

        Returns:
            准备好的参数字典。
        """
        # Pytdx的参数相对简单，主要处理市场和代码信息
        prepared_params = {}

        # 复制所有批次参数
        prepared_params.update(batch_params)

        # 确保必要的参数存在
        if 'market' not in prepared_params:
            prepared_params['market'] = 'SH'  # 默认上海市场

        if 'code' not in prepared_params:
            raise ValueError("批次参数中必须包含 'code'")

        return prepared_params

    async def fetch_batch(self, batch_params: Dict, stop_event: Optional[asyncio.Event] = None) -> pd.DataFrame:
        """
        从 Pytdx API 获取一批数据。

        Args:
            batch_params: 批次参数。

        Returns:
            获取到的数据 DataFrame。
        """
        try:
            # 检查是否被取消
            if stop_event and stop_event.is_set():
                self.logger.warning("任务被取消")
                raise asyncio.CancelledError("任务在fetch_batch中被取消")

            # 确保连接
            if not await self.ensure_connection():
                self.logger.error("无法连接到Pytdx服务器")
                return pd.DataFrame()

            # 检查是否被取消
            if stop_event and stop_event.is_set():
                self.logger.warning("任务被取消")
                raise asyncio.CancelledError("任务在连接后被取消")

            # 准备参数
            params = await self.prepare_params(batch_params)

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
        从Pytdx API获取原始数据（由子类实现）

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
            self.api.disconnect()
        self._connected = False
