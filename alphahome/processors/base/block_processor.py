#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
分块处理器支持

为数据处理器提供分块处理能力，支持大数据集的分块处理。
与新的处理器基类架构集成。
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, Optional, List
import pandas as pd
from datetime import datetime

from .processor import BaseProcessor
from ...common.logging_utils import get_logger


class BlockProcessorMixin(ABC):
    """
    为处理器提供分块处理能力的Mixin。

    一个分块处理器需要实现 `get_data_blocks` 和 `process_block` 两个核心方法。
    `run_all_blocks` 方法则驱动整个分块处理流程。

    与新的BaseProcessor架构集成，提供更强大的分块处理功能。
    """

    # 处理器子类可以通过设置此属性来表明自己是一个分块处理器
    is_block_processor: bool = False

    def __init__(self, *args, **kwargs):
        """初始化分块处理器"""
        # 提取config参数
        config = kwargs.pop('config', {})

        # 尝试调用父类的__init__，如果失败则忽略
        try:
            super().__init__(*args, **kwargs)
        except TypeError:
            # 如果父类是object，则不传递参数
            pass

        # 设置config属性
        if not hasattr(self, 'config'):
            self.config = config
        else:
            # 如果已有config，则合并
            self.config.update(config)

        # 从config中获取分块处理相关配置
        self.block_size = self.config.get('block_size', 10000)
        self.overlap_size = self.config.get('overlap_size', 0)
        self.parallel_blocks = self.config.get('parallel_blocks', False)
        self.max_workers = self.config.get('max_workers', 4)

        # 分块处理统计
        self._block_count = 0
        self._processed_blocks = 0
        self._failed_blocks = 0

        # 确保有logger属性
        if not hasattr(self, 'logger'):
            self.logger = get_logger(f"block_processor.{getattr(self, 'name', 'unknown')}")

    @abstractmethod
    def get_data_blocks(self, **kwargs) -> Iterator[Dict[str, Any]]:
        """
        将整个任务分解成可独立处理的数据块。

        这个方法应该被实现为一个生成器 (yield)，返回一个迭代器。
        每个迭代的元素都是一个字典，包含了处理单个数据块所需的参数。
        例如: `yield {'ts_code': '000001.SZ'}`

        Args:
            **kwargs: 从任务执行器传递过来的参数，例如 `start_date`, `end_date`。

        Returns:
            一个包含数据块参数字典的迭代器。
        """
        raise NotImplementedError("分块处理器必须实现 get_data_blocks 方法。")

    @abstractmethod
    def process_block(self, block_params: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """
        处理单个数据块的核心逻辑。

        Args:
            block_params (Dict[str, Any]): 从 `get_data_blocks` 生成的单个块的参数。

        Returns:
            一个可选的DataFrame，包含处理结果，用于后续可能的保存操作。
        """
        raise NotImplementedError("分块处理器必须实现 process_block 方法。")

    async def run_all_blocks(self, data: Optional[pd.DataFrame] = None, **kwargs) -> pd.DataFrame:
        """
        驱动所有数据块处理的顶层方法。

        Args:
            data: 要处理的DataFrame数据，如果提供则直接分块处理
            **kwargs: 传递给分块处理的参数

        Returns:
            处理后的DataFrame
        """
        if not hasattr(self, 'logger'):
            self.logger = get_logger(f"block_processor.{getattr(self, 'name', 'unknown')}")

        start_time = datetime.now()
        self.logger.info(f"处理器 '{getattr(self, 'name', 'unknown')}' 开始分块处理...")

        # 重置统计信息
        self._block_count = 0
        self._processed_blocks = 0
        self._failed_blocks = 0

        results = []
        errors = []

        try:
            if data is not None:
                # 直接处理传入的DataFrame
                blocks = self.get_data_blocks(data)
                for i, block in enumerate(blocks):
                    self._block_count += 1
                    self.logger.info(f"--> 正在处理块 #{self._block_count}, 行数: {len(block)}")

                    try:
                        block_result = await self.process_block(block, i, **kwargs)
                        if block_result is not None:
                            results.append(block_result)
                        self._processed_blocks += 1

                    except Exception as e:
                        self._failed_blocks += 1
                        error_info = {
                            "block_id": i,
                            "error": str(e),
                            "block_number": self._block_count
                        }
                        errors.append(error_info)
                        self.logger.error(f"处理块 #{i} 时发生错误: {e}", exc_info=True)

                        # 根据配置决定是否继续处理
                        if not getattr(self, 'continue_on_error', True):
                            break
            else:
                # 使用抽象方法获取数据块
                for block_params in self.get_data_blocks(**kwargs):
                    self._block_count += 1
                    self.logger.info(f"--> 正在处理块 #{self._block_count}: {block_params}")

                    try:
                        block_result = await self.process_block(block_params)
                        if block_result is not None:
                            results.append(block_result)
                        self._processed_blocks += 1

                    except Exception as e:
                        self._failed_blocks += 1
                        error_info = {
                            "block_params": block_params,
                            "error": str(e),
                            "block_number": self._block_count
                        }
                        errors.append(error_info)
                        self.logger.error(f"处理块 {block_params} 时发生错误: {e}", exc_info=True)

                        # 根据配置决定是否继续处理
                        if not getattr(self, 'continue_on_error', True):
                            break

            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()

            self.logger.info(
                f"分块处理完成，总块数: {self._block_count}，"
                f"成功: {self._processed_blocks}，"
                f"失败: {self._failed_blocks}，"
                f"处理时间: {processing_time:.2f}秒"
            )

            # 合并结果
            if results:
                return self._combine_block_results(results)
            else:
                return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"在分块处理时发生错误: {e}", exc_info=True)
            return pd.DataFrame()

    def _split_data_into_blocks(self, data: pd.DataFrame) -> List[pd.DataFrame]:
        """
        将DataFrame分割成多个块

        Args:
            data: 要分割的DataFrame

        Returns:
            分割后的DataFrame列表
        """
        if len(data) == 0:
            return [data]

        blocks = []
        start_idx = 0

        while start_idx < len(data):
            end_idx = min(start_idx + self.block_size, len(data))

            # 如果有重叠，调整起始位置
            if self.overlap_size > 0 and start_idx > 0:
                actual_start = max(0, start_idx - self.overlap_size)
            else:
                actual_start = start_idx

            block = data.iloc[actual_start:end_idx].copy()
            blocks.append(block)

            start_idx = end_idx

        return blocks

    def _combine_block_results(self, results: List[pd.DataFrame]) -> pd.DataFrame:
        """
        合并分块处理的结果

        默认实现是简单的垂直拼接，子类可以重写此方法实现自定义的合并逻辑。

        Args:
            results: 分块处理结果列表

        Returns:
            合并后的DataFrame
        """
        if not results:
            return pd.DataFrame()

        try:
            combined = pd.concat(results, ignore_index=True)
            self.logger.info(f"合并 {len(results)} 个块的结果，总行数: {len(combined)}")
            return combined
        except Exception as e:
            self.logger.error(f"合并分块结果时发生错误: {e}")
            raise


class BlockProcessor(BaseProcessor, BlockProcessorMixin):
    """
    分块数据处理器

    结合了BaseProcessor和BlockProcessorMixin的功能，
    提供了完整的分块数据处理能力。

    示例:
    ```python
    class MyBlockProcessor(BlockProcessor):
        def __init__(self, config=None):
            super().__init__(name="MyBlockProcessor", config=config)
            self.is_block_processor = True

        def get_data_blocks(self, **kwargs):
            # 定义如何分块
            for i in range(0, 1000, 100):
                yield {"start": i, "end": i + 100}

        def process_block(self, block_params):
            # 处理单个块
            start, end = block_params["start"], block_params["end"]
            # ... 处理逻辑
            return result_df

        def process(self, data, **kwargs):
            # 非分块处理逻辑（可选）
            return data
    ```
    """

    def __init__(
        self,
        name: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        logger: Optional[Any] = None
    ):
        """初始化分块处理器"""
        super().__init__(name=name, config=config, logger=logger)

        # 从配置中读取分块处理参数
        self.is_block_processor = self.config.get("is_block_processor", False)
        self.block_size = self.config.get("block_size", 1000)
        self.overlap_size = self.config.get("overlap_size", 0)
        self.continue_on_error = self.config.get("continue_on_error", True)
        self.parallel_processing = self.config.get("parallel_processing", False)
        self.max_workers = self.config.get("max_workers", 4)

    def execute(self, data: Optional[pd.DataFrame] = None, **kwargs) -> Dict[str, Any]:
        """
        执行处理器

        根据is_block_processor属性决定使用分块处理还是常规处理。

        Args:
            data: 输入数据（对于分块处理可能为None）
            **kwargs: 处理参数

        Returns:
            处理结果字典
        """
        if self.is_block_processor:
            # 使用分块处理
            return self.run_all_blocks(**kwargs)
        else:
            # 使用常规处理
            if data is None:
                raise ValueError("非分块处理器需要提供输入数据")
            return super().execute(data, **kwargs)

    def process(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        常规处理方法的默认实现

        对于纯分块处理器，此方法可能不会被调用。
        子类可以重写此方法提供非分块处理的逻辑。
        """
        # 默认返回原数据
        return data