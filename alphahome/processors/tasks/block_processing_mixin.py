#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
分块处理能力混入

为ProcessorTaskBase提供分块处理大数据集的能力。
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, Optional, List
import pandas as pd
from datetime import datetime
import asyncio


class BlockProcessingTaskMixin(ABC):
    """
    为任务提供分块处理能力的Mixin。

    一个分块处理任务需要实现 `get_data_blocks` 和 `process_block` 两个核心方法。
    这个Mixin会重写 `process_data` 方法来驱动整个分块处理流程。
    """
    def __init__(self, *args, **kwargs):
        """初始化分块处理器"""
        super().__init__(*args, **kwargs)

        # 从task_config中获取分块处理相关配置
        # self.task_config 来自于 BaseTask
        self.block_size = self.task_config.get('block_size', 10000)
        self.overlap_size = self.task_config.get('overlap_size', 0)
        self.continue_on_error = self.task_config.get('continue_on_error', True)
        
        # 分块处理统计
        self._block_count = 0
        self._processed_blocks = 0
        self._failed_blocks = 0

    @abstractmethod
    def get_data_blocks(self, data: Optional[pd.DataFrame] = None, **kwargs) -> Iterator[Any]:
        """
        将整个任务分解成可独立处理的数据块。

        这个方法应该被实现为一个生成器 (yield)，返回一个迭代器。
        每个迭代的元素都是一个数据块，其类型取决于任务的实现。
        它可以是一个DataFrame块，或者是一个包含处理参数的字典。

        Args:
            data: 从 `fetch_data` 传递过来的数据，可能为None。
            **kwargs: 从任务执行器传递过来的参数。

        Returns:
            一个包含数据块的迭代器。
        """
        raise NotImplementedError("分块处理任务必须实现 get_data_blocks 方法。")

    @abstractmethod
    async def process_block(self, block: Any, **kwargs) -> Optional[pd.DataFrame]:
        """
        处理单个数据块的核心逻辑。

        Args:
            block: 从 `get_data_blocks` 生成的单个块。

        Returns:
            一个可选的DataFrame，包含处理结果，用于后续可能的合并操作。
        """
        raise NotImplementedError("分块处理任务必须实现 process_block 方法。")

    async def process_data(self, data: pd.DataFrame, stop_event: Optional[asyncio.Event] = None, **kwargs) -> Optional[pd.DataFrame]:
        """
        驱动所有数据块处理的顶层方法。
        这会重写 ProcessorTaskBase 的 process_data。
        """
        start_time = datetime.now()
        self.logger.info(f"任务 '{self.name}' 开始分块处理...")

        self._block_count = 0
        self._processed_blocks = 0
        self._failed_blocks = 0

        results = []
        errors = []

        try:
            for block in self.get_data_blocks(data=data, **kwargs):
                if stop_event and stop_event.is_set():
                    self.logger.info("分块处理被中断。")
                    break

                self._block_count += 1
                self.logger.info(f"--> 正在处理块 #{self._block_count}")

                try:
                    block_result = await self.process_block(block, **kwargs)
                    if block_result is not None and not block_result.empty:
                        results.append(block_result)
                    self._processed_blocks += 1

                except Exception as e:
                    self._failed_blocks += 1
                    errors.append({ "block_number": self._block_count, "error": str(e) })
                    self.logger.error(f"处理块 #{self._block_count} 时发生错误: {e}", exc_info=True)

                    if not self.continue_on_error:
                        break
            
            processing_time = (datetime.now() - start_time).total_seconds()
            self.logger.info(
                f"分块处理完成: 总块数={self._block_count}, "
                f"成功={self._processed_blocks}, 失败={self._failed_blocks}. "
                f"耗时: {processing_time:.2f}秒"
            )

            if errors and not self.continue_on_error:
                raise Exception("分块处理因错误提前终止。查看日志了解详情。")

            return self._combine_block_results(results)

        except Exception as e:
            self.logger.error(f"分块处理流程发生严重错误: {e}", exc_info=True)
            raise

    def _split_data_into_blocks(self, data: pd.DataFrame) -> Iterator[pd.DataFrame]:
        """
        一个将DataFrame分割成多个块的辅助生成器。
        可以直接在 get_data_blocks 中使用，当任务是从一个大的DataFrame分块时。
        
        示例:
        ```python
        def get_data_blocks(self, data, **kwargs):
            yield from self._split_data_into_blocks(data)
        ```
        """
        if data is None or data.empty:
            return iter([])

        start_idx = 0
        while start_idx < len(data):
            end_idx = min(start_idx + self.block_size, len(data))

            if self.overlap_size > 0 and start_idx > 0:
                actual_start = max(0, start_idx - self.overlap_size)
            else:
                actual_start = start_idx

            block = data.iloc[actual_start:end_idx]
            yield block
            start_idx = end_idx

    def _combine_block_results(self, results: List[pd.DataFrame]) -> Optional[pd.DataFrame]:
        """
        合并分块处理的结果。
        子类可以重写此方法以实现自定义的合并逻辑。
        """
        if not results:
            return None

        try:
            combined = pd.concat(results, ignore_index=True)
            self.logger.info(f"成功合并 {len(results)} 个块的结果，总行数: {len(combined)}")
            return combined
        except Exception as e:
            self.logger.error(f"合并分块结果时发生错误: {e}")
            raise 