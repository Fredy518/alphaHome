from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, Optional
import pandas as pd


class BlockProcessorMixin(ABC):
    """
    为处理器任务提供分块处理能力的Mixin。

    一个分块处理器需要实现 `get_data_blocks` 和 `process_block` 两个核心方法。
    `run_all_blocks` 方法则驱动整个分块处理流程。
    """
    
    # 任务子类可以通过设置此属性来表明自己是一个分块处理器
    is_block_processor: bool = False

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

    def run_all_blocks(self, **kwargs) -> None:
        """
        驱动所有数据块处理的顶层方法。

        它会迭代 `get_data_blocks` 生成的所有块，并依次调用 `process_block`。
        这个方法提供了任务执行的整体框架，例如日志记录、错误处理等。
        (注意: 当前为框架搭建阶段，只包含基础的迭代和日志记录)
        """
        if not hasattr(self, 'logger'):
            import logging
            self.logger = logging.getLogger(__name__)

        self.logger.info(f"任务 '{self.name}' 开始分块处理...")
        block_count = 0
        try:
            for block_params in self.get_data_blocks(**kwargs):
                block_count += 1
                self.logger.info(f"--> 正在处理块 #{block_count}: {block_params}")
                try:
                    # 在未来的实现中，这里可以添加数据获取、结果保存等逻辑
                    self.process_block(block_params)
                except Exception as e:
                    self.logger.error(f"处理块 {block_params} 时发生错误: {e}", exc_info=True)
                    # 可选：决定是跳过失败的块还是中止整个任务
            
            self.logger.info(f"所有块处理完成，共处理 {block_count} 个块。")
        except Exception as e:
            self.logger.error(f"在生成数据块时发生错误: {e}", exc_info=True) 