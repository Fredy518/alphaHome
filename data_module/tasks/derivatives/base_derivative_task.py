import asyncio
import logging
import pandas as pd
from abc import ABC, abstractmethod
from typing import Dict, Any, Callable, Optional
from concurrent.futures import ProcessPoolExecutor
from functools import partial

from ...base_task import Task
from ...task_factory import TaskFactory

class BaseDerivativeTask(Task, ABC):
    """衍生数据计算任务的基类

    提供了处理输入规范 (input_spec) 和在进程池中执行计算的通用逻辑。
    """

    # 覆盖基类的任务类型标识
    task_type: str = 'derivative'

    # 类属性：定义计算所需的输入数据及其来源
    # 结构示例:
    # input_spec = {
    #     'logical_name_1': {'table': 'db_table_1', 'columns': ['col_a', 'col_b'], 'date_col': 'date'},
    #     'logical_name_2': {'table': 'db_table_2', 'columns': ['col_x', 'col_y'], 'date_col': 'trade_dt'}
    # }
    input_spec: Dict[str, Dict[str, Any]] = {}

    def __init__(self, db_connection, api_token=None):
        """初始化衍生任务基类"""
        super().__init__(db_connection, api_token)
        if not self.input_spec: # 检查子类是否定义了 input_spec
            self.logger.warning(f"任务 {self.name} 未定义 input_spec，fetch_data 可能无法按预期工作。")

    @abstractmethod
    async def fetch_data(self, **kwargs) -> Dict[str, pd.DataFrame]:
        """获取所有在 input_spec 中定义的输入数据

        子类必须实现此方法，根据 input_spec 和 kwargs (如日期范围)
        从数据库查询数据，并返回一个包含 DataFrames 的字典，
        字典的键应与 input_spec 中的键对应。

        Args:
            **kwargs: 传递给任务执行的参数，通常包含 start_date, end_date 等

        Returns:
            Dict[str, pd.DataFrame]: 包含输入数据的字典
        """
        raise NotImplementedError("Derivative tasks must implement fetch_data based on input_spec.")

    @abstractmethod
    def _run_calculation(self, data_dict: Dict[str, pd.DataFrame], **kwargs) -> pd.DataFrame:
        """实际的同步计算逻辑

        子类必须实现此方法，包含 CPU 密集型的计算代码。
        此方法将在单独的进程中执行。

        Args:
            data_dict (Dict[str, pd.DataFrame]): 从 fetch_data 获取的输入数据字典
            **kwargs: 从 execute 传递过来的计算参数

        Returns:
            pd.DataFrame: 计算得到的衍生数据
        """
        raise NotImplementedError("Derivative tasks must implement _run_calculation.")

    async def process_data(self, data_dict: Dict[str, pd.DataFrame], **kwargs) -> Optional[pd.DataFrame]:
        """协调计算过程，将计算卸载到进程池

        Args:
            data_dict (Dict[str, pd.DataFrame]): fetch_data 返回的数据字典
            **kwargs: 传递给 _run_calculation 的额外参数

        Returns:
            Optional[pd.DataFrame]: 计算结果，如果执行器不可用或计算失败则可能为 None
        """
        self.logger.info(f"准备将计算任务 {self.name} 提交到进程池...")
        executor = TaskFactory.get_process_executor()

        if executor is None:
            self.logger.error("无法获取进程池执行器，计算无法执行。请检查 TaskFactory 初始化。")
            return None

        loop = asyncio.get_running_loop()

        try:
            # 注意：传递 self 给 _run_calculation 可能不是最佳实践，因为它需要在进程间序列化。
            # 更好的方式是确保 _run_calculation 是静态的或仅依赖于传入的参数。
            # 如果 _run_calculation 确实需要访问实例属性，需要仔细考虑序列化问题。
            # 这里我们假设 _run_calculation 只需要 data_dict 和 kwargs。
            # 如果需要 self，则需确保 Task 实例是可序列化的，或者重构 _run_calculation。

            # 为了安全和清晰，我们明确传递需要的参数
            calculation_args = (data_dict,) # 将 data_dict 作为元组传递
            calculation_kwargs = kwargs # 将其他参数作为字典传递

            self.logger.debug(f"提交计算到执行器: args={calculation_args}, kwargs={calculation_kwargs}")

            # run_in_executor 不直接支持传递关键字参数给目标函数
            # 需要包装一下或者使用 functools.partial
            target_func = partial(self._run_calculation, **calculation_kwargs)

            result_df = await loop.run_in_executor(
                executor,
                target_func,
                *calculation_args # 解包元组作为位置参数
            )
            self.logger.info(f"从进程池接收到计算结果，共 {len(result_df) if result_df is not None else 0} 行。")
            return result_df
        except Exception as e:
            self.logger.error(f"在进程池中执行计算时发生错误: {str(e)}", exc_info=True)
            # 可以根据需要返回 None 或重新抛出异常
            return None

    # 子类可以选择性地重写 validate_data 和 save_data
    # save_data 通常可以使用基类 Task 的默认实现（基于 upsert） 