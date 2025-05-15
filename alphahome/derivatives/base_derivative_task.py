# alphahome/derivatives/base_derivative_task.py
import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Type
from datetime import datetime, timedelta
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

from alphahome.fetchers.db_manager import DBManager # 实际导入DBManager

# class DBManager: # Placeholder - REMOVE THIS AND IMPORT ACTUAL DBManager
# ... (占位符 DBManager 定义已删除)


class BaseDerivativeTask(ABC):
    """
    所有衍生品计算任务的抽象基类。
    定义了衍生品任务的核心接口和执行流程。
    """
    name: str # 任务的唯一名称，用于注册和识别
    description: str # 任务的描述信息
    input_tables: List[str] # 任务执行所依赖的输入表名列表
    output_table: str # 任务计算结果输出的表名
    output_schema: Dict[str, Any] # 输出表的结构定义，例如: {"col_name": {"type": "VARCHAR(255)", "constraints": "NOT NULL"}}
    primary_keys: List[str] # 输出表的主键列名列表
    date_column: Optional[str] = None # 输出表中用于增量更新的日期列名 (如果支持增量)
    default_start_date: Optional[str] = None # 增量更新时，若无历史数据，默认的起始日期 (YYYYMMDD)
    task_type: str = "derivative" # 任务类型，默认为 "derivative"

    def __init__(self, 
                 db_manager: DBManager, 
                 executor: Optional[ThreadPoolExecutor] = None, 
                 logger: Optional[logging.Logger] = None, 
                 **kwargs):
        """
        初始化衍生品任务实例。

        参数:
            db_manager: 数据库管理器实例。
            executor: (可选) 用于执行CPU密集型计算的线程池执行器。
            logger: (可选) 日志记录器实例。如果未提供，则会创建一个默认的记录器。
            **kwargs: 其他特定于任务的配置参数。
        """
        self.db_manager = db_manager
        self.executor = executor
        self.logger = logger or logging.getLogger(f"DerivativeTask.{getattr(self, 'name', 'UnnamedTask')}")
        self.config = kwargs # 存储额外的配置参数

        # --- 核心属性校验 ---
        if not hasattr(self, 'name') or not self.name:
            raise ValueError("衍生品任务子类必须定义 \'name\' 属性。")
        if not hasattr(self, 'description') or not self.description: # 增加描述属性校验
            self.logger.warning(f"任务 {self.name} 未定义 \'description\' 描述属性。")
        if not hasattr(self, 'input_tables') or not self.input_tables: # 增加输入表属性校验
             self.logger.warning(f"任务 {self.name} 未定义 \'input_tables\' 输入表属性。")
        if not hasattr(self, 'output_table') or not self.output_table:
            raise ValueError(f"任务 {self.name} 必须定义 \'output_table\' 输出表属性。")
        if not hasattr(self, 'output_schema') or not self.output_schema:
             raise ValueError(f"任务 {self.name} 必须定义 \'output_schema\' 输出表结构属性。")
        if not hasattr(self, 'primary_keys') or not self.primary_keys:
            raise ValueError(f"任务 {self.name} 必须为其输出表定义 \'primary_keys\' 主键。")

    @abstractmethod
    async def load_input_data(self, **kwargs) -> Dict[str, pd.DataFrame]:
        """
        加载任务所需的输入数据。
        子类必须实现此方法。

        参数:
            **kwargs: 执行时传递的参数，例如日期范围、特定ID等。
        返回:
            一个字典，键是输入表名，值是对应的 pandas DataFrame。
        """
        pass

    @abstractmethod
    async def calculate(self, input_data: Dict[str, pd.DataFrame], **kwargs) -> pd.DataFrame:
        """
        执行核心的衍生品计算逻辑。
        子类必须实现此方法。

        参数:
            input_data: `load_input_data` 方法返回的输入数据字典。
            **kwargs: 执行时传递的参数。
        返回:
            一个包含计算结果的 pandas DataFrame。
        """
        pass

    async def save_output_data(self, output_df: pd.DataFrame, **kwargs) -> Dict[str, Any]:
        """
        将计算结果保存到数据库。

        参数:
            output_df: `calculate` 方法返回的计算结果 DataFrame。
            **kwargs: 执行时传递的参数。
        返回:
            一个包含保存操作状态和影响行数的字典。
        """
        if output_df is None or output_df.empty:
            self.logger.info(f"任务 {self.name}: 没有数据需要保存到输出表 {self.output_table}。")
            return {"status": "no_data", "rows_affected": 0}
        
        try:
            self.logger.info(f"任务 {self.name}: 正在保存 {len(output_df)} 行数据到 {self.output_table}...")
            # 从DataFrame列中排除主键列，剩下的作为更新列
            update_columns = [col for col in output_df.columns if col not in self.primary_keys]
            
            # 关于 created_at 和 updated_at 时间戳:
            # 通常更建议在数据库层面通过默认值 (如 CURRENT_TIMESTAMP) 或触发器来处理这些时间戳，
            # 以确保数据的一致性和准确性，而不是在应用层设置。
            # 如果应用层必须设置，可以取消以下注释：
            # timestamp_now = datetime.now()
            # if 'created_at' not in output_df.columns:
            #     output_df['created_at'] = timestamp_now
            # if 'updated_at' not in output_df.columns: 
            #      output_df['updated_at'] = timestamp_now

            await self.db_manager.upsert(
                table_name=self.output_table,
                data=output_df,
                conflict_columns=self.primary_keys, # 用于冲突检测的列 (通常是主键)
                update_columns=update_columns       # 发生冲突时需要更新的列
            )
            self.logger.info(f"任务 {self.name}: 成功保存 {len(output_df)} 行数据到 {self.output_table}。")
            return {"status": "success", "rows_affected": len(output_df)}
        except Exception as e:
            self.logger.error(f"任务 {self.name}: 保存数据到 {self.output_table} 时出错: {e}", exc_info=True)
            return {"status": "error", "error_message": str(e), "rows_affected": 0}

    async def pre_execute(self, **kwargs):
        """
        任务执行前的准备工作，例如检查和创建输出表。
        """
        self.logger.info(f"任务 {self.name}: 正在运行执行前检查...")
        table_exists = await self.db_manager.table_exists(self.output_table)
        if not table_exists:
            self.logger.info(f"任务 {self.name}: 输出表 {self.output_table} 不存在。正在创建表...")
            try:
                # output_indexes 属性可以用于定义除主键外的其他索引
                indexes_to_create = getattr(self, 'output_indexes', None)
                await self.db_manager.create_table(
                    table_name=self.output_table,
                    schema=self.output_schema,
                    primary_keys=self.primary_keys,
                    indexes=indexes_to_create 
                )
                self.logger.info(f"任务 {self.name}: 成功创建表 {self.output_table}。")
            except Exception as e:
                self.logger.error(f"任务 {self.name}: 创建表 {self.output_table} 失败: {e}", exc_info=True)
                raise # 创建表失败通常是关键错误，直接抛出
        else:
            self.logger.info(f"任务 {self.name}: 输出表 {self.output_table} 已存在。")
        self.logger.info(f"任务 {self.name}: 执行前检查完成。")

    async def post_execute(self, result: Dict[str, Any], **kwargs):
        """
        任务执行后的清理或收尾工作。
        """
        self.logger.info(f"任务 {self.name}: 执行后操作。结果: {result.get('status', 'unknown')}")
        # 例如，可以在这里记录任务执行统计信息或发送通知

    async def _get_latest_output_date(self) -> Optional[datetime]:
        """
        获取输出表中已存在的最新日期，用于增量更新。
        """
        if not self.date_column:
            self.logger.warning(f"任务 {self.name}: 未定义日期列 (date_column)，无法进行增量更新的日期检查。")
            return None
        try:
            # 假设 conditions 参数可以用于更复杂的最新日期查询，例如按特定ID分组
            latest_date = await self.db_manager.get_latest_date(self.output_table, self.date_column)
            if latest_date:
                 self.logger.info(f"任务 {self.name}: 输出表 {self.output_table} 中 {self.date_column} 的最新日期是 {latest_date.strftime('%Y-%m-%d')}。")
            else:
                self.logger.info(f"任务 {self.name}: 输出表 {self.output_table} 中没有找到 {self.date_column} 的数据。")
            return latest_date
        except Exception as e:
            self.logger.error(f"任务 {self.name}: 从 {self.output_table} 获取最新日期时出错: {e}", exc_info=True)
            return None 

    async def execute(self, **kwargs) -> Dict[str, Any]:
        """
        执行衍生品计算任务的完整流程：
        1. 执行前检查 (pre_execute)
        2. 加载输入数据 (load_input_data)
        3. 执行计算 (calculate)
        4. 保存输出数据 (save_output_data)
        5. 执行后操作 (post_execute)
        """
        self.logger.info(f"任务 {self.name}: 开始执行，参数: {kwargs}")
        result_summary = { # 用于记录任务执行结果的摘要
            "task_name": self.name,
            "status": "initiated", # 初始状态
            "rows_calculated": 0,
            "rows_saved": 0,
            "error_message": None
        }
        try:
            await self.pre_execute(**kwargs) # 1. 执行前操作

            input_data_dict = await self.load_input_data(**kwargs) # 2. 加载输入数据
            if not input_data_dict or any(df.empty for df in input_data_dict.values() if isinstance(df, pd.DataFrame)):
                self.logger.warning(f"任务 {self.name}: 未加载到输入数据或输入数据为空，跳过计算。")
                result_summary["status"] = "no_input_data"
                await self.post_execute(result_summary, **kwargs)
                return result_summary
            self.logger.info(f"任务 {self.name}: 输入数据加载完成。")

            calculated_df = await self.calculate(input_data_dict, **kwargs) # 3. 执行计算
            if calculated_df is None or calculated_df.empty:
                self.logger.warning(f"任务 {self.name}: 计算未产生输出数据，跳过保存。")
                result_summary["status"] = "no_output_from_calculation"
                await self.post_execute(result_summary, **kwargs)
                return result_summary
            result_summary["rows_calculated"] = len(calculated_df)
            self.logger.info(f"任务 {self.name}: 计算完成，生成 {len(calculated_df)} 行数据。")

            save_result = await self.save_output_data(calculated_df, **kwargs) # 4. 保存输出数据
            result_summary["status"] = save_result.get("status", "error_during_save")
            result_summary["rows_saved"] = save_result.get("rows_affected", 0)
            if save_result.get("status") != "success":
                result_summary["error_message"] = save_result.get("error_message", "保存数据时发生未知错误")

            await self.post_execute(result_summary, **kwargs) # 5. 执行后操作
            self.logger.info(f"任务 {self.name}: 执行完成。状态: {result_summary['status']}")
            return result_summary
        except Exception as e:
            self.logger.error(f"任务 {self.name}: 执行过程中发生未处理的异常: {e}", exc_info=True)
            result_summary["status"] = "fatal_error" # 标记为致命错误
            result_summary["error_message"] = str(e)
            try: 
                # 即使发生致命错误，也尝试执行 post_execute 以进行可能的清理
                await self.post_execute(result_summary, **kwargs) 
            except Exception as post_exc: 
                self.logger.error(f"任务 {self.name}: 在处理致命错误后的 post_execute 中也发生错误: {post_exc}", exc_info=True)
            return result_summary

    async def smart_incremental_update(self, lookback_days: int = 0, **kwargs) -> Dict[str, Any]:
        """
        智能增量更新方法。
        如果输出表中有数据，则从最新日期的下一天开始计算。
        如果输出表中没有数据，则根据 `default_start_date` 或 `lookback_days` 确定起始日期。

        参数:
            lookback_days: (可选) 如果没有历史数据且没有 `default_start_date`，则从 (当前日期 - lookback_days) 开始计算。
            **kwargs: 其他传递给 `execute` 方法的参数，例如 `end_date`。
        返回:
            `execute` 方法的执行结果字典。
        """
        self.logger.info(f"任务 {self.name}: 开始智能增量更新。")
        if not self.date_column:
            self.logger.error(f"任务 {self.name}: 未定义 \'date_column\'，无法执行智能增量更新。")
            return {"task_name": self.name, "status": "error", "error_message": "未定义date_column"}

        latest_output_date = await self._get_latest_output_date() # 获取输出表中最新的日期
        start_date_dt: Optional[datetime] = None
        
        if latest_output_date: # 如果已有数据
            start_date_dt = latest_output_date + timedelta(days=1) # 从最新日期的下一天开始
            self.logger.info(f"任务 {self.name}: 增量更新将从 {start_date_dt.strftime('%Y%m%d')} 开始。")
        else: # 如果没有历史数据
            self.logger.info(f"任务 {self.name}: 输出表中无历史数据，正在确定起始日期。")
            if self.default_start_date: # 优先使用任务定义的默认起始日期
                try: 
                    start_date_dt = datetime.strptime(self.default_start_date, '%Y%m%d')
                except ValueError: 
                    self.logger.error(f"任务 {self.name}: 无效的 default_start_date 格式: {self.default_start_date}。将尝试使用 lookback_days。")
                    if lookback_days > 0: 
                        start_date_dt = datetime.now() - timedelta(days=lookback_days)
            elif lookback_days > 0: # 如果没有默认起始日期，但提供了 lookback_days
                start_date_dt = datetime.now() - timedelta(days=lookback_days)
            
            if not start_date_dt: # 如果最终无法确定起始日期
                 self.logger.error(f"任务 {self.name}: 无法确定初次运行的起始日期。请提供 default_start_date 或 lookback_days。")
                 return {"task_name": self.name, "status": "error", "error_message": "无法确定起始日期"}
            self.logger.info(f"任务 {self.name}: 确定初次运行的起始日期为 {start_date_dt.strftime('%Y%m%d')}。")

        effective_start_date_str = start_date_dt.strftime('%Y%m%d')
        
        # 结束日期默认为根据当前时间判断，除非在kwargs中明确指定
        if 'end_date' in kwargs:
            end_date_str = kwargs.pop('end_date')
            self.logger.info(f"使用指定的结束日期: {end_date_str}")
        else:
            # 根据当前时间判断使用哪一天作为结束日期
            current_time = datetime.now()
            if current_time.hour >= 18:
                # 晚于18:00，使用当天日期作为end_date
                end_date_str = current_time.strftime('%Y%m%d')
                self.logger.info(f"当前时间晚于18:00，使用当前日期作为结束日期: {end_date_str}")
            else:
                # 早于18:00，使用昨天日期作为end_date
                yesterday = current_time - timedelta(days=1)
                end_date_str = yesterday.strftime('%Y%m%d')
                self.logger.info(f"当前时间早于18:00，使用昨天日期作为结束日期: {end_date_str}")
        
        try: # 校验结束日期
            end_date_dt = datetime.strptime(end_date_str, '%Y%m%d')
            if start_date_dt > end_date_dt: # 如果计算出的开始日期晚于结束日期
                self.logger.info(f"任务 {self.name}: 起始日期 {effective_start_date_str} 晚于结束日期 {end_date_str}。无需更新。")
                return {"task_name": self.name, "status": "up_to_date", "message": "数据已是最新"}
        except ValueError:
            self.logger.error(f"任务 {self.name}: 无效的结束日期格式: {end_date_str}。")
            return {"task_name": self.name, "status": "error", "error_message": f"无效的结束日期: {end_date_str}"}
        except TypeError:
            # 处理end_date_str为None的情况
            self.logger.error(f"任务 {self.name}: 结束日期不能为None。")
            return {"task_name": self.name, "status": "error", "error_message": "结束日期不能为None"}

        # 准备传递给 execute 方法的参数
        kwargs_for_execute = {'start_date': effective_start_date_str, 'end_date': end_date_str, **kwargs}
        return await self.execute(**kwargs_for_execute) 