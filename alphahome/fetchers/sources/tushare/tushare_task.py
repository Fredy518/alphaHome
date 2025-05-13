import abc
import os
import asyncio
import pandas as pd
import numpy as np  # 导入 numpy
from typing import Dict, List, Any, Optional, Callable
from ...base_task import Task
from .tushare_api import TushareAPI
from .tushare_data_transformer import TushareDataTransformer
from .tushare_batch_processor import TushareBatchProcessor
from tqdm.asyncio import tqdm
from datetime import datetime, timedelta
import time
import logging # 确保导入 logging
import math # 引入 math 用于 ceil 计算

class TushareTask(Task):
    """基于 Tushare API 的数据任务基类
    
    核心设计：
    1. 基类只提供最基础的数据获取功能
    2. 具体的参数映射和批处理策略由子类决定
    """
    
    # 默认配置
    default_concurrent_limit = 5  # 默认并发限制
    default_page_size = 5000  # 默认每页数据量
    default_max_retries = 3     # 默认最大重试次数
    default_retry_delay = 2     # 默认重试延迟（秒）
    
    api_name = None  # Tushare API名称，子类必须定义
    fields = None    # 需要获取的字段列表，子类必须定义
    
    def __init__(self, db_connection, api_token=None, api=None):
        """初始化 TushareTask
        
        Args:
            db_connection: 数据库连接
            api_token (str, optional): Tushare API 令牌
            api (TushareAPI, optional): 已初始化的TushareAPI实例
        """
        super().__init__(db_connection)
        
        if self.api_name is None or self.fields is None:
            raise ValueError("必须定义api_name和fields属性")
            
        # 如果未提供token，则从环境变量获取
        if api_token is None:
            api_token = os.environ.get('TUSHARE_TOKEN')
            if not api_token:
                raise ValueError("未提供Tushare API令牌，请通过参数传入或设置TUSHARE_TOKEN环境变量")
                
        # 创建Tushare API客户端
        self.api = api or TushareAPI(api_token)
        
        # 初始化配置
        self.concurrent_limit = None
        self.page_size = None
        self.max_retries = self.default_max_retries
        self.retry_delay = self.default_retry_delay
        self.task_specific_config = {} # 存储原始任务配置
        
        # 初始化 Helper Classes
        self.data_transformer = TushareDataTransformer(self)
        self.batch_processor = TushareBatchProcessor(self, self.data_transformer)
        
    def set_config(self, task_config: Dict):
        """应用来自配置文件的设置，覆盖代码默认值"""
        self._apply_config(task_config)

    def _apply_config(self, task_config: Dict):
        """合并代码默认值和配置文件设置"""
        cls = type(self) # 获取实际的子类类型

        # 从子类或基类获取代码默认值
        code_default_limit = getattr(cls, 'default_concurrent_limit', TushareTask.default_concurrent_limit)
        code_default_page_size = getattr(cls, 'default_page_size', TushareTask.default_page_size)

        # 从 task_config (来自 config.json) 获取值，如果不存在则使用代码默认值
        self.concurrent_limit = task_config.get('concurrent_limit', code_default_limit)
        self.page_size = task_config.get('page_size', code_default_page_size)

        # 添加类型检查和转换，确保是整数
        try:
            self.concurrent_limit = int(self.concurrent_limit)
        except (ValueError, TypeError):
            self.logger.warning(f"配置中的 concurrent_limit \"{task_config.get('concurrent_limit')}\" 无效，将使用代码默认值 {code_default_limit}")
            self.concurrent_limit = code_default_limit
        
        try:
            self.page_size = int(self.page_size)
        except (ValueError, TypeError):
            self.logger.warning(f"配置中的 page_size \"{task_config.get('page_size')}\" 无效，将使用代码默认值 {code_default_page_size}")
            self.page_size = code_default_page_size

        # 确保存储为整数后记录日志
        self.logger.info(f"任务 {self.name}: 应用配置 concurrent_limit={self.concurrent_limit}, page_size={self.page_size}")
        
        # 存储原始配置字典以供子类可能需要访问其他特定键
        self.task_specific_config = task_config
        
        # 设置重试配置
        if 'max_retries' in task_config:
            self.max_retries = task_config['max_retries']
            self.logger.debug(f"设置最大重试次数: {self.max_retries}")
        if 'retry_delay' in task_config:
            self.retry_delay = task_config['retry_delay']
            self.logger.debug(f"设置重试延迟: {self.retry_delay}s")
        
    async def get_latest_date(self) -> Optional[datetime.date]:
        """获取当前任务对应表中的最新日期。
        
        Returns:
            Optional[datetime.date]: 最新日期，如果表不存在或没有数据则返回 None。
        """
        if not hasattr(self, 'db') or not self.db:
            self.logger.error("数据库管理器未初始化 (self.db is missing)，无法获取最新日期。")
            return None
            
        if not self.table_name or not self.date_column:
            self.logger.error("任务未定义 table_name 或 date_column，无法获取最新日期。")
            return None
            
        try:
            query = f"SELECT MAX({self.date_column}) as latest_date FROM {self.table_name}"
            result = await self.db.fetch_one(query)
            
            if result and result['latest_date']:
                latest_date = result['latest_date']
                from datetime import date, datetime 
                if isinstance(latest_date, datetime):
                    return latest_date.date() # 如果是 datetime 对象，返回 date 部分
                elif isinstance(latest_date, date):
                    return latest_date # 如果已经是 date 对象，直接返回
                else:
                    self.logger.warning(f"从数据库获取的最新日期类型未知: {type(latest_date)}")
                    # 尝试解析常见日期格式
                    try:
                        return datetime.strptime(str(latest_date), '%Y-%m-%d').date()
                    except ValueError:
                        try:
                            return datetime.strptime(str(latest_date), '%Y%m%d').date()
                        except ValueError:
                            self.logger.error(f"无法将 {latest_date} 解析为日期。")
                            return None
            else:
                self.logger.info(f"表 '{self.table_name}' 中没有找到日期数据。")
                return None
        except Exception as e:
            # 特别处理表不存在的情况
            # Specific check for asyncpg.exceptions.UndefinedTableError
            if isinstance(e, getattr(__import__('asyncpg.exceptions', fromlist=['UndefinedTableError']), 'UndefinedTableError', None)) or \
               ("does not exist" in str(e).lower() or ("relation" in str(e).lower() and "does not exist" in str(e).lower())):
                self.logger.info(f"表 '{self.table_name}' 不存在 (在 get_latest_date 中检测到)。")
                return None
            else:
                self.logger.error(f"查询最新日期时出错: {e}", exc_info=True)
                return None
        
    async def _ensure_table_exists(self):
        """确保当前任务的表在数据库中存在，如果不存在则尝试创建。"""
        if not hasattr(self, 'db') or not self.db:
            self.logger.error("数据库管理器未初始化 (self.db is missing)，无法确保表存在。")
            return
        if not self.table_name:
            self.logger.error("任务未定义 table_name，无法确保表存在。")
            return
        if not hasattr(self, 'schema') or not self.schema:
            self.logger.warning(f"任务 {self.name} 未定义 schema，无法自动创建表 {self.table_name}。")
            return

        try:
            # 尝试执行一个简单的查询来检查表是否存在，例如查询表的行数
            # 这比直接查询元数据更通用，但如果表非常大，可能会慢。
            # 或者，某些数据库驱动程序有特定的检查表存在的方法。
            # 对于asyncpg，通常是捕获UndefinedTableError。
            # 我们先尝试一个快速查询，如果失败则认为表可能不存在。
            await self.db.execute(f"SELECT 1 FROM {self.table_name} LIMIT 1")
            self.logger.debug(f"表 '{self.table_name}' 已存在。")
        except getattr(__import__('asyncpg.exceptions', fromlist=['UndefinedTableError']), 'UndefinedTableError', Exception) as e_undefined:
            # 捕获特定的 UndefinedTableError
            self.logger.info(f"表 '{self.table_name}' 不存在，正在尝试创建... ({e_undefined})")
            try:
                # 使用 db_manager 中的 create_table_from_schema 方法
                # 假设 db_manager (self.db) 有 create_table_from_schema 方法
                await self.db.create_table_from_schema(
                    table_name=self.table_name,
                    schema_def=self.schema,
                    primary_keys=getattr(self, 'primary_keys', []),
                    date_column=getattr(self, 'date_column', None),
                    indexes=getattr(self, 'indexes', [])
                )
                self.logger.info(f"表 '{self.table_name}' 创建成功。")
            except Exception as create_e:
                self.logger.error(f"创建表 '{self.table_name}' 失败: {create_e}", exc_info=True)
        except Exception as e:
            # 其他可能的错误，例如连接问题
            self.logger.error(f"检查表 '{self.table_name}' 是否存在时发生非预期的错误: {e}", exc_info=True)

    async def _determine_execution_dates(self, **kwargs) -> tuple[Optional[str], Optional[str], bool, str]:
        """根据输入参数和数据库状态确定最终的执行日期范围。
        
        Args:
            **kwargs: 包含 'start_date', 'end_date', 'force_full' 的可选参数。
            
        Returns:
            tuple: (final_start_date, final_end_date, should_skip, skip_message)
                   - final_start_date: YYYYMMDD 格式或 None
                   - final_end_date: YYYYMMDD 格式或 None
                   - should_skip: 是否应跳过执行
                   - skip_message: 跳过原因
        """
        original_start_date = kwargs.get('start_date')
        original_end_date = kwargs.get('end_date')
        force_full = kwargs.get('force_full', False)

        final_start_date = None
        final_end_date = None
        should_skip = False
        skip_message = ""

        if original_start_date is None and original_end_date is None and not force_full:
            # 智能增量模式
            self.logger.info(f"任务 {self.name}: 未提供 start_date 和 end_date，进入智能增量模式。")
            latest_date = await self.get_latest_date()
            self.logger.info(f"任务 {self.name}: 数据库中最新日期为: {latest_date}")
            default_start = getattr(self, 'default_start_date', None)
            if not default_start:
                self.logger.error(f"任务 {self.name}: 未定义 default_start_date，无法执行智能增量。")
                should_skip = True
                skip_message = "任务未定义默认开始日期"
                return final_start_date, final_end_date, should_skip, skip_message
            
            if latest_date:
                start_date_dt = latest_date + timedelta(days=1)
            else:
                try:
                    start_date_dt = datetime.strptime(default_start, '%Y%m%d').date()
                    self.logger.info(f"任务 {self.name}: 表为空或不存在，使用默认开始日期: {default_start}")
                except ValueError:
                    self.logger.error(f"任务 {self.name}: 默认开始日期格式错误 ('{default_start}')，应为 YYYYMMDD。")
                    should_skip = True
                    skip_message = "默认开始日期格式错误"
                    return final_start_date, final_end_date, should_skip, skip_message

            end_date_dt = datetime.now().date()
            
            if start_date_dt > end_date_dt:
                self.logger.info(f"任务 {self.name}: 计算出的开始日期 ({start_date_dt}) 晚于结束日期 ({end_date_dt})，数据已是最新。")
                should_skip = True
                skip_message = "数据已是最新"
                return final_start_date, final_end_date, should_skip, skip_message

            final_start_date = start_date_dt.strftime('%Y%m%d')
            final_end_date = end_date_dt.strftime('%Y%m%d')
            self.logger.info(f"任务 {self.name}: 智能增量计算出的日期范围: {final_start_date} 到 {final_end_date}")
            
        elif force_full:
            # 强制全量模式
            self.logger.info(f"任务 {self.name}: 强制执行全量模式。将使用默认开始日期 {getattr(self, 'default_start_date', 'N/A')} 到今天。")
            final_start_date = original_start_date if original_start_date is not None else getattr(self, 'default_start_date', None)
            final_end_date = original_end_date if original_end_date is not None else datetime.now().strftime('%Y%m%d')
            
            if final_start_date is None:
                self.logger.error(f"任务 {self.name}: 全量模式需要 default_start_date 或手动提供 start_date，但两者均未提供。")
                should_skip = True
                skip_message = "全量模式缺少开始日期"
                return final_start_date, final_end_date, should_skip, skip_message
            # 可选：添加对日期格式的验证
            self.logger.info(f"任务 {self.name}: 全量模式实际日期范围: {final_start_date} 到 {final_end_date}")
            
        else:
            # 手动指定日期模式
            # 可选：添加对手动提供的日期格式和有效性的验证
            if not original_start_date or not original_end_date:
                self.logger.error(f"任务 {self.name}: 手动模式需要提供 start_date 和 end_date。")
                should_skip = True
                skip_message = "手动模式缺少 start_date 或 end_date"
                return final_start_date, final_end_date, should_skip, skip_message
                
            final_start_date = original_start_date
            final_end_date = original_end_date
            self.logger.info(f"任务 {self.name}: 使用提供的日期范围: {final_start_date} 到 {final_end_date}")

        return final_start_date, final_end_date, should_skip, skip_message

    async def execute(self, **kwargs):
        """执行任务的完整生命周期 (已重构日期逻辑, 批处理聚合, 最终结果构造)"""
        self.logger.info(f"开始执行任务: {self.name}, 原始参数: {kwargs}")

        progress_callback = kwargs.get('progress_callback') # Not used in current TushareTask execute
        stop_event = kwargs.get('stop_event')

        # Initialize execution state variables
        total_rows = 0
        failed_batches = 0
        error_message = ""
        final_status = "success" # Default to success, will be updated by aggregation or errors
        batch_list = [] # Initialize batch_list

        try:
            await self.pre_execute(stop_event=stop_event)
            await self._ensure_table_exists()

            final_start_date, final_end_date, should_skip, skip_message = await self._determine_execution_dates(**kwargs)

            if should_skip:
                self.logger.warning(f"任务 {self.name}: 跳过执行，原因: {skip_message}")
                skip_status = "failed" if "错误" in skip_message or "未定义" in skip_message or "缺少" in skip_message else "skipped"
                # Use _build_final_result for skipped result
                final_result_dict = self._build_final_result(skip_status, skip_message, 0, 0)
                await self.post_execute(result=final_result_dict, stop_event=stop_event)
                return final_result_dict

            kwargs_for_batch_list = kwargs.copy()
            kwargs_for_batch_list['start_date'] = final_start_date
            kwargs_for_batch_list['end_date'] = final_end_date

            self.logger.info(f"任务 {self.name}: 准备调用 get_batch_list，使用最终参数: {kwargs_for_batch_list}")
            batch_list = await self.get_batch_list(**kwargs_for_batch_list)

            if not batch_list:
                self.logger.info(f"任务 {self.name}: get_batch_list 返回空列表。无需处理。")
                # Use _build_final_result for no_data result
                final_result_dict = self._build_final_result("no_data", "批处理列表为空", 0, 0)
                await self.post_execute(result=final_result_dict, stop_event=stop_event)
                return final_result_dict

            total_batches = len(batch_list)
            self.logger.info(f"生成了 {total_batches} 个批处理任务")
            
            semaphore = asyncio.Semaphore(self.concurrent_limit or self.default_concurrent_limit)
            tasks_to_run = []

            async def process_batch_wrapper(i, batch_params_item):
                async with semaphore:
                    if stop_event and stop_event.is_set():
                        raise asyncio.CancelledError(f"批次 {i+1} (任务 {self.name}) 在开始前被取消")
                    return await self.batch_processor.process_single_batch(i, total_batches, batch_params_item, stop_event=stop_event)

            for i, batch_params_item in enumerate(batch_list):
                tasks_to_run.append(process_batch_wrapper(i, batch_params_item))

            batch_execution_results = []
            if tasks_to_run:
                batch_execution_results = await asyncio.gather(*tasks_to_run, return_exceptions=True)
            
            # Aggregate results. total_rows and failed_batches are re-assigned here.
            # final_status and error_message are also determined here.
            total_rows, failed_batches, final_status, error_message = self._aggregate_batch_results(
                batch_execution_results,
                initial_total_rows=0, # Start fresh for this execution run
                initial_failed_batches=0 # Start fresh for this execution run
            )

        except asyncio.CancelledError:
            self.logger.warning(f"任务 {self.name} 在执行主流程中被取消。")
            final_status = "cancelled"
            error_message = "任务被用户取消"
            # total_rows and failed_batches reflect progress up to cancellation if aggregation occurred.
            # If cancellation was before aggregation, they remain at their initial values (likely 0).
        except Exception as e:
            self.logger.error(f"任务 {self.name} 执行过程中发生未处理的严重错误", exc_info=True)
            final_status = "failed"
            error_message = f"严重错误: {type(e).__name__} - {str(e)}"
            total_rows = 0 # Reset on catastrophic failure before aggregation
            failed_batches = len(batch_list) if batch_list else 0 # Mark all as failed if batch_list was generated
        
        finally:
            # Use _build_final_result in the finally block
            final_result_dict = self._build_final_result(final_status, error_message, total_rows, failed_batches)
            await self.post_execute(result=final_result_dict, stop_event=stop_event)
            self.logger.info(f"任务 {self.name} 最终处理完成，状态: {final_status}, 总行数: {total_rows}, 失败/取消批次数: {failed_batches}")

        return final_result_dict

    def _build_final_result(self, status: str, message: str, rows: int, failed_batches: int) -> Dict:
        """构建标准的任务执行结果字典。"""
        return {
            "status": status,
            "message": message.strip(),
            "rows": rows,
            "failed_batches": failed_batches,
            "task": self.name # Assuming self.name is available
        }

    def _aggregate_batch_results(
        self,
        batch_execution_results: List[Any],
        initial_total_rows: int,
        initial_failed_batches: int
    ) -> tuple[int, int, str, str]:
        """聚合来自并发批处理执行的结果。
        
        Args:
            batch_execution_results: asyncio.gather 返回的结果列表。
            initial_total_rows: 聚合前的总行数。
            initial_failed_batches: 聚合前的失败批次数。
            
        Returns:
            tuple: (final_total_rows, final_failed_batches, final_status, final_error_message)
        """
        current_total_rows = initial_total_rows
        current_failed_batches = initial_failed_batches
        aggregated_error_message = ""
        # Initial status assumes success if all batches go through.
        # If any batch is cancelled or fails, this will be updated.
        current_final_status = "success" 
        
        num_cancelled_internally = 0

        for idx, result in enumerate(batch_execution_results):
            batch_num_for_log = idx + 1 # For logging purposes
            if isinstance(result, asyncio.CancelledError):
                # This typically means the cancellation was raised from within _process_single_batch or its sub-methods
                # or process_batch_wrapper itself before _process_single_batch was called.
                # or process_batch_wrapper itself before _process_single_batch was called.
                self.logger.warning(f"批次 {batch_num_for_log} (任务 {self.name}) 被内部取消: {result}")
                current_failed_batches += 1
                aggregated_error_message += f"批次 {batch_num_for_log} 取消: {result}; "
                current_final_status = "cancelled" # If one is cancelled, the whole task might be considered cancelled.
                num_cancelled_internally +=1
            elif isinstance(result, Exception):
                self.logger.error(f"批次 {batch_num_for_log} (任务 {self.name}) 执行失败: {result}", exc_info=result)
                current_failed_batches += 1
                aggregated_error_message += f"批次 {batch_num_for_log} 失败: {result}; "
                if current_final_status != "cancelled": # Don't override cancelled status
                    current_final_status = "failed"
            elif isinstance(result, int):
                if result > 0:
                    current_total_rows += result
                elif result == 0: # _process_single_batch returns 0 for failure or 0 rows processed successfully
                    # The logger inside _process_single_batch would have indicated the specific cause.
                    # We count it as a "failed batch" for aggregation if it intended to process data but didn't.
                    # If it was an empty fetch that correctly returned 0, it's not a "failed batch" in the same sense,
                    # but for simplicity in aggregation, we might still log it or count it.
                    # Let's assume _process_single_batch returning 0 means it effectively failed or had no impact.
                    self.logger.warning(f"批次 {batch_num_for_log} (任务 {self.name}) 返回 0 (可能内部失败或无数据处理)，计为失败批次。")
                    current_failed_batches += 1
                    aggregated_error_message += f"批次 {batch_num_for_log} 返回0; "
                    if current_final_status != "cancelled":
                        current_final_status = "failed"
                # else: negative result from _process_single_batch - should not happen, but good to log if it does.
                # Already logged within _process_single_batch if it were to return negative.
            else: # Unexpected result type
                self.logger.error(f"批次 {batch_num_for_log} (任务 {self.name}) 返回未知类型: {type(result)} ({result})")
                current_failed_batches += 1
                aggregated_error_message += f"批次 {batch_num_for_log} 返回未知类型 ({type(result)}); "
                if current_final_status != "cancelled":
                    current_final_status = "failed"
        
        # Final status adjustment based on aggregated results
        if current_final_status == "cancelled":
            # If any internal batch was cancelled, the overall status is cancelled.
            self.logger.warning(f"任务 {self.name}: {num_cancelled_internally} 个批次被内部取消。整体任务状态设为 \'cancelled\'。")
            aggregated_error_message = f"任务因 {num_cancelled_internally} 个批次内部取消而被标记为取消。 " + aggregated_error_message
        elif current_failed_batches > 0:
            current_final_status = "partial_success" if current_total_rows > 0 else "failed"
            self.logger.warning(f"任务 {self.name} 执行完成，但有 {current_failed_batches} 个批次处理失败或被取消。")
            if current_final_status == "partial_success":
                aggregated_error_message = f"完成但有 {current_failed_batches} 个批次失败/取消。 " + aggregated_error_message
            else: # failed
                aggregated_error_message = f"任务失败，共 {current_failed_batches} 个批次失败/取消。 " + aggregated_error_message
        # If no failures and not cancelled, current_final_status remains "success" (initial value)

        return current_total_rows, current_failed_batches, current_final_status, aggregated_error_message.strip()

            
    async def fetch_batch(self, batch_params: Dict) -> Optional[pd.DataFrame]:
        """获取单个批次的数据，处理API调用。
        
        Args:
            batch_params: 由get_batch_list方法生成的单个批次参数字典
            
        Returns:
            Optional[pd.DataFrame]: 获取到的数据，如果失败或无数据则为None或空DataFrame。
        """
        api_params = self.prepare_params(batch_params)
        self.logger.debug(f"任务 {self.name}: 调用API {self.api_name} 使用参数: {api_params}")

        try:
            # 确保 api 实例存在
            if not hasattr(self, 'api') or not self.api:
                self.logger.error(f"任务 {self.name}: TushareAPI 实例 (self.api) 未初始化。")
                return None
            
            # 确保 api_name 已定义
            if not self.api_name:
                self.logger.error(f"任务 {self.name}: api_name 未在子类中定义。")
                return None

            data = await self.api.query(api_name=self.api_name, params=api_params, fields=self.fields)
            
            if data is None:
                self.logger.warning(f"任务 {self.name} API调用 {self.api_name} (参数: {api_params}) 返回 None")
                return None # 或者 pd.DataFrame() 根据后续处理逻辑
            
            if not isinstance(data, pd.DataFrame):
                self.logger.error(f"任务 {self.name} API调用 {self.api_name} (参数: {api_params}) 返回非DataFrame类型: {type(data)}")
                return None # 或者根据错误处理策略转换/记录

            if data.empty:
                self.logger.info(f"任务 {self.name} API调用 {self.api_name} (参数: {api_params}) 返回空DataFrame")
            else:
                self.logger.info(f"任务 {self.name} API调用 {self.api_name} (参数: {api_params}) 成功获取 {len(data)} 行数据")
            return data
        except Exception as e:
            self.logger.error(f"任务 {self.name} API调用 {self.api_name} (参数: {api_params}) 失败: {e}", exc_info=True)
            return None # 或 pd.DataFrame() 以避免None导致后续错误
        
    @abc.abstractmethod
    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """生成批处理参数列表
        
        该方法负责将用户的查询参数转换为一系列批处理参数，每个批处理参数将用于一次API调用。
        这个方法的主要目的是将大查询分解为多个小查询，以避免超出API限制或提高并行性。
        
        实现此方法时，应考虑以下几点：
        1. 数据量大小（如日期范围、股票数量等）
        2. API的限制（如单次查询的最大记录数）
        3. 性能考虑（如并行查询的优化）
        
        Args:
            **kwargs: 用户提供的查询参数，如start_date, end_date, ts_code等
            
        Returns:
            List[Dict]: 批处理参数列表，每个元素是一个参数字典，将用于一次API调用
        """
        pass
    
    def prepare_params(self, batch_params: Dict) -> Dict:
        """准备API调用参数
        
        该方法负责将批处理参数转换为Tushare API调用所需的确切参数格式。
        这个方法的主要目的是处理参数格式转换、默认值设置、参数验证等工作。
        
        默认实现直接返回批处理参数，适用于参数无需特殊处理的API。
        子类可以根据需要重写此方法，以处理更复杂的参数转换。
        
        Args:
            batch_params: 由get_batch_list方法生成的单个批次参数字典
            
        Returns:
            Dict: 准备好的API调用参数字典
        """
        # 提供默认实现，直接返回批次参数
        # 仅当API需要特殊参数处理时，子类才需要重写此方法
        return batch_params
        
    async def fetch_data(self, **kwargs):
        """从Tushare获取数据
        
        实现了Task基类的抽象方法，但在TushareTask中，我们更推荐使用execute方法直接执行
        任务的完整生命周期，它会分批获取和处理数据，更适合大数据集。
        
        此方法仅作为兼容基类接口保留，不建议直接调用。
        
        Args:
            **kwargs: 额外参数，将传递给get_batch_list和prepare_params方法
            
        Returns:
            DataFrame: 获取的数据
        """
        self.logger.warning("TushareTask.fetch_data方法不建议直接调用，请使用execute方法代替")
        
        # 获取批处理参数列表
        batch_list = self.get_batch_list(**kwargs)
        if not batch_list:
            self.logger.warning("批处理参数列表为空，无法获取数据")
            return pd.DataFrame()
            
        self.logger.info(f"生成了 {len(batch_list)} 个批处理任务")
        
        # 存储所有批次的数据
        all_data = []
        
        # 处理每个批次
        for i, batch_params in enumerate(batch_list):
            try:
                result = await self.fetch_batch(batch_params)
                if not result.empty:
                    self.logger.info(f"批次 {i+1}/{len(batch_list)}: 获取到 {len(result)} 行数据")
                    all_data.append(result)
                else:
                    self.logger.warning(f"批次 {i+1}/{len(batch_list)}: 未获取到数据")
            except Exception as e:
                self.logger.error(f"批次 {i+1}/{len(batch_list)} 获取数据失败: {str(e)}")
                # 继续处理下一个批次，而不是直接失败
                continue
        
        # 合并所有批次的数据
        if not all_data:
            self.logger.warning("所有批次都未获取到数据")
            return pd.DataFrame()
            
        combined_data = pd.concat(all_data, ignore_index=True)
        self.logger.info(f"成功获取数据，共 {len(combined_data)} 行")
        
        return combined_data

