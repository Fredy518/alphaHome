import abc
import os
import asyncio
import pandas as pd
import numpy as np  # 导入 numpy
from typing import Dict, List, Any, Optional, Callable
from ...base_task import Task
from .tushare_api import TushareAPI
from tqdm.asyncio import tqdm
from datetime import datetime, timedelta
import time
import logging # 确保导入 logging

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
            if "does not exist" in str(e).lower() or "relation" in str(e).lower() and "does not exist" in str(e).lower():
                self.logger.info(f"表 '{self.table_name}' 不存在，无法获取最新日期。")
                return None
            else:
                self.logger.error(f"查询最新日期时出错: {e}", exc_info=True)
                return None
        
    async def execute(self, **kwargs):
        """执行任务的完整生命周期
        
        重写Task基类的execute方法，实现分批获取、处理和保存数据的流程。
        这种方式更适合处理大数据集，因为它避免了将所有数据一次性加载到内存中。
        
        Args:
            progress_callback (Callable, optional): 异步回调函数，用于报告进度。
                                                  签名: async def callback(task_name: str, progress: str)
            stop_event (asyncio.Event, optional): 事件对象，用于外部请求停止任务。
            force_full (bool, optional): 如果为 True，则强制执行全量更新，忽略智能增量逻辑。
            start_date (str, optional): 手动指定的开始日期 (YYYYMMDD)。
            end_date (str, optional): 手动指定的结束日期 (YYYYMMDD)。
            **kwargs: 额外参数，将传递给get_batch_list和prepare_params方法
            
        Returns:
            Dict: 任务执行结果，包含状态和影响的行数
        """
        self.logger.info(f"开始执行任务: {self.name}, 参数: {kwargs}") # Log initial kwargs
        
        progress_callback = kwargs.get('progress_callback')
        stop_event = kwargs.get('stop_event') # 获取停止事件
        
        total_rows = 0
        failed_batches = 0
        error_message = ""
        final_status = "success" # Assume success initially
        batch_list = [] # Initialize batch_list

        try:
            await self.pre_execute(stop_event=stop_event) # Pass stop_event to pre_execute

            # --- Date Logic ---
            original_start_date = kwargs.get('start_date')
            original_end_date = kwargs.get('end_date')
            force_full = kwargs.get('force_full', False)

            if original_start_date is None and original_end_date is None and not force_full:
                self.logger.info(f"任务 {self.name}: 未提供 start_date 和 end_date，进入智能增量模式。")
                latest_date = await self.get_latest_date()
                self.logger.info(f"任务 {self.name}: 数据库中最新日期为: {latest_date}")
                default_start = getattr(self, 'default_start_date', None)
                if not default_start:
                    self.logger.error(f"任务 {self.name}: 未定义 default_start_date，无法执行智能增量。")
                    failed_result = {"status": "failed", "message": "任务未定义默认开始日期", "rows": 0}
                    # Corrected Call
                    await self.post_execute(result=failed_result, stop_event=stop_event) 
                    return failed_result
                
                if latest_date:
                    start_date_dt = latest_date + timedelta(days=1)
                else:
                    try:
                        start_date_dt = datetime.strptime(default_start, '%Y%m%d').date()
                        self.logger.info(f"任务 {self.name}: 表为空或不存在，使用默认开始日期: {default_start}")
                    except ValueError:
                        self.logger.error(f"任务 {self.name}: 默认开始日期格式错误 ('{default_start}')，应为 YYYYMMDD。")
                        failed_result = {"status": "failed", "message": "默认开始日期格式错误", "rows": 0}
                        # Corrected Call
                        await self.post_execute(result=failed_result, stop_event=stop_event) 
                        return failed_result

                end_date_dt = datetime.now().date()
                
                if start_date_dt > end_date_dt:
                    self.logger.info(f"任务 {self.name}: 计算出的开始日期 ({start_date_dt}) 晚于结束日期 ({end_date_dt})，数据已是最新。")
                    skipped_result = {"status": "skipped", "message": "数据已是最新", "rows": 0}
                    # Corrected Call
                    await self.post_execute(result=skipped_result, stop_event=stop_event) 
                    return skipped_result
                    
                calculated_start_date = start_date_dt.strftime('%Y%m%d')
                calculated_end_date = end_date_dt.strftime('%Y%m%d')
                kwargs['start_date'] = calculated_start_date
                kwargs['end_date'] = calculated_end_date
                self.logger.info(f"任务 {self.name}: 智能增量计算出的日期范围: {calculated_start_date} 到 {calculated_end_date}")
            
            elif force_full:
                self.logger.info(f"任务 {self.name}: 强制执行全量模式。将使用默认开始日期 {getattr(self, 'default_start_date', 'N/A')} 到今天。")
                if original_start_date is None:
                    kwargs['start_date'] = getattr(self, 'default_start_date', None)
                if original_end_date is None:
                    kwargs['end_date'] = datetime.now().strftime('%Y%m%d')
                if kwargs['start_date'] is None:
                    self.logger.error(f"任务 {self.name}: 全量模式需要 default_start_date，但未定义。")
                    failed_result = {"status": "failed", "message": "全量模式缺少默认开始日期", "rows": 0}
                    # Corrected Call
                    await self.post_execute(result=failed_result, stop_event=stop_event) 
                    return failed_result
                self.logger.info(f"任务 {self.name}: 全量模式实际日期范围: {kwargs['start_date']} 到 {kwargs['end_date']}")
            
            else:
                self.logger.info(f"任务 {self.name}: 使用提供的日期范围: {original_start_date} 到 {original_end_date}")
            # --- End Date Logic ---

            self.logger.info(f"任务 {self.name}: 准备调用 get_batch_list，使用最终参数: {kwargs}")
            batch_list = await self.get_batch_list(**kwargs)
            if not batch_list:
                self.logger.info(f"任务 {self.name}: get_batch_list 返回空列表。可能原因：日期范围无数据、API限制或子类逻辑。")
                no_data_result = {"status": "no_data", "message": "批处理列表为空", "rows": 0}
                # Corrected Call
                await self.post_execute(result=no_data_result, stop_event=stop_event) 
                return no_data_result

            total_batches = len(batch_list)
            self.logger.info(f"生成了 {total_batches} 个批处理任务")
            
            # --- Batch Processing Logic ---
            # (Assuming the internal batch processing logic using _process_single_batch is correct)
            # ... existing batch processing logic ...
            # It calculates total_rows, failed_batches, final_status, error_message

            # --- Start of section copied from previous attempt, verify correctness ---
            semaphore = asyncio.Semaphore(self.concurrent_limit or self.default_concurrent_limit)
            tasks = []
            # Removed tqdm progress bar logic for simplicity in fix
            # pbar = tqdm(total=len(batch_list), desc=f"任务 {self.name}", unit="批次")

            async def process_batch_wrapper(i, batch_params):
                async with semaphore:
                    if stop_event and stop_event.is_set():
                        # pbar.update(1) # Removed pbar
                        raise asyncio.CancelledError(f"批次 {i+1} 在开始前被取消")
                    result_rows = await self._process_single_batch(i, len(batch_list), batch_params, stop_event=stop_event)
                    # pbar.update(1) # Removed pbar
                    return result_rows # _process_single_batch returns int (rows) or raises

            for i, batch_params in enumerate(batch_list):
                tasks.append(process_batch_wrapper(i, batch_params))

                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            # pbar.close() # Removed pbar

            # Process batch_results
                for result in batch_results:
                    if isinstance(result, asyncio.CancelledError):
                        self.logger.warning(f"一个批次被取消: {result}")
                        final_status = "cancelled"
                        failed_batches += 1
                    elif isinstance(result, Exception):
                        self.logger.error(f"一个批次执行失败: {result}", exc_info=result)
                        failed_batches += 1
                        error_message += f"批次失败: {result}; "
                        final_status = "failed" if final_status != "cancelled" else "cancelled"
                    elif isinstance(result, int): # Success case returns rows (int)
                        if result >= 0: # Should be >= 0
                            total_rows += result
                        else: # Should not happen, but handle negative return as failure
                            self.logger.error(f"批次返回无效行数: {result}")
                            failed_batches += 1
                            error_message += f"批次返回无效行数 ({result}); "
                            final_status = "failed" if final_status != "cancelled" else "cancelled"
                else: # Unexpected return type
                    self.logger.error(f"批次返回未知类型: {type(result)} ({result})")
                    failed_batches += 1
                    error_message += f"批次返回未知类型 ({type(result)}); "
                    final_status = "failed" if final_status != "cancelled" else "cancelled"
            
            if failed_batches > 0:
                if final_status != "cancelled":
                    final_status = "partial_success" if total_rows > 0 else "failed"
                self.logger.warning(f"任务执行完成，但有 {failed_batches} 个批次处理失败或被取消。")
                
            if final_status == "partial_success":
                error_message = f"完成但有 {failed_batches} 个批次失败。 " + error_message
            elif final_status == "failed":
                error_message = f"任务失败，共 {failed_batches} 个批次失败。 " + error_message
            elif final_status == "cancelled":
                error_message = f"任务被取消，有 {failed_batches} 个批次未完成或被取消。"
            # --- End of section copied from previous attempt ---

        except asyncio.CancelledError:
            self.logger.warning(f"任务 {self.name} 在执行过程中被取消。")
            final_status = "cancelled"
            error_message = "任务被用户取消"
            # Corrected Call in CancelledError block
            cancelled_result = {
                "status": final_status,
                "message": error_message,
                "rows": total_rows, # Rows processed before cancellation
                "failed_batches": failed_batches, # Batches failed before cancellation
                "task": self.name
            }
            await self.post_execute(result=cancelled_result, stop_event=stop_event)
            return cancelled_result # Return the result dict
            
        except Exception as e:
            self.logger.error(f"任务 {self.name} 执行过程中发生未处理的严重错误", exc_info=True)
            final_status = "failed"
            error_message = f"严重错误: {type(e).__name__} - {str(e)}"
            total_rows = 0 # Reset rows on catastrophic failure
            failed_batches = len(batch_list) if batch_list else 0 # Mark all as failed if list exists
            # Corrected Call in generic Exception block
            failed_result = {
                "status": final_status,
                "message": error_message,
                "rows": total_rows, 
                "failed_batches": failed_batches, 
                "task": self.name
            }
            await self.post_execute(result=failed_result, stop_event=stop_event)
            return failed_result # Return the result dict
        
        finally:
            # This block executes regardless of exceptions in try
            # Construct the final result based on state determined in try/except
            final_result_dict = {
                "status": final_status, # Determined by try/except logic
                "message": error_message.strip(),
                "rows": total_rows,
                "failed_batches": failed_batches,
                "task": self.name
            }
            # Corrected Call in finally block
            await self.post_execute(result=final_result_dict, stop_event=stop_event)
            self.logger.info(f"任务 {self.name} 最终处理完成，状态: {final_status}, 总行数: {total_rows}, 失败/取消批次数: {failed_batches}")

        # Return the final result dictionary constructed in 'finally' 
        # (Note: except blocks now also return their constructed dicts, 
        # so this return might only be reached if no exception occurred)
        return final_result_dict

    async def _process_single_batch(self, batch_index, total_batches, batch_params, stop_event: Optional[asyncio.Event] = None):
        """处理单个批次的数据：获取、处理、验证、保存。
        
        获取、处理、验证并保存单个批次的数据。
        针对获取和保存步骤实现了重试机制。
        
        Args:
            batch_index: 批次索引
            total_batches: 总批次数
            batch_params: 批次参数
            stop_event (asyncio.Event, optional): 事件对象，用于外部请求停止任务。
            
        Returns:
            int: 处理的行数，如果最终处理失败则返回 0
        """
        batch_log_prefix = f"批次 {batch_index+1}/{total_batches}"
        batch_data = None
        processed_data = None
        rows = 0

        # --- 在处理开始前检查停止事件 ---
        if stop_event and stop_event.is_set():
            self.logger.info(f"任务 {self.name}: 在处理批次 {batch_index + 1}/{total_batches} 前检测到停止信号，跳过。")
            raise asyncio.CancelledError("Task cancelled by stop event before processing batch") # Raise cancellation

        batch_start_time = time.time()
        processed_rows_count = 0
        batch_status = "failed" # Default to failed unless success
        error_info = None

        # 1. 获取数据（带重试）
        for attempt in range(self.max_retries + 1):
            try:
                if attempt > 0: # 如果是重试
                    self.logger.info(f"{batch_log_prefix}: 重试获取数据 (尝试 {attempt+1}/{self.max_retries+1})")
                else:
                    self.logger.info(f"{batch_log_prefix}: 开始获取数据")

                batch_data = await self.fetch_batch(batch_params)
                # 如果成功获取数据（即使是空的），则跳出重试循环
                self.logger.info(f"{batch_log_prefix}: 获取数据成功")
                break
            except Exception as e:
                self.logger.warning(f"{batch_log_prefix}: 获取数据时发生错误 (尝试 {attempt+1}/{self.max_retries+1}): {str(e)}")
                if attempt >= self.max_retries:
                    self.logger.error(f"{batch_log_prefix}: 获取数据失败，已达最大重试次数。")
                    return 0 # 获取数据最终失败
                # 等待后重试
                await asyncio.sleep(self.retry_delay)
                continue # 继续下一次尝试

        # 如果获取的数据为空，直接返回
        if batch_data is None or batch_data.empty:
            self.logger.info(f"{batch_log_prefix}: 没有获取到数据或获取最终失败")
            return 0

        # 2. 处理数据 (通常不重试逻辑错误)
        try:
            self.logger.info(f"{batch_log_prefix}: 处理 {len(batch_data)} 行数据")
            processed_data = await self.process_data(batch_data)
            
            # 增强型错误检测：检查返回值是否是协程对象而不是DataFrame
            import inspect
            if inspect.iscoroutine(processed_data):
                self.logger.warning(f"{batch_log_prefix}: process_data返回了协程对象而不是DataFrame，尝试await该协程")
                try:
                    # 尝试await协程以获取实际的DataFrame
                    processed_data = await processed_data
                except Exception as co_err:
                    self.logger.error(f"{batch_log_prefix}: 尝试await协程时出错: {str(co_err)}")
                    return 0  # 处理失败
            
            # 再次检查是否获得了有效的DataFrame
            if not isinstance(processed_data, pd.DataFrame):
                self.logger.error(f"{batch_log_prefix}: 处理数据后没有得到有效的DataFrame，而是: {type(processed_data)}")
                return 0  # 处理失败
                
        except Exception as e:
            self.logger.error(f"{batch_log_prefix}: 处理数据时发生错误: {str(e)}")
            return 0 # 处理数据失败

        # 3. 验证数据 (通常不重试逻辑错误)
        validated_data = None # 初始化变量
        try:
            self.logger.info(f"{batch_log_prefix}: 验证数据")
            validated_data = await self.validate_data(processed_data) # 获取验证和过滤后的数据
            
            # 同样检查validate_data是否返回了协程
            import inspect
            if inspect.iscoroutine(validated_data):
                self.logger.warning(f"{batch_log_prefix}: validate_data返回了协程对象而不是DataFrame，尝试await该协程")
                try:
                    validated_data = await validated_data
                except Exception as co_err:
                    self.logger.error(f"{batch_log_prefix}: 尝试await validate_data协程时出错: {str(co_err)}")
                    return 0  # 验证失败
            
            # 检查返回的是否是DataFrame以及是否为空
            if not isinstance(validated_data, pd.DataFrame) or validated_data.empty:
                # 如果不是DataFrame或为空，则认为验证失败或无数据
                self.logger.error(f"{batch_log_prefix}: 数据验证失败或所有数据均被过滤")
                return 0 # 验证失败或没有数据可保存
            
            if isinstance(validated_data, pd.DataFrame) and not validated_data.empty and hasattr(self, 'primary_keys') and self.primary_keys:
                original_count = len(validated_data)
                # 使用 inplace=True 直接修改 DataFrame
                validated_data.drop_duplicates(subset=self.primary_keys, keep='last', inplace=True)
                deduped_count = len(validated_data)
                if deduped_count < original_count:
                    # 使用 DEBUG 级别，避免过多干扰 INFO 日志
                    self.logger.debug(f"{batch_log_prefix}: 基于主键 {self.primary_keys} 去重，移除了 {original_count - deduped_count} 行重复数据。")
                    
        except Exception as e:
            self.logger.error(f"{batch_log_prefix}: 验证数据时发生错误: {str(e)}")
            return 0 # 验证过程中出错

        # 4. 保存数据（带重试） - 使用验证后的数据 validated_data
        for attempt in range(self.max_retries + 1):
            try:
                if attempt > 0:
                    self.logger.info(f"{batch_log_prefix}: 重试保存数据 (尝试 {attempt+1}/{self.max_retries+1})")
                else:
                    self.logger.info(f"{batch_log_prefix}: 保存数据到表 {self.table_name}")

                # 确保 validated_data 是 DataFrame 且不为空
                if not isinstance(validated_data, pd.DataFrame) or validated_data.empty:
                    self.logger.info(f"{batch_log_prefix}: 没有有效数据需要保存。")
                    return 0 # 没有数据保存，返回0行

                result_dict = await self.save_data(validated_data) # save_data returns a dict
                if isinstance(result_dict, dict) and result_dict.get("status") == "success":
                    rows = result_dict.get('rows', 0) # 从字典中正确提取行数
                    if not isinstance(rows, int): # 添加额外的类型检查
                        self.logger.error(f"{batch_log_prefix}: save_data 返回的 'rows' 不是整数: {repr(rows)}")
                        rows = 0 # 视为失败
                        return 0 # 明确返回失败
                    else:
                        self.logger.info(f"{batch_log_prefix}: 已保存 {rows} 行数据")
                        return rows # 保存成功，返回整数行数
                else:
                    # save_data 返回了错误状态或非预期格式
                    error_info = repr(result_dict) if isinstance(result_dict, dict) else f"非字典类型 ({type(result_dict).__name__})"
                    self.logger.error(f"{batch_log_prefix}: 保存数据失败或 save_data 返回非成功状态。Result: {error_info}")
                    # 不需要 break，因为 return 0 会结束循环
                    return 0 # 保存失败
            except Exception as e:
                self.logger.warning(f"{batch_log_prefix}: 保存数据时发生错误 (尝试 {attempt+1}/{self.max_retries+1}): {str(e)}")
                if attempt >= self.max_retries:
                    self.logger.error(f"{batch_log_prefix}: 保存数据失败，已达最大重试次数。")
                    return 0 # 保存数据最终失败
                # 等待后重试
                await asyncio.sleep(self.retry_delay)
                continue # 继续下一次尝试

        batch_end_time = time.time()
        duration = batch_end_time - batch_start_time
        self.logger.debug(f"{batch_log_prefix}: 批次 {batch_index + 1}/{total_batches} 处理完成。状态: {batch_status}, 行数: {processed_rows_count}, 耗时: {duration:.2f}s")
        # 返回处理的行数，如果失败则返回0或抛出异常
        # Let's return row count on success, 0 on handled failure, raise on critical/cancellation
        if batch_status == "success":
            return processed_rows_count
        elif batch_status == "no_data" or batch_status == "skipped":
            return 0 # No rows processed, but not a critical failure
        else:
            # We already logged the error inside the try block
            # Raise an exception or return 0 to indicate handled failure?
            # Returning 0 might be less disruptive for the overall task execution summary.
            # Critical errors should raise exceptions though.
            # Let's return 0 for now for non-critical failures within a batch.
            # Cancellation errors are raised.
            return 0 
            
    async def fetch_batch(self, batch_params: Dict) -> pd.DataFrame:
        """获取单批次数据
        
        该方法负责获取单个批次的数据，通常通过Tushare API调用实现。
        
        Args:
            batch_params (Dict): 批次参数
            
        Returns:
            pd.DataFrame: 批次数据
        """
        # 准备API调用参数
        api_params = self.prepare_params(batch_params)
        
        try:
            # 调用Tushare API
            df = await self.api.query(
                self.api_name, 
                params=api_params, 
                fields=self.fields,
                page_size=self.page_size # Use configured page size
            )
            return df
        except Exception as e:
            self.logger.error(f"获取批次数据失败: {str(e)}，参数: {api_params}")
            return pd.DataFrame() # 返回空DataFrame
        
    def _apply_column_mapping(self, data):
        """应用列名映射
        
        将原始列名映射为目标列名，只处理数据中存在的列。
        
        Args:
            data (DataFrame): 原始数据
            
        Returns:
            DataFrame: 应用列名映射后的数据
        """
        if not hasattr(self, 'column_mapping') or not self.column_mapping:
            return data
            
        # 检查映射前的列是否存在
        missing_original_cols = [orig_col for orig_col in self.column_mapping.keys() 
                                if orig_col not in data.columns]
        if missing_original_cols:
            self.logger.warning(f"列名映射失败：原始数据中缺少以下列: {missing_original_cols}")
        
        # 执行重命名，只重命名数据中存在的列
        rename_map = {k: v for k, v in self.column_mapping.items() if k in data.columns}
        if rename_map:
            data.rename(columns=rename_map, inplace=True)
            self.logger.info(f"已应用列名映射: {rename_map}")
            
        return data

    def _process_date_column(self, data):
        """处理日期列
        
        将日期列转换为标准的日期时间格式，并移除无效日期的行。
        
        Args:
            data (DataFrame): 原始数据
            
        Returns:
            DataFrame: 处理日期后的数据，如果日期列不存在则返回原始数据
        """
        if not self.date_column or self.date_column not in data.columns:
            if self.date_column:
                self.logger.warning(f"指定的日期列 '{self.date_column}' 不在数据中，无法进行日期格式转换。")
            return data
            
        try:
            # 如果是字符串格式（如'20210101'），转换为日期对象
            data[self.date_column] = pd.to_datetime(data[self.date_column], format='%Y%m%d', errors='coerce')
            # 删除转换失败的行 (NaT)
            original_count = len(data)
            data.dropna(subset=[self.date_column], inplace=True)
            if len(data) < original_count:
                self.logger.warning(f"移除了 {original_count - len(data)} 行，因为日期列 '{self.date_column}' 格式无效。")
        except Exception as e:
            self.logger.warning(f"日期列 {self.date_column} 格式转换时发生错误: {str(e)}")
            
        return data

    def _sort_data(self, data):
        """对数据进行排序
        
        根据日期列和主键列对数据进行排序。
        
        Args:
            data (DataFrame): 原始数据
            
        Returns:
            DataFrame: 排序后的数据
        """
        # 构建排序键列表
        sort_keys = []
        if self.date_column and self.date_column in data.columns:
            sort_keys.append(self.date_column)

        if self.primary_keys:
            other_keys = [pk for pk in self.primary_keys if pk != self.date_column and pk in data.columns]
            sort_keys.extend(other_keys)
            
        # 如果没有有效的排序键，则返回原始数据
        if not sort_keys:
            return data
            
        # 检查所有排序键是否都在数据中
        missing_keys = [key for key in sort_keys if key not in data.columns]
        if missing_keys:
            self.logger.warning(f"排序失败：数据中缺少以下排序键: {missing_keys}")
            # 从排序键列表中移除缺失的键
            sort_keys = [key for key in sort_keys if key not in missing_keys]
            if not sort_keys:
                return data
                
        try:
            # 执行排序
            data = data.sort_values(by=sort_keys)
            self.logger.info(f"数据已按以下键排序: {sort_keys}")
        except Exception as e:
            self.logger.warning(f"排序时发生错误: {str(e)}")
            
        return data

    def _apply_transformations(self, data):
        """应用数据转换
        
        根据转换规则对指定列应用转换函数。
        增加了对None/NaN值的安全处理。
        
        Args:
            data (DataFrame): 原始数据
            
        Returns:
            DataFrame: 应用转换后的数据
        """
        if not hasattr(self, 'transformations') or not self.transformations:
            return data
            
        for column, transform_func in self.transformations.items():
            if column in data.columns:
                try:
                    # 确保处理前列中没有Python原生的None，统一使用np.nan
                    # （虽然理论上在process_data开始时已经处理，但这里再确认一次更保险）
                    if data[column].dtype == 'object':
                        data[column] = data[column].fillna(np.nan)
                    
                    # 定义一个安全的转换函数，处理np.nan值
                    def safe_transform(x):
                        if pd.isna(x):
                            return np.nan  # 保持np.nan
                        try:
                            return transform_func(x) # 应用原始转换
                        except Exception as e:
                            # 记录详细错误，但只记录一次或采样记录，避免日志爆炸
                            # 这里暂时只记录警告，具体策略可以后续优化
                            # self.logger.warning(f"转换值 '{x}' (类型: {type(x)}) 到列 '{column}' 时失败: {str(e)}")
                            return np.nan # 转换失败时返回np.nan

                    # 应用安全转换
                    original_dtype = data[column].dtype
                    data[column] = data[column].apply(safe_transform)
                    
                    # 尝试恢复原始数据类型（如果转换后类型改变且非object）
                    # 例如，如果原来是float，转换后可能变成object（因为混入了nan），尝试转回float
                    try:
                        if data[column].dtype == 'object' and original_dtype != 'object':
                            data[column] = pd.to_numeric(data[column], errors='coerce')
                    except Exception as type_e:
                        self.logger.debug(f"尝试恢复列 '{column}' 类型失败: {str(type_e)}")
                        
                except Exception as e:
                    self.logger.error(f"处理列 '{column}' 的转换时发生意外错误: {str(e)}", exc_info=True)
                    # 如果整个列的处理失败，可以选择将该列填充为NaN，而不是中断
                    # data[column] = np.nan 
                    # 暂时保持原样，让错误暴露
                    
        return data
        
    async def process_data(self, data):
        """处理从Tushare获取的数据
        
        包括列名映射、日期处理、数据排序和数据转换。
        """
        if data is None or data.empty:
            self.logger.warning("没有数据需要处理")
            return pd.DataFrame()
            
        # 1. 应用列名映射
        data = self._apply_column_mapping(data)
        
        # 2. 处理主要的 date_column (如果定义)
        data = self._process_date_column(data) 
        
        # 3. 应用通用数据类型转换 (from transformations dict)
        data = self._apply_transformations(data)

        # 4. 显式处理 schema 中定义的其他 DATE/TIMESTAMP 列
        if hasattr(self, 'schema') and self.schema:
            date_columns_to_process = []
            # 识别需要处理的日期列
            for col_name, col_def in self.schema.items():
                col_type = col_def.get('type', '').upper() if isinstance(col_def, dict) else str(col_def).upper()
                if ('DATE' in col_type or 'TIMESTAMP' in col_type) and col_name in data.columns and col_name != self.date_column:
                    # 仅处理尚未是日期时间类型的列
                    if data[col_name].dtype == 'object' or pd.api.types.is_string_dtype(data[col_name]):
                        date_columns_to_process.append(col_name)
            
            # 批量处理识别出的日期列
            if date_columns_to_process:
                self.logger.info(f"转换以下列为日期时间格式 (YYYYMMDD): {', '.join(date_columns_to_process)}")
                original_count = len(data)
                for col_name in date_columns_to_process:
                    # 尝试使用 YYYYMMDD 格式转换
                    converted_col = pd.to_datetime(data[col_name], format='%Y%m%d', errors='coerce')
                    
                    # 检查失败率，如果过高则尝试通用解析
                    # if converted_col.isna().sum() > len(data) * 0.5: # 可选：添加失败率高的回退逻辑
                    #    self.logger.warning(f"列 '{col_name}': YYYYMMDD 转换失败率高，尝试通用解析...")
                    #    converted_col_alt = pd.to_datetime(data[col_name], errors='coerce')
                    #    if converted_col_alt.isna().sum() < converted_col.isna().sum():
                    #        converted_col = converted_col_alt
                        
                    data[col_name] = converted_col
                
                # 一次性移除所有日期转换失败的行
                # data.dropna(subset=date_columns_to_process, inplace=True) # <-- 注释掉这一行
                if len(data) < original_count:
                    # 这个警告现在可能需要调整，因为它可能不再准确反映因为dropna而移除的行
                    # 或者在转换失败时(产生NaT)记录更具体的警告信息
                    self.logger.warning(f"处理日期列: 移除了 {original_count - len(data)} 行 (注意：移除逻辑已修改)。") # 调整警告信息

        # 5. 对数据进行排序 (应该在所有转换后进行)
        data = self._sort_data(data)
        
        # 6. 不再调用 super().process_data() 因为 TushareTask 应该处理其特定需求
        # return super().process_data(data) 
        return data
        
    async def validate_data(self, data):
        """验证从Tushare获取的数据
        
        除了基类的数据验证外，还添加了Tushare特有的数据验证。
        不符合验证规则的数据会被过滤掉，而不是整批拒绝。
        
        Args:
            data (pd.DataFrame): 待验证的数据
            
        Returns:
            pd.DataFrame: 验证后的数据（已过滤掉不符合规则的数据）
        """
        if data is None or data.empty:
            self.logger.warning("没有数据需要验证")
            return data
            
        # 记录原始数据行数
        original_count = len(data)
        valid_mask = pd.Series(True, index=data.index)
        
        # 应用自定义验证规则
        if hasattr(self, 'validations') and self.validations:
            for validation_func in self.validations:
                try:
                    # 获取每行数据的验证结果
                    validation_result = validation_func(data[valid_mask])
                    if isinstance(validation_result, pd.Series):
                        # 如果验证函数返回Series，直接使用
                        valid_mask &= validation_result
                    else:
                        # 如果验证函数返回单个布尔值，应用到所有行
                        if not validation_result:
                            self.logger.warning(f"整批数据未通过验证: {validation_func.__name__ if hasattr(validation_func, '__name__') else '未命名验证'}")
                            valid_mask &= False
                except Exception as e:
                    self.logger.warning(f"执行验证时发生错误: {str(e)}")
                    # 发生错误时，将对应的数据标记为无效
                    valid_mask &= False
        # 应用验证结果
        filtered_data = data[valid_mask].copy()
        filtered_count = len(filtered_data)
        
        if filtered_count < original_count:
            self.logger.warning(f"数据验证: 过滤掉 {original_count - filtered_count} 行不符合规则的数据")
            
        return filtered_data
        
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