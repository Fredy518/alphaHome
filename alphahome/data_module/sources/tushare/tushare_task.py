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
            **kwargs: 额外参数，将传递给get_batch_list和prepare_params方法
            
        Returns:
            Dict: 任务执行结果，包含状态和影响的行数
        """
        self.logger.info(f"开始执行任务: {self.name}")
        
        # <<< Get the callback >>>
        progress_callback = kwargs.get('progress_callback')
        # Removed DEBUG log for callback existence

        try:
            await self.pre_execute()

            # 依赖任务检查已移除
            # 直接进行数据获取
            
            # 获取批处理参数列表
            self.logger.info(f"获取数据，参数: {kwargs}")
            batch_list = await self.get_batch_list(**kwargs)
            if not batch_list:
                self.logger.info("批处理参数列表为空，无法获取数据")
                return {"status": "no_data", "rows": 0}

            total_batches = len(batch_list)
            self.logger.info(f"生成了 {total_batches} 个批处理任务") # Keep INFO log

            # 处理并保存每个批次的数据
            total_rows = 0
            failed_batches = 0 # 记录失败的批次数
            first_error = None # 记录第一个遇到的异常信息以备返回
            successful_batches_count = 0 # <--- ADDED: Initialize success counter
            
            # 获取并发限制参数，优先使用传入的参数，其次使用实例属性
            concurrent_limit = kwargs.get('concurrent_limit', self.concurrent_limit)
            show_progress = kwargs.get('show_progress', False)
            progress_desc = kwargs.get('progress_desc', f"执行任务: {self.name}")
            
            if concurrent_limit > 1:
                # 并发处理批次
                self.logger.info(f"使用并发模式处理批次，并发数: {concurrent_limit}")
                semaphore = asyncio.Semaphore(concurrent_limit)
                success_counter_lock = asyncio.Lock() # <--- ADDED: Lock for counter
                pbar = None # 初始化进度条变量
                
                # 如果需要显示进度，则创建 tqdm 实例
                if show_progress:
                    pbar = tqdm(total=len(batch_list), desc=progress_desc, ascii=True, position=0, leave=True)
                
                # 修改内部函数以接受并更新 pbar 和调用回调
                async def process_batch(i, batch_params, pbar_instance, cb):
                    nonlocal successful_batches_count # <--- ADDED: Access outer counter
                    processed_rows = 0 # Default value
                    try:
                        async with semaphore:
                            processed_rows = await self._process_single_batch(i, total_batches, batch_params)
                            # Removed DEBUG log for _process_single_batch return value
                            if processed_rows > 0: # <--- MODIFIED: Check for success
                                async with success_counter_lock: # <--- ADDED: Lock
                                    successful_batches_count += 1 # <--- ADDED: Increment on success
                                    # Removed DEBUG log for counter increment
                            if processed_rows == 0:
                                return None # Still return None on failure
                            return processed_rows # Return rows on success
                    finally:
                        # 更新控制台进度条 (如果存在)
                        if pbar_instance:
                            pbar_instance.update(1)

                        # --- 调用 GUI 进度回调 (移出 if pbar_instance) ---
                        # 读取成功计数
                        current_successful_count = 0
                        async with success_counter_lock:
                            current_successful_count = successful_batches_count
                        total_count = total_batches

                        # 计算百分比
                        percentage = int((current_successful_count / total_count) * 100) if total_count > 0 else 0
                        # Removed DEBUG log before calling callback

                        # 调用回调 (如果存在且总数大于0)
                        if cb and total_count > 0:
                            try:
                                # Removed DEBUG log before calling cb
                                if asyncio.iscoroutinefunction(cb):
                                    await cb(self.name, f"{percentage}%")
                                else:
                                    cb(self.name, f"{percentage}%")
                                # Removed DEBUG log after calling cb
                            except Exception as cb_err:
                                self.logger.error(f"执行进度回调时出错: {cb_err}", exc_info=False)

                        # 添加一个微小的延迟 (保持控制台流畅性，对GUI回调无直接影响)
                        await asyncio.sleep(0.01)
                
                # 创建所有批次的任务, 传递 pbar 实例和回调
                tasks = [process_batch(i, batch_params, pbar, progress_callback) for i, batch_params in enumerate(batch_list)]
                
                # 使用 asyncio.gather 等待所有任务完成 (不再使用 tqdm.gather)
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # 如果创建了进度条，确保关闭它
                if pbar:
                    pbar.close()
                
                # 处理结果
                for result in batch_results:
                    if isinstance(result, Exception):
                        # gather 捕获的异常通常表示 process_batch 内部未处理的异常
                        self.logger.error(f"批次处理中发生未捕获异常: {str(result)}")
                        failed_batches += 1
                        # 记录第一个遇到的异常信息以备返回
                        if first_error is None:
                            first_error = str(result)
                    elif result is None: # 我们用 None 表示批次处理失败（重试后）
                        failed_batches += 1
                        # 记录一个通用错误信息（因为具体错误可能已在 _process_single_batch 中记录）
                        if first_error is None:
                            first_error = "批次处理失败 (重试后仍失败)"
                    elif isinstance(result, int): # 成功处理的行数
                        total_rows += result
            
            else:
                # 串行处理批次
                progress_iterator = range(len(batch_list))
                if show_progress:
                    # Keep using tqdm for the iterator itself for console
                    progress_iterator = tqdm(range(len(batch_list)), desc=progress_desc, ascii=True, position=0, leave=True)

                for i in progress_iterator: # i is now the index 0, 1, 2...
                    batch_params = batch_list[i]
                    rows = await self._process_single_batch(i, total_batches, batch_params)
                    # Removed DEBUG log for _process_single_batch return value
                    if rows > 0:
                        total_rows += rows
                        successful_batches_count += 1 # <--- ADDED: Increment on success
                        # Removed DEBUG log for counter increment
                    else: # rows == 0 表示失败
                        failed_batches += 1

                    # <<< Call the progress callback for serial execution >>>
                    if progress_callback:
                        total_count = total_batches # <--- MODIFIED: Use stored total_batches
                        percentage = int((successful_batches_count / total_count) * 100) if total_count > 0 else 0
                        # Removed DEBUG log before calling callback
                        if total_count > 0:
                            try:
                                # Removed DEBUG log before calling callback
                                # Ensure callback is awaited if it's a coroutine
                                if asyncio.iscoroutinefunction(progress_callback):
                                     await progress_callback(self.name, f"{percentage}%")
                                else:
                                     progress_callback(self.name, f"{percentage}%") # Allow non-async callback
                                # Removed DEBUG log after calling callback
                            except Exception as cb_err:
                                self.logger.error(f"执行进度回调时出错: {cb_err}", exc_info=False)
            
            # 后处理
            final_status = "success"
            error_message = None # 初始化错误信息
            if failed_batches > 0:
                final_status = "failure" if failed_batches == len(batch_list) else "partial_success"
                self.logger.warning(f"任务执行完成，但有 {failed_batches} 个批次处理失败。")
                error_message = first_error # 使用记录的第一个错误信息

            result_dict = {
                "status": final_status,
                "rows": total_rows,
                "failed_batches": failed_batches
            }
            # 如果有错误信息，添加到字典中
            if error_message:
                result_dict['error'] = error_message 

            await self.post_execute(result_dict)
            self.logger.info(f"任务执行完成: {result_dict}")
            return result_dict
        except Exception as e:
            self.logger.error(f"任务执行失败: {str(e)}", exc_info=True)
            return self.handle_error(e)
            
    async def _process_single_batch(self, batch_index, total_batches, batch_params):
        """处理单个批次的数据（包含重试逻辑）
        
        获取、处理、验证并保存单个批次的数据。
        针对获取和保存步骤实现了重试机制。
        
        Args:
            batch_index: 批次索引
            total_batches: 总批次数
            batch_params: 批次参数
            
        Returns:
            int: 处理的行数，如果最终处理失败则返回 0
        """
        batch_log_prefix = f"批次 {batch_index+1}/{total_batches}"
        batch_data = None
        processed_data = None
        rows = 0

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

                result = await self.save_data(validated_data) # 使用 validated_data
                rows = result.get('rows', 0)
                self.logger.info(f"{batch_log_prefix}: 已保存 {rows} 行数据")
                return rows # 保存成功，返回行数
            except Exception as e:
                self.logger.warning(f"{batch_log_prefix}: 保存数据时发生错误 (尝试 {attempt+1}/{self.max_retries+1}): {str(e)}")
                if attempt >= self.max_retries:
                    self.logger.error(f"{batch_log_prefix}: 保存数据失败，已达最大重试次数。")
                    return 0 # 保存数据最终失败
                # 等待后重试
                await asyncio.sleep(self.retry_delay)
                continue # 继续下一次尝试

        return 0 # 如果代码能执行到这里，说明保存逻辑有问题，返回 0
            
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