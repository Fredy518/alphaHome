#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
基于 Tushare API 的数据任务基类

核心设计：
1. 基类只提供最基础的数据获取功能
2. 具体的参数映射和批处理策略由子类决定

重要说明：
如果子类需要自定义数据处理逻辑，可以重写 process_data 方法。
在重写时，确保:
1. 方法签名为 `def process_data(self, data)` - 注意这是同步方法
2. 必须调用 `super().process_data(data)` 确保基础处理逻辑执行
3. 返回最终处理的 DataFrame

例如:
```python
def process_data(self, data):
    # 首先调用父类的处理方法
    data = super().process_data(data)

    # 自定义处理逻辑
    # ...

    return data
```
"""

import abc
import asyncio
import logging  # 确保导入 logging
import math  # 引入 math 用于 ceil 计算
import os
import time
from datetime import datetime, timedelta, date
from typing import Any, Callable, Dict, List, Optional

import aiohttp
import numpy as np  # 导入 numpy
import pandas as pd
from tqdm.asyncio import tqdm

from ....common.task_system.base_task import BaseTask as Task
from .tushare_api import TushareAPI
from .tushare_batch_processor import TushareBatchProcessor
from .tushare_data_transformer import TushareDataTransformer


class TushareTask(Task):
    """基于 Tushare API 的数据任务基类

    核心设计：
    1. 基类只提供最基础的数据获取功能
    2. 具体的参数映射和批处理策略由子类决定

    重要说明：
    如果子类需要自定义数据处理逻辑，可以重写 process_data 方法。
    在重写时，确保:
    1. 方法签名为 `def process_data(self, data)` - 注意这是同步方法
    2. 必须调用 `super().process_data(data)` 确保基础处理逻辑执行
    3. 返回最终处理的 DataFrame

    例如:
    ```python
    def process_data(self, data):
        # 首先调用父类的处理方法
        data = super().process_data(data)

        # 自定义处理逻辑
        # ...

        return data
    ```
    """

    # 任务类型标识
    task_type = "fetch"
    
    # 数据源标识
    data_source = "tushare"

    # 默认配置
    default_concurrent_limit = 5  # 默认并发限制
    default_page_size = 5000  # 默认每页数据量
    default_max_retries = 3  # 默认最大重试次数
    default_retry_delay = 2  # 默认重试延迟（秒）

    api_name: Optional[str] = None  # Tushare API名称，子类必须定义
    fields: Optional[List[str]] = None  # 需要获取的字段列表，子类必须定义
    timestamp_column_name: Optional[str] = (
        "update_time"  # 时间戳列名，默认为 'update_time'，None表示不使用
    )
    single_batch = False  # 单批次处理模式，适用于数据量小的任务，例如宏观数据
    
    # 列名映射，子类可定义
    column_mapping: Optional[Dict[str, str]] = None

    # 抽象属性，子类必须定义
    schema_def: Optional[Any] = None  # 表结构定义
    default_start_date: Optional[str] = None  # 默认开始日期

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
            api_token = os.environ.get("TUSHARE_TOKEN")
            if not api_token:
                raise ValueError(
                    "未提供Tushare API令牌，请通过参数传入或设置TUSHARE_TOKEN环境变量"
                )

        # 创建Tushare API客户端
        self.api = api or TushareAPI(api_token)

        # 初始化配置
        self.concurrent_limit = None
        self.page_size = None
        self.max_retries = self.default_max_retries
        self.retry_delay = self.default_retry_delay
        self.task_specific_config = {}  # 存储原始任务配置

        # 初始化 Helper Classes
        self.data_transformer = TushareDataTransformer(self)
        self.batch_processor = TushareBatchProcessor(self, self.data_transformer)

    def set_config(self, task_config: Dict):
        """应用来自配置文件的设置，覆盖代码默认值"""
        self._apply_config(task_config)

    def _apply_config(self, task_config: Dict):
        """合并代码默认值和配置文件设置"""
        cls = type(self)  # 获取实际的子类类型

        # 从子类或基类获取代码默认值
        code_default_limit = getattr(
            cls, "default_concurrent_limit", TushareTask.default_concurrent_limit
        )
        code_default_page_size = getattr(
            cls, "default_page_size", TushareTask.default_page_size
        )

        # 从 task_config (来自 config.json) 获取值，如果不存在则使用代码默认值
        self.concurrent_limit = task_config.get("concurrent_limit", code_default_limit)
        self.page_size = task_config.get("page_size", code_default_page_size)

        # 添加类型检查和转换，确保是整数
        try:
            self.concurrent_limit = int(self.concurrent_limit)
        except (ValueError, TypeError):
            self.logger.warning(
                f"配置中的 concurrent_limit \"{task_config.get('concurrent_limit')}\" 无效，将使用代码默认值 {code_default_limit}"
            )
            self.concurrent_limit = code_default_limit

        try:
            self.page_size = int(self.page_size)
        except (ValueError, TypeError):
            self.logger.warning(
                f"配置中的 page_size \"{task_config.get('page_size')}\" 无效，将使用代码默认值 {code_default_page_size}"
            )
            self.page_size = code_default_page_size

        # 确保存储为整数后记录日志
        self.logger.info(
            f"任务 {self.name}: 应用配置 concurrent_limit={self.concurrent_limit}, page_size={self.page_size}"
        )

        # 存储原始配置字典以供子类可能需要访问其他特定键
        self.task_specific_config = task_config

        # 设置重试配置
        if "max_retries" in task_config:
            self.max_retries = task_config["max_retries"]
            self.logger.debug(f"设置最大重试次数: {self.max_retries}")
        if "retry_delay" in task_config:
            self.retry_delay = task_config["retry_delay"]
            self.logger.debug(f"设置重试延迟: {self.retry_delay}s")

    async def get_latest_date(self) -> Optional[date]:
        """获取当前任务对应表中的最新日期。

        Returns:
            Optional[datetime.date]: 最新日期，如果表不存在或没有数据则返回 None。
        """
        if not hasattr(self, "db") or not self.db:
            self.logger.error(
                "数据库管理器未初始化 (self.db is missing)，无法获取最新日期。"
            )
            return None

        if not self.table_name or not self.date_column:
            self.logger.error(
                "任务未定义 table_name 或 date_column，无法获取最新日期。"
            )
            return None

        try:
            # 使用 db_manager 的 table_exists 方法检查表是否存在
            query = (
                f"SELECT MAX({self.date_column}) as latest_date FROM {self.get_full_table_name()}"
            )
            result = await self.db.fetch_one(query)

            if result and result["latest_date"]:
                latest_date = result["latest_date"]
                from datetime import date, datetime

                if isinstance(latest_date, datetime):
                    return latest_date.date()  # 如果是 datetime 对象，返回 date 部分
                elif isinstance(latest_date, date):
                    return latest_date  # 如果已经是 date 对象，直接返回
                else:
                    self.logger.warning(
                        f"从数据库获取的最新日期类型未知: {type(latest_date)}"
                    )
                    # 尝试解析常见日期格式
                    try:
                        return datetime.strptime(str(latest_date), "%Y-%m-%d").date()
                    except ValueError:
                        try:
                            return datetime.strptime(str(latest_date), "%Y%m%d").date()
                        except ValueError:
                            self.logger.error(f"无法将 {latest_date} 解析为日期。")
                            return None
            else:
                self.logger.info(f"表 '{self.table_name}' 中没有找到日期数据。")
                return None
        except Exception as e:
            # 特别处理表不存在的情况
            # Specific check for asyncpg.exceptions.UndefinedTableError
            if isinstance(
                e,
                getattr(  # type: ignore
                    __import__("asyncpg.exceptions", fromlist=["UndefinedTableError"]),  # type: ignore
                    "UndefinedTableError",
                    None,
                ),
            ) or (
                "does not exist" in str(e).lower()
                or ("relation" in str(e).lower() and "does not exist" in str(e).lower())
            ):
                self.logger.info(
                    f"表 '{self.table_name}' 不存在 (在 get_latest_date 中检测到)。"
                )
                return None
            else:
                self.logger.error(f"查询最新日期时出错: {e}", exc_info=True)
                return None

    async def _ensure_table_exists(self):
        """确保当前任务的表在数据库中存在，如果不存在则尝试创建。"""
        if not hasattr(self, "db") or not self.db:
            self.logger.error(
                "数据库管理器未初始化 (self.db is missing)，无法确保表存在。"
            )
            return
        if not self.table_name:
            self.logger.error("任务未定义 table_name，无法确保表存在。")
            return
        if not hasattr(self, "schema_def") or not self.schema_def:
            self.logger.warning(
                f"任务 {self.name} 未定义 schema_def，无法自动创建表 {self.table_name}。"
            )
            return

        try:
            # 使用 db_manager 的 table_exists 方法检查表是否存在
            if not await self.db.table_exists(self):
                self.logger.info(f"表 '{self.table_name}' 不存在，正在尝试创建...")
                try:
                    # 使用 db_manager 中的 create_table_from_schema 方法
                    await self.db.create_table_from_schema(
                        self
                    )
                    self.logger.info(f"表 '{self.table_name}' 创建成功。")
                except Exception as create_e:
                    self.logger.error(
                        f"创建表 '{self.table_name}' 失败: {create_e}", exc_info=True
                    )
            else:
                self.logger.debug(f"表 '{self.table_name}' 已存在。")
        except Exception as e:
            # 其他可能的错误，例如连接问题
            self.logger.error(
                f"检查表 '{self.table_name}' 是否存在时发生非预期的错误: {e}",
                exc_info=True,
            )

    async def _determine_execution_dates(
        self, **kwargs
    ) -> tuple[Optional[str], Optional[str], bool, str]:
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
        original_start_date = kwargs.get("start_date")
        original_end_date = kwargs.get("end_date")
        force_full = kwargs.get("force_full", False)

        final_start_date = None
        final_end_date = None
        should_skip = False
        skip_message = ""

        if original_start_date is None and original_end_date is None and not force_full:
            # 智能增量模式
            self.logger.info(
                f"任务 {self.name}: 未提供 start_date 和 end_date，进入智能增量模式。"
            )
            latest_date = await self.get_latest_date()
            self.logger.info(f"任务 {self.name}: 数据库中最新日期为: {latest_date}")
            default_start = getattr(self, "default_start_date", None)
            if not default_start:
                self.logger.error(
                    f"任务 {self.name}: 未定义 default_start_date，无法执行智能增量。"
                )
                should_skip = True
                skip_message = "任务未定义默认开始日期"
                return final_start_date, final_end_date, should_skip, skip_message

            if latest_date:
                start_date_dt = latest_date + timedelta(days=1)
            else:
                try:
                    start_date_dt = datetime.strptime(default_start, "%Y%m%d").date()
                    self.logger.info(
                        f"任务 {self.name}: 表为空或不存在，使用默认开始日期: {default_start}"
                    )
                except ValueError:
                    self.logger.error(
                        f"任务 {self.name}: 默认开始日期格式错误 ('{default_start}')，应为 YYYYMMDD。"
                    )
                    should_skip = True
                    skip_message = "默认开始日期格式错误"
                    return final_start_date, final_end_date, should_skip, skip_message

            end_date_dt = datetime.now().date()

            if start_date_dt > end_date_dt:
                self.logger.info(
                    f"任务 {self.name}: 计算出的开始日期 ({start_date_dt}) 晚于结束日期 ({end_date_dt})，数据已是最新。"
                )
                should_skip = True
                skip_message = "数据已是最新"
                return final_start_date, final_end_date, should_skip, skip_message

            final_start_date = start_date_dt.strftime("%Y%m%d")
            final_end_date = end_date_dt.strftime("%Y%m%d")
            self.logger.info(
                f"任务 {self.name}: 智能增量计算出的日期范围: {final_start_date} 到 {final_end_date}"
            )

        elif force_full:
            # 强制全量模式
            self.logger.info(
                f"任务 {self.name}: 强制执行全量模式。将使用默认开始日期 {getattr(self, 'default_start_date', 'N/A')} 到今天。"
            )
            final_start_date = (
                original_start_date
                if original_start_date is not None
                else getattr(self, "default_start_date", None)
            )
            final_end_date = (
                original_end_date
                if original_end_date is not None
                else datetime.now().strftime("%Y%m%d")
            )

            if final_start_date is None:
                self.logger.error(
                    f"任务 {self.name}: 全量模式需要 default_start_date 或手动提供 start_date，但两者均未提供。"
                )
                should_skip = True
                skip_message = "全量模式缺少开始日期"
                return final_start_date, final_end_date, should_skip, skip_message
            # 可选：添加对日期格式的验证
            self.logger.info(
                f"任务 {self.name}: 全量模式实际日期范围: {final_start_date} 到 {final_end_date}"
            )

        else:
            # 手动指定日期模式
            # 可选：添加对手动提供的日期格式和有效性的验证
            if not original_start_date or not original_end_date:
                self.logger.error(
                    f"任务 {self.name}: 手动模式需要提供 start_date 和 end_date。"
                )
                should_skip = True
                skip_message = "手动模式缺少 start_date 或 end_date"
                return final_start_date, final_end_date, should_skip, skip_message

            final_start_date = original_start_date
            final_end_date = original_end_date
            self.logger.info(
                f"任务 {self.name}: 使用提供的日期范围: {final_start_date} 到 {final_end_date}"
            )

        return final_start_date, final_end_date, should_skip, skip_message

    async def run(self, **kwargs):
        """手动增量模式执行任务
        
        提供与基类兼容的run方法接口，内部调用execute方法
        """
        return await self.execute(**kwargs)

    async def execute(self, **kwargs):
        """执行任务的完整生命周期 (已重构日期逻辑, 批处理聚合, 最终结果构造)"""
        self.logger.info(f"开始执行任务: {self.name}, 原始参数: {kwargs}")

        progress_callback = kwargs.get(
            "progress_callback"
        )  # 当前 TushareTask execute 中未使用
        stop_event = kwargs.get("stop_event")

        # 初始化执行状态变量
        total_rows = 0
        failed_batches = 0
        error_message = ""
        final_status = "success"  # 默认为成功，将由聚合或错误更新
        batch_list = []  # 初始化批处理列表

        try:
            await self.pre_execute(stop_event=stop_event)
            await self._ensure_table_exists()

            final_start_date, final_end_date, should_skip, skip_message = (
                await self._determine_execution_dates(**kwargs)
            )

            if should_skip:
                self.logger.warning(f"任务 {self.name}: 跳过执行，原因: {skip_message}")
                skip_status = (
                    "failed"
                    if "错误" in skip_message
                    or "未定义" in skip_message
                    or "缺少" in skip_message
                    else "skipped"
                )
                # 使用 _build_final_result 生成跳过结果
                final_result_dict = self._build_final_result(
                    skip_status, skip_message, 0, 0
                )
                await self.post_execute(result=final_result_dict, stop_event=stop_event)
                return final_result_dict

            # 对于单批次处理模式，直接执行单次获取
            if self.single_batch:
                self.logger.info(f"任务 {self.name} 使用单批次模式执行")
                batch = {
                    "start_date": self.default_start_date,
                    "end_date": None,
                }  # end_date为None表示获取到当前
                results = await self.fetch_batch(batch)
                if results is not None and not results.empty:
                    processed_data = await self.data_transformer.process_data(results)
                    validated_data = await self.data_transformer.validate_data(
                        processed_data
                    )
                    if not validated_data.empty:
                        # 使用BatchProcessor来保存数据，而不是直接调用save_batch
                        rows_saved = await self.batch_processor._save_validated_batch_data_with_retry(
                            validated_data, f"单批次模式 (任务 {self.name})", stop_event
                        )
                        total_rows = rows_saved
                        self.logger.info(
                            f"单批次模式：成功处理并保存 {total_rows} 行数据"
                        )
                        final_result_dict = self._build_final_result(
                            "success", "单批次模式执行成功", total_rows, 0
                        )
                    else:
                        self.logger.warning("单批次模式：数据验证后为空")
                        final_result_dict = self._build_final_result(
                            "no_data", "数据验证后为空", 0, 0
                        )
                else:
                    self.logger.warning("单批次模式：未获取到数据或获取失败")
                    final_result_dict = self._build_final_result(
                        "no_data", "未获取到数据或获取失败", 0, 1
                    )

                await self.post_execute(result=final_result_dict, stop_event=stop_event)
                return final_result_dict

            kwargs_for_batch_list = kwargs.copy()
            kwargs_for_batch_list["start_date"] = final_start_date
            kwargs_for_batch_list["end_date"] = final_end_date

            self.logger.info(
                f"任务 {self.name}: 准备调用 get_batch_list，使用最终参数: {kwargs_for_batch_list}"
            )
            batch_list = await self.get_batch_list(**kwargs_for_batch_list)

            if not batch_list:
                self.logger.info(
                    f"任务 {self.name}: get_batch_list 返回空列表。无需处理。"
                )
                # 使用 _build_final_result 生成无数据结果
                final_result_dict = self._build_final_result(
                    "no_data", "批处理列表为空", 0, 0
                )
                await self.post_execute(result=final_result_dict, stop_event=stop_event)
                return final_result_dict

            total_batches = len(batch_list)
            self.logger.info(f"生成了 {total_batches} 个批处理任务")

            semaphore = asyncio.Semaphore(
                self.concurrent_limit or self.default_concurrent_limit
            )
            tasks_to_run = []

            async def process_batch_wrapper(i, batch_params_item):
                async with semaphore:
                    if stop_event and stop_event.is_set():
                        raise asyncio.CancelledError(
                            f"批次 {i+1} (任务 {self.name}) 在开始前被取消"
                        )
                    return await self.batch_processor.process_single_batch(
                        i, total_batches, batch_params_item, stop_event=stop_event
                    )

            for i, batch_params_item in enumerate(batch_list):
                tasks_to_run.append(process_batch_wrapper(i, batch_params_item))

            batch_execution_results = []
            if tasks_to_run:
                batch_execution_results = await asyncio.gather(
                    *tasks_to_run, return_exceptions=True
                )

            # 汇总结果。total_rows 和 failed_batches 在此处重新赋值。
            # final_status 和 error_message 也在此处确定。
            total_rows, failed_batches, final_status, error_message = (
                self._aggregate_batch_results(
                    batch_execution_results,
                    initial_total_rows=0,  # 为此次执行运行重新开始计数
                    initial_failed_batches=0,  # 为此次执行运行重新开始计数
                )
            )

        except asyncio.CancelledError:
            self.logger.warning(f"任务 {self.name} 在执行主流程中被取消。")
            final_status = "cancelled"
            error_message = "任务被用户取消"
            # total_rows 和 failed_batches 反映了直到取消时的进度（如果发生了汇总）。
            # 如果取消发生在汇总之前，它们保持初始值（可能为0）。
        except Exception as e:
            self.logger.error(
                f"任务 {self.name} 执行过程中发生未处理的严重错误", exc_info=True
            )
            final_status = "failed"
            error_message = f"严重错误: {type(e).__name__} - {str(e)}"
            total_rows = 0  # 在汇总前发生灾难性失败时重置
            failed_batches = (
                len(batch_list) if batch_list else 0
            )  # 如果已生成批处理列表，则将所有批次标记为失败

        finally:
            # 在finally块中使用 _build_final_result
            final_result_dict = self._build_final_result(
                final_status, error_message, total_rows, failed_batches
            )
            await self.post_execute(result=final_result_dict, stop_event=stop_event)
            self.logger.info(
                f"任务 {self.name} 最终处理完成，状态: {final_status}, 总行数: {total_rows}, 失败/取消批次数: {failed_batches}"
            )

        return final_result_dict

    def _build_final_result(
        self, status: str, message: str, rows: int, failed_batches: int
    ) -> Dict:
        """构建标准的任务执行结果字典。"""
        return {
            "status": status,
            "message": message.strip(),
            "rows": rows,
            "failed_batches": failed_batches,
            "task": self.name,  # Assuming self.name is available
        }

    def _aggregate_batch_results(
        self,
        batch_execution_results: List[Any],
        initial_total_rows: int,
        initial_failed_batches: int,
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
        # 初始状态假定如果所有批次都成功执行，则为成功。
        # 如果任何批次被取消或失败，这将被更新。
        current_final_status = "success"

        num_cancelled_internally = 0

        for idx, result in enumerate(batch_execution_results):
            batch_num_for_log = idx + 1  # 用于日志记录目的
            if isinstance(result, asyncio.CancelledError):
                # 这通常意味着取消操作是从 _process_single_batch 内部或其子方法中引发的
                # 或者是在调用 _process_single_batch 前的 process_batch_wrapper 自身引发的。
                self.logger.warning(
                    f"批次 {batch_num_for_log} (任务 {self.name}) 被内部取消: {result}"
                )
                current_failed_batches += 1
                aggregated_error_message += f"批次 {batch_num_for_log} 取消: {result}; "
                current_final_status = (
                    "cancelled"  # 如果有一个被取消，整个任务可能被视为已取消。
                )
                num_cancelled_internally += 1
            elif isinstance(result, Exception):
                self.logger.error(
                    f"批次 {batch_num_for_log} (任务 {self.name}) 执行失败: {result}",
                    exc_info=result,
                )
                current_failed_batches += 1
                aggregated_error_message += f"批次 {batch_num_for_log} 失败: {result}; "
                if (
                    current_final_status != "cancelled"
                ):  # Don't override cancelled status
                    current_final_status = "failed"
            elif isinstance(result, int):
                if result > 0:
                    current_total_rows += result
                elif (
                    result == 0
                ):  # _process_single_batch 返回 0 表示失败或成功处理了 0 行数据
                    # _process_single_batch 内部的日志应该已经指明具体原因。
                    # 如果它本应处理数据但未能处理，我们将其计为"失败批次"进行汇总。
                    # 如果是正确返回 0 的空获取，严格来说它不是同一意义上的"失败批次"，
                    # 但为了汇总的简单性，我们仍可能记录或计数。
                    # 我们假设 _process_single_batch 返回 0 意味着它实际上失败或没有产生影响。
                    self.logger.warning(
                        f"批次 {batch_num_for_log} (任务 {self.name}) 返回 0 (可能内部失败或无数据处理)，计为失败批次。"
                    )
                    current_failed_batches += 1
                    aggregated_error_message += f"批次 {batch_num_for_log} 返回0; "
                    if current_final_status != "cancelled":
                        current_final_status = "failed"
                # 其他情况：_process_single_batch 返回负数 - 这不应该发生，但如果发生则记录日志。
                # 如果返回负数，_process_single_batch 内部应该已经记录了日志。
            else:  # 意外的结果类型
                self.logger.error(
                    f"批次 {batch_num_for_log} (任务 {self.name}) 返回未知类型: {type(result)} ({result})"
                )
                current_failed_batches += 1
                aggregated_error_message += (
                    f"批次 {batch_num_for_log} 返回未知类型 ({type(result)}); "
                )
                if current_final_status != "cancelled":
                    current_final_status = "failed"

        # 根据汇总结果调整最终状态
        if current_final_status == "cancelled":
            # 如果任何内部批次被取消，整体状态为已取消。
            self.logger.warning(
                f"任务 {self.name}: {num_cancelled_internally} 个批次被内部取消。整体任务状态设为 'cancelled'。"
            )
            aggregated_error_message = (
                f"任务因 {num_cancelled_internally} 个批次内部取消而被标记为取消。 "
                + aggregated_error_message
            )
        elif current_failed_batches > 0:
            current_final_status = (
                "partial_success" if current_total_rows > 0 else "failed"
            )
            self.logger.warning(
                f"任务 {self.name} 执行完成，但有 {current_failed_batches} 个批次处理失败或被取消。"
            )
            if current_final_status == "partial_success":
                aggregated_error_message = (
                    f"完成但有 {current_failed_batches} 个批次失败/取消。 "
                    + aggregated_error_message
                )
            else:  # 失败
                aggregated_error_message = (
                    f"任务失败，共 {current_failed_batches} 个批次失败/取消。 "
                    + aggregated_error_message
                )
        # 如果没有失败且未取消，current_final_status 保持为 "success"（初始值）

        return (
            current_total_rows,
            current_failed_batches,
            current_final_status,
            aggregated_error_message.strip(),
        )

    async def fetch_batch(self, batch_params: Dict) -> Optional[pd.DataFrame]:
        """
        获取单个批次的数据，包含重试逻辑。

        Args:
            batch_params (Dict): API调用所需的参数

        Returns:
            Optional[pd.DataFrame]: 获取到的数据，如果失败则返回 None
        """
        attempt = 0
        while attempt < self.max_retries:
            try:
                # 准备最终API参数
                api_params = self.prepare_params(
                    batch_params.copy()
                )  # 使用副本以防 prepare_params 修改原始批次参数

                self.logger.debug(
                    f"任务 {self.name} API调用 {self.api_name} (尝试 {attempt + 1}/{self.max_retries}), 参数: {api_params}"
                )

                # 调用 TushareAPI 的 query 方法
                df = await self.api.query(
                    api_name=self.api_name,  # type: ignore
                    fields=self.fields,  # type: ignore
                    params=api_params,  # 使用处理后的参数
                    page_size=self.page_size,  # type: ignore
                )

                if (
                    df is None
                ):  # 如果API调用返回None (通常是请求失败且未抛出异常，或内部处理决定返回None)
                    self.logger.warning(
                        f"任务 {self.name} API调用 {self.api_name} (参数: {api_params}) 返回 None，尝试次数 {attempt + 1}/{self.max_retries}"
                    )
                    # 可以在这里选择是否立即重试或允许循环继续
                elif df.empty:
                    self.logger.info(
                        f"任务 {self.name} API调用 {self.api_name} (参数: {api_params}) 返回空DataFrame，尝试次数 {attempt + 1}/{self.max_retries}"
                    )
                    # Tushare API 返回空 DataFrame 通常表示在该参数下没有数据，不一定是错误
                    # 对于某些任务，空DataFrame是有效响应，例如查询某日期无数据。
                    # 对于其他任务，可能需要更严格的处理。
                    # 此处仅记录日志，不视为需要重试的硬性错误，除非子类逻辑覆盖。

                return df  # 成功获取数据（可能为空）

            except ValueError as ve:  # 通常是API返回的业务错误，如token无效、参数错误等
                self.logger.error(
                    f"任务 {self.name} (参数: {api_params}) API业务错误: {ve} (尝试 {attempt + 1}/{self.max_retries})"
                )
                # 根据错误类型判断是否重试。例如，token错误不应重试。
                # "Tushare API 返回错误" 通常是这种类型
                if "token" in str(ve).lower() or "权限" in str(
                    ve
                ):  # 假设这些错误不应重试
                    self.logger.warning(f"检测到Token或权限相关错误，不再重试: {ve}")
                    break  # 跳出重试循环
                # 其他 ValueError 可能值得重试

            except aiohttp.ClientError as ce:  # 网络连接相关的客户端错误
                self.logger.error(
                    f"任务 {self.name} (参数: {api_params}) 网络/客户端错误: {ce} (尝试 {attempt + 1}/{self.max_retries})"
                )
                # 网络问题通常值得重试

            except asyncio.TimeoutError:  # 请求超时
                self.logger.error(
                    f"任务 {self.name} (参数: {api_params}) 请求超时 (尝试 {attempt + 1}/{self.max_retries})"
                )
                # 超时通常值得重试

            except Exception as e:  # 其他未知错误
                self.logger.error(
                    f"任务 {self.name} (参数: {api_params}) 未知错误: {e} (尝试 {attempt + 1}/{self.max_retries})",
                    exc_info=True,
                )
                # 未知错误也尝试重试

            # 如果执行到这里，说明发生了可重试的错误
            attempt += 1
            if attempt < self.max_retries:
                self.logger.info(
                    f"任务 {self.name} (参数: {api_params}): {self.retry_delay}秒后进行第 {attempt + 1} 次重试..."
                )
                await asyncio.sleep(self.retry_delay)
            else:
                self.logger.error(
                    f"任务 {self.name} (参数: {api_params}): 所有 {self.max_retries} 次重试均失败。"
                )
                break  # 跳出循环

        return None  # 所有重试失败或因不可重试错误退出

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

    # save_batch 方法已移除，改用 TushareBatchProcessor 中的 _save_validated_batch_data_with_retry 方法

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
        self.logger.warning(
            "TushareTask.fetch_data方法不建议直接调用，请使用execute方法代替"
        )

        # 获取批处理参数列表
        batch_list = await self.get_batch_list(**kwargs)  # type: ignore
        if not batch_list:
            self.logger.warning("批处理参数列表为空，无法获取数据")
            return pd.DataFrame()

        self.logger.info(f"生成了 {len(batch_list)} 个批处理任务")  # type: ignore

        # 存储所有批次的数据
        all_data = []

        # 处理每个批次
        for i, batch_params in enumerate(batch_list):  # type: ignore
            try:
                result = await self.fetch_batch(batch_params)
                if result is not None and not result.empty:  # type: ignore
                    self.logger.info(
                        f"批次 {i+1}/{len(batch_list)}: 获取到 {len(result)} 行数据"  # type: ignore
                    )
                    all_data.append(result)
                else:
                    self.logger.warning(f"批次 {i+1}/{len(batch_list)}: 未获取到数据")  # type: ignore
            except Exception as e:
                self.logger.error(
                    f"批次 {i+1}/{len(batch_list)} 获取数据失败: {str(e)}"  # type: ignore
                )
                # 继续处理下一个批次，而不是直接失败
                continue

        # 合并所有批次的数据
        if not all_data:
            self.logger.warning("所有批次都未获取到数据")
            return pd.DataFrame()

        combined_data = pd.concat(all_data, ignore_index=True)
        self.logger.info(f"成功获取数据，共 {len(combined_data)} 行")

        return combined_data
