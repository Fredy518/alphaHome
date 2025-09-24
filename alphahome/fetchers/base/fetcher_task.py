import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional

import pandas as pd
from tqdm.asyncio import tqdm

from ...common.task_system.base_task import BaseTask
from ..sources.tushare.batch_utils import (
    generate_natural_day_batches,
    generate_trade_day_batches,
)
from ...common.constants import UpdateTypes

logger = logging.getLogger(__name__)


class FetcherTask(BaseTask, ABC):
    """
    一个抽象基类，为所有数据获取任务提供通用框架。

    该类实现了 BaseTask 的 _fetch_data 方法，封装了数据采集的通用流程：
    - 管理不同的更新类型（manual, smart, full）来确定日期范围。
    - 调用 get_batch_list 生成批次。
    - 并发执行数据获取请求，并处理重试逻辑。
    - 聚合所有批次的结果并返回一个 DataFrame。

    子类需要实现 `get_batch_list`, `prepare_params` 和 `fetch_batch` 方法，
    以处理特定于数据源的批处理、API参数准备和数据获取逻辑。
    """

    task_type: str = "fetch"
    
    # --- 子类必须或建议定义的属性 ---
    api_name: Optional[str] = None  # API接口名称，对日志和调试很有用
    
    # --- 通用配置默认值 ---
    default_concurrent_limit = 5
    default_max_retries = 3
    default_retry_delay = 2
    smart_lookback_days = 10

    def __init__(
        self,
        db_connection,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        update_type: str = UpdateTypes.SMART,
        task_config: Optional[Dict] = None,
        **kwargs,
    ):
        """
        初始化 FetcherTask。
        """
        super().__init__(db_connection, **kwargs)

        # 规范化日期格式
        if start_date:
            try:
                self.start_date = pd.to_datetime(start_date).strftime('%Y%m%d')
            except (ValueError, TypeError) as e:
                self.logger.error(f"无效的 start_date 格式: {start_date}。错误: {e}")
                raise ValueError(f"无法解析 start_date: {start_date}") from e
        else:
            self.start_date = None

        if end_date:
            try:
                self.end_date = pd.to_datetime(end_date).strftime('%Y%m%d')
            except (ValueError, TypeError) as e:
                self.logger.error(f"无效的 end_date 格式: {end_date}。错误: {e}")
                raise ValueError(f"无法解析 end_date: {end_date}") from e
        else:
            self.end_date = None

        self.update_type = update_type
        
        # 应用配置
        self.task_specific_config = task_config or {}
        self._apply_config(self.task_specific_config)

    def _apply_config(self, task_config: Dict):
        """合并代码默认值和配置文件设置。"""
        cls = type(self)

        self.concurrent_limit = int(task_config.get("concurrent_limit", cls.default_concurrent_limit))
        self.max_retries = int(task_config.get("max_retries", cls.default_max_retries))
        self.retry_delay = int(task_config.get("retry_delay", cls.default_retry_delay))
        self.smart_lookback_days = int(task_config.get("smart_lookback_days", cls.smart_lookback_days))

        # 处理数据保存批次大小配置 (优先使用save_batch_size，向后兼容batch_size)
        self.save_batch_size = int(
            task_config.get("save_batch_size",
                           task_config.get("batch_size", cls.default_save_batch_size))
        )

        self.logger.debug(
            f"'{self.name}': Applied config - concurrent_limit={self.concurrent_limit}, "
            f"max_retries={self.max_retries}, retry_delay={self.retry_delay}, "
            f"save_batch_size={self.save_batch_size}"
        )

    @abstractmethod
    async def get_batch_list(self, **kwargs) -> List[Any]:
        """
        根据日期范围等参数生成批次列表 (子类必须实现)。
        """
        raise NotImplementedError

    @abstractmethod
    async def prepare_params(self, batch: Any) -> Dict[str, Any]:
        """为给定的批次准备API请求参数 (子类必须实现)。"""
        raise NotImplementedError

    @abstractmethod
    async def fetch_batch(self, params: Dict[str, Any], stop_event: Optional[asyncio.Event] = None) -> Optional[Any]:
        """获取单个批次的数据 (子类必须实现)。"""
        raise NotImplementedError

    async def _determine_date_range(self) -> Optional[Dict[str, str]]:
        """根据更新类型确定并返回开始和结束日期。"""
        self.logger.info(f"'{self.name}' - Determining date range for update_type='{self.update_type}'...")
        
        start, end = None, None
        
        if self.update_type == UpdateTypes.MANUAL:
            if not self.start_date or not self.end_date:
                raise ValueError("Manual update requires start_date and end_date.")
            start, end = self.start_date, self.end_date
            
        elif self.update_type == UpdateTypes.SMART:
            latest_date_in_db = await self.get_latest_date_for_task()
            
            if latest_date_in_db:
                start_dt = latest_date_in_db + timedelta(days=1) - timedelta(days=self.smart_lookback_days)
                default_start_dt = datetime.strptime(self.default_start_date, "%Y%m%d").date()
                start_dt = max(start_dt, default_start_dt)
            else:
                start_dt = datetime.strptime(self.default_start_date, "%Y%m%d").date()
            
            end_dt = datetime.now().date()
            
            if start_dt > end_dt:
                self.logger.info(f"'{self.name}' - Data is already up to date. No batches to generate.")
                return None
            start, end = start_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d")
            
        elif self.update_type == UpdateTypes.FULL:
            start, end = self.default_start_date, datetime.now().strftime("%Y%m%d")
            
        else:
            raise ValueError(f"Unsupported update_type: {self.update_type}")
            
        return {"start_date": start, "end_date": end}

    async def _execute_batches(self, batches: List[Any], stop_event: Optional[asyncio.Event] = None) -> List[Any]:
        """
        使用信号量并发执行所有批次的数据获取，并包含重试逻辑。
        """
        if not batches:
            return []
            
        semaphore = asyncio.Semaphore(self.concurrent_limit)
        progress_bar = tqdm(total=len(batches), desc=f"Executing {self.name}", unit="batch")
        
        async def process_batch_with_retry(batch):
            for attempt in range(self.max_retries):
                if stop_event and stop_event.is_set():
                    progress_bar.close()
                    raise asyncio.CancelledError
                try:
                    async with semaphore:
                        params = await self.prepare_params(batch)
                        return await self.fetch_batch(params, stop_event=stop_event)
                except asyncio.CancelledError:
                    raise  # Propagate cancellation
                except Exception as e:
                    self.logger.warning(
                        f"'{self.name}' - Batch {batch} failed on attempt {attempt + 1}/{self.max_retries}. Error: {e}"
                    )
                    if attempt + 1 == self.max_retries:
                        self.logger.error(f"'{self.name}' - Batch {batch} failed after all retries.")
                        return None
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
            return None

        tasks = []
        for batch in batches:
            if stop_event and stop_event.is_set():
                self.logger.warning(f"'{self.name}' - Stop signal detected before creating all tasks. Halting batch creation.")
                break # 停止创建新的批处理任务
            tasks.append(asyncio.create_task(process_batch_with_retry(batch)))

        results = []
        for future in asyncio.as_completed(tasks):
            try:
                result = await future
                if result is not None:
                    results.append(result)
            except asyncio.CancelledError:
                self.logger.warning(f"'{self.name}' - Batch processing was cancelled.")
                # Ensure remaining tasks are cancelled
                for t in tasks:
                    if not t.done():
                        t.cancel()
                raise # Re-raise the cancellation to be caught by the calling method
            finally:
                progress_bar.update(1)

        progress_bar.close()
        return results

    async def _fetch_data(self, stop_event: Optional[asyncio.Event] = None, **kwargs) -> Optional[pd.DataFrame]:
        """
        实现 BaseTask 的数据获取钩子。
        这是数据获取任务的主入口点。
        """
        self.logger.info(f"'{self.name}' - Starting _fetch_data with update_type='{self.update_type}'...")

        try:
            # 首先处理全量更新，因为它最简单
            if self.update_type == UpdateTypes.FULL:
                start_date, end_date = self.default_start_date, datetime.now().strftime("%Y%m%d")
            
            # 手动模式：直接使用传入的日期，这是最优先的
            elif self.update_type == UpdateTypes.MANUAL:
                if not self.start_date or not self.end_date:
                    self.logger.error("手动模式需要提供 start_date 和 end_date。")
                    return None
                start_date, end_date = self.start_date, self.end_date
            
            # 智能增量模式：动态确定日期范围
            elif self.update_type == UpdateTypes.SMART:
                date_range = await self._determine_date_range()
                if not date_range:
                    self.logger.warning(
                        f"任务 {self.name}: 无法确定智能增量更新的日期范围，将跳过执行。"
                    )
                    return None
                start_date, end_date = date_range["start_date"], date_range["end_date"]

            else:
                self.logger.error(f"未知的更新类型: {self.update_type}")
                return None

            # 确保日期范围有效
            if not start_date or not end_date:
                self.logger.info(f"'{self.name}' - No date range determined. Task finished.")
                return None

            # 将实例属性中的日期更新到 kwargs，以确保传递给 get_batch_list 的是一致的
            if self.start_date:
                kwargs['start_date'] = self.start_date
            if self.end_date:
                kwargs['end_date'] = self.end_date

            # 确保 update_type 被传递给 get_batch_list
            kwargs['update_type'] = self.update_type

            # 将计算出的日期范围和 kwargs 合并，传递给 get_batch_list
            batch_gen_params = {**kwargs, **{"start_date": start_date, "end_date": end_date}}
            batches = await self.get_batch_list(**batch_gen_params)
            
            if not batches:
                self.logger.info(f"'{self.name}' - No batches to process. Task finished.")
                return None

            raw_results = await self._execute_batches(batches, stop_event=stop_event)
            if not raw_results:
                self.logger.warning(f"'{self.name}' - No data returned from batches.")
                return None

            self.logger.info(f"'{self.name}' - Aggregating {len(raw_results)} results...")
            combined_df = pd.concat(raw_results, ignore_index=True) if raw_results else pd.DataFrame()

            if combined_df.empty:
                self.logger.info(f"'{self.name}' - Data is empty after combining batches.")
                return None
            
            return combined_df

        except asyncio.CancelledError:
            self.logger.warning(f"'{self.name}' - _fetch_data was cancelled.")
            # Let the main execute loop handle the final status
            raise
        except Exception as e:
            self.logger.error(f"'{self.name}' - _fetch_data failed with an unhandled exception: {e}", exc_info=True)
            # Re-raise the exception to be handled by the main execute loop
            raise

    async def get_latest_date(self) -> Optional[date]:
        """获取当前任务对应表中的最新日期。"""
        if not self.table_name or not self.date_column:
            self.logger.warning(f"'{self.name}' - Task has no table_name or date_column defined. Cannot get latest date.")
            return None
        
        try:
            table_exists = await self.db.table_exists(self.get_full_table_name())
            if not table_exists:
                self.logger.info(f"'{self.name}' - Table '{self.get_full_table_name()}' does not exist. Cannot get latest date.")
                return None
                
            query = f"SELECT MAX({self.date_column}) as latest_date FROM {self.get_full_table_name()}"
            result = await self.db.fetch_one(query)

            if result and result["latest_date"]:
                latest_date = result["latest_date"]
                if isinstance(latest_date, datetime):
                    return latest_date.date()
                elif isinstance(latest_date, date):
                    return latest_date
                # Add more flexible parsing if needed
                return pd.to_datetime(latest_date).date()
            return None
        except Exception as e:
            self.logger.error(f"'{self.name}' - Error getting latest date: {e}", exc_info=True)
            return None 