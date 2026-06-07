import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional

import pandas as pd
from tqdm.asyncio import tqdm

from ...common.task_system.base_task import BaseTask
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
    default_stream_batches = False
    default_continue_on_stream_batch_failure = False
    default_stream_save_batch_size = BaseTask.default_save_batch_size
    default_stream_update_types = (UpdateTypes.FULL,)
    smart_lookback_days = 10
    smart_refresh_interval_days: Optional[int] = None

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

    @staticmethod
    def _parse_bool(value: Any, default: bool = False) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return default
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"1", "true", "yes", "y", "on"}:
                return True
            if normalized in {"0", "false", "no", "n", "off", ""}:
                return False
        return default

    @staticmethod
    def _parse_stream_update_types(value: Any, default: Any) -> set[str]:
        raw_value = default if value is None else value
        if isinstance(raw_value, str):
            values = [item.strip() for item in raw_value.replace(";", ",").split(",")]
        elif isinstance(raw_value, (list, tuple, set)):
            values = [str(item).strip() for item in raw_value]
        else:
            values = [str(raw_value).strip()]
        return {item.lower() for item in values if item}

    def _apply_config(self, task_config: Dict):
        """合并代码默认值和配置文件设置。"""
        cls = type(self)

        self.concurrent_limit = int(task_config.get("concurrent_limit", cls.default_concurrent_limit))
        self.max_retries = int(task_config.get("max_retries", cls.default_max_retries))
        self.retry_delay = int(task_config.get("retry_delay", cls.default_retry_delay))
        self.stream_batches = self._parse_bool(
            task_config.get("stream_batches", cls.default_stream_batches),
            cls.default_stream_batches,
        )
        self.continue_on_stream_batch_failure = self._parse_bool(
            task_config.get(
                "continue_on_stream_batch_failure",
                cls.default_continue_on_stream_batch_failure,
            ),
            cls.default_continue_on_stream_batch_failure,
        )
        self.stream_save_batch_size = int(
            task_config.get(
                "stream_save_batch_size",
                task_config.get(
                    "stream_flush_rows",
                    task_config.get(
                        "save_batch_size",
                        task_config.get("batch_size", cls.default_stream_save_batch_size),
                    ),
                ),
            )
        )
        self.stream_update_types = self._parse_stream_update_types(
            task_config.get("stream_update_types"),
            cls.default_stream_update_types,
        )
        self.smart_lookback_days = int(task_config.get("smart_lookback_days", cls.smart_lookback_days))
        raw_refresh_interval = task_config.get(
            "smart_refresh_interval_days",
            cls.smart_refresh_interval_days,
        )
        if raw_refresh_interval in (None, ""):
            self.smart_refresh_interval_days = None
        else:
            self.smart_refresh_interval_days = int(raw_refresh_interval)

        # 处理数据保存批次大小配置 (优先使用save_batch_size，向后兼容batch_size)
        self.save_batch_size = int(
            task_config.get("save_batch_size",
                           task_config.get("batch_size", cls.default_save_batch_size))
        )

        self.logger.debug(
            f"'{self.name}': Applied config - concurrent_limit={self.concurrent_limit}, "
            f"max_retries={self.max_retries}, retry_delay={self.retry_delay}, "
            f"save_batch_size={self.save_batch_size}, stream_batches={self.stream_batches}"
        )

    def _should_stream_batches(self, kwargs: Optional[Dict[str, Any]] = None) -> bool:
        params = kwargs or {}
        stream_enabled = self._parse_bool(
            params.get(
                "stream_batches",
                self.task_specific_config.get("stream_batches", self.stream_batches),
            ),
            self.stream_batches,
        )
        if not stream_enabled:
            return False

        stream_update_types = self._parse_stream_update_types(
            params.get(
                "stream_update_types",
                self.task_specific_config.get(
                    "stream_update_types",
                    getattr(self, "stream_update_types", self.default_stream_update_types),
                ),
            ),
            getattr(self, "stream_update_types", self.default_stream_update_types),
        )
        if "*" in stream_update_types or "all" in stream_update_types:
            return True
        update_type = str(params.get("update_type") or self.update_type or "").strip().lower()
        return update_type in stream_update_types

    def _continue_on_stream_batch_failure(self, kwargs: Optional[Dict[str, Any]] = None) -> bool:
        params = kwargs or {}
        return self._parse_bool(
            params.get(
                "continue_on_stream_batch_failure",
                self.task_specific_config.get(
                    "continue_on_stream_batch_failure",
                    self.continue_on_stream_batch_failure,
                ),
            ),
            self.continue_on_stream_batch_failure,
        )

    def _resolve_stream_save_batch_size(self, kwargs: Optional[Dict[str, Any]] = None) -> int:
        params = kwargs or {}
        raw_value = params.get(
            "stream_save_batch_size",
            params.get(
                "stream_flush_rows",
                self.task_specific_config.get(
                    "stream_save_batch_size",
                    self.task_specific_config.get(
                        "stream_flush_rows",
                        self.task_specific_config.get(
                            "save_batch_size",
                            self.task_specific_config.get(
                                "batch_size",
                                getattr(self, "stream_save_batch_size", self.default_stream_save_batch_size),
                            ),
                        ),
                    ),
                ),
            ),
        )
        try:
            return max(1, int(raw_value))
        except (TypeError, ValueError):
            return max(1, int(self.default_stream_save_batch_size))

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
        self._smart_skip_reason = None
        
        start, end = None, None
        
        if self.update_type == UpdateTypes.MANUAL:
            if not self.start_date or not self.end_date:
                raise ValueError("Manual update requires start_date and end_date.")
            start, end = self.start_date, self.end_date
            
        elif self.update_type == UpdateTypes.SMART:
            if await self._should_skip_smart_by_recent_update_time():
                return None

            latest_date_in_db = await self.get_latest_date_for_task()
            # 兼容 TIMESTAMP 类型日期列：统一转换为 date，避免 datetime/date 比较报错
            if isinstance(latest_date_in_db, datetime):
                latest_date_in_db = latest_date_in_db.date()

            end_dt = datetime.now().date()
            
            if latest_date_in_db:
                anchor_dt = latest_date_in_db
                if anchor_dt > end_dt:
                    self.logger.warning(
                        "'%s' - 数据库最新日期 %s 晚于当前日期 %s，SMART 将以当前日期作为回看锚点，避免跳过历史修订窗口。",
                        self.name,
                        anchor_dt.strftime("%Y%m%d"),
                        end_dt.strftime("%Y%m%d"),
                    )
                    anchor_dt = end_dt
                start_dt = anchor_dt + timedelta(days=1) - timedelta(days=self.smart_lookback_days)
                default_start_dt = datetime.strptime(self.default_start_date, "%Y%m%d").date()
                start_dt = max(start_dt, default_start_dt)
            else:
                start_dt = datetime.strptime(self.default_start_date, "%Y%m%d").date()

            if start_dt > end_dt:
                self.logger.info(f"'{self.name}' - Data is already up to date. No batches to generate.")
                return None
            start, end = start_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d")
            self.logger.info(
                "'%s' - SMART date range: latest_date=%s, lookback_days=%s, start_date=%s, end_date=%s",
                self.name,
                latest_date_in_db.strftime("%Y%m%d") if latest_date_in_db else None,
                self.smart_lookback_days,
                start,
                end,
            )
            
        elif self.update_type == UpdateTypes.FULL:
            start, end = self.default_start_date, datetime.now().strftime("%Y%m%d")
            
        else:
            raise ValueError(f"Unsupported update_type: {self.update_type}")
            
        return {"start_date": start, "end_date": end}

    async def _should_skip_smart_by_recent_update_time(self) -> bool:
        """Skip SMART refreshes for tables that are intentionally low-frequency."""
        if self.update_type != UpdateTypes.SMART:
            return False

        interval_days = getattr(self, "smart_refresh_interval_days", None)
        if interval_days is None or interval_days <= 0:
            return False

        try:
            if not await self.db.table_exists(self):
                return False

            getter = getattr(self.db, "get_latest_update_time", None)
            if callable(getter):
                latest_update_time = await getter(self)
            else:
                latest_update_time = await self.db.get_latest_date(self, "update_time")
            if not latest_update_time:
                return False

            if not isinstance(latest_update_time, datetime):
                latest_update_time = pd.to_datetime(latest_update_time).to_pydatetime()
            latest_update_time = latest_update_time.replace(tzinfo=None)

            age = datetime.now() - latest_update_time
            if age <= timedelta(days=interval_days):
                self._smart_skip_reason = (
                    f"SMART 模式检测到表最近更新时间 {latest_update_time}，"
                    f"{interval_days} 天内无需重复拉取，自动跳过。"
                )
                self.logger.info("%s: %s", self.name, self._smart_skip_reason)
                return True
        except Exception as e:
            self.logger.warning(
                "%s: SMART 最近更新时间跳过判断失败，将继续执行: %s",
                self.name,
                e,
            )

        return False

    async def _execute_batches(self, batches: List[Any], stop_event: Optional[asyncio.Event] = None) -> List[Any]:
        """
        使用信号量并发执行所有批次的数据获取，并包含重试逻辑。
        """
        if not batches:
            return []
            
        semaphore = asyncio.Semaphore(self.concurrent_limit)
        progress_bar = tqdm(total=len(batches), desc=f"Executing {self.name}", unit="batch")
        failed_batches: List[Dict[str, Any]] = []
        
        async def process_batch_with_retry(batch):
            last_error = None
            for attempt in range(self.max_retries):
                if stop_event and stop_event.is_set():
                    progress_bar.close()
                    raise asyncio.CancelledError
                try:
                    async with semaphore:
                        params = await self.prepare_params(batch)
                        return {
                            "success": True,
                            "batch": batch,
                            "data": await self.fetch_batch(params, stop_event=stop_event),
                        }
                except asyncio.CancelledError:
                    raise  # Propagate cancellation
                except Exception as e:
                    last_error = e
                    self.logger.warning(
                        f"'{self.name}' - Batch {batch} failed on attempt {attempt + 1}/{self.max_retries}. Error: {e}"
                    )
                    if attempt + 1 == self.max_retries:
                        self.logger.error(f"'{self.name}' - Batch {batch} failed after all retries.")
                        return {
                            "success": False,
                            "batch": batch,
                            "error": str(last_error),
                        }
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
            return {
                "success": False,
                "batch": batch,
                "error": str(last_error) if last_error else "unknown batch failure",
            }

        tasks = []
        for batch in batches:
            if stop_event and stop_event.is_set():
                self.logger.warning(f"'{self.name}' - Stop signal detected before creating all tasks. Halting batch creation.")
                break # 停止创建新的批处理任务
            tasks.append(asyncio.create_task(process_batch_with_retry(batch)))

        results = []
        for future in asyncio.as_completed(tasks):
            try:
                batch_result = await future
                if not batch_result:
                    continue
                if not batch_result.get("success"):
                    failed_batches.append(batch_result)
                    continue
                result = batch_result.get("data")
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

        if failed_batches:
            sample_errors = "; ".join(
                f"batch={item.get('batch')}, error={item.get('error')}"
                for item in failed_batches[:3]
            )
            raise RuntimeError(
                f"'{self.name}' - {len(failed_batches)}/{len(batches)} batches failed; "
                f"aborting save to avoid partial data. Sample errors: {sample_errors}"
            )

        return results

    async def _get_effective_batch_list(self, **kwargs: Any) -> List[Any]:
        date_range = await self._determine_date_range()
        if not date_range:
            return []

        start_date = date_range["start_date"]
        end_date = date_range["end_date"]
        self._effective_start_date = start_date
        self._effective_end_date = end_date

        if self.start_date:
            kwargs["start_date"] = self.start_date
        if self.end_date:
            kwargs["end_date"] = self.end_date
        kwargs["update_type"] = self.update_type

        batch_gen_params = {**kwargs, "start_date": start_date, "end_date": end_date}
        from ..tools.calendar import reset_calendar_db_manager, set_calendar_db_manager

        calendar_token = set_calendar_db_manager(self.db)
        try:
            return await self.get_batch_list(**batch_gen_params)
        finally:
            reset_calendar_db_manager(calendar_token)

    async def _fetch_stream_batch_with_retry(
        self,
        batch: Any,
        *,
        stop_event: Optional[asyncio.Event],
    ) -> Dict[str, Any]:
        last_error = None
        for attempt in range(self.max_retries):
            if stop_event and stop_event.is_set():
                raise asyncio.CancelledError
            try:
                params = await self.prepare_params(batch)
                data = await self.fetch_batch(params, stop_event=stop_event)
                return {"success": True, "batch": batch, "params": params, "data": data}
            except asyncio.CancelledError:
                raise
            except Exception as e:
                last_error = e
                self.logger.warning(
                    "'%s' - Streaming batch %s failed on attempt %s/%s. Error: %s",
                    self.name,
                    batch,
                    attempt + 1,
                    self.max_retries,
                    e,
                )
                if attempt + 1 < self.max_retries:
                    await asyncio.sleep(self.retry_delay * (attempt + 1))

        return {
            "success": False,
            "batch": batch,
            "error": str(last_error) if last_error else "unknown batch failure",
        }

    async def _process_validate_stream_frame(
        self,
        raw_data: pd.DataFrame,
        *,
        params: Dict[str, Any],
        stop_event: Optional[asyncio.Event],
        runtime_kwargs: Dict[str, Any],
    ) -> Dict[str, Any]:
        if raw_data is None or raw_data.empty:
            return {"data": None, "validation": True, "validation_details": None}

        process_kwargs = {**runtime_kwargs, **params}
        processed = self.process_data(raw_data, stop_event=stop_event, **process_kwargs)
        if asyncio.iscoroutine(processed):
            processed = await processed
        if processed is None or (isinstance(processed, pd.DataFrame) and processed.empty):
            return {"data": None, "validation": True, "validation_details": None}

        validation_passed, validated_data, validation_details = self._validate_data(
            processed,
            stop_event=stop_event,
            validation_mode=getattr(self, "validation_mode", "report"),
        )
        if validated_data is None or (isinstance(validated_data, pd.DataFrame) and validated_data.empty):
            return {
                "data": None,
                "validation": validation_passed,
                "validation_details": validation_details,
            }

        return {
            "data": validated_data,
            "validation": validation_passed,
            "validation_details": validation_details,
        }

    async def _save_stream_buffer(
        self,
        buffer: List[pd.DataFrame],
        *,
        stop_event: Optional[asyncio.Event],
        ensure_table: bool,
    ) -> Dict[str, Any]:
        if not buffer:
            return {"rows": 0, "table_checked": False}
        data = buffer[0] if len(buffer) == 1 else pd.concat(buffer, ignore_index=True)
        save_result = await self._save_data(
            data,
            stop_event=stop_event,
            ensure_table=ensure_table,
        )
        rows = save_result.get("rows", 0) if isinstance(save_result, dict) else 0
        return {"rows": rows, "table_checked": True}

    async def _process_validate_save_stream_frame(
        self,
        raw_data: pd.DataFrame,
        *,
        params: Dict[str, Any],
        stop_event: Optional[asyncio.Event],
        runtime_kwargs: Dict[str, Any],
        ensure_table: bool,
    ) -> Dict[str, Any]:
        process_result = await self._process_validate_stream_frame(
            raw_data,
            params=params,
            stop_event=stop_event,
            runtime_kwargs=runtime_kwargs,
        )
        validated_data = process_result.get("data")
        if validated_data is None or (isinstance(validated_data, pd.DataFrame) and validated_data.empty):
            return {
                "rows": 0,
                "validation": process_result.get("validation", True),
                "validation_details": process_result.get("validation_details"),
                "table_checked": False,
            }
        save_result = await self._save_stream_buffer(
            [validated_data],
            stop_event=stop_event,
            ensure_table=ensure_table,
        )
        return {
            "rows": save_result.get("rows", 0),
            "validation": process_result.get("validation", True),
            "validation_details": process_result.get("validation_details"),
            "table_checked": save_result.get("table_checked", False),
        }

    async def _execute_streaming(
        self,
        stop_event: Optional[asyncio.Event] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        self.logger.info("'%s' - Streaming fetch/process/save enabled.", self.name)
        await self._pre_execute(stop_event=stop_event, **kwargs)

        batches = await self._get_effective_batch_list(**kwargs)
        if not batches:
            self.logger.info("'%s' - No batches to process. Task finished.", self.name)
            return {"status": "no_data", "rows": 0, "task": self.name}

        total_rows = 0
        processed_batches = 0
        empty_batches = 0
        saved_batches = 0
        failed_batches: List[Dict[str, Any]] = []
        validation_passed_all = True
        last_validation_details = None
        table_checked = False
        continue_on_failure = self._continue_on_stream_batch_failure(kwargs)
        concurrency = max(1, int(getattr(self, "concurrent_limit", 1)))
        stream_save_batch_size = self._resolve_stream_save_batch_size(kwargs)
        save_buffer: List[pd.DataFrame] = []
        save_buffer_rows = 0
        save_buffer_batch_count = 0
        progress_completed_batches = 0
        progress_log_interval = max(1, min(50, len(batches) // 20 or 1))

        progress_bar = tqdm(total=len(batches), desc=f"Executing {self.name}", unit="batch")
        iterator = iter(batches)
        pending: set[asyncio.Task] = set()

        async def cancel_pending() -> None:
            if not pending:
                return
            for item in pending:
                item.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

        def schedule_next() -> None:
            try:
                batch = next(iterator)
            except StopIteration:
                return
            pending.add(
                asyncio.create_task(
                    self._fetch_stream_batch_with_retry(batch, stop_event=stop_event)
                )
            )

        def update_stream_progress(batch_count: int = 1) -> None:
            nonlocal progress_completed_batches
            if batch_count <= 0:
                return

            progress_bar.update(batch_count)
            progress_completed_batches += batch_count
            progress_bar.set_postfix(
                rows=total_rows,
                buffered=save_buffer_rows,
                saves=saved_batches,
                empty=empty_batches,
                failed=len(failed_batches),
                refresh=False,
            )
            if (
                progress_completed_batches <= 3
                or progress_completed_batches >= len(batches)
                or progress_completed_batches % progress_log_interval == 0
            ):
                self.logger.info(
                    "'%s' - Streaming progress: %s/%s batches handled, %s rows saved, "
                    "%s rows buffered, %s empty, %s failed.",
                    self.name,
                    progress_completed_batches,
                    len(batches),
                    total_rows,
                    save_buffer_rows,
                    empty_batches,
                    len(failed_batches),
                )

        async def flush_save_buffer() -> None:
            nonlocal save_buffer, save_buffer_rows, save_buffer_batch_count
            nonlocal total_rows, saved_batches, table_checked
            if not save_buffer:
                return
            buffered_batch_count = save_buffer_batch_count
            save_result = await self._save_stream_buffer(
                save_buffer,
                stop_event=stop_event,
                ensure_table=not table_checked,
            )
            total_rows += int(save_result.get("rows", 0) or 0)
            if save_result.get("table_checked"):
                table_checked = True
                saved_batches += 1
            save_buffer = []
            save_buffer_rows = 0
            save_buffer_batch_count = 0
            update_stream_progress(buffered_batch_count)

        for _ in range(min(concurrency, len(batches))):
            schedule_next()

        try:
            while pending:
                if stop_event and stop_event.is_set():
                    raise asyncio.CancelledError

                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    batch_result = await task

                    if not batch_result.get("success"):
                        failed_batches.append(batch_result)
                        if not continue_on_failure:
                            update_stream_progress(1)
                            await cancel_pending()
                            sample = batch_result.get("error", "unknown batch failure")
                            partial = f"; {total_rows} rows were already saved" if total_rows else ""
                            raise RuntimeError(f"'{self.name}' - Streaming batch failed: {sample}{partial}")
                        update_stream_progress(1)
                        schedule_next()
                        continue

                    raw_data = batch_result.get("data")
                    if raw_data is None or raw_data.empty:
                        empty_batches += 1
                        update_stream_progress(1)
                        schedule_next()
                        continue

                    processed_batches += 1
                    chunk_result = await self._process_validate_stream_frame(
                        raw_data,
                        params=batch_result.get("params") or {},
                        stop_event=stop_event,
                        runtime_kwargs=kwargs,
                    )
                    if not chunk_result.get("validation", True):
                        validation_passed_all = False
                    if chunk_result.get("validation_details") is not None:
                        last_validation_details = chunk_result.get("validation_details")

                    validated_data = chunk_result.get("data")
                    if validated_data is not None and not validated_data.empty:
                        save_buffer.append(validated_data)
                        save_buffer_rows += len(validated_data)
                        save_buffer_batch_count += 1
                        if save_buffer_rows >= stream_save_batch_size:
                            await flush_save_buffer()
                    else:
                        update_stream_progress(1)

                    schedule_next()
            await flush_save_buffer()
        finally:
            progress_bar.close()

        status = "no_data" if total_rows == 0 and not failed_batches else "success"
        if failed_batches or not validation_passed_all:
            status = "partial_success" if total_rows > 0 else "error"

        final_result = {
            "status": status,
            "table": self.table_name,
            "rows": total_rows,
            "processed_batches": processed_batches,
            "empty_batches": empty_batches,
            "saved_batches": saved_batches,
            "failed_batches": len(failed_batches),
            "stream_batches": True,
            "validation": validation_passed_all,
            "validation_details": last_validation_details
            or {
                "status": "passed" if validation_passed_all else "failed",
                "validation_mode": getattr(self, "validation_mode", "report"),
            },
        }
        await self._post_execute(final_result, stop_event=stop_event)
        self.logger.info("任务执行完成: %s", final_result)
        return final_result

    async def execute(
        self,
        stop_event: Optional[asyncio.Event] = None,
        **kwargs: Any,
    ):
        if not self._should_stream_batches(kwargs):
            return await super().execute(stop_event=stop_event, **kwargs)

        try:
            return await self._execute_streaming(stop_event=stop_event, **kwargs)
        except asyncio.CancelledError:
            self.logger.warning("任务 %s 被取消。", self.name)
            return self._handle_error(asyncio.CancelledError("任务被用户取消"))
        except Exception as e:
            self.logger.error(
                "任务执行失败: 类型=%s, 错误=%s",
                type(e).__name__,
                str(e),
                exc_info=True,
            )
            return self._handle_error(e)

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
                    skip_reason = getattr(self, "_smart_skip_reason", None)
                    if skip_reason:
                        self.logger.info("任务 %s: %s", self.name, skip_reason)
                    else:
                        self.logger.warning(
                            f"任务 {self.name}: 无法确定智能增量更新的日期范围，将跳过执行。"
                        )
                    return None
                start_date, end_date = date_range["start_date"], date_range["end_date"]

            else:
                self.logger.error(f"未知的更新类型: {self.update_type}")
                return None

            # 记录本次执行实际生效的日期范围（供子类在 process_data 中做窗口过滤/增强模式使用）
            self._effective_start_date = start_date
            self._effective_end_date = end_date

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
            from ..tools.calendar import reset_calendar_db_manager, set_calendar_db_manager

            calendar_token = set_calendar_db_manager(self.db)
            try:
                batches = await self.get_batch_list(**batch_gen_params)
            finally:
                reset_calendar_db_manager(calendar_token)
            
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
