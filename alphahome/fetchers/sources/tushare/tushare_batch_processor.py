import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any, Dict, Optional

import pandas as pd

# 避免循环导入，仅用于类型提示
if TYPE_CHECKING:
    from .tushare_data_transformer import TushareDataTransformer  # type: ignore
    from .tushare_task import TushareTask  # type: ignore


class TushareBatchProcessor:
    """负责 Tushare 任务的单个批次处理流程，包括获取、转换、验证和保存。"""

    def __init__(
        self, task_instance: "TushareTask", data_transformer: "TushareDataTransformer"
    ):
        """初始化 BatchProcessor。

        Args:
            task_instance: 关联的 TushareTask 实例。
            data_transformer: TushareDataTransformer 实例。
        """
        self.task = task_instance
        self.data_transformer = data_transformer
        self.logger = (
            self.task.logger
            if hasattr(self.task, "logger")
            else logging.getLogger(__name__)
        )

    async def _fetch_raw_batch_with_retry(
        self, batch_params: Dict, batch_log_prefix: str
    ) -> Optional[pd.DataFrame]:
        """获取单个原始批次数据，包含重试逻辑。"""
        # 确保可以从任务实例中访问 default_max_retries 和 default_retry_delay
        max_retries = getattr(self.task, "max_retries", 3)  # 如果未找到则默认为 3
        retry_delay = getattr(self.task, "retry_delay", 2)  # 如果未找到则默认为 2

        for attempt in range(max_retries + 1):
            try:
                if attempt > 0:
                    self.logger.info(
                        f"{batch_log_prefix}: 重试获取数据 (尝试 {attempt+1}/{max_retries+1})"
                    )
                else:
                    self.logger.info(f"{batch_log_prefix}: 开始获取数据")

                # 调用 TushareTask 实例的 fetch_batch 方法
                batch_data = await self.task.fetch_batch(batch_params)

                if isinstance(batch_data, pd.DataFrame):
                    if not batch_data.empty:
                        self.logger.info(
                            f"{batch_log_prefix}: 获取数据成功 ({len(batch_data)} 行)"
                        )
                        return batch_data
                    else:
                        self.logger.info(
                            f"{batch_log_prefix}: 获取数据成功，但数据为空。"
                        )
                        return batch_data
                elif batch_data is None:
                    self.logger.info(f"{batch_log_prefix}: 获取数据返回 None。")
                else:
                    self.logger.warning(
                        f"{batch_log_prefix}: fetch_batch 返回非预期类型: {type(batch_data)} (尝试 {attempt+1})"
                    )

            except Exception as e:
                self.logger.warning(
                    f"{batch_log_prefix}: 获取数据时发生错误 (尝试 {attempt+1}/{max_retries+1}): {e}",
                    exc_info=False,
                )

            if attempt >= max_retries:
                self.logger.error(
                    f"{batch_log_prefix}: 获取数据失败，已达最大重试次数。"
                )
                return None
            else:
                await asyncio.sleep(retry_delay)

        return None

    def _transform_and_validate_batch_data(
        self, batch_data: pd.DataFrame, batch_log_prefix: str
    ) -> Optional[pd.DataFrame]:
        """处理和验证批次数据。"""
        processed_data = None
        validated_data = None

        try:
            self.logger.info(f"{batch_log_prefix}: 处理 {len(batch_data)} 行数据")
            # 调用 TushareDataTransformer 的 process_data
            processed_data = self.data_transformer.process_data(batch_data)

            if not isinstance(processed_data, pd.DataFrame):
                self.logger.error(
                    f"{batch_log_prefix}: 处理数据后没有得到有效的DataFrame，而是: {type(processed_data)}"
                )
                return None

            if processed_data.empty:
                self.logger.info(f"{batch_log_prefix}: 处理后数据为空。")
                return processed_data

        except Exception as e:
            self.logger.error(
                f"{batch_log_prefix}: 处理数据时发生错误: {e}", exc_info=True
            )
            return None

        try:
            # 验证功能已统一到 BaseTask._validate_data 方法中，这里不再单独验证
            # 直接使用处理后的数据进行去重等操作
            validated_data = processed_data
            
            self.logger.info(
                f"{batch_log_prefix}: 准备保存处理后的数据 ({len(validated_data)} 行)"
            )

            # 在批处理器中执行基本的去重操作（如果需要）
            if hasattr(self.task, "primary_keys") and self.task.primary_keys:
                original_count = len(validated_data)
                validated_data.drop_duplicates(
                    subset=self.task.primary_keys, keep="last", inplace=True
                )
                deduped_count = len(validated_data)
                if deduped_count < original_count:
                    self.logger.debug(
                        f"{batch_log_prefix}: 基于主键 {self.task.primary_keys} 去重，移除了 {original_count - deduped_count} 行重复数据。"
                    )

            self.logger.info(
                f"{batch_log_prefix}: 数据处理完成，得到 {len(validated_data)} 行有效数据。"
            )
            return validated_data

        except Exception as e:
            self.logger.error(
                f"{batch_log_prefix}: 处理数据或去重时发生错误: {e}", exc_info=True
            )
            return None

    async def _save_validated_batch_data_with_retry(
        self,
        validated_data: pd.DataFrame,
        batch_log_prefix: str,
        stop_event: Optional[asyncio.Event],
    ) -> int:
        """保存验证后的批次数据，包含重试逻辑。"""
        rows_saved = 0
        max_retries = getattr(self.task, "max_retries", 3)
        retry_delay = getattr(self.task, "retry_delay", 2)

        for attempt in range(max_retries + 1):
            if stop_event and stop_event.is_set():
                self.logger.warning(
                    f"{batch_log_prefix}: 在保存重试尝试 {attempt+1} 前检测到停止信号。"
                )
                raise asyncio.CancelledError(
                    "Task cancelled by stop event during save retry"
                )

            try:
                if attempt > 0:
                    self.logger.info(
                        f"{batch_log_prefix}: 重试保存数据 (尝试 {attempt+1}/{max_retries+1}) - {len(validated_data)} 行"
                    )
                else:
                    self.logger.info(
                        f"{batch_log_prefix}: 保存 {len(validated_data)} 行数据到表 {self.task.table_name}"
                    )

                # 确保 self.task.db 存在且可用
                if not hasattr(self.task, "db") or not self.task.db:
                    self.logger.error(
                        f"{batch_log_prefix}: 数据库连接 (self.task.db) 不可用。无法保存数据。"
                    )
                    return 0  # 返回0表示保存失败

                result_rows = await self.task.db.upsert(
                    target=self.task,
                    df=validated_data,
                    conflict_columns=getattr(self.task, "primary_keys", []),
                    timestamp_column=self.task.timestamp_column_name,
                )
                if isinstance(result_rows, int) and result_rows >= 0:
                    self.logger.info(
                        f"{batch_log_prefix}: DB操作调度成功，处理了 {result_rows} 行 (COPY到临时表)。"
                    )
                    rows_saved = result_rows
                    return rows_saved
                else:
                    self.logger.error(
                        f"{batch_log_prefix}: DB操作返回非预期结果: {repr(result_rows)} (尝试 {attempt+1})。"
                    )

            except asyncio.CancelledError as ce:
                self.logger.warning(
                    f"{batch_log_prefix}: 保存操作在尝试 {attempt+1} 时被取消: {ce}"
                )
                raise
            except Exception as e:
                self.logger.warning(
                    f"{batch_log_prefix}: 保存数据时发生错误 (尝试 {attempt+1}/{max_retries+1}): {e}",
                    exc_info=False,
                )

            if attempt >= max_retries:
                self.logger.error(
                    f"{batch_log_prefix}: 保存数据失败，已达最大重试次数。"
                )
                return 0
            else:
                await asyncio.sleep(retry_delay)

        return 0

    async def process_single_batch(
        self,
        batch_index: int,
        total_batches: int,
        batch_params: Dict,
        stop_event: Optional[asyncio.Event] = None,
    ) -> int:
        """协调单个批次数据的获取、处理、验证和保存。"""
        # 使用 self.task.name 获取任务名称
        batch_log_prefix = f"批次 {batch_index+1}/{total_batches} (任务 {getattr(self.task, 'name', 'UnknownTask')})"
        batch_start_time = time.time()
        processed_rows_count = 0

        if stop_event and stop_event.is_set():
            self.logger.info(f"{batch_log_prefix}: 在处理开始前检测到停止信号，跳过。")
            raise asyncio.CancelledError(
                "Task cancelled by stop event before processing batch"
            )

        try:
            batch_data = await self._fetch_raw_batch_with_retry(
                batch_params, batch_log_prefix
            )
            if batch_data is None:
                self.logger.error(f"{batch_log_prefix}: 获取数据最终失败。")
                return 0

            if batch_data.empty:
                self.logger.info(f"{batch_log_prefix}: 获取到空数据，无需进一步处理。")
                return 0

            validated_data = self._transform_and_validate_batch_data(
                batch_data, batch_log_prefix
            )
            if validated_data is None:
                self.logger.error(f"{batch_log_prefix}: 处理或验证数据失败。")
                return 0

            if validated_data.empty:
                self.logger.info(
                    f"{batch_log_prefix}: 处理和验证后数据为空，无需保存。"
                )
                return 0

            if stop_event and stop_event.is_set():
                self.logger.info(
                    f"{batch_log_prefix}: 在保存前检测到停止信号，跳过保存。"
                )
                raise asyncio.CancelledError(
                    "Task cancelled by stop event before saving batch"
                )

            processed_rows_count = await self._save_validated_batch_data_with_retry(
                validated_data, batch_log_prefix, stop_event
            )
            if (
                processed_rows_count == 0 and not validated_data.empty
            ):  # 检查即使有数据但保存是否失败
                self.logger.error(f"{batch_log_prefix}: 保存数据最终失败。")

        except asyncio.CancelledError as ce:
            self.logger.warning(f"{batch_log_prefix}: 处理被取消。 ({ce})")
            raise
        except Exception as e:
            self.logger.error(
                f"{batch_log_prefix}: 处理过程中发生意外错误: {e}", exc_info=True
            )
            processed_rows_count = 0

        finally:
            duration = time.time() - batch_start_time
            status_msg = "成功" if processed_rows_count > 0 else "失败或无数据"
            self.logger.debug(
                f"{batch_log_prefix}: 处理完成。状态: {status_msg}, 保存行数: {processed_rows_count}, 耗时: {duration:.2f}s"
            )

        return processed_rows_count
