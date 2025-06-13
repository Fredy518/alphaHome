import asyncio
import inspect
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Union

import numpy as np
import pandas as pd
from ..db_manager import DBManager


class BaseTask(ABC):
    """统一数据任务基类
    
    支持数据采集(fetch)和数据处理(processor)两种任务类型的统一框架。
    为所有任务提供基础功能，包括数据获取、处理、验证和保存。
    """

    # 任务类型标识 ('fetch', 'processor', 'derivative', etc.)
    task_type: str = "base"

    # 必须由子类定义的属性
    name = None
    table_name = None

    # 可选属性（有合理默认值）
    primary_keys = []
    date_column = None
    description = ""
    auto_add_update_time = True  # 是否自动添加更新时间
    data_source: Optional[str] = None  # 数据源标识（如'tushare', 'wind', 'jqdata'等）
    
    # 新增：支持processor任务的属性
    source_tables = []        # 源数据表列表（processor任务使用）
    dependencies = []         # 依赖的其他任务（processor任务使用）

    def __init__(self, db_connection):
        """初始化任务"""
        if self.name is None or self.table_name is None:
            raise ValueError("必须定义name和table_name属性")

        self.db = db_connection
        self.logger = logging.getLogger(f"task.{self.name}")

    async def execute(
        self, stop_event: Optional[asyncio.Event] = None, **kwargs
    ):
        """执行任务的完整生命周期"""
        self.logger.info(f"开始执行任务: {self.name} (类型: {self.task_type})")

        try:
            if stop_event and stop_event.is_set():
                self.logger.warning(f"任务 {self.name} 在开始前被取消。")
                raise asyncio.CancelledError("任务在开始前被取消")

            await self.pre_execute(stop_event=stop_event)

            if stop_event and stop_event.is_set():
                raise asyncio.CancelledError("任务在 pre_execute 后被取消")

            # 获取数据
            self.logger.info(f"获取数据，参数: {kwargs}")
            data = await self.fetch_data(stop_event=stop_event, **kwargs)

            if stop_event and stop_event.is_set():
                raise asyncio.CancelledError("任务在 fetch_data 后被取消")

            if data is None or (isinstance(data, pd.DataFrame) and data.empty):
                self.logger.info("没有获取到数据")
                return {"status": "no_data", "rows": 0}

            # 处理数据
            self.logger.info(f"处理数据，共 {len(data) if isinstance(data, pd.DataFrame) else '多源'} 行")
            data = self.process_data(data, stop_event=stop_event, **kwargs)

            if stop_event and stop_event.is_set():
                raise asyncio.CancelledError("任务在 process_data 后被取消")

            # 再次检查处理后的数据是否为空
            if data is None or (isinstance(data, pd.DataFrame) and data.empty):
                self.logger.warning("数据处理后为空")
                return {"status": "no_data", "rows": 0}

            # 验证数据
            self.logger.info("验证数据")
            validation_passed = self.validate_data(data)
            if not validation_passed:
                self.logger.warning("数据验证未通过，但将继续保存")

            # 保存数据
            self.logger.info(f"保存数据到表 {self.table_name}")
            result = await self.save_data(data, stop_event=stop_event)

            if not validation_passed and result.get("status") == "success":
                result["status"] = "partial_success"
                result["validation"] = False

            if stop_event and stop_event.is_set():
                raise asyncio.CancelledError("任务在 save_data 后被取消")

            # 后处理
            await self.post_execute(result, stop_event=stop_event)

            self.logger.info(f"任务执行完成: {result}")
            return result
            
        except asyncio.CancelledError:
            self.logger.warning(f"任务 {self.name} 被取消。")
            return self.handle_error(asyncio.CancelledError("任务被用户取消"))
        except Exception as e:
            self.logger.error(
                f"任务执行失败: 类型={type(e).__name__}, 错误={str(e)}", exc_info=True
            )
            return self.handle_error(e)

    # 新增：多表数据获取支持
    async def fetch_multiple_sources(self, source_configs, **kwargs):
        """支持从多个表获取数据，为processor任务提供"""
        if len(source_configs) == 1:
            # 单源，使用现有逻辑
            return await self.fetch_data(**kwargs)
        else:
            # 多源，调用子类实现
            return await self._fetch_from_multiple_tables(source_configs, **kwargs)

    async def _fetch_from_multiple_tables(self, source_configs, **kwargs):
        """从多个表获取数据，子类可重写此方法实现具体的多表查询逻辑"""
        raise NotImplementedError("多源查询需要子类实现")

    async def _get_latest_date(self):
        """获取数据库中该表的最新数据日期"""
        if not self.table_name or not self.date_column:
            return None

        try:
            query = f"SELECT MAX({self.date_column}) FROM {self.get_full_table_name()}"
            result = await self.db.fetch_one(query)

            if result and result[0]:
                latest_date = result[0]
                if isinstance(latest_date, str):
                    if "-" in latest_date:
                        latest_date = datetime.strptime(
                            latest_date, "%Y-%m-%d"
                        ).strftime("%Y%m%d")
                else:
                    latest_date = latest_date.strftime("%Y%m%d")

                return latest_date
        except Exception as e:
            self.logger.warning(f"获取最新日期失败: {str(e)}")

        return None

    async def pre_execute(self, stop_event: Optional[asyncio.Event] = None, **kwargs):
        """任务执行前的准备工作"""
        pass

    async def post_execute(self, result, stop_event: Optional[asyncio.Event] = None, **kwargs):
        """任务执行后的清理工作"""
        pass

    def handle_error(self, error):
        """处理任务执行过程中的错误"""
        if isinstance(error, asyncio.CancelledError):
            return {"status": "cancelled", "error": str(error), "task": self.name}
        return {"status": "error", "error": str(error), "task": self.name}

    @abstractmethod
    async def fetch_data(self, stop_event: Optional[asyncio.Event] = None, **kwargs):
        """获取原始数据（子类必须实现）"""
        raise NotImplementedError

    def process_data(self, data, stop_event: Optional[asyncio.Event] = None, **kwargs):
        """处理原始数据"""
        # 支持数据转换
        if hasattr(self, "transformations") and self.transformations:
            # 检查data是否为DataFrame（processor可能返回字典）
            if isinstance(data, pd.DataFrame):
                for column, transform in self.transformations.items():
                    if column in data.columns:
                        try:
                            # 先将所有None值转换为np.nan，确保一致的缺失值处理
                            data[column] = data[column].fillna(np.nan)
                            
                            # 检查列是否包含nan值
                            has_nan = data[column].isna().any()

                            if has_nan:
                                self.logger.debug(f"列 {column} 包含缺失值(NaN)，将使用安全转换")

                                # 定义一个安全的转换函数，处理np.nan值
                                def safe_transform(x):
                                    if pd.isna(x):  # 检查是否为np.nan
                                        return np.nan  # 保持np.nan不变
                                    try:
                                        return transform(x)  # 应用原始转换函数
                                    except Exception as e:
                                        self.logger.warning(f"转换值 {x} 时发生错误: {str(e)}")
                                        return np.nan  # 转换失败时返回np.nan

                                # 使用安全转换函数
                                data[column] = data[column].apply(safe_transform)
                            else:
                                # 没有np.nan值，直接应用转换函数，但仍然捕获可能的异常
                                try:
                                    data[column] = data[column].apply(transform)
                                except Exception as e:
                                    self.logger.error(f"应用转换函数到列 {column} 时发生错误: {str(e)}")
                                    # 不中断处理，继续处理其他列
                        except Exception as e:
                            self.logger.error(f"处理列 {column} 时发生错误: {str(e)}")
                            # 不中断处理，继续处理其他列

        return data

    def validate_data(self, data):
        """验证数据有效性
        
        如果验证失败，会返回False，但不会抛出异常，以允许数据处理继续进行。
        开发者可以根据需要在子类中重写此方法，实现更严格的验证策略。
        
        Args:
            data (pd.DataFrame): 要验证的数据
            
        Returns:
            bool: 验证结果，True表示验证通过，False表示验证失败
        """
        if not hasattr(self, "validations") or not self.validations:
            return True
            
        validation_passed = True
        
        for validator in self.validations:
            try:
                # 检查验证器信息
                validator_name = (
                    validator.__name__ 
                    if hasattr(validator, "__name__") 
                    else "未命名验证器"
                )
                self.logger.debug(f"执行验证器: {validator_name}")
                
                validation_result = validator(data)
                if isinstance(validation_result, bool) and not validation_result:
                    self.logger.warning(f"数据验证失败: {validator_name}")
                    validation_passed = False
                    # 不立即退出，继续运行其他验证器以获取更多信息
            except Exception as e:
                self.logger.error(f"执行验证器时发生错误: {str(e)}")
                validation_passed = False
        
        if not validation_passed:
            self.logger.warning("数据验证未完全通过，但将继续处理")
            
            # 记录包含NaN值的列，帮助诊断
            if isinstance(data, pd.DataFrame):
                nan_columns = data.columns[data.isna().any()].tolist()
                if nan_columns:
                    self.logger.info(f"以下列包含NaN值，可能影响验证结果: {', '.join(nan_columns)}")
        
        return validation_passed

    async def save_data(self, data, stop_event: Optional[asyncio.Event] = None, **kwargs):
        """将处理后的数据保存到数据库"""
        await self._ensure_table_exists()
        if stop_event and stop_event.is_set():
            raise asyncio.CancelledError("任务在 _ensure_table_exists 后被取消")

        # 检查数据中的NaN值
        if isinstance(data, pd.DataFrame):
            # 将 'inf', '-inf' 替换为 NaN
            data.replace([np.inf, -np.inf], np.nan, inplace=True)
            
            nan_count = data.isna().sum().sum()
            if nan_count > 0:
                self.logger.warning(f"数据中包含 {nan_count} 个NaN值，这些值在保存到数据库时将被转换为NULL")
                cols_with_nan = data.columns[data.isna().any()].tolist()
                if cols_with_nan:
                    self.logger.debug(f"包含NaN值的列: {', '.join(cols_with_nan)}")

        # 将数据保存到数据库
        affected_rows = await self._save_to_database(data, stop_event=stop_event)
        if stop_event and stop_event.is_set():
            raise asyncio.CancelledError("任务在 _save_to_database 后被取消")

        return {"status": "success", "table": self.table_name, "rows": affected_rows}

    async def _ensure_table_exists(self, stop_event: Optional[asyncio.Event] = None, **kwargs):
        """确保表存在，如果不存在则创建"""
        table_exists = await self.db.table_exists(self)
        
        if stop_event and stop_event.is_set():
            raise asyncio.CancelledError("任务在检查表存在后被取消")

        if not table_exists:
            # 只有在表不存在时才检查 schema_def，避免不必要的属性检查
            if not hasattr(self, "schema_def") or not self.schema_def:
                self.logger.error(f"表 '{self.table_name}' 不存在且任务未定义 schema_def，无法自动创建。")
                raise ValueError(f"无法创建表 {self.table_name}，未定义 schema_def")

            self.logger.info(f"表 '{self.table_name}' 在 schema '{self.data_source or 'public'}' 中不存在，正在创建...")
            await self._create_table(stop_event=stop_event)

    async def _create_table(self, stop_event: Optional[asyncio.Event] = None, **kwargs):
        """通过调用DB Manager中的方法来创建数据表和索引"""
        await self.db.create_table_from_schema(self)
        if stop_event and stop_event.is_set():
            raise asyncio.CancelledError("任务在 _create_table 后被取消")

    async def _save_to_database(self, data, stop_event: Optional[asyncio.Event] = None, **kwargs):
        """将DataFrame保存到数据库（提供给子类重写的高级接口）"""
        
        if not self.primary_keys:
            self.logger.warning(f"任务 {self.name} 未定义主键 (primary_keys)，将使用简单的 COPY 插入，可能导致重复数据。")
            return await self.db.copy_from_dataframe(df=data, target=self)

        df_columns = list(data.columns)
        update_columns = [
            col for col in df_columns if col not in self.primary_keys
        ]
        
        try:
            affected_rows = await self.db.upsert(
                df=data,
                target=self,
                conflict_columns=self.primary_keys,
                update_columns=update_columns,
                timestamp_column="update_time" if self.auto_add_update_time else None,
            )
            return affected_rows
        except Exception as e:
            self.logger.error(f"保存数据到表 {self.table_name} 时发生错误: {str(e)}", exc_info=True)
            raise

    # --- 新增和重构的辅助方法 ---
    async def get_latest_date_for_task(self) -> Optional[datetime.date]:
        """获取当前任务的最新数据日期。"""
        if not self.date_column:
            self.logger.warning(
                f"任务 {self.name} 未定义 date_column，无法获取最新日期。"
            )
            return None
        return await self.db.get_latest_date(self, self.date_column)

    def get_full_table_name(self) -> str:
        """获取包含schema的完整表名。
        
        Returns:
            str: 格式为 '"schema_name"."table_name"' 的完整表名。
        """
        from ..db_components.table_name_resolver import TableNameResolver
        resolver = TableNameResolver()
        return resolver.get_full_name(self)

    async def get_all_codes_from_db(self, code_column: Optional[str] = None) -> List[str]:
        """从数据库中获取所有已存储的代码。
        
        Args:
            code_column (Optional[str]): 要查询的代码列名。如果为None，则使用任务的 self.code_column 属性。
        """
        target_code_column = code_column or getattr(self, 'code_column', None)
        if not target_code_column:
            self.logger.warning(
                f"任务 {self.name} 未定义 code_column 属性，无法从数据库获取代码列表。"
            )
            return []
        
        codes = await self.db.get_distinct_values(self, target_code_column)
        return codes

    async def smart_incremental_update(
        self,
        stop_event: Optional[asyncio.Event] = None,
        lookback_days=None,
        end_date=None,
        use_trade_days=False,
        safety_days=1,
        **kwargs,
    ):
        """智能增量更新方法"""
        self.logger.info(f"开始执行 {self.name} 的智能增量更新")

        if stop_event and stop_event.is_set():
            self.logger.warning(f"智能增量更新 {self.name} 在开始前被取消。")
            raise asyncio.CancelledError("智能增量更新在开始前被取消")

        # 对于Basic类型任务（date_column=None），直接执行全量更新，避免日期逻辑
        if self.date_column is None:
            self.logger.info(f"任务 {self.name} 为Basic类型任务（date_column=None），将直接执行全量更新")
            
            try:
                # 确保表存在
                await self._ensure_table_exists()
                if stop_event and stop_event.is_set():
                    raise asyncio.CancelledError("任务在确保表存在后被取消")
                
                # 直接调用get_batch_list获取批次，避免日期逻辑
                self.logger.info(f"任务 {self.name}: 调用 get_batch_list 进行全量获取")
                batch_list = await self.get_batch_list(stop_event=stop_event, **kwargs)
                
                if not batch_list:
                    self.logger.info(f"任务 {self.name}: get_batch_list 返回空列表，无数据需要处理")
                    return {"status": "no_data", "task": self.name, "rows": 0, "message": "批处理列表为空"}
                
                total_rows = 0
                total_batches = len(batch_list)
                self.logger.info(f"任务 {self.name}: 生成了 {total_batches} 个批处理任务")
                
                # 处理每个批次
                for i, batch_params in enumerate(batch_list):
                    if stop_event and stop_event.is_set():
                        raise asyncio.CancelledError(f"任务在处理第 {i+1} 个批次前被取消")
                    
                    self.logger.info(f"批次 {i+1}/{total_batches} (任务 {self.name}): 开始获取数据")
                    
                    # 获取数据 - 直接调用fetch_batch避免fetch_data的复杂逻辑
                    data = await self.fetch_batch(batch_params)
                    if data is None or (hasattr(data, 'empty') and data.empty):
                        self.logger.info(f"批次 {i+1}/{total_batches}: 无数据")
                        continue
                    
                    self.logger.info(f"批次 {i+1}/{total_batches} (任务 {self.name}): 获取数据成功 ({len(data)} 行)")
                    
                    # 处理数据
                    self.logger.info(f"批次 {i+1}/{total_batches} (任务 {self.name}): 处理 {len(data)} 行数据")
                    # 检查process_data方法是否为async方法
                    import inspect
                    if inspect.iscoroutinefunction(self.process_data):
                        # async方法
                        processed_data = await self.process_data(data, stop_event=stop_event)
                    else:
                        # 同步方法  
                        processed_data = self.process_data(data, stop_event=stop_event)
                    
                    # 验证数据
                    self.logger.info(f"批次 {i+1}/{total_batches} (任务 {self.name}): 验证处理后的数据 ({len(processed_data)} 行)")
                    # 检查validate_data方法是否为async方法
                    if inspect.iscoroutinefunction(self.validate_data):
                        # async方法
                        validated_data = await self.validate_data(processed_data)
                        validation_passed = True  # 如果没有抛出异常，则验证通过
                        processed_data = validated_data  # 使用验证后的数据
                    else:
                        # 同步方法（返回布尔值）
                        validation_passed = self.validate_data(processed_data)
                    
                    if not validation_passed:
                        self.logger.warning(f"批次 {i+1}/{total_batches}: 数据验证失败，跳过此批次")
                        continue
                    
                    self.logger.info(f"批次 {i+1}/{total_batches} (任务 {self.name}): 数据处理和验证完成，得到 {len(processed_data)} 行有效数据。")
                    
                    # 保存数据
                    self.logger.info(f"批次 {i+1}/{total_batches} (任务 {self.name}): 保存 {len(processed_data)} 行数据到表 {self.table_name}")
                    save_result = await self.save_data(processed_data)
                    batch_rows = save_result.get("rows", 0)
                    total_rows += batch_rows
                    
                    self.logger.info(f"批次 {i+1}/{total_batches} (任务 {self.name}): DB操作调度成功，处理了 {batch_rows} 行 (COPY到临时表)。")
                
                # 记录最终状态
                self.logger.info(f"任务 {self.name} 最终处理完成，状态: success, 总行数: {total_rows}, 失败/取消批次数: 0")
                self.logger.info(f"Basic任务全量更新完成: 处理 {total_rows} 行数据")
                return {
                    "status": "success",
                    "task": self.name,
                    "rows": total_rows,
                    "message": f"Basic任务全量更新完成，共处理 {total_batches} 个批次"
                }
                
            except asyncio.CancelledError as e:
                self.logger.warning(f"Basic任务全量更新被取消: {str(e)}")
                return {"status": "cancelled", "task": self.name, "rows": 0, "error": str(e)}
            except Exception as e:
                self.logger.error(f"Basic任务全量更新失败: {str(e)}", exc_info=True)
                return {"status": "error", "task": self.name, "rows": 0, "error": str(e)}

        # 确定结束日期
        if end_date is None:
            current_time = datetime.now()
            if current_time.hour >= 18:
                end_date = current_time.strftime("%Y%m%d")
                self.logger.info(f"未指定结束日期，当前时间晚于18:00，使用当前日期: {end_date}")
            else:
                yesterday = current_time - timedelta(days=1)
                end_date = yesterday.strftime("%Y%m%d")
                self.logger.info(f"未指定结束日期，当前时间早于18:00，使用昨天日期: {end_date}")

        # 获取数据库中最新的数据日期
        latest_date = await self._get_latest_date()
        start_date = None

        if use_trade_days:
            # 使用交易日模式 - 需要从原fetchers模块导入
            try:
                from ...fetchers.tools.calendar import get_last_trade_day, get_next_trade_day

                if lookback_days is not None and lookback_days > 0:
                    try:
                        start_date = await get_last_trade_day(end_date, n=lookback_days)
                        self.logger.info(f"按回溯 {lookback_days} 个交易日计算，起始日期为: {start_date}")
                    except Exception as e:
                        self.logger.error(f"计算交易日回溯失败: {str(e)}")
                        return {"status": "error", "error": f"交易日计算失败: {str(e)}"}
                elif latest_date:
                    try:
                        start_date = await get_next_trade_day(latest_date)
                        self.logger.info(f"数据库最新日期: {latest_date}，下一个交易日: {start_date}")
                    except Exception as e:
                        self.logger.error(f"计算下一个交易日失败: {str(e)}")
                        return {"status": "error", "error": f"交易日计算失败: {str(e)}"}
                else:
                    start_date = self.default_start_date if hasattr(self, 'default_start_date') else "20200101"
                    self.logger.info(f"无历史数据，使用默认起始日期: {start_date}")
            except ImportError:
                self.logger.error("无法导入交易日历工具，回退到自然日模式")
                use_trade_days = False

        if not use_trade_days:
            # 使用自然日模式
            if lookback_days is not None and lookback_days > 0:
                end_date_obj = datetime.strptime(end_date, "%Y%m%d")
                start_date_obj = end_date_obj - timedelta(days=lookback_days + safety_days)
                start_date = start_date_obj.strftime("%Y%m%d")
                self.logger.info(f"按回溯 {lookback_days} 天计算（含安全天数 {safety_days}），起始日期为: {start_date}")
            elif latest_date:
                latest_date_obj = datetime.strptime(latest_date, "%Y%m%d")
                start_date_obj = latest_date_obj + timedelta(days=1) - timedelta(days=safety_days)
                start_date = start_date_obj.strftime("%Y%m%d")
                self.logger.info(f"数据库最新日期: {latest_date}，增量起始日期（含安全天数）: {start_date}")
            else:
                start_date = self.default_start_date if hasattr(self, 'default_start_date') else "20200101"
                self.logger.info(f"无历史数据，使用默认起始日期: {start_date}")

        # 执行更新 - 借鉴旧版本的安全检查逻辑
        if start_date and end_date:
            # 检查日期有效性
            if start_date > end_date:
                self.logger.info(f"起始日期 {start_date} 晚于结束日期 {end_date}，无需更新")
                return {"status": "no_update", "message": "无需更新，数据已是最新"}
            
            # 执行增量更新
            self.logger.info(f"最终执行更新范围: {start_date} 到 {end_date}")
            kwargs.update({"start_date": start_date, "end_date": end_date})
            
            result = await self.execute(stop_event=stop_event, smart_incremental=True, **kwargs)
            
            if result.get("status") == "no_data":
                self.logger.info("没有新数据需要更新")
            elif result.get("status") == "cancelled":
                self.logger.warning(f"增量更新被取消: {result.get('error')}")
            else:
                self.logger.info(f"增量更新完成: 新增/更新 {result.get('rows', 0)} 行数据")
            
            return result
        else:
            self.logger.warning("未能确定有效的更新日期范围，任务未执行")
            return {"status": "no_range", "rows": 0, "message": "无法确定有效的日期范围"}


# 为了向后兼容，创建Task的别名
Task = BaseTask 