import asyncio
import inspect
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, date
from typing import Any, Callable, Dict, List, Optional, Union

import numpy as np
import pandas as pd
from ..db_manager import DBManager


class BaseTask(ABC):
    """统一数据任务基类
    
    支持数据采集(fetch)和数据处理(processor)两种任务类型的统一框架。
    为所有任务提供基础功能，包括数据获取、处理、验证和保存。
    """

    # 任务类型标识 (例如 'fetch', 'processor', 'derivative' 等)
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
    default_start_date: str = "20200101" # 默认起始日期
    transformations: Optional[Dict[str, Callable]] = None # 数据转换函数字典
    validations: Optional[List[Callable]] = None # 数据验证函数列表
    schema_def: Optional[Dict[str, Any]] = None # 表结构定义
    set_config: Optional[Callable[[Dict[str, Any]], None]] = None # 可选的任务配置方法
    
    # 新增：支持processor任务的属性
    source_tables = []        # 源数据表列表（processor任务使用）
    dependencies = []         # 依赖的其他任务（processor任务使用）

    def __init__(self, db_connection, **kwargs):
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

    async def fetch_data(self, stop_event: Optional[asyncio.Event] = None, **kwargs):
        """获取原始数据（子类必须实现）"""
        raise NotImplementedError

    async def get_batch_list(self, stop_event: Optional[asyncio.Event] = None, **kwargs) -> List[Dict]:
        """获取批处理任务列表（子类必须实现）"""
        self.logger.warning(f"任务 {self.name} (类型: {self.task_type}) 未实现 get_batch_list 方法。")
        raise NotImplementedError(f"任务 {self.name} 不支持批处理列表生成。")

    async def fetch_batch(self, batch_params: Dict, stop_event: Optional[asyncio.Event] = None, **kwargs) -> Optional[pd.DataFrame]:
        """获取单个批次的数据（子类必须实现）"""
        self.logger.warning(f"任务 {self.name} (类型: {self.task_type}) 未实现 fetch_batch 方法。")
        raise NotImplementedError(f"任务 {self.name} 不支持批处理数据获取。")

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
                            explicit_boolean_for_nan_check: bool = (data[column].isna().any() == True)
                            if explicit_boolean_for_nan_check:
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
    async def get_latest_date_for_task(self) -> Optional[date]:
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




# 为了向后兼容，创建Task的别名
Task = BaseTask 