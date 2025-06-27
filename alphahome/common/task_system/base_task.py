import asyncio
import inspect
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, date
from typing import Any, Callable, Dict, List, Optional, Union, Tuple

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
    timestamp_column_name: Optional[str] = "update_time" # 时间戳列名，None表示不使用
    data_source: Optional[str] = None  # 数据源标识（如'tushare', 'wind', 'jqdata'等）
    default_start_date: str = "20200101" # 默认起始日期
    transformations: Optional[Dict[str, Callable]] = None # 数据转换函数字典
    validations: Optional[List[Union[Callable, Tuple[Callable, str]]]] = None # 数据验证函数列表
    schema_def: Optional[Dict[str, Any]] = None # 表结构定义
    set_config: Optional[Callable[[Dict[str, Any]], None]] = None # 可选的任务配置方法

    # 新增：数据保存批次大小
    default_save_batch_size: int = 10000  # 默认每次保存 10000 行
    
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
        """
        执行任务的完整生命周期 (模板方法)。

        这是一个 final 方法，子类不应重写。
        子类应通过实现 _fetch_data, process_data 等钩子方法来定义具体行为。

        标准处理流程：
        1. 获取数据 (_fetch_data)
        2. 处理数据 (process_data -> _apply_transformations + 业务逻辑)
        3. 验证数据 (_validate_data)
        4. 保存数据 (_save_data)
        """
        self.logger.info(f"开始执行任务: {self.name} (类型: {self.task_type})")

        try:
            if stop_event and stop_event.is_set():
                self.logger.warning(f"任务 {self.name} 在开始前被取消。")
                raise asyncio.CancelledError("任务在开始前被取消")

            await self._pre_execute(stop_event=stop_event, **kwargs)

            if stop_event and stop_event.is_set():
                raise asyncio.CancelledError("任务在 _pre_execute 后被取消")

            # 获取数据
            self.logger.info(f"获取数据，参数: {kwargs}")
            data = await self._fetch_data(stop_event=stop_event, **kwargs)

            if stop_event and stop_event.is_set():
                raise asyncio.CancelledError("任务在 _fetch_data 后被取消")

            if data is None or (isinstance(data, pd.DataFrame) and data.empty):
                self.logger.info("没有获取到数据")
                return {"status": "no_data", "rows": 0}

            # 处理数据（模板方法模式）
            self.logger.info(f"处理数据，共 {len(data) if isinstance(data, pd.DataFrame) else '多源'} 行")
            processed_data = self.process_data(data, stop_event=stop_event, **kwargs)

            if stop_event and stop_event.is_set():
                raise asyncio.CancelledError("任务在 process_data 后被取消")

            # 再次检查处理后的数据是否为空
            if processed_data is None or (isinstance(processed_data, pd.DataFrame) and processed_data.empty):
                self.logger.warning("数据处理后为空")
                return {"status": "no_data", "rows": 0}

            # 验证数据（统一验证入口）
            self.logger.debug(f"验证数据，共 {len(processed_data) if isinstance(processed_data, pd.DataFrame) else '多源'} 行")
            validation_passed, validated_data, validation_details = self._validate_data(
                processed_data,
                stop_event=stop_event,
                validation_mode=getattr(self, 'validation_mode', 'report')
            )

            if stop_event and stop_event.is_set():
                raise asyncio.CancelledError("任务在 _validate_data 后被取消")

            # 使用验证后的数据（可能被过滤）
            final_data = validated_data

            # 保存数据
            self.logger.info(f"保存数据到表 {self.table_name}")
            save_result = await self._save_data(final_data, stop_event=stop_event)

            if stop_event and stop_event.is_set():
                raise asyncio.CancelledError("任务在 _save_data 后被取消")

            # 构建最终结果，包含验证详情
            final_result = {
                "status": "success",
                "table": self.table_name,
                "rows": save_result.get("rows", 0) if isinstance(save_result, dict) else 0
            }

            # 添加验证信息
            if not validation_passed:
                final_result["status"] = "partial_success"
                final_result["validation"] = False
                final_result["validation_warning"] = "数据验证未完全通过，请检查日志"
            else:
                final_result["validation"] = True

            # 添加验证详情
            final_result["validation_details"] = validation_details

            # 后处理
            await self._post_execute(final_result, stop_event=stop_event)

            self.logger.info(f"任务执行完成: {final_result}")
            return final_result
            
        except asyncio.CancelledError:
            self.logger.warning(f"任务 {self.name} 被取消。")
            return self._handle_error(asyncio.CancelledError("任务被用户取消"))
        except Exception as e:
            self.logger.error(
                f"任务执行失败: 类型={type(e).__name__}, 错误={str(e)}", exc_info=True
            )
            return self._handle_error(e)

    # 新增：多表数据获取支持
    async def fetch_multiple_sources(self, source_configs, **kwargs):
        """支持从多个表获取数据，为processor任务提供"""
        if len(source_configs) == 1:
            # 单源，使用现有逻辑
            return await self._fetch_data(**kwargs)
        else:
            # 多源，调用子类实现
            return await self._fetch_from_multiple_tables(source_configs, **kwargs)

    async def _fetch_from_multiple_tables(self, source_configs, **kwargs):
        """从多个表获取数据，子类可重写此方法实现具体的多表查询逻辑"""
        raise NotImplementedError("多源查询需要子类实现")



    async def _pre_execute(self, stop_event: Optional[asyncio.Event] = None, **kwargs):
        """任务执行前的准备工作"""
        pass

    async def _post_execute(self, result, stop_event: Optional[asyncio.Event] = None, **kwargs):
        """任务执行后的清理工作"""
        pass

    def _handle_error(self, error):
        """处理任务执行过程中的错误"""
        if isinstance(error, asyncio.CancelledError):
            return {"status": "cancelled", "error": str(error), "task": self.name}
        return {"status": "error", "error": str(error), "task": self.name}

    @abstractmethod
    async def _fetch_data(self, stop_event: Optional[asyncio.Event] = None, **kwargs):
        """获取原始数据（子类必须实现）"""
        raise NotImplementedError

    def _apply_transformations(self, data, stop_event: Optional[asyncio.Event] = None, **kwargs):
        """
        应用通用的数据转换（私有方法，不允许子类重写）

        该方法专门负责应用基础的数据转换，包括：
        - 数据类型转换（transformations）
        - 列名映射（column_mapping）
        - 其他通用的数据标准化操作

        Args:
            data: 原始数据
            stop_event: 停止事件（可选）
            **kwargs: 额外参数

        Returns:
            处理后的数据
        """
        if data is None:
            return data

        # 支持数据转换
        if hasattr(self, "transformations") and self.transformations:
            # 检查data是否为DataFrame（processor可能返回字典）
            if isinstance(data, pd.DataFrame):
                self.logger.debug(f"任务 {self.name}: 开始应用 {len(self.transformations)} 个数据转换规则")

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

                self.logger.debug(f"任务 {self.name}: 数据转换完成")

        return data

    def process_data(self, data, stop_event: Optional[asyncio.Event] = None, **kwargs):
        """
        处理从 _fetch_data 获取的原始数据。
        这是数据处理的主要扩展点，子类可以重写此方法以实现自定义处理逻辑。

        默认实现会调用 _apply_transformations 来应用基础转换。

        子类重写时应该：
        1. 调用 super().process_data() 来应用基础转换
        2. 添加特定的业务逻辑处理
        3. 返回处理后的数据

        Args:
            data: 原始数据
            stop_event: 停止事件（可选）
            **kwargs: 额外参数

        Returns:
            处理后的数据
        """
        # 首先应用基础转换
        data = self._apply_transformations(data, stop_event=stop_event, **kwargs)

        # 默认实现不添加额外处理，子类可以重写此方法添加特定逻辑
        return data

    def _validate_data(self, data, stop_event: Optional[asyncio.Event] = None, validation_mode: str = "report"):
        """
        统一的数据验证方法（重构后的版本）

        该方法是数据验证的唯一入口点，支持两种验证模式：
        - report: 报告模式，记录验证结果但不过滤数据（默认）
        - filter: 过滤模式，移除不符合验证规则的数据行

        Args:
            data: 待验证的数据
            stop_event: 停止事件（可选）
            validation_mode: 验证模式 ("report" 或 "filter")

        Returns:
            tuple: (验证结果, 处理后的数据, 验证详情)
                - 验证结果 (bool): True表示验证通过
                - 处理后的数据: 根据验证模式可能被过滤
                - 验证详情 (dict): 包含验证状态和失败信息
        """
        if not hasattr(self, "validations") or not self.validations:
            self.logger.debug(f"任务 {self.name} 未定义验证规则，跳过验证")
            return True, data, {"status": "no_rules", "failed_validations": {}}

        # 获取验证模式配置（任务可以重写默认模式）
        task_validation_mode = getattr(self, 'validation_mode', validation_mode)

        if isinstance(data, pd.DataFrame):
            return self._validate_dataframe(data, stop_event, task_validation_mode)
        else:
            # 非DataFrame数据的验证（如字典等）
            return self._validate_non_dataframe(data, stop_event, task_validation_mode)


    def _validate_dataframe(self, data: pd.DataFrame, stop_event: Optional[asyncio.Event], validation_mode: str):
        """
        验证DataFrame数据的具体实现

        Args:
            data: DataFrame数据
            stop_event: 停止事件
            validation_mode: 验证模式 ("report" 或 "filter")

        Returns:
            tuple: (验证结果, 处理后的数据, 验证详情)
        """
        total_rows = len(data)
        validation_passed = True
        failed_validation_details = {}

        # 用于过滤模式的掩码
        valid_mask = pd.Series(True, index=data.index) if validation_mode == "filter" else None

        self.logger.info(f"开始验证数据，共 {len(self.validations)} 个验证规则，数据行数: {total_rows}，模式: {validation_mode}") # type: ignore

        for i, validation_item in enumerate(self.validations): # type: ignore
            if stop_event and stop_event.is_set():
                self.logger.warning("验证在执行期间被取消")
                raise asyncio.CancelledError("验证取消")

            # 解析验证规则（支持新旧格式）
            if isinstance(validation_item, tuple) and len(validation_item) == 2:
                validator, validator_name = validation_item
            else:
                validator = validation_item
                validator_name = f"验证规则_{i+1}"

            try:
                self.logger.debug(f"执行验证器: \"{validator_name}\"")

                # 根据验证模式选择数据源
                validation_data = data[valid_mask] if validation_mode == "filter" and valid_mask is not None else data
                validation_result = validator(validation_data) # type: ignore

                if isinstance(validation_result, pd.Series) and validation_result.dtype == bool:
                    if not validation_result.all():
                        failed_count = (~validation_result).sum()
                        failed_percentage = (failed_count / len(validation_data)) * 100 if len(validation_data) > 0 else 0

                        self.logger.warning(
                            f"  - 验证失败: \"{validator_name}\"，"
                            f"失败行数: {failed_count}/{len(validation_data)} ({failed_percentage:.2f}%)"
                        )
                        failed_validation_details[validator_name] = f"{failed_count}行失败"
                        validation_passed = False

                        # 在过滤模式下更新掩码
                        if validation_mode == "filter" and valid_mask is not None:
                            # 需要将validation_result对齐到原始数据的索引
                            aligned_result = pd.Series(True, index=data.index)
                            aligned_result.loc[validation_data.index] = validation_result
                            valid_mask &= aligned_result

                elif isinstance(validation_result, bool) and not validation_result:
                    self.logger.warning(f"  - 验证失败: \"{validator_name}\" (返回False，影响整批数据)")
                    failed_validation_details[validator_name] = "整批失败"
                    validation_passed = False

                    # 在过滤模式下，整批失败意味着所有数据都无效
                    if validation_mode == "filter" and valid_mask is not None:
                        valid_mask[:] = False

            except Exception as e:
                self.logger.error(f"  - 执行验证器 \"{validator_name}\" 时发生错误: {e}", exc_info=True)
                failed_validation_details[validator_name] = "执行错误"
                validation_passed = False

        # 根据验证模式处理结果
        if validation_mode == "filter" and valid_mask is not None:
            filtered_data = data[valid_mask].copy()
            filtered_count = len(filtered_data)

            if filtered_count < total_rows:
                self.logger.warning(f"过滤模式: 移除了 {total_rows - filtered_count} 行不符合验证规则的数据")

            result_data = filtered_data
        else:
            result_data = data

        # 输出验证结果摘要
        if validation_passed:
            self.logger.info(f"✅ 任务 {self.name} 数据验证通过 (通过 {len(self.validations)} 个验证规则)") # type: ignore
        else:
            self.logger.warning(f"⚠️ 任务 {self.name} 验证未完全通过，失败详情: {failed_validation_details}")
            if isinstance(data, pd.DataFrame):
                nan_columns = data.columns[data.isna().any()].tolist()
                if nan_columns:
                    self.logger.info(f"以下列包含NaN值，可能影响验证结果: {', '.join(nan_columns)}")

        return validation_passed, result_data, {
            "status": "passed" if validation_passed else "failed",
            "failed_validations": failed_validation_details,
            "original_rows": total_rows,
            "result_rows": len(result_data) if isinstance(result_data, pd.DataFrame) else total_rows,
            "validation_mode": validation_mode
        }

    def _validate_non_dataframe(self, data, stop_event: Optional[asyncio.Event], validation_mode: str):
        """
        验证非DataFrame数据的具体实现

        Args:
            data: 非DataFrame数据
            stop_event: 停止事件
            validation_mode: 验证模式

        Returns:
            tuple: (验证结果, 处理后的数据, 验证详情)
        """
        validation_passed = True
        failed_validation_details = {}

        self.logger.info(f"开始验证非DataFrame数据，共 {len(self.validations)} 个验证规则，模式: {validation_mode}") # type: ignore

        for i, validation_item in enumerate(self.validations): # type: ignore
            if stop_event and stop_event.is_set():
                raise asyncio.CancelledError("验证取消")

            # 解析验证规则
            if isinstance(validation_item, tuple) and len(validation_item) == 2:
                validator, validator_name = validation_item
            else:
                validator = validation_item
                validator_name = f"验证规则_{i+1}"

            try:
                validation_result = validator(data) # type: ignore
                if not validation_result:
                    self.logger.warning(f"  - 验证失败: \"{validator_name}\"")
                    failed_validation_details[validator_name] = "验证失败"
                    validation_passed = False
            except Exception as e:
                self.logger.error(f"  - 执行验证器 \"{validator_name}\" 时发生错误: {e}", exc_info=True)
                failed_validation_details[validator_name] = "执行错误"
                validation_passed = False

        return validation_passed, data, {
            "status": "passed" if validation_passed else "failed",
            "failed_validations": failed_validation_details,
            "validation_mode": validation_mode
        }

    async def _save_data(self, data: pd.DataFrame, stop_event: Optional[asyncio.Event] = None) -> Dict[str, Any]:
        """将处理后的数据保存到数据库"""
        await self._ensure_table_exists()
        if stop_event and stop_event.is_set():
            raise asyncio.CancelledError("任务在 _ensure_table_exists 后被取消")

        # 新增：在保存前根据主键去重
        if self.primary_keys and not data.empty:
            initial_rows = len(data)
            # 确保主键列存在
            valid_primary_keys = [pk for pk in self.primary_keys if pk in data.columns]
            if valid_primary_keys:
                data.drop_duplicates(subset=valid_primary_keys, keep='last', inplace=True)
                dropped_rows = initial_rows - len(data)
                if dropped_rows > 0:
                    self.logger.warning(f"数据中发现并移除了 {dropped_rows} 条基于主键 {valid_primary_keys} 的重复记录。")
            else:
                self.logger.warning("任务定义了主键，但在数据中未找到相应列，无法去重。")

        # 新增：主键字段空值检查和过滤
        if self.primary_keys and not data.empty:
            initial_rows = len(data)
            # 检查每个主键字段的空值情况
            null_mask = pd.Series(False, index=data.index)
            filtered_details = []
            
            for pk_col in self.primary_keys:
                if pk_col in data.columns:
                    # 检查NaN、None、空字符串
                    col_null_mask = (
                        data[pk_col].isna() | 
                        data[pk_col].isnull() | 
                        (data[pk_col] == '') |
                        (data[pk_col].astype(str).str.strip() == '')
                    )
                    col_null_count = col_null_mask.sum()
                    if col_null_count > 0:
                        null_mask |= col_null_mask
                        filtered_details.append(f"'{pk_col}': {col_null_count} 条")
                        self.logger.debug(f"主键字段 '{pk_col}' 发现 {col_null_count} 条空值记录")
            
            # 如果发现需要过滤的记录，执行过滤
            if null_mask.any():
                filtered_count = null_mask.sum()
                data = data[~null_mask].copy()
                data = data.reset_index(drop=True)
                
                self.logger.warning(
                    f"主键字段空值过滤: 移除了 {filtered_count} 条记录 "
                    f"(共 {initial_rows} 条)，详情: {', '.join(filtered_details)}"
                )
                self.logger.info(f"主键字段空值过滤完成: 剩余 {len(data)} 条有效记录")
            else:
                self.logger.debug("主键字段空值检查: 未发现需要过滤的记录")

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

        total_affected_rows = 0
        # 获取实际的保存批次大小
        save_batch_size = getattr(self, "save_batch_size", self.default_save_batch_size)

        if not data.empty and save_batch_size > 0 and len(data) > save_batch_size:
            self.logger.info(f"数据量较大 ({len(data)} 行)，将分批保存，每批 {save_batch_size} 行。")
            num_batches = (len(data) + save_batch_size - 1) // save_batch_size
            for i in range(num_batches):
                if stop_event and stop_event.is_set():
                    raise asyncio.CancelledError("任务在分批保存期间被取消")

                start_idx = i * save_batch_size
                end_idx = min((i + 1) * save_batch_size, len(data))
                batch_data = data.iloc[start_idx:end_idx].copy()
                
                self.logger.info(f"正在保存批次 {i + 1}/{num_batches} (行范围: {start_idx}-{end_idx-1})")
                affected_rows = await self._save_to_database(batch_data, stop_event=stop_event)
                total_affected_rows += affected_rows
        else:
            self.logger.info(f"数据量 ({len(data)} 行) 小于等于批次大小 ({save_batch_size} 行)，将一次性保存。")
            total_affected_rows = await self._save_to_database(data, stop_event=stop_event)

        if stop_event and stop_event.is_set():
            raise asyncio.CancelledError("任务在 _save_to_database 后被取消")

        # Construct final result
        final_result = {
            "status": "success",
            "table": self.table_name,
            "rows": total_affected_rows
        }

        return final_result

    async def _ensure_table_exists(self, stop_event: Optional[asyncio.Event] = None, **kwargs):
        """确保数据库表存在，如果不存在则创建"""
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