#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据处理任务基类

定义了具体数据处理任务的基础接口。
任务层负责编排一个或多个原子操作来完成复杂的处理逻辑。

数据分层架构（Requirements 8.1-8.5）：
- 支持 fetch → clean → feature → save 流程
- 支持 skip_features 参数跳过特征计算
- 支持 feature_dependencies 声明特征依赖
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union
import pandas as pd
import asyncio
import uuid
from datetime import datetime, timezone

from ...common.task_system.base_task import BaseTask
from ...common.logging_utils import get_logger

# Clean Layer 组件导入
from ..clean import (
    TableSchema,
    ValidationResult,
    DataValidator,
    ValidationError,
    DataAligner,
    AlignmentError,
    DataStandardizer,
    StandardizationError,
    LineageTracker,
    LineageError,
)


class ProcessorTaskBase(BaseTask, ABC):
    """
    数据处理任务基类
    
    这是所有处理器任务的统一基类，负责定义一个完整的业务处理流程。
    它编排了数据获取、处理和保存的整个过程。
    
    子类需要实现的核心方法：
    1. `fetch_data`: 定义从哪里获取源数据。
    2. `process_data`: 定义如何通过编排一个或多个`Operation`来处理数据。
    3. `save_result`: 定义如何保存处理后的结果。
    
    数据血缘追踪属性：
    - `source_tables`: 源数据表列表，用于追踪数据来源
    - `table_name`: 目标数据表名，用于标识输出位置
    
    数据分层属性（Requirements 8.1, 8.4, 8.5）：
    - `clean_table`: clean schema 目标表名
    - `feature_dependencies`: 依赖的特征函数列表
    - `skip_features`: 是否跳过特征计算
    
    示例:
    ```python
    from .operations import FillNAOperation, MovingAverageOperation
    from .operations.base_operation import OperationPipeline
    
    @task_register()
    class MyProcessorTask(ProcessorTaskBase):
        name = "my_processor"
        table_name = "my_result_table"
        source_tables = ["stock_daily", "stock_basic"]
        clean_table = "clean.my_clean_table"  # clean schema 目标表
        feature_dependencies = ["rolling_zscore", "rolling_percentile"]  # 特征依赖
        
        async def fetch_data(self, **kwargs) -> pd.DataFrame:
            ... # 实现数据获取逻辑
        
        async def process_data(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
            # Task内部负责编排Operations
            pipeline = OperationPipeline(name="MyInternalPipeline")
            pipeline.add_operation(FillNAOperation(method='ffill'))
            pipeline.add_operation(MovingAverageOperation(window=5))
            
            processed_data = await pipeline.apply(data)
            return processed_data
            
        async def save_result(self, data: pd.DataFrame, **kwargs):
            ... # 实现结果保存逻辑
    ```
    """
    
    # 任务类型标识
    task_type: str = "processor"
    
    # 数据血缘追踪属性（显式定义，覆盖 BaseTask 的默认值）
    source_tables: List[str] = []  # 源数据表列表
    table_name: str = ""  # 目标数据表名
    
    # 处理器任务特有属性
    calculation_method: Optional[str] = None  # 计算方法标识
    
    # 数据分层属性（Requirements 8.1, 8.4, 8.5）
    clean_table: str = ""  # clean schema 目标表名
    feature_dependencies: List[str] = []  # 依赖的特征函数列表
    skip_features: bool = False  # 是否跳过特征计算（默认不跳过）
    
    def __init__(self, db_connection=None, **kwargs):
        """初始化处理任务"""
        super().__init__(db_connection=db_connection, **kwargs)
        
        if not self.source_tables:
            self.logger.warning(f"处理任务 {self.name} 未定义 source_tables")

    async def _fetch_data(self, stop_event: Optional[asyncio.Event] = None, **kwargs) -> Optional[pd.DataFrame]:
        """
        获取数据（内部实现）。
        
        这是 BaseTask._fetch_data 的具体实现，它会调用子类定义的 fetch_data。
        """
        self.logger.info(f"从源表获取数据: {self.source_tables}")
        return await self.fetch_data(**kwargs)

    @abstractmethod
    async def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        获取数据的抽象方法。
        
        子类必须实现此方法来定义具体的数据获取逻辑，例如从多个数据源查询并合并数据。
        
        Args:
            **kwargs: 数据获取参数
            
        Returns:
            Optional[pd.DataFrame]: 获取并准备好的数据。
        """
        raise NotImplementedError("子类必须实现 fetch_data 方法")

    @abstractmethod
    async def process_data(self, data: pd.DataFrame, stop_event: Optional[asyncio.Event] = None, **kwargs) -> Optional[pd.DataFrame]:
        """
        处理数据的抽象方法。
        
        子类必须实现此方法，通过编排一个或多个`Operation`来定义核心处理逻辑。
        
        Args:
            data: 从 fetch_data 获取的源数据
            stop_event: 停止事件（可选）
            **kwargs: 额外参数
            
        Returns:
            Optional[pd.DataFrame]: 处理后的数据
        """
        raise NotImplementedError("子类必须实现 process_data 方法")

    async def _save_data(self, data: pd.DataFrame, stop_event: Optional[asyncio.Event] = None, **kwargs):
        """
        保存处理结果（内部实现）。
        
        这是 BaseTask._save_data 的具体实现，它会调用子类定义的 save_result。
        """
        if data is None or data.empty:
            self.logger.warning("没有数据需要保存")
            return
            
        if not hasattr(self, 'table_name') or not self.table_name:
            self.logger.warning("未定义 table_name，无法保存结果")
            return
        
        self.logger.info(f"保存结果到表: {self.table_name}，行数: {len(data)}")
        await self.save_result(data, **kwargs)


    @abstractmethod
    async def save_result(self, data: pd.DataFrame, **kwargs):
        """
        保存处理结果的抽象方法。
        
        子类必须实现此方法来定义具体的结果保存逻辑。
        
        Args:
            data: 要保存的数据。
            **kwargs: 保存参数。
        """
        raise NotImplementedError("子类必须实现 save_result 方法")

    # =========================================================================
    # 数据分层方法（Requirements 8.1-8.5）
    # =========================================================================

    async def clean_data(
        self, 
        data: pd.DataFrame, 
        stop_event: Optional[asyncio.Event] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        清洗数据（Requirements 8.1）。
        
        组合以下 Clean Layer 组件：
        1. DataValidator.validate() - 校验
        2. DataAligner.align_date() + align_identifier() - 对齐
        3. DataStandardizer.convert_*() - 标准化
        4. LineageTracker.add_lineage() - 添加血缘
        
        子类可覆盖以自定义清洗逻辑。
        
        Args:
            data: 从 fetch_data 获取的原始数据
            stop_event: 停止事件（可选）
            **kwargs: 额外参数，支持：
                - schema: TableSchema 实例（可选，用于校验）
                - date_col: 日期列名（可选，用于对齐）
                - identifier_col: 标的列名（可选，用于对齐）
                - primary_keys: 主键列表（可选，用于去重）
                - monetary_cols: 货币列转换配置（可选）
                - volume_cols: 成交量列转换配置（可选）
                - price_cols: 价格列（可选，用于保留未复权价格）
                - job_id: 任务执行ID（可选，用于血缘追踪）
            
        Returns:
            pd.DataFrame: 清洗后的数据
            
        Raises:
            ValidationError: 当数据校验失败时
            AlignmentError: 当数据对齐失败时
        """
        if data is None or data.empty:
            self.logger.warning("clean_data: 输入数据为空")
            return data
        
        self.logger.info(f"开始清洗数据，输入行数: {len(data)}")
        result = data.copy()
        
        # 1. 数据校验（如果提供了 schema）
        schema = kwargs.get('schema')
        if schema is not None:
            result = await self._validate_clean_data(result, schema, **kwargs)
        
        # 2. 数据对齐
        result = await self._align_clean_data(result, **kwargs)
        
        # 3. 数据标准化
        result = await self._standardize_clean_data(result, **kwargs)
        
        # 4. 添加血缘元数据
        result = await self._add_lineage(result, **kwargs)
        
        self.logger.info(f"数据清洗完成，输出行数: {len(result)}")
        return result
    
    async def _validate_clean_data(
        self, 
        data: pd.DataFrame, 
        schema: TableSchema,
        **kwargs
    ) -> pd.DataFrame:
        """
        校验数据（内部方法）。
        
        Args:
            data: 待校验的数据
            schema: 表 schema 定义
            **kwargs: 额外参数
            
        Returns:
            校验后的数据（可能添加了 _validation_flag 列）
            
        Raises:
            ValidationError: 当校验失败时
        """
        validator = DataValidator(schema)
        result = validator.validate(data)
        
        if not result.is_valid:
            error_summary = result.get_error_summary()
            self.logger.error(f"数据校验失败: {error_summary}")
            raise ValidationError(f"数据校验失败: {error_summary}", result)
        
        # 处理重复记录
        primary_keys = kwargs.get('primary_keys', self.primary_keys)
        if primary_keys:
            duplicates = validator.detect_duplicates(data, primary_keys)
            if len(duplicates) > 0:
                self.logger.warning(f"检测到 {len(duplicates)} 条重复记录，将保留最新记录")
                data, removed_count = validator.deduplicate(data, primary_keys, keep='last')
                self.logger.info(f"去重完成，移除 {removed_count} 条记录")
        
        # 处理超出范围的值
        if result.has_out_of_range_values():
            self.logger.warning(f"检测到 {len(result.out_of_range_rows)} 行超出有效范围")
            data = validator.add_validation_flag(data, result.out_of_range_rows)
        
        return data
    
    async def _align_clean_data(
        self, 
        data: pd.DataFrame,
        **kwargs
    ) -> pd.DataFrame:
        """
        对齐数据（内部方法）。
        
        **异常语义**：
        对齐操作采用 best-effort 策略，遇到未知格式或部分列缺失时：
        - 记录 warning 日志
        - 尽量完成可处理的部分
        - 不抛出致命异常（除非显式配置 strict 模式）
        
        这种设计允许处理不完整或格式多样的数据源，同时保留可追溯的日志。
        
        Args:
            data: 待对齐的数据
            **kwargs: 额外参数
            
        Returns:
            对齐后的数据
        """
        aligner = DataAligner()
        result = data
        
        # 对齐日期列
        date_col = kwargs.get('date_col')
        if date_col and date_col in result.columns:
            try:
                result = aligner.align_date(result, date_col, target_col='trade_date')
            except Exception as e:
                self.logger.warning(f"日期对齐失败 (列: {date_col}): {e}，保留原值")
        
        # 对齐标的列
        identifier_col = kwargs.get('identifier_col')
        if identifier_col and identifier_col in result.columns:
            try:
                result = aligner.align_identifier(result, identifier_col, target_col='ts_code')
            except Exception as e:
                self.logger.warning(f"标的对齐失败 (列: {identifier_col}): {e}，保留原值")
        
        # 构建主键并去重
        primary_keys = kwargs.get('primary_keys', self.primary_keys)
        if primary_keys:
            # 检查主键列是否存在
            existing_keys = [k for k in primary_keys if k in result.columns]
            if existing_keys:
                try:
                    result = aligner.build_primary_key(result, existing_keys)
                except Exception as e:
                    self.logger.warning(f"主键构建失败 (列: {existing_keys}): {e}")
        
        return result
    
    async def _standardize_clean_data(
        self, 
        data: pd.DataFrame,
        **kwargs
    ) -> pd.DataFrame:
        """
        标准化数据（内部方法）。
        
        **异常语义**：
        标准化操作采用 best-effort 策略，遇到未知单位或部分列缺失时：
        - 记录 warning 日志
        - 跳过无法处理的列
        - 不抛出致命异常
        
        这种设计允许处理单位不统一或列定义不完整的数据源，同时保留可追溯的日志。
        
        Args:
            data: 待标准化的数据
            **kwargs: 额外参数
            
        Returns:
            标准化后的数据
        """
        standardizer = DataStandardizer()
        result = data
        
        # 转换货币列
        monetary_cols = kwargs.get('monetary_cols')
        if monetary_cols:
            for col, unit in monetary_cols.items():
                if col in result.columns:
                    try:
                        result = standardizer.convert_monetary(result, col, unit)
                    except Exception as e:
                        self.logger.warning(f"货币转换失败 (列: {col}, 单位: {unit}): {e}，跳过")
        
        # 转换成交量列
        volume_cols = kwargs.get('volume_cols')
        if volume_cols:
            for col, unit in volume_cols.items():
                if col in result.columns:
                    try:
                        result = standardizer.convert_volume(result, col, unit)
                    except Exception as e:
                        self.logger.warning(f"成交量转换失败 (列: {col}, 单位: {unit}): {e}，跳过")
        
        # 保留未复权价格
        price_cols = kwargs.get('price_cols')
        if price_cols:
            existing_price_cols = [c for c in price_cols if c in result.columns]
            if existing_price_cols:
                try:
                    result = standardizer.preserve_unadjusted(result, existing_price_cols)
                except Exception as e:
                    self.logger.warning(f"未复权价格保留失败 (列: {existing_price_cols}): {e}")
        
        return result
    
    async def _add_lineage(
        self, 
        data: pd.DataFrame,
        **kwargs
    ) -> pd.DataFrame:
        """
        添加血缘元数据（内部方法）。
        
        Args:
            data: 待添加血缘的数据
            **kwargs: 额外参数
            
        Returns:
            添加血缘后的数据
        """
        tracker = LineageTracker()
        
        # 获取源表列表
        source_tables = self.source_tables if self.source_tables else ['unknown']
        
        # 获取或生成 job_id
        job_id = kwargs.get('job_id')
        if not job_id:
            job_id = LineageTracker.generate_job_id(prefix=self.name or 'task')
        
        # 获取数据版本
        data_version = kwargs.get('data_version')
        
        result = tracker.add_lineage(
            data,
            source_tables=source_tables,
            job_id=job_id,
            data_version=data_version
        )
        
        return result

    async def compute_features(
        self, 
        data: pd.DataFrame, 
        stop_event: Optional[asyncio.Event] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        计算特征（Requirements 8.2, 8.3）。
        
        子类实现，调用 operations/transforms.py 中的函数。
        
        约束：
        - 必须通过 feature_dependencies 声明依赖的特征函数
        - 不得内嵌特征计算逻辑
        
        Args:
            data: 清洗后的数据
            stop_event: 停止事件（可选）
            **kwargs: 额外参数
            
        Returns:
            pd.DataFrame: 计算特征后的数据
        """
        # 默认实现不计算特征，子类可覆盖
        return data
    
    def _validate_feature_dependencies(self) -> None:
        """
        校验 feature_dependencies 中的函数是否存在于 operations 模块。
        
        Raises:
            ValueError: 当依赖的特征函数不存在时
        """
        if not self.feature_dependencies:
            return
        
        from ..operations import transforms
        
        missing_deps = []
        for dep in self.feature_dependencies:
            if not hasattr(transforms, dep):
                missing_deps.append(dep)
        
        if missing_deps:
            raise ValueError(
                f"Unknown feature dependencies: {missing_deps}. "
                f"Available functions: {[name for name in dir(transforms) if not name.startswith('_')]}"
            )

    async def run(self, **kwargs) -> Dict[str, Any]:
        """
        任务执行入口点（增强版流程）。
        
        实现 fetch → clean → feature (optional) → save 流程（Requirements 8.1, 8.5）。
        
        流程说明：
        1. 获取数据 (fetch_data)
        2. 清洗数据 (clean_data) - 校验、对齐、标准化、添加血缘
        3. 保存 clean 数据（如果配置了 clean_table）
        4. 计算特征 (compute_features) - 可选，通过 skip_features 控制
        5. 保存特征结果（如果未跳过特征计算）
        
        保存目标：
        - skip_features=True: 仅保存到 clean_table（处理层）
        - skip_features=False: 保存到 clean_table 和 table_name（特征层）
        
        增量计算约定：
        - 增量计算时应回溯 max(window) 天以确保滚动计算正确
        - 子类可通过 kwargs 传递 lookback_days 参数
        
        Args:
            **kwargs: 执行参数，支持：
                - skip_features: 是否跳过特征计算（覆盖类属性）
                - stop_event: 停止事件
                - lookback_days: 增量计算回溯天数
                - 其他 clean_data/compute_features 参数
                
        Returns:
            Dict[str, Any]: 执行结果
        """
        stop_event = kwargs.get('stop_event')
        
        # 允许通过 kwargs 覆盖 skip_features
        skip_features = kwargs.get('skip_features', self.skip_features)
        
        self.logger.info(
            f"开始执行任务: {self.name} (类型: {self.task_type}, "
            f"skip_features={skip_features})"
        )
        
        try:
            # 0. 校验特征依赖（如果不跳过特征计算）
            if not skip_features and self.feature_dependencies:
                self._validate_feature_dependencies()
            
            # 1. 获取数据
            if stop_event and stop_event.is_set():
                self.logger.warning(f"任务 {self.name} 在开始前被取消")
                return {"status": "cancelled", "task": self.name}
            
            self.logger.info(f"从源表获取数据: {self.source_tables}")
            raw_data = await self.fetch_data(**kwargs)
            
            if raw_data is None or raw_data.empty:
                self.logger.info("没有获取到数据")
                return {"status": "no_data", "rows": 0}
            
            self.logger.info(f"获取到 {len(raw_data)} 行数据")
            
            # 2. 清洗数据
            if stop_event and stop_event.is_set():
                return {"status": "cancelled", "task": self.name}
            
            clean_data = await self.clean_data(raw_data, stop_event=stop_event, **kwargs)
            
            if clean_data is None or clean_data.empty:
                self.logger.warning("数据清洗后为空")
                return {"status": "no_data", "rows": 0}
            
            # 3. 保存 clean 数据（如果配置了 clean_table）
            clean_rows = 0
            if self.clean_table:
                if stop_event and stop_event.is_set():
                    return {"status": "cancelled", "task": self.name}
                
                self.logger.info(f"保存 clean 数据到表: {self.clean_table}，行数: {len(clean_data)}")
                clean_rows = await self._save_to_clean(clean_data, **kwargs)
            
            # 4. 计算特征（可选）
            if skip_features:
                self.logger.info("跳过特征计算 (skip_features=True)")
                return {
                    "status": "success",
                    "rows": clean_rows,
                    "clean_table": self.clean_table,
                    "skip_features": True
                }
            
            if stop_event and stop_event.is_set():
                return {"status": "cancelled", "task": self.name}
            
            feature_data = await self.compute_features(clean_data, stop_event=stop_event, **kwargs)
            
            if feature_data is None or feature_data.empty:
                self.logger.warning("特征计算后数据为空")
                return {
                    "status": "partial_success",
                    "rows": clean_rows,
                    "clean_table": self.clean_table,
                    "feature_rows": 0
                }
            
            # 5. 保存特征结果
            if stop_event and stop_event.is_set():
                return {"status": "cancelled", "task": self.name}
            
            self.logger.info(f"保存特征结果到表: {self.table_name}，行数: {len(feature_data)}")
            await self.save_result(feature_data, **kwargs)
            
            return {
                "status": "success",
                "rows": len(feature_data),
                "clean_table": self.clean_table,
                "clean_rows": clean_rows,
                "table_name": self.table_name
            }
            
        except asyncio.CancelledError:
            self.logger.warning(f"任务 {self.name} 被取消")
            return {"status": "cancelled", "task": self.name}
        except (ValidationError, AlignmentError) as e:
            self.logger.error(f"数据清洗失败: {e}")
            return {"status": "error", "error": str(e), "task": self.name}
        except Exception as e:
            self.logger.error(f"任务执行失败: {type(e).__name__}: {e}", exc_info=True)
            return {"status": "error", "error": str(e), "task": self.name}
    
    async def _save_to_clean(self, data: pd.DataFrame, **kwargs) -> int:
        """
        保存数据到 clean schema 表。
        
        **重要提示**：
        当前实现仅为占位符（计数+日志），不执行真正的数据库写入。
        
        **生产环境使用要求**：
        子类必须覆盖此方法以实现真正的数据库写入逻辑，推荐方案：
        1. 引入 CleanLayerWriter 适配 DBManager
        2. 从 clean_table 解析 schema/table 名称
        3. 调用 writer.upsert() 执行幂等写入
        
        **中长期改进方向**：
        提供基于 CleanLayerWriter + DBManager 的默认实现，例如：
        ```python
        from ..clean import CleanLayerWriter
        writer = CleanLayerWriter(self.db)
        schema, table = self.clean_table.split('.')
        await writer.upsert(
            data, 
            table_name=f"{schema}.{table}",
            primary_keys=self.primary_keys
        )
        ```
        
        Args:
            data: 要保存的清洗后数据
            **kwargs: 额外参数
            
        Returns:
            int: 保存的行数（当前仅返回计数，未实际写入）
            
        Raises:
            NotImplementedError: 提醒子类必须实现此方法
        """
        if not self.clean_table:
            self.logger.warning("未配置 clean_table，跳过 clean 数据保存")
            return 0
        
        # 占位实现：仅记录日志和计数
        self.logger.warning(
            f"_save_to_clean 当前为占位实现，未实际写入数据库。"
            f"目标表: {self.clean_table}，数据行数: {len(data)}。"
            f"生产环境请覆盖此方法或引入 CleanLayerWriter。"
        )
        
        # 返回行数（但未实际写入）
        return len(data)

    def get_task_info(self) -> Dict[str, Any]:
        """获取处理任务的详细信息"""
        info = {
            "name": self.name,
            "type": self.task_type,
            "source_tables": self.source_tables,
            "target_table": self.table_name,
            "clean_table": self.clean_table,
            "feature_dependencies": self.feature_dependencies,
            "skip_features": self.skip_features,
            "dependencies": self.dependencies,
            "description": self.description,
        }
        return info
