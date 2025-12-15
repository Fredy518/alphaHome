"""
物化视图任务基类

定义了物化视图任务的基础接口和实现。
"""

from abc import abstractmethod
from typing import Any, Dict, List, Optional
import pandas as pd

from alphahome.processors.tasks.base_task import ProcessorTaskBase
from .refresh import MaterializedViewRefresh
from .validator import MaterializedViewValidator
from .monitor import MaterializedViewMonitor


class MaterializedViewTask(ProcessorTaskBase):
    """
    物化视图任务基类
    
    职责：
    1. 定义物化视图 SQL（清洗 + 预计算）
    2. 执行刷新操作
    3. 验证数据质量
    4. 记录刷新元数据
    
    属性：
    - is_materialized_view: 标识这是一个物化视图任务
    - materialized_view_name: 物化视图名称（不含 schema）
    - materialized_view_schema: 物化视图所在的 schema（默认 materialized_views）
    - refresh_strategy: 刷新策略（full 或 concurrent）
    - source_tables: 数据源表列表（rawdata.* 表）
    - quality_checks: 数据质量检查配置
    """
    
    # 物化视图标识
    is_materialized_view: bool = True
    materialized_view_name: str = ""  # 物化视图名称（不含 schema）
    materialized_view_schema: str = "materialized_views"  # 物化视图所在的 schema
    
    # 刷新策略
    refresh_strategy: str = "full"  # full 或 concurrent
    
    # 数据源
    source_tables: List[str] = []  # rawdata.* 表
    
    # 数据质量检查
    quality_checks: Dict[str, Any] = {}  # 质量检查配置
    
    def __init__(self, db_connection=None, **kwargs):
        """初始化物化视图任务"""
        super().__init__(db_connection=db_connection, **kwargs)
        
        # Help DBManager resolvers: treat MV schema as data_source, and MV name as table_name.
        # This does not change SQL execution, but improves consistency when MV tasks are passed
        # into DBManager helpers.
        self.data_source = self.materialized_view_schema
        self.table_name = self.materialized_view_name

        if not self.materialized_view_name:
            self.logger.warning(
                f"物化视图任务 {self.name} 未定义 materialized_view_name"
            )
    
    @abstractmethod
    async def define_materialized_view_sql(self) -> str:
        """
        定义物化视图 SQL
        
        子类必须实现此方法来定义物化视图的 SQL 语句。
        SQL 应该包含以下步骤：
        1. 数据对齐（格式标准化）
        2. 数据标准化（单位转换）
        3. 业务逻辑（如果适合 SQL）
        4. 血缘元数据
        5. 数据校验（缺失值、异常值）
        
        Returns:
            str: 物化视图的 SQL 语句
        """
        raise NotImplementedError("子类必须实现 define_materialized_view_sql 方法")
    
    async def refresh_materialized_view(self) -> Dict[str, Any]:
        """
        刷新物化视图
        
        执行以下步骤：
        1. 执行 REFRESH MATERIALIZED VIEW 命令
        2. 记录刷新元数据
        3. 执行数据质量检查
        4. 返回刷新结果
        
        Returns:
            Dict: 刷新结果，包含：
                - status: success/failed
                - view_name: 物化视图名称
                - refresh_time: 刷新时间
                - duration_seconds: 刷新耗时
                - row_count: 刷新后的行数
                - error_message: 错误信息（如果失败）
        """
        if not self.db:
            raise RuntimeError("Database connection is not available")

        refresh = MaterializedViewRefresh(db_connection=self.db, logger=self.logger)
        result = await refresh.refresh(
            view_name=self.materialized_view_name,
            schema=self.materialized_view_schema,
            strategy=self.refresh_strategy,
        )
        # Normalize keys for downstream components (monitor expects refresh_strategy).
        result.setdefault("refresh_strategy", self.refresh_strategy)
        result.setdefault("view_schema", self.materialized_view_schema)
        return result
    
    async def validate_data_quality(
        self,
        *,
        previous_row_count: Optional[int] = None,
        current_row_count: Optional[int] = None,
        sample_limit: int = 10_000,
    ) -> Dict[str, Any]:
        """
        验证数据质量
        
        执行以下检查：
        1. 缺失值检查
        2. 异常值检查
        3. 行数变化检查
        4. 重复值检查
        5. 类型检查
        
        Returns:
            Dict: 检查结果，包含：
                - status: pass/warning/error
                - checks: 各项检查的结果列表
        """
        if not self.db:
            raise RuntimeError("Database connection is not available")

        self.logger.info(
            f"验证数据质量: {self.materialized_view_schema}.{self.materialized_view_name} "
            f"(sample_limit={sample_limit})"
        )

        validator = MaterializedViewValidator(logger=self.logger)

        # Pull a bounded sample for column-level checks (null/outlier/duplicate/type).
        # Note: row_count_change uses explicit current/previous row counts, not sample size.
        sample_df = pd.DataFrame()
        try:
            full_name = f"{self.materialized_view_schema}.{self.materialized_view_name}"
            rows = await self.db.fetch(f"SELECT * FROM {full_name} LIMIT $1;", sample_limit)
            sample_df = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
        except Exception as e:
            self.logger.warning(f"获取物化视图样本数据失败，将跳过基于样本的检查: {e}")

        checks: List[Dict[str, Any]] = []

        for check_name, config in (self.quality_checks or {}).items():
            try:
                if check_name == "null_check":
                    checks.append(await validator.validate_null_values(sample_df, config))
                elif check_name == "outlier_check":
                    checks.append(await validator.validate_outliers(sample_df, config))
                elif check_name == "row_count_change":
                    checks.append(
                        await validator.validate_row_count_change(
                            sample_df,
                            config,
                            previous_row_count=previous_row_count,
                            current_row_count=current_row_count,
                        )
                    )
                elif check_name == "duplicate_check":
                    checks.append(await validator.validate_duplicates(sample_df, config))
                elif check_name == "type_check":
                    checks.append(await validator.validate_types(sample_df, config))
                else:
                    self.logger.warning(f"未知的数据质量检查类型: {check_name}")
            except Exception as e:
                self.logger.warning(f"数据质量检查 {check_name} 执行失败: {e}", exc_info=True)
                checks.append(
                    {
                        "check_name": check_name,
                        "status": "warning",
                        "message": f"Quality check failed: {type(e).__name__}: {e}",
                        "details": {},
                    }
                )

        overall = "pass"
        if any(c.get("status") in ("warning", "error") for c in checks):
            overall = "warning"

        return {"status": overall, "checks": checks}

    async def run(self, **kwargs) -> Dict[str, Any]:
        """
        Materialized view task entrypoint.

        This overrides ProcessorTaskBase.run(), which is designed for fetch/clean/feature/save.
        MV tasks instead:
        1) Ensure metadata tables exist
        2) Create MV if missing (optional)
        3) Refresh MV
        4) Record refresh metadata
        5) Run configured quality checks and record results
        """
        stop_event = kwargs.get("stop_event")
        create_if_missing: bool = kwargs.get("create_if_missing", True)
        sample_limit: int = kwargs.get("quality_sample_limit", 10_000)

        if not self.db:
            raise RuntimeError("Database connection is not available")

        from alphahome.common.db_components.materialized_views_schema import (
            initialize_materialized_views_schema,
        )

        await initialize_materialized_views_schema(self.db)

        monitor = MaterializedViewMonitor(self.db)
        from .alerting import MaterializedViewAlerting

        alerting = MaterializedViewAlerting(self.db)

        previous_row_count: Optional[int] = None
        try:
            previous_status = await monitor.get_latest_refresh_status(self.materialized_view_name)
            previous_row_count = previous_status.get("row_count") if previous_status else None
        except Exception:
            previous_row_count = None

        if stop_event and stop_event.is_set():
            return {"status": "cancelled", "task": self.name}

        # Ensure materialized view exists (best-effort).
        if create_if_missing:
            try:
                exists = await self.db.fetch_val(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_matviews
                        WHERE schemaname = $1
                          AND matviewname = $2
                    );
                    """,
                    self.materialized_view_schema,
                    self.materialized_view_name,
                )
                if not exists:
                    self.logger.info(
                        f"物化视图不存在，创建: {self.materialized_view_schema}.{self.materialized_view_name}"
                    )
                    sql = await self.define_materialized_view_sql()
                    await self.db.execute(sql)
            except Exception as e:
                self.logger.warning(f"创建物化视图失败（将继续尝试刷新）: {e}", exc_info=True)

        if stop_event and stop_event.is_set():
            return {"status": "cancelled", "task": self.name}

        refresh_result = await self.refresh_materialized_view()
        refresh_result["source_tables"] = self.source_tables
        refresh_result["refresh_strategy"] = self.refresh_strategy
        refresh_result["view_schema"] = self.materialized_view_schema

        await monitor.record_refresh_metadata(self.materialized_view_name, refresh_result)

        if refresh_result.get("status") != "success":
            try:
                await alerting.alert_refresh_failed(
                    view_name=self.materialized_view_name,
                    error_message=refresh_result.get("error_message") or "Unknown error",
                    refresh_strategy=self.refresh_strategy,
                    duration_seconds=float(refresh_result.get("duration_seconds") or 0.0),
                    additional_details={
                        "view_schema": self.materialized_view_schema,
                        "row_count": refresh_result.get("row_count"),
                    },
                )
            except Exception as e:
                self.logger.warning(
                    f"记录刷新失败告警失败（将继续返回失败结果）: {e}",
                    exc_info=True,
                )
            return {
                "status": "failed",
                "refresh": refresh_result,
                "quality": {"status": "warning", "checks": []},
            }

        if stop_event and stop_event.is_set():
            return {"status": "cancelled", "task": self.name, "refresh": refresh_result}

        quality_result = await self.validate_data_quality(
            previous_row_count=previous_row_count,
            current_row_count=refresh_result.get("row_count"),
            sample_limit=sample_limit,
        )

        for check in quality_result.get("checks", []):
            await monitor.record_quality_check(
                self.materialized_view_name,
                check_name=check.get("check_name", "unknown"),
                check_status=check.get("status", "warning"),
                check_message=check.get("message", ""),
                check_details=check.get("details", {}),
            )
            try:
                await alerting.alert_data_quality_issue(
                    view_name=self.materialized_view_name,
                    check_name=check.get("check_name", "unknown"),
                    check_status=check.get("status", "warning"),
                    check_message=check.get("message", ""),
                    check_details=check.get("details", {}),
                )
            except Exception as e:
                self.logger.warning(
                    f"记录数据质量告警失败（将继续记录质量检查结果）: {e}",
                    exc_info=True,
                )

        return {"status": "success", "refresh": refresh_result, "quality": quality_result}
    
    async def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        获取数据
        
        物化视图任务通常不需要获取数据，因为数据来自 SQL 定义。
        此方法为占位实现。
        
        Returns:
            None
        """
        return None
    
    async def process_data(
        self, 
        data: pd.DataFrame, 
        stop_event=None, 
        **kwargs
    ) -> Optional[pd.DataFrame]:
        """
        处理数据
        
        物化视图任务通常不需要处理数据，因为处理逻辑在 SQL 中定义。
        此方法为占位实现。
        
        Returns:
            None
        """
        return None
    
    async def save_result(self, data: pd.DataFrame, **kwargs):
        """
        保存结果
        
        物化视图任务通常不需要保存数据，因为数据已经在物化视图中。
        此方法为占位实现。
        """
        pass
    
    def get_materialized_view_info(self) -> Dict[str, Any]:
        """获取物化视图的详细信息"""
        return {
            "name": self.name,
            "materialized_view_name": self.materialized_view_name,
            "materialized_view_schema": self.materialized_view_schema,
            "full_name": f"{self.materialized_view_schema}.{self.materialized_view_name}",
            "refresh_strategy": self.refresh_strategy,
            "source_tables": self.source_tables,
            "quality_checks": self.quality_checks,
        }
