"""
增量刷新物化视图基类

设计思路：
PostgreSQL 原生不支持增量刷新物化视图，但对于按日期分区的数据，
可以通过以下方式实现"伪增量"刷新：

1. 删除最近 N 天的数据
2. 重新计算并插入这 N 天的数据

这样可以将刷新时间从全量的 10+ 分钟降低到增量的几秒钟。

使用场景：
- 数据按 trade_date 分区
- 只需要更新最近若干天的数据
- 历史数据稳定不变

刷新策略：
- incremental: 增量刷新（默认最近 30 天）
- full: 全量刷新（清空重建）
"""

import logging
from abc import abstractmethod
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from .base_view import BaseFeatureView
from .refresh_log import log_mv_refresh

logger = logging.getLogger(__name__)


class IncrementalFeatureView(BaseFeatureView):
    """
    支持增量刷新的物化视图基类。

    子类需要实现：
    - get_create_sql(): 创建物化视图的 SQL
    - get_incremental_sql(start_date, end_date): 增量计算的 SQL

    配置属性：
    - incremental_days: 增量刷新的天数范围（默认 30 天）
    - date_column: 日期列名（默认 trade_date）
    """

    # 增量刷新配置
    incremental_days: int = 30  # 默认刷新最近 30 天
    date_column: str = "trade_date"  # 日期列名
    refresh_strategy: str = "incremental"  # 默认使用增量刷新

    async def _is_materialized_view(self) -> bool:
        """检查当前对象是否为物化视图（而非普通表）。"""
        if self._db_manager is None:
            return False

        sql = f"""
        SELECT EXISTS (
            SELECT 1 FROM pg_matviews
            WHERE schemaname = '{self._schema}'
              AND matviewname = '{self.view_name}'
        ) AS is_matview;
        """
        result = await self._db_manager.fetch(sql)
        return result and result[0]["is_matview"]

    @abstractmethod
    def get_incremental_sql(self, start_date: str, end_date: str) -> str:
        """
        返回增量计算的 SELECT SQL（不含 INSERT）。

        Args:
            start_date: 开始日期 (YYYYMMDD 格式)
            end_date: 结束日期 (YYYYMMDD 格式)

        Returns:
            str: SELECT SQL，用于获取指定日期范围的数据
        """
        pass

    async def refresh(self, strategy: Optional[str] = None) -> Dict[str, Any]:
        """
        刷新物化视图。

        Args:
            strategy: 刷新策略
                - "incremental": 增量刷新（删除旧数据 + 插入新数据）
                - "full": 全量刷新
                - "concurrent": 并发全量刷新

        Returns:
            Dict[str, Any]: 刷新结果
        """
        if self._db_manager is None:
            raise RuntimeError("db_manager 未设置")

        # GUI 层使用 "default" 表示“使用配方默认策略”
        if strategy == "default":
            strategy = None

        actual_strategy = strategy or self.refresh_strategy

        if actual_strategy == "incremental":
            return await self._incremental_refresh()
        else:
            # 使用父类的全量刷新逻辑
            return await super().refresh(strategy=actual_strategy)

    async def _incremental_refresh(self) -> Dict[str, Any]:
        """
        执行增量刷新。

        流程：
        1. 检查是否为物化视图（物化视图不支持 DELETE，需回退到全量刷新）
        2. 计算日期范围
        3. 删除范围内的旧数据
        4. 插入新计算的数据
        5. 更新元数据
        """
        # 检查是否为物化视图 - 物化视图无法 DELETE，需回退到全量刷新
        is_matview = await self._is_materialized_view()
        if is_matview:
            self.logger.warning(
                f"{self.full_name} 是物化视图，不支持增量刷新，回退到全量刷新。"
                f"如需真正的增量刷新，请使用 IncrementalTableView (CREATE TABLE) 而非物化视图。"
            )
            return await super().refresh(strategy="full")

        import asyncpg

        start_time = datetime.now()

        # 计算日期范围（自然日）
        end_date = datetime.now()
        start_date = end_date - timedelta(days=self.incremental_days)

        start_date_str = start_date.strftime("%Y%m%d")
        end_date_str = end_date.strftime("%Y%m%d")

        self.logger.info(
            f"开始增量刷新 {self.full_name}, "
            f"日期范围: {start_date_str} ~ {end_date_str}"
        )

        try:
            # 重要：使用单连接 + 单事务执行 delete+insert，避免刷新中间态（空表/半成品）被读到
            conn_str = self._db_manager.connection_string
            conn = await asyncpg.connect(conn_str, command_timeout=7200)
            try:
                async with conn.transaction():
                    # Step 1: 删除旧数据
                    delete_sql = f"""
                    DELETE FROM {self.full_name}
                    WHERE {self.date_column} >= '{start_date_str}'
                      AND {self.date_column} <= '{end_date_str}';
                    """
                    await conn.execute(delete_sql)

                    # Step 2: 插入新数据
                    incremental_sql = self.get_incremental_sql(start_date_str, end_date_str)

                    # 显式列名插入，避免“表列顺序”与“SELECT 列顺序”不一致导致的类型错位
                    columns_sql = """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = $1 AND table_name = $2
                    ORDER BY ordinal_position
                    """
                    cols = await conn.fetch(columns_sql, self._schema, self.view_name)
                    col_names = [r["column_name"] for r in (cols or [])]
                    if not col_names:
                        raise RuntimeError(f"无法获取表列信息: {self.full_name}")

                    insert_cols = ", ".join([f'"{c}"' for c in col_names])
                    select_cols = ", ".join([f'd."{c}"' for c in col_names])

                    insert_sql = f"""
                    INSERT INTO {self.full_name} ({insert_cols})
                    SELECT {select_cols}
                    FROM (
                    {incremental_sql}
                    ) AS d;
                    """
                    await conn.execute(insert_sql)

                    # Step 3: 获取影响范围内行数（便于 UI 展示“本次刷新覆盖量”）
                    count_sql = f"""
                    SELECT COUNT(*) AS cnt FROM {self.full_name}
                    WHERE {self.date_column} >= '{start_date_str}'
                      AND {self.date_column} <= '{end_date_str}';
                    """
                    row = await conn.fetchrow(count_sql)
                    rows_affected = row["cnt"] if row else 0
            finally:
                await conn.close()

            # Step 4: 记录刷新日志
            duration = (datetime.now() - start_time).total_seconds()
            await self._log_refresh(
                strategy="incremental",
                success=True,
                duration=duration,
                rows_affected=rows_affected,
                date_range=f"{start_date_str}~{end_date_str}"
            )

            self.logger.info(
                f"增量刷新 {self.full_name} 完成, "
                f"影响行数: {rows_affected}, 耗时: {duration:.2f}s"
            )

            return {
                "status": "success",
                "view_name": self.view_name,
                "view_schema": self._schema,
                "full_name": self.full_name,
                "refresh_time": datetime.now(),
                "duration_seconds": duration,
                "row_count": rows_affected,
                "refresh_strategy": "incremental",
                "strategy": "incremental",
                "date_range": f"{start_date_str}~{end_date_str}"
            }

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            await self._log_refresh(
                strategy="incremental",
                success=False,
                duration=duration,
                error=str(e)
            )
            self.logger.error(f"增量刷新 {self.full_name} 失败: {e}")
            raise

    async def _log_refresh(
        self,
        strategy: str,
        success: bool,
        duration: float,
        rows_affected: int = 0,
        date_range: str = "",
        error: str = ""
    ) -> None:
        """记录刷新日志到 features.mv_refresh_log。"""
        if self._db_manager is None:
            return

        refresh_strategy = strategy + (f" ({date_range})" if date_range else "")
        await log_mv_refresh(
            self._db_manager,
            view_name=self.view_name,
            schema_name=self._schema,
            refresh_strategy=refresh_strategy,
            success=success,
            duration_seconds=duration,
            row_count=rows_affected,
            error_message=error if error else None,
        )


class IncrementalTableView(IncrementalFeatureView):
    """
    使用普通表（而非物化视图）的增量刷新基类。

    适用场景：
    - 需要频繁增量更新
    - 数据量大，全量刷新太慢
    - 不需要 REFRESH MATERIALIZED VIEW 的原子性

    注意：
    - 使用 CREATE TABLE 而非 CREATE MATERIALIZED VIEW
    - get_create_sql() 应返回 CREATE TABLE 语句
    """
    storage_type: str = "数据表"

    async def refresh(self, strategy: Optional[str] = None) -> Dict[str, Any]:
        """
        刷新表数据。

        Args:
            strategy: 刷新策略
                - "incremental": 增量刷新（删除旧数据 + 插入新数据）
                - "full": 全量刷新（TRUNCATE + 全量INSERT）
                - "default": 使用类定义的默认策略

        Returns:
            Dict[str, Any]: 刷新结果
        """
        if self._db_manager is None:
            raise RuntimeError("db_manager 未设置")

        # 显式 guardrail：该类仅用于普通表（CREATE TABLE），如果数据库中仍是物化视图，
        # 说明发生了配方/存储类型迁移，需要先 DROP 再 CREATE。
        if await self._is_materialized_view():
            raise RuntimeError(
                f"{self.full_name} 当前为物化视图（pg_matviews），但配方为数据表类型。"
                f"请先执行: DROP MATERIALIZED VIEW IF EXISTS {self.full_name}; 然后重新创建特征。"
            )

        # GUI 层使用 "default" 表示“使用配方默认策略”
        if strategy == "default":
            strategy = None

        actual_strategy = strategy or self.refresh_strategy

        if actual_strategy == "incremental":
            return await self._incremental_refresh()
        elif actual_strategy == "full":
            return await self._full_refresh_table()
        else:
            # 对于其他策略，仍然调用增量刷新
            return await self._incremental_refresh()

    async def _full_refresh_table(self) -> Dict[str, Any]:
        """
        执行普通表的全量刷新。

        流程：
        1. 构建临时表并全量写入（避免刷新中间空表）
        2. 快速 rename swap 切换新表
        3. 更新元数据
        
        注意：使用独立的长超时连接，避免连接池的180秒超时限制
        """
        import asyncpg
        import uuid
        
        start_time = datetime.now()

        self.logger.info(f"开始全量刷新表 {self.full_name}")

        try:
            # 获取连接字符串并创建长超时连接（2小时）
            conn_str = self._db_manager.connection_string
            conn = await asyncpg.connect(conn_str, command_timeout=7200)

            tmp_full_name: Optional[str] = None
            
            try:
                # Step 0: 确认目标是普通表（不是物化视图）
                exists_sql = """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = $1 AND table_name = $2 AND table_type = 'BASE TABLE'
                ) AS is_table;
                """
                is_table_row = await conn.fetchrow(exists_sql, self._schema, self.view_name)
                if not is_table_row or not is_table_row["is_table"]:
                    raise RuntimeError(f"{self.full_name} 不存在或不是普通表，请先创建表或检查是否仍为物化视图。")

                # Step 1: 创建临时表（复制结构）
                suffix = uuid.uuid4().hex[:8]
                tmp_suffix = f"__tmp_{suffix}"
                bak_suffix = f"__bak_{suffix}"

                max_ident_len = 63
                base_name = self.view_name
                tmp_base = base_name[: max_ident_len - len(tmp_suffix)] if len(base_name) + len(tmp_suffix) > max_ident_len else base_name
                bak_base = base_name[: max_ident_len - len(bak_suffix)] if len(base_name) + len(bak_suffix) > max_ident_len else base_name

                tmp_table = f"{tmp_base}{tmp_suffix}"
                bak_table = f"{bak_base}{bak_suffix}"

                tmp_full_name = f"{self._schema}.{tmp_table}"

                await conn.execute(
                    f"CREATE TABLE {tmp_full_name} (LIKE {self.full_name} INCLUDING ALL);"
                )

                # Step 2: 全量插入数据（写入临时表）
                far_past = "19000101"
                far_future = "20991231"
                full_select_sql = self.get_incremental_sql(far_past, far_future)

                # 获取列信息
                columns_sql = """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2
                ORDER BY ordinal_position
                """
                cols = await conn.fetch(columns_sql, self._schema, self.view_name)
                col_names = [r["column_name"] for r in (cols or [])]
                if not col_names:
                    raise RuntimeError(f"无法获取表列信息: {self.full_name}")

                insert_cols = ", ".join([f'"{c}"' for c in col_names])
                select_cols = ", ".join([f'd."{c}"' for c in col_names])

                insert_sql = f"""
                INSERT INTO {tmp_full_name} ({insert_cols})
                SELECT {select_cols}
                FROM (
                {full_select_sql}
                ) AS d;
                """
                
                self.logger.info(f"执行全量INSERT（可能需要较长时间）...")
                await conn.execute(insert_sql)

                # Step 3: 获取新表总行数
                count_sql = f"SELECT COUNT(*) AS cnt FROM {tmp_full_name};"
                result = await conn.fetchrow(count_sql)
                row_count = result["cnt"] if result else 0

                # Step 4: rename swap（短时间锁切换，避免中间空表/半成品）
                async with conn.transaction():
                    await conn.execute(
                        f"ALTER TABLE {self.full_name} RENAME TO {bak_table};"
                    )
                    await conn.execute(
                        f"ALTER TABLE {tmp_full_name} RENAME TO {self.view_name};"
                    )

                # Step 5: 清理备份表（避免长事务阻塞，设置 lock_timeout）
                try:
                    await conn.execute("SET lock_timeout = '1s';")
                    await conn.execute(f"DROP TABLE {self._schema}.{bak_table};")
                except Exception as drop_err:
                    self.logger.warning(
                        f"全量刷新完成但未能删除备份表 {self._schema}.{bak_table}: {drop_err}"
                    )
                finally:
                    try:
                        await conn.execute("RESET lock_timeout;")
                    except Exception:
                        pass

            except Exception:
                # 尽力清理临时表，避免失败后遗留脏对象（不影响主错误抛出）
                if tmp_full_name:
                    try:
                        await conn.execute(f"DROP TABLE IF EXISTS {tmp_full_name};")
                    except Exception:
                        pass
                raise
                
            finally:
                await conn.close()

            # Step 4: 记录刷新日志
            duration = (datetime.now() - start_time).total_seconds()
            await self._log_refresh(
                strategy="full",
                success=True,
                duration=duration,
                rows_affected=row_count,
                date_range="all"
            )

            self.logger.info(
                f"全量刷新表 {self.full_name} 完成, "
                f"总行数: {row_count}, 耗时: {duration:.2f}s"
            )

            return {
                "status": "success",
                "view_name": self.view_name,
                "view_schema": self._schema,
                "full_name": self.full_name,
                "row_count": row_count,
                "duration_seconds": duration,
                "refresh_strategy": "full",
                "strategy": "full",
            }

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            error_msg = f"{type(e).__name__}: {str(e)}"

            await self._log_refresh(
                strategy="full",
                success=False,
                duration=duration,
                rows_affected=0,
                date_range="all",
                error=error_msg
            )

            self.logger.error(f"全量刷新表 {self.full_name} 失败: {error_msg}")

            return {
                "status": "failed",
                "view_name": self.view_name,
                "view_schema": self._schema,
                "full_name": self.full_name,
                "row_count": 0,
                "duration_seconds": duration,
                "error_message": error_msg,
                "refresh_strategy": "full",
                "strategy": "full",
            }

    async def create(self, if_not_exists: bool = True) -> bool:
        """
        创建普通表。
        """
        if self._db_manager is None:
            raise RuntimeError("db_manager 未设置")

        try:
            if await self._is_materialized_view():
                raise RuntimeError(
                    f"{self.full_name} 当前为物化视图（pg_matviews），但配方为数据表类型。"
                    f"请先执行: DROP MATERIALIZED VIEW IF EXISTS {self.full_name}; 然后重新创建特征。"
                )

            # 检查表是否已存在
            check_sql = f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = '{self._schema}'
                  AND table_name = '{self.view_name}'
            ) AS exists;
            """
            result = await self._db_manager.fetch(check_sql)
            table_exists = result and result[0]["exists"]

            if if_not_exists and table_exists:
                self.logger.info(f"表 {self.full_name} 已存在，跳过创建")
                await self._upsert_metadata()
                return True

            # 获取创建 SQL
            create_sql = self.get_create_sql()
            self.logger.info(f"创建表: {self.full_name}")

            # 执行创建
            await self._db_manager.execute(create_sql)

            # 执行创建后的附加 SQL（如索引）
            post_sqls = self.get_post_create_sqls() or []
            for stmt in post_sqls:
                if not isinstance(stmt, str) or not stmt.strip():
                    continue
                await self._db_manager.execute(stmt)

            # 写入元数据
            await self._upsert_metadata()

            self.logger.info(f"表 {self.full_name} 创建成功")
            return True

        except Exception as e:
            self.logger.error(f"创建表 {self.full_name} 失败: {e}")
            raise

    async def exists(self) -> bool:
        """检查表是否存在。"""
        if self._db_manager is None:
            raise RuntimeError("db_manager 未设置")

        sql = f"""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = '{self._schema}'
              AND table_name = '{self.view_name}'
        ) AS exists;
        """
        result = await self._db_manager.fetch(sql)
        return result and result[0]["exists"]

    async def drop(self, if_exists: bool = True) -> bool:
        """删除表。"""
        if self._db_manager is None:
            raise RuntimeError("db_manager 未设置")

        try:
            if_exists_clause = "IF EXISTS " if if_exists else ""
            sql = f"DROP TABLE {if_exists_clause}{self.full_name};"
            self.logger.info(f"删除表: {self.full_name}")
            await self._db_manager.execute(sql)
            await self._deactivate_metadata()
            self.logger.info(f"表 {self.full_name} 删除成功")
            return True

        except Exception as e:
            self.logger.error(f"删除表 {self.full_name} 失败: {e}")
            raise
