import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import asyncpg
import pandas as pd


class DataOperationsMixin:
    """数据操作Mixin - 提供复杂的数据操作功能，如批量导入、UPSERT等"""

    async def copy_from_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        conflict_columns: Optional[List[str]] = None,
        update_columns: Optional[List[str]] = None,
        timestamp_column: Optional[str] = None,
    ):
        """将DataFrame数据高效复制并可选地UPSERT到数据库表中。

        利用 PostgreSQL 的 COPY 命令和临时表实现高效数据加载。
        如果指定了 conflict_columns，则执行 UPSERT (插入或更新) 操作。

        Args:
            df (pd.DataFrame): 要复制的DataFrame。
            table_name (str): 目标表名。
            conflict_columns (Optional[List[str]]): 用于检测冲突的列名列表。如果为None，则执行简单插入。
            update_columns (Optional[List[str]]): 发生冲突时要更新的列名列表。
                                                如果为None且conflict_columns已指定，则更新所有非冲突列。
            timestamp_column (Optional[str]): 时间戳列名。如果指定并在冲突时更新，
                                             如果其他数据列发生变化或特定条件下，该列将自动更新为当前时间。

        Returns:
            int: 影响的总行数 (指通过COPY命令加载到临时表的行数)。

        Raises:
            ValueError: 如果参数无效或DataFrame为空。
            Exception: 如果发生数据库操作错误。
        """
        if self.pool is None:
            await self.connect()

        if df.empty:
            self.logger.info(
                f"copy_from_dataframe: 表 '{table_name}' 的DataFrame为空，跳过操作。"
            )
            return 0

        if conflict_columns and not isinstance(conflict_columns, list):
            raise ValueError("conflict_columns 必须是一个列表或None。")
        if conflict_columns is not None and not conflict_columns:
            raise ValueError("如果提供了 conflict_columns，则它不能为空列表。")

        # 获取DataFrame的列名 - 这些将用于COPY命令
        df_columns = list(df.columns)

        # --- 针对timestamp_column的防御性检查和添加 ---
        if timestamp_column and timestamp_column not in df.columns:
            self.logger.warning(
                f"COPY_FROM_DATAFRAME (表: {table_name}): 时间戳列 '{timestamp_column}' 未在DataFrame列中找到，现用None值添加。"
            )
            df[timestamp_column] = None
            df_columns = list(df.columns)

        # --- 验证列名 ---
        if conflict_columns:
            for col in conflict_columns:
                if col not in df_columns:
                    raise ValueError(f"冲突列 '{col}' 在DataFrame的列中未找到。")
        if update_columns:
            if not isinstance(update_columns, list):
                raise ValueError("update_columns必须是一个列表或None。")
            for col in update_columns:
                if col not in df_columns:
                    raise ValueError(f"更新列 '{col}' 在DataFrame的列中未找到。")
        if timestamp_column and timestamp_column not in df_columns:
            self.logger.warning(
                f"时间戳列 '{timestamp_column}' 未在DataFrame中找到，"
                f"如果在UPDATE时需要会被添加，但如果表结构要求此列，初始INSERT可能会失败。"
            )

        # 创建一个唯一的临时表名
        timestamp_ms = int(datetime.now().timestamp() * 1000)
        temp_table = f"temp_{table_name}_{timestamp_ms}_{id(df)}"

        create_temp_table_sql = f"""
        CREATE TEMPORARY TABLE "{temp_table}" (LIKE "{table_name}" INCLUDING DEFAULTS) ON COMMIT DROP;
        """

        # --- 使用生成器准备记录以减少内存占用 ---
        async def _df_to_records_generator(df_internal: pd.DataFrame):
            for row_tuple in df_internal.itertuples(index=False, name=None):
                processed_values = []
                for val in row_tuple:
                    if pd.isna(val):
                        processed_values.append(None)
                    else:
                        processed_values.append(val)
                yield tuple(processed_values)

        records_iterable = _df_to_records_generator(df)

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                try:
                    # 1. 创建临时表
                    await conn.execute(create_temp_table_sql)
                    self.logger.debug(f"已创建临时表 {temp_table}")

                    # 2. 使用 COPY 高效加载数据到临时表
                    copy_count = await conn.copy_records_to_table(
                        temp_table,
                        records=records_iterable,
                        columns=df_columns,
                        timeout=300,
                    )
                    self.logger.debug(f"已复制 {copy_count} 条记录到 {temp_table}")

                    # 3. 从临时表插入/更新到目标表
                    target_col_str = ", ".join([f'"{col}"' for col in df_columns])

                    if conflict_columns:
                        # --- UPSERT 逻辑 ---
                        conflict_col_str = ", ".join(
                            [f'"{col}"' for col in conflict_columns]
                        )

                        # 确定要更新的列
                        actual_update_columns = []
                        if update_columns is None:
                            actual_update_columns = [
                                col for col in df_columns if col not in conflict_columns
                            ]
                        else:
                            actual_update_columns = update_columns

                        # 构建SET子句
                        if actual_update_columns:
                            set_clauses = [
                                f'"{col}" = EXCLUDED."{col}"'
                                for col in actual_update_columns
                            ]

                            if timestamp_column and timestamp_column in df_columns:
                                if timestamp_column not in conflict_columns:
                                    # 从set_clauses中移除任何"EXCLUDED"版本的时间戳列
                                    excluded_timestamp_clause = f'"{timestamp_column}" = EXCLUDED."{timestamp_column}"'
                                    if excluded_timestamp_clause in set_clauses:
                                        set_clauses.remove(excluded_timestamp_clause)

                                    # 确定用于比较的内容列（排除冲突列和时间戳列）
                                    content_columns_to_compare = [
                                        col
                                        for col in df_columns
                                        if col not in conflict_columns
                                        and col != timestamp_column
                                    ]

                                    if not content_columns_to_compare:
                                        set_clauses.append(
                                            f'"{timestamp_column}" = NOW()'
                                        )
                                    else:
                                        # 构建时间戳列的条件更新
                                        conditions = [
                                            f'"{table_name}"."{col}" IS DISTINCT FROM EXCLUDED."{col}"'
                                            for col in content_columns_to_compare
                                        ]
                                        condition_str = " OR ".join(conditions)
                                        timestamp_update_clause = (
                                            f'"{timestamp_column}" = CASE '
                                            f"WHEN {condition_str} THEN NOW() "
                                            f'ELSE "{table_name}"."{timestamp_column}" END'
                                        )
                                        set_clauses.append(timestamp_update_clause)

                            if set_clauses:
                                update_clause = ", ".join(set_clauses)
                                action_sql = f"DO UPDATE SET {update_clause}"
                            else:
                                self.logger.warning(
                                    f"UPSERT (表: {table_name}): set_clauses变为空。默认为DO NOTHING。"
                                )
                                action_sql = "DO NOTHING"
                        else:
                            # actual_update_columns 为空
                            if (
                                timestamp_column
                                and timestamp_column in df_columns
                                and timestamp_column not in conflict_columns
                            ):
                                set_clauses = [f'"{timestamp_column}" = NOW()']
                                update_clause = ", ".join(set_clauses)
                                action_sql = f"DO UPDATE SET {update_clause}"
                            else:
                                action_sql = "DO NOTHING"

                        insert_sql = f"""
                        INSERT INTO "{table_name}" ({target_col_str})
                        SELECT {target_col_str}
                        FROM "{temp_table}"
                        ON CONFLICT ({conflict_col_str}) {action_sql};
                        """

                        self.logger.debug(
                            f"UPSERT (表: {table_name}): DataFrame头部 (前3行, 用于EXCLUDED上下文):\n{df.head(3).to_string()}"
                        )
                        self.logger.debug(
                            f"为表 {table_name} 执行最终UPSERT SQL:\n{insert_sql}"
                        )

                    else:
                        # --- 简单 INSERT 逻辑 ---
                        insert_sql = f"""
                        INSERT INTO "{table_name}" ({target_col_str})
                        SELECT {target_col_str} 
                        FROM "{temp_table}";
                        """

                    # 执行最终传输
                    status = await conn.execute(insert_sql)
                    self.logger.debug(f"最终插入/更新命令状态: {status}")

                    # --- 从状态字符串中解析整数 ---
                    rows_copied = 0
                    try:
                        if isinstance(copy_count, str) and copy_count.startswith(
                            "COPY "
                        ):
                            rows_copied = int(copy_count.split()[-1])
                        else:
                            self.logger.warning(
                                f"copy_records_to_table 返回了意外状态: {copy_count}。假设复制了0行。"
                            )
                    except (ValueError, IndexError) as parse_err:
                        self.logger.error(
                            f"从copy_count '{copy_count}' 解析行数失败: {parse_err}。假设复制了0行。"
                        )

                    return rows_copied

                except asyncpg.exceptions.UndefinedTableError as e:
                    self.logger.error(
                        f"创建临时表失败 (LIKE '{table_name}')。目标表可能不存在。错误: {e}"
                    )
                    raise ValueError(f"目标表 '{table_name}' 未找到。") from e
                except asyncpg.exceptions.UndefinedColumnError as e:
                    self.logger.error(
                        f"表 '{table_name}' 的复制/插入过程中发生列不匹配错误。错误: {e}"
                    )
                    self.logger.error(f"DataFrame 列: {df_columns}")
                    raise ValueError(
                        f"DataFrame和表 '{table_name}' 之间列不匹配。请检查DataFrame列和表结构。"
                    ) from e
                except Exception as e:
                    self.logger.error(
                        f"高效复制/UPSERT操作失败 (表: {table_name}): {str(e)}"
                    )
                    self.logger.error(f"临时表曾是: {temp_table}")
                    raise

    async def upsert(
        self,
        table_name: str,
        data: Union[pd.DataFrame, List[Dict[str, Any]]],
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None,
        timestamp_column: Optional[str] = None,
        stop_event: Optional[asyncio.Event] = None,
    ) -> int:
        """通用UPSERT操作：插入数据，如果发生冲突则根据条件更新。

        利用 COPY + 临时表 + INSERT ON CONFLICT 策略以提高效率。

        Args:
            table_name (str): 目标表名。
            data (Union[pd.DataFrame, List[Dict[str, Any]]]): 要插入的数据，可以是DataFrame或字典列表。
            conflict_columns (List[str]): 用于检测冲突的列名列表（通常是主键或唯一索引的列）。
            update_columns (Optional[List[str]]): 发生冲突时要更新的列名列表。
                                                如果为None，则更新所有非冲突列。
            timestamp_column (Optional[str]): 时间戳列名。如果指定，在冲突更新时，
                                             若其他数据列发生变化，此列将更新为NOW()。
                                             插入时，如果DataFrame中此列无值，会尝试添加。
            stop_event (Optional[asyncio.Event]): 用于取消操作的异步事件。

        Returns:
            int: 影响的行数 (基于COPY到临时表的行数)。

        Raises:
            ValueError: 当数据为空或参数无效时。
            Exception: 如果发生数据库操作错误。
        """
        if self.pool is None:
            await self.connect()

        # --- 参数检查 ---
        if not table_name:
            raise ValueError("表名不能为空")
        if not conflict_columns:
            raise ValueError("必须指定冲突检测列 (conflict_columns)。")
        if not isinstance(conflict_columns, list) or len(conflict_columns) == 0:
            raise ValueError("conflict_columns 必须是一个非空的字符串列表。")

        # --- 数据准备 ---
        df_to_process = None
        if isinstance(data, pd.DataFrame):
            if data.empty:
                self.logger.info(f"UPSERT ({table_name}): 输入的DataFrame为空。")
                return 0
            df_to_process = data.copy()
        elif isinstance(data, list):
            if not data:
                self.logger.info(f"UPSERT ({table_name}): 输入的列表为空。")
                return 0
            try:
                df_to_process = pd.DataFrame(data)
            except Exception as e:
                self.logger.error(
                    f"UPSERT ({table_name}): 无法将字典列表转换为DataFrame: {e}"
                )
                raise ValueError("无法将输入的字典列表转换为DataFrame。") from e
            if df_to_process.empty:
                self.logger.info(f"UPSERT ({table_name}): 从列表转换的DataFrame为空。")
                return 0
        else:
            raise ValueError("参数 data 必须是DataFrame或字典列表。")

        # --- 预处理 ---

        # 1. 检查 Stop Event (在耗时操作前)
        if stop_event and stop_event.is_set():
            self.logger.warning(f"UPSERT 操作 ({table_name}) 在开始前被取消。")
            raise asyncio.CancelledError("UPSERT操作在开始前被取消")

        # 2. 时间戳处理 (确保列存在于 DataFrame)
        current_time = datetime.now()
        if timestamp_column:
            if timestamp_column not in df_to_process.columns:
                self.logger.info(
                    f"UPSERT ({table_name}): 未检测到 {timestamp_column} 列，已自动为所有行添加当前时间戳。"
                )
                df_to_process[timestamp_column] = current_time

        # 3. 移除冲突列为 NULL 的行
        initial_rows = len(df_to_process)
        null_mask = df_to_process[conflict_columns].isnull().any(axis=1)
        if null_mask.any():
            df_to_process = df_to_process[~null_mask]
            removed_count = initial_rows - len(df_to_process)
            self.logger.warning(
                f"UPSERT ({table_name}): 移除了 {removed_count} 行，因为其冲突列 "
                f"({', '.join(conflict_columns)}) 中包含NULL值。"
            )
            if df_to_process.empty:
                self.logger.info(
                    f"UPSERT ({table_name}): 移除包含NULL主键的行后，没有有效数据可执行。"
                )
                return 0

        # --- 执行 ---
        try:
            affected_rows = await self.copy_from_dataframe(
                df=df_to_process,
                table_name=table_name,
                conflict_columns=conflict_columns,
                update_columns=update_columns,
                timestamp_column=timestamp_column,
            )
            self.logger.info(
                f"UPSERT ({table_name}) 完成，通过 COPY 处理了 {affected_rows} 行数据到临时表。"
            )
            return affected_rows
        except asyncio.CancelledError:
            self.logger.warning(f"UPSERT 操作 ({table_name}) 被取消。")
            raise
        except Exception as e:
            self.logger.error(
                f"UPSERT操作失败 (使用COPY策略): {str(e)}\n表: {table_name}"
            )
            raise
