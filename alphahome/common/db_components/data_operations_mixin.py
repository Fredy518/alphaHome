import asyncio
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Union

import asyncpg
import pandas as pd


class DataOperationsMixin:
    """数据操作Mixin - 提供复杂的数据操作功能，如批量导入、UPSERT等"""

    async def copy_from_dataframe(
        self,
        df: pd.DataFrame,
        target: Any,
        conflict_columns: Optional[List[str]] = None,
        update_columns: Optional[List[str]] = None,
        timestamp_column: Optional[str] = None,
    ):
        """将DataFrame数据高效复制并可选地UPSERT到数据库表中。

        利用 PostgreSQL 的 COPY 命令和临时表实现高效数据加载。
        如果指定了 conflict_columns，则执行 UPSERT (插入或更新) 操作。

        Args:
            df (pd.DataFrame): 要复制的DataFrame。
            target (Any): 目标表，可以是表名字符串或任务对象。
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

        schema, table_name = self.resolver.get_schema_and_table(target)
        resolved_table_name = f'"{schema}"."{table_name}"'

        if df.empty:
            self.logger.info(
                f"copy_from_dataframe: 表 '{resolved_table_name}' 的DataFrame为空，跳过操作。"
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
            self.logger.info(
                f"COPY_FROM_DATAFRAME (表: {resolved_table_name}): 时间戳列 '{timestamp_column}' 未在DataFrame列中找到，自动添加当前时间。"
            )
            df[timestamp_column] = datetime.now()
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
        # 从解析后的名称中获取不带schema的表名用于临时表
        simple_table_name = table_name.strip('"')
        temp_table = f"temp_{simple_table_name}_{timestamp_ms}_{id(df)}"

        create_temp_table_sql = f'''
        CREATE TEMPORARY TABLE "{temp_table}" (LIKE {resolved_table_name} INCLUDING DEFAULTS) ON COMMIT DROP;
        '''

        # --- 使用生成器准备记录以减少内存占用 ---
        async def _df_to_records_generator(df_internal: pd.DataFrame):
            for row_tuple in df_internal.itertuples(index=False, name=None):
                processed_values = []
                for val in row_tuple:
                    if pd.isna(val):
                        processed_values.append(None)
                    elif isinstance(val, str):
                        # 仅清理真正有问题的字符，保留正常的空格
                        cleaned_val = val.replace('\x00', '')  # 移除NULL字符
                        cleaned_val = cleaned_val.replace('\r', '')  # 移除回车符
                        cleaned_val = cleaned_val.replace('\n', '')  # 移除换行符
                        cleaned_val = cleaned_val.replace('\t', ' ')  # 制表符替换为空格
                        # 不要替换双引号，让asyncpg自己处理
                        processed_values.append(cleaned_val if cleaned_val else None)
                    elif pd.api.types.is_datetime64_any_dtype(pd.Series([val])):
                        # 处理pandas datetime对象 - 这是关键修复！
                        if pd.isnull(val):
                            processed_values.append(None)
                        else:
                            # 将pandas timestamp转换为Python date对象
                            processed_values.append(val.date() if hasattr(val, 'date') else val)
                    else:
                        processed_values.append(val)
                
                record = tuple(processed_values)
                yield record

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
                                            f'{resolved_table_name}."{col}" IS DISTINCT FROM EXCLUDED."{col}"'
                                            for col in content_columns_to_compare
                                        ]
                                        condition_str = " OR ".join(conditions)
                                        timestamp_update_clause = (
                                            f'"{timestamp_column}" = CASE '
                                            f"WHEN {condition_str} THEN NOW() "
                                            f'ELSE {resolved_table_name}."{timestamp_column}" END'
                                        )
                                        set_clauses.append(timestamp_update_clause)

                            if set_clauses:
                                update_clause = ", ".join(set_clauses)
                                action_sql = f"DO UPDATE SET {update_clause}"
                            else:
                                self.logger.warning(
                                    f"UPSERT (表: {resolved_table_name}): set_clauses变为空。默认为DO NOTHING。"
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

                        insert_sql = f'''
                        INSERT INTO {resolved_table_name} ({target_col_str})
                        SELECT {target_col_str}
                        FROM "{temp_table}"
                        ON CONFLICT ({conflict_col_str}) {action_sql};
                        '''

                        self.logger.debug(
                            f"UPSERT (表: {resolved_table_name}): DataFrame头部 (前3行, 用于EXCLUDED上下文):\n{df.head(3).to_string()}"
                        )
                        self.logger.debug(
                            f"为表 {resolved_table_name} 执行最终UPSERT SQL:\n{insert_sql}"
                        )

                    else:
                        # --- 简单 INSERT 逻辑 ---
                        insert_sql = f'''
                        INSERT INTO {resolved_table_name} ({target_col_str})
                        SELECT {target_col_str} 
                        FROM "{temp_table}";
                        '''

                    # 执行最终传输
                    status = await conn.execute(insert_sql)
                    self.logger.debug(f"最终插入/更新命令状态: {status}")

                    # --- 从状态字符串中解析整数 ---
                    rows_copied = 0
                    try:
                        if isinstance(copy_count, str):
                            if copy_count.startswith("COPY "):
                                rows_copied = int(copy_count.split()[-1])
                            elif copy_count.startswith("INSERT "):
                                # 处理INSERT状态格式: "INSERT 0 {count}"
                                rows_copied = int(copy_count.split()[-1])
                            else:
                                self.logger.warning(
                                    f"未知的状态格式: {copy_count}。假设复制了0行。"
                                )
                        elif isinstance(copy_count, int):
                            # 如果直接返回整数
                            rows_copied = copy_count
                        else:
                            self.logger.warning(
                                f"意外的copy_count类型: {type(copy_count)}，值: {copy_count}。假设复制了0行。"
                            )
                    except (ValueError, IndexError) as parse_err:
                        self.logger.error(
                            f"从copy_count '{copy_count}' 解析行数失败: {parse_err}。假设复制了0行。"
                        )

                    return rows_copied

                except asyncpg.exceptions.UndefinedTableError as e:
                    self.logger.error(
                        f"创建临时表失败 (LIKE '{resolved_table_name}')。目标表可能不存在。错误: {e}"
                    )
                    raise ValueError(f"目标表 '{resolved_table_name}' 未找到。") from e
                except asyncpg.exceptions.UndefinedColumnError as e:
                    self.logger.error(
                        f"表 '{resolved_table_name}' 的复制/插入过程中发生列不匹配错误。错误: {e}"
                    )
                    self.logger.error(f"DataFrame 列: {df_columns}")
                    raise ValueError(
                        f"DataFrame和表 '{resolved_table_name}' 之间列不匹配。请检查DataFrame列和表结构。"
                    ) from e
                except Exception as e:
                    self.logger.error(
                        f"高效复制/UPSERT操作失败 (表: {resolved_table_name}): {str(e)}"
                    )
                    self.logger.error(f"临时表曾是: {temp_table}")
                    raise

    async def _fallback_insert_to_temp_table(
        self, conn, temp_table: str, df: pd.DataFrame, df_columns: List[str]
    ) -> str:
        """
        当COPY操作失败时的回退方法，使用批量INSERT语句将数据插入临时表。
        
        Args:
            conn: 数据库连接
            temp_table: 临时表名
            df: 要插入的DataFrame
            df_columns: 列名列表
            
        Returns:
            str: 插入状态字符串，格式为"INSERT 0 {count}"
        """
        import asyncio
        
        # 创建占位符
        placeholders = ", ".join([f"${i+1}" for i in range(len(df_columns))])
        column_names = ", ".join([f'"{col}"' for col in df_columns])
        
        insert_sql = f'INSERT INTO "{temp_table}" ({column_names}) VALUES ({placeholders})'
        
        # 准备数据
        records = []
        for _, row in df.iterrows():
            record = []
            for col in df_columns:
                val = row[col]
                if pd.isna(val):
                    record.append(None)
                elif isinstance(val, str):
                    # 清理字符串数据以避免 COPY 操作的格式问题
                    # 移除可能导致问题的字符
                    cleaned_val = val.replace('\x00', '')  # 移除NULL字符
                    cleaned_val = cleaned_val.replace('\r', ' ')  # 替换回车符
                    cleaned_val = cleaned_val.replace('\n', ' ')  # 替换换行符
                    cleaned_val = cleaned_val.replace('\t', ' ')  # 替换制表符
                    cleaned_val = cleaned_val.replace('"', "'")  # 替换双引号为单引号
                    cleaned_val = cleaned_val.strip()
                    record.append(cleaned_val if cleaned_val else None)
                elif pd.api.types.is_datetime64_any_dtype(pd.Series([val])):
                    # 处理pandas datetime对象 - 这是关键修复！
                    if pd.isnull(val):
                        record.append(None)
                    else:
                        # 将pandas timestamp转换为Python date对象
                        record.append(val.date() if hasattr(val, 'date') else val)
                else:
                    record.append(val)
            records.append(record)
        
        # 批量插入
        batch_size = 1000
        total_inserted = 0
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            try:
                await conn.executemany(insert_sql, batch)
                total_inserted += len(batch)
                self.logger.debug(f"批量INSERT: 已插入 {len(batch)} 条记录到 {temp_table}")
            except Exception as e:
                self.logger.error(f"批量INSERT失败: {e}")
                # 如果批量失败，尝试逐行插入
                for record in batch:
                    try:
                        await conn.execute(insert_sql, *record)
                        total_inserted += 1
                    except Exception as row_error:
                        self.logger.warning(f"跳过有问题的行: {row_error}")
                        continue
        
        return f"INSERT 0 {total_inserted}"

    async def upsert(
        self,
        df: pd.DataFrame,
        target: Any,
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None,
        timestamp_column: Optional[str] = None,
    ):
        """
        使用 COPY 和 ON CONFLICT...DO UPDATE 实现高效的 UPSERT 操作。
        
        Args:
            df (pd.DataFrame): 待插入或更新的数据。
            target (Any): 目标表，可以是表名字符串或任务对象。
            conflict_columns (List[str]): 唯一约束或主键列，用于判断冲突。
            update_columns (Optional[List[str]]): 发生冲突时需要更新的列。
                如果为 None，则更新 DataFrame 中除冲突列外的所有列。
            timestamp_column (Optional[str]): 记录更新时间的列。
        """
        # upsert 本质上是 copy_from_dataframe 的一种特定用法
        return await self.copy_from_dataframe(
            df=df,
            target=target,
            conflict_columns=conflict_columns,
            update_columns=update_columns,
            timestamp_column=timestamp_column,
        )
