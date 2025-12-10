#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Clean Layer Writer for idempotent data writing.

This module provides the CleanLayerWriter class for writing DataFrames
to the clean schema with UPSERT semantics. It implements the idempotent
data loading requirements specified in the design document (Requirements 5.1-5.5).

Key features:
- UPSERT with configurable conflict strategy (default: replace)
- Transaction management for atomicity
- Batch writing with configurable batch size
- Exponential backoff retry on failures
"""

import asyncio
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from datetime import datetime, timezone
import pandas as pd

logger = logging.getLogger(__name__)


class WriteError(Exception):
    """
    Exception raised when data writing fails.
    
    Attributes:
        message: Human-readable error message.
        details: Additional error details.
    """
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}
    
    def __str__(self):
        if self.details:
            return f"{self.message}: {self.details}"
        return self.message


class CleanLayerWriter:
    """
    Clean Layer data writer with UPSERT semantics.
    
    Provides idempotent data writing to the clean schema with:
    - UPSERT (INSERT ON CONFLICT UPDATE) for primary key conflicts
    - Transaction management for atomicity
    - Batch writing for large DataFrames
    - Exponential backoff retry on failures
    
    Default configuration:
    - batch_size: 10000 (configurable)
    - max_retries: 3 (configurable)
    - conflict_strategy: 'replace' (default, full row replacement)
    
    Constraints:
    - Uses database transactions to ensure atomicity
    - Failed batches are rolled back entirely
    - Supports exponential backoff retry
    
    Example:
        >>> writer = CleanLayerWriter(db_connection)
        >>> rows_written = await writer.upsert(
        ...     df,
        ...     table_name='clean.index_valuation_base',
        ...     primary_keys=['trade_date', 'ts_code']
        ... )
    """

    # Supported conflict strategies
    STRATEGY_REPLACE = 'replace'
    STRATEGY_MERGE = 'merge'
    SUPPORTED_STRATEGIES = [STRATEGY_REPLACE]  # merge requires explicit approval
    
    def __init__(
        self,
        db_connection: Any,
        batch_size: int = 10000,
        max_retries: int = 3,
        retry_delay_base: float = 2.0,
    ):
        """
        Initialize the CleanLayerWriter.
        
        Args:
            db_connection: Database connection object. Must support:
                - execute(sql, params) for SQL execution
                - transaction context manager for atomicity
            batch_size: Number of rows per batch (default: 10000).
            max_retries: Maximum retry attempts on failure (default: 3).
            retry_delay_base: Base delay for exponential backoff in seconds (default: 2.0).
        """
        self.db = db_connection
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.retry_delay_base = retry_delay_base
    
    async def upsert(
        self,
        df: pd.DataFrame,
        table_name: str,
        primary_keys: List[str],
        conflict_strategy: str = 'replace',
    ) -> int:
        """
        Perform idempotent UPSERT of DataFrame to table.
        
        Writes data with UPSERT semantics: inserts new rows and updates
        existing rows based on primary key conflicts.
        
        Args:
            df: DataFrame to write.
            table_name: Target table name (should include schema, e.g., 'clean.table_name').
            primary_keys: List of column names forming the primary key.
            conflict_strategy: Conflict resolution strategy.
                - 'replace': Full row replacement (default, recommended)
                - 'merge': Not implemented (requires explicit approval per design.md)
        
        Returns:
            Number of rows written.
        
        Raises:
            WriteError: When writing fails after all retry attempts.
            ValueError: When invalid parameters are provided.
        
        Note:
            - The 'merge' strategy is intentionally not implemented.
              Per design.md, merge strategy requires explicit approval
              and column-level strategy definition before implementation.
            - Empty DataFrames are handled gracefully (returns 0).
        """
        # Validate inputs
        self._validate_inputs(df, table_name, primary_keys, conflict_strategy)
        
        # Handle empty DataFrame
        if df is None or df.empty:
            logger.info(f"Empty DataFrame, skipping write to {table_name}")
            return 0
        
        # Verify all primary key columns exist
        missing_keys = [k for k in primary_keys if k not in df.columns]
        if missing_keys:
            raise ValueError(f"Primary key columns not found in DataFrame: {missing_keys}")
        
        # Split into batches and write
        total_rows = 0
        batches = self._split_into_batches(df)
        
        logger.info(
            f"Writing {len(df)} rows to {table_name} in {len(batches)} batches "
            f"(batch_size={self.batch_size})"
        )
        
        for batch_idx, batch_df in enumerate(batches):
            rows_written = await self._write_batch_with_retry(
                batch_df,
                table_name,
                primary_keys,
                conflict_strategy,
                batch_idx,
            )
            total_rows += rows_written
        
        logger.info(f"Successfully wrote {total_rows} rows to {table_name}")
        return total_rows
    
    def _validate_inputs(
        self,
        df: pd.DataFrame,
        table_name: str,
        primary_keys: List[str],
        conflict_strategy: str,
    ) -> None:
        """
        Validate input parameters for upsert.
        
        Args:
            df: DataFrame to validate.
            table_name: Target table name.
            primary_keys: Primary key columns.
            conflict_strategy: Conflict resolution strategy.
        
        Raises:
            ValueError: When validation fails.
        """
        if not table_name or not table_name.strip():
            raise ValueError("table_name cannot be empty")
        
        if not primary_keys:
            raise ValueError("primary_keys cannot be empty")
        
        if conflict_strategy not in self.SUPPORTED_STRATEGIES:
            if conflict_strategy == self.STRATEGY_MERGE:
                raise ValueError(
                    "Merge strategy is not implemented. "
                    "Per design.md, merge strategy requires explicit approval "
                    "and column-level strategy definition before implementation."
                )
            raise ValueError(
                f"Unsupported conflict strategy: {conflict_strategy}. "
                f"Supported strategies: {self.SUPPORTED_STRATEGIES}"
            )
    
    def _split_into_batches(self, df: pd.DataFrame) -> List[pd.DataFrame]:
        """
        Split DataFrame into batches.
        
        Args:
            df: DataFrame to split.
        
        Returns:
            List of DataFrame batches.
        """
        if len(df) <= self.batch_size:
            return [df]
        
        batches = []
        for start_idx in range(0, len(df), self.batch_size):
            end_idx = min(start_idx + self.batch_size, len(df))
            batches.append(df.iloc[start_idx:end_idx])
        
        return batches

    async def _write_batch_with_retry(
        self,
        batch_df: pd.DataFrame,
        table_name: str,
        primary_keys: List[str],
        conflict_strategy: str,
        batch_idx: int,
    ) -> int:
        """
        Write a single batch with exponential backoff retry.
        
        Args:
            batch_df: DataFrame batch to write.
            table_name: Target table name.
            primary_keys: Primary key columns.
            conflict_strategy: Conflict resolution strategy.
            batch_idx: Batch index for logging.
        
        Returns:
            Number of rows written.
        
        Raises:
            WriteError: When all retry attempts fail.
        """
        last_error = None
        
        for attempt in range(self.max_retries):
            try:
                rows_written = await self._execute_upsert(
                    batch_df,
                    table_name,
                    primary_keys,
                    conflict_strategy,
                )
                return rows_written
                
            except Exception as e:
                last_error = e
                
                if attempt < self.max_retries - 1:
                    delay = self._calculate_retry_delay(attempt)
                    logger.warning(
                        f"Batch {batch_idx} write attempt {attempt + 1} failed: {e}. "
                        f"Retrying in {delay:.1f}s..."
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        f"Batch {batch_idx} write failed after {self.max_retries} attempts: {e}"
                    )
        
        raise WriteError(
            f"Failed to write batch {batch_idx} after {self.max_retries} attempts",
            details={
                'batch_idx': batch_idx,
                'batch_size': len(batch_df),
                'table_name': table_name,
                'last_error': str(last_error),
            }
        )
    
    def _calculate_retry_delay(self, attempt: int) -> float:
        """
        Calculate exponential backoff delay.
        
        Args:
            attempt: Current attempt number (0-indexed).
        
        Returns:
            Delay in seconds.
        """
        return self.retry_delay_base ** (attempt + 1)
    
    async def _execute_upsert(
        self,
        df: pd.DataFrame,
        table_name: str,
        primary_keys: List[str],
        conflict_strategy: str,
    ) -> int:
        """
        Execute the actual UPSERT operation within a transaction.
        
        This method handles the database-specific UPSERT logic.
        Currently supports PostgreSQL-style ON CONFLICT syntax.
        
        Args:
            df: DataFrame to write.
            table_name: Target table name.
            primary_keys: Primary key columns.
            conflict_strategy: Conflict resolution strategy.
        
        Returns:
            Number of rows written.
        
        Note:
            This is a template implementation. The actual database
            interaction depends on the db_connection interface.
            Subclasses or adapters may override this for specific databases.
        """
        # Build column list
        columns = list(df.columns)
        
        # Build UPSERT SQL (PostgreSQL style)
        sql = self._build_upsert_sql(table_name, columns, primary_keys, conflict_strategy)
        
        # Convert DataFrame to list of tuples for insertion
        values = [tuple(row) for row in df.itertuples(index=False, name=None)]
        
        # Execute within transaction
        try:
            # Check if db supports async context manager
            if hasattr(self.db, 'transaction'):
                async with self.db.transaction():
                    await self._execute_batch_insert(sql, values)
            else:
                # Fallback for simpler db interfaces
                await self._execute_batch_insert(sql, values)
            
            return len(df)
            
        except Exception as e:
            logger.error(f"UPSERT execution failed: {e}")
            raise
    
    def _build_upsert_sql(
        self,
        table_name: str,
        columns: List[str],
        primary_keys: List[str],
        conflict_strategy: str,
    ) -> str:
        """
        Build PostgreSQL-style UPSERT SQL statement.
        
        Args:
            table_name: Target table name.
            columns: List of column names.
            primary_keys: Primary key columns.
            conflict_strategy: Conflict resolution strategy.
        
        Returns:
            SQL statement string.
        """
        # Column names for INSERT
        col_names = ', '.join(columns)
        
        # Placeholders for VALUES
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Primary key constraint for ON CONFLICT
        pk_constraint = ', '.join(primary_keys)
        
        # Build UPDATE SET clause for non-primary-key columns
        non_pk_columns = [c for c in columns if c not in primary_keys]
        
        if conflict_strategy == self.STRATEGY_REPLACE:
            # Full row replacement: update all non-PK columns
            if non_pk_columns:
                update_clause = ', '.join(
                    f"{col} = EXCLUDED.{col}" for col in non_pk_columns
                )
                sql = f"""
                    INSERT INTO {table_name} ({col_names})
                    VALUES ({placeholders})
                    ON CONFLICT ({pk_constraint})
                    DO UPDATE SET {update_clause}
                """
            else:
                # All columns are primary keys, just do nothing on conflict
                sql = f"""
                    INSERT INTO {table_name} ({col_names})
                    VALUES ({placeholders})
                    ON CONFLICT ({pk_constraint})
                    DO NOTHING
                """
        else:
            # Should not reach here due to validation
            raise ValueError(f"Unsupported conflict strategy: {conflict_strategy}")
        
        return sql.strip()
    
    async def _execute_batch_insert(
        self,
        sql: str,
        values: List[Tuple],
    ) -> None:
        """
        Execute batch insert with the database connection.
        
        Args:
            sql: SQL statement to execute.
            values: List of value tuples to insert.
        
        Note:
            This method adapts to different database connection interfaces.
        """
        if hasattr(self.db, 'executemany'):
            # Standard DB-API style
            await self._async_executemany(sql, values)
        elif hasattr(self.db, 'execute_batch'):
            # Some async libraries use execute_batch
            await self.db.execute_batch(sql, values)
        else:
            # Fallback: execute one by one
            for value_tuple in values:
                await self._async_execute(sql, value_tuple)
    
    async def _async_executemany(self, sql: str, values: List[Tuple]) -> None:
        """Execute many with async support."""
        if asyncio.iscoroutinefunction(self.db.executemany):
            await self.db.executemany(sql, values)
        else:
            self.db.executemany(sql, values)
    
    async def _async_execute(self, sql: str, params: Tuple) -> None:
        """Execute single statement with async support."""
        if asyncio.iscoroutinefunction(self.db.execute):
            await self.db.execute(sql, params)
        else:
            self.db.execute(sql, params)
    
    def verify_columns_preserved(
        self,
        input_df: pd.DataFrame,
        output_df: pd.DataFrame,
        allowed_new_columns: Optional[List[str]] = None,
    ) -> Tuple[bool, List[str]]:
        """
        Verify that no columns were dropped during processing.
        
        This method supports Property 6: Column preservation.
        Output columns should be a superset of input columns.
        
        Args:
            input_df: Original input DataFrame.
            output_df: Processed output DataFrame.
            allowed_new_columns: List of columns that are allowed to be added
                (e.g., lineage columns, validation flags).
        
        Returns:
            Tuple of (is_preserved, dropped_columns).
            is_preserved is True if no columns were dropped.
            dropped_columns is the list of columns that were dropped.
        """
        if allowed_new_columns is None:
            allowed_new_columns = [
                '_source_table',
                '_processed_at',
                '_data_version',
                '_ingest_job_id',
                '_validation_flag',
            ]
        
        input_cols = set(input_df.columns)
        output_cols = set(output_df.columns)
        
        # Check for dropped columns
        dropped = input_cols - output_cols
        
        # Check that new columns are only allowed ones
        new_cols = output_cols - input_cols
        unexpected_new = new_cols - set(allowed_new_columns)
        
        is_preserved = len(dropped) == 0
        
        if unexpected_new:
            logger.warning(f"Unexpected new columns added: {unexpected_new}")
        
        return is_preserved, list(dropped)
