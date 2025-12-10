#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Lineage Tracker for Clean Layer.

This module provides the LineageTracker class for adding data lineage
metadata to DataFrames. It implements the lineage tracking requirements
specified in the design document (Requirements 4.1-4.5).

Lineage columns added:
- _source_table: Source table name(s), comma-separated if multiple
- _processed_at: Processing timestamp in UTC
- _data_version: Data version (format: YYYYMMDD_HHMMSS or batch ID)
- _ingest_job_id: Task execution instance ID for traceability
"""

from datetime import datetime, timezone
from typing import List, Optional, Union
import pandas as pd
import uuid


class LineageError(Exception):
    """
    Exception raised when lineage tracking fails.
    
    Attributes:
        message: Human-readable error message.
    """
    
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


class LineageTracker:
    """
    Lineage tracker for Clean Layer.
    
    Adds data lineage metadata columns to DataFrames, enabling
    traceability of data from source to destination.
    
    Lineage columns:
    - _source_table: Source table name(s)
    - _processed_at: Processing timestamp (UTC)
    - _data_version: Data version identifier
    - _ingest_job_id: Task execution instance ID
    
    Example:
        >>> tracker = LineageTracker()
        >>> df = pd.DataFrame({'col1': [1, 2, 3]})
        >>> result = tracker.add_lineage(
        ...     df,
        ...     source_tables=['tushare.index_daily'],
        ...     job_id='job_123'
        ... )
        >>> '_source_table' in result.columns
        True
    """
    
    # Column names for lineage metadata
    SOURCE_TABLE_COL = '_source_table'
    PROCESSED_AT_COL = '_processed_at'
    DATA_VERSION_COL = '_data_version'
    INGEST_JOB_ID_COL = '_ingest_job_id'
    
    # All lineage columns
    LINEAGE_COLUMNS = [
        SOURCE_TABLE_COL,
        PROCESSED_AT_COL,
        DATA_VERSION_COL,
        INGEST_JOB_ID_COL,
    ]

    def __init__(self):
        """Initialize the LineageTracker."""
        pass
    
    def add_lineage(
        self,
        df: pd.DataFrame,
        source_tables: List[str],
        job_id: str,
        data_version: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Add lineage metadata columns to a DataFrame.
        
        Adds the following columns:
        - _source_table: Source table name(s), comma-separated if multiple
        - _processed_at: Processing timestamp in UTC
        - _data_version: Data version (auto-generated if not provided)
        - _ingest_job_id: Task execution instance ID
        
        Args:
            df: DataFrame to add lineage metadata to.
            source_tables: List of source table names. Multiple sources
                are joined with commas.
            job_id: Task execution instance ID for traceability.
            data_version: Optional data version string. If not provided,
                auto-generates using format YYYYMMDD_HHMMSS.
        
        Returns:
            DataFrame with lineage metadata columns added.
            
        Raises:
            LineageError: When required parameters are invalid.
            
        Note:
            - The input DataFrame is not modified; a copy is returned.
            - If lineage columns already exist, they are overwritten.
            - Empty DataFrames are handled gracefully (columns added but no rows).
        """
        # Validate inputs
        self._validate_inputs(source_tables, job_id)
        
        # Create a copy to avoid modifying the input
        result = df.copy()
        
        # Generate data version if not provided
        if data_version is None:
            data_version = self._generate_data_version()
        
        # Get current UTC timestamp
        processed_at = self._get_utc_timestamp()
        
        # Format source tables as comma-separated string
        source_table_str = self._format_source_tables(source_tables)
        
        # Add lineage columns
        result[self.SOURCE_TABLE_COL] = source_table_str
        result[self.PROCESSED_AT_COL] = processed_at
        result[self.DATA_VERSION_COL] = data_version
        result[self.INGEST_JOB_ID_COL] = job_id
        
        return result
    
    def _validate_inputs(
        self,
        source_tables: List[str],
        job_id: str,
    ) -> None:
        """
        Validate input parameters for add_lineage.
        
        Args:
            source_tables: List of source table names.
            job_id: Task execution instance ID.
            
        Raises:
            LineageError: When validation fails.
        """
        # Validate source_tables
        if source_tables is None:
            raise LineageError("source_tables cannot be None")
        
        if not isinstance(source_tables, list):
            raise LineageError(
                f"source_tables must be a list, got {type(source_tables).__name__}"
            )
        
        if len(source_tables) == 0:
            raise LineageError("source_tables cannot be empty")
        
        for table in source_tables:
            if not isinstance(table, str):
                raise LineageError(
                    f"source_tables must contain strings, got {type(table).__name__}"
                )
            if not table.strip():
                raise LineageError("source_tables cannot contain empty strings")
        
        # Validate job_id
        if job_id is None:
            raise LineageError("job_id cannot be None")
        
        if not isinstance(job_id, str):
            raise LineageError(
                f"job_id must be a string, got {type(job_id).__name__}"
            )
        
        if not job_id.strip():
            raise LineageError("job_id cannot be empty")
    
    def _generate_data_version(self) -> str:
        """
        Generate a data version string.
        
        Format: YYYYMMDD_HHMMSS
        
        Returns:
            Data version string based on current UTC time.
        """
        now = datetime.now(timezone.utc)
        return now.strftime('%Y%m%d_%H%M%S')
    
    def _get_utc_timestamp(self) -> datetime:
        """
        Get the current UTC timestamp.
        
        Returns:
            Current datetime in UTC timezone.
        """
        return datetime.now(timezone.utc)
    
    def _format_source_tables(self, source_tables: List[str]) -> str:
        """
        Format source tables as a comma-separated string.
        
        Args:
            source_tables: List of source table names.
            
        Returns:
            Comma-separated string of source table names.
        """
        # Strip whitespace and join with comma
        cleaned = [table.strip() for table in source_tables]
        return ','.join(cleaned)
    
    @classmethod
    def has_lineage_columns(cls, df: pd.DataFrame) -> bool:
        """
        Check if a DataFrame has all lineage columns.
        
        Args:
            df: DataFrame to check.
            
        Returns:
            True if all lineage columns are present, False otherwise.
        """
        if df is None:
            return False
        return all(col in df.columns for col in cls.LINEAGE_COLUMNS)
    
    @classmethod
    def get_missing_lineage_columns(cls, df: pd.DataFrame) -> List[str]:
        """
        Get the list of missing lineage columns.
        
        Args:
            df: DataFrame to check.
            
        Returns:
            List of lineage column names that are missing.
        """
        if df is None:
            return list(cls.LINEAGE_COLUMNS)
        return [col for col in cls.LINEAGE_COLUMNS if col not in df.columns]
    
    @classmethod
    def validate_lineage_completeness(cls, df: pd.DataFrame) -> bool:
        """
        Validate that all lineage columns have non-null values.
        
        Args:
            df: DataFrame to validate.
            
        Returns:
            True if all lineage columns exist and have no null values,
            False otherwise.
        """
        if df is None or df.empty:
            return False
        
        # Check all lineage columns exist
        if not cls.has_lineage_columns(df):
            return False
        
        # Check no null values in lineage columns
        for col in cls.LINEAGE_COLUMNS:
            if df[col].isna().any():
                return False
        
        return True
    
    @staticmethod
    def generate_job_id(prefix: str = 'job') -> str:
        """
        Generate a unique job ID.
        
        Args:
            prefix: Optional prefix for the job ID.
            
        Returns:
            Unique job ID string.
        """
        unique_id = uuid.uuid4().hex[:12]
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
        return f"{prefix}_{timestamp}_{unique_id}"
