#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Data Aligner for Clean Layer.

This module provides the DataAligner class for aligning DataFrames
to standard formats for dates, identifiers, and primary keys.

Implements Requirements 2.1-2.6 from the design document:
- Align date columns to trade_date format (YYYYMMDD or datetime)
- Align security identifiers to ts_code format (e.g., 000001.SZ)
- Build composite primary keys
- Enforce primary key uniqueness
"""

import logging
import re
from datetime import datetime
from typing import Callable, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class AlignmentError(Exception):
    """
    Exception raised when data alignment fails.
    
    Attributes:
        message: Human-readable error message.
        failed_rows: Index of rows that failed alignment.
    """
    
    def __init__(self, message: str, failed_rows: Optional[pd.Index] = None):
        super().__init__(message)
        self.message = message
        self.failed_rows = failed_rows
    
    def __str__(self):
        if self.failed_rows is not None and len(self.failed_rows) > 0:
            return f"{self.message} (failed rows: {len(self.failed_rows)})"
        return self.message


class DataAligner:
    """
    Data aligner for Clean Layer.
    
    Aligns DataFrames to standard formats:
    - Date columns to trade_date format (YYYYMMDD integer or datetime)
    - Security identifiers to ts_code format (e.g., 000001.SZ)
    - Builds composite primary keys
    
    Dependencies:
    - security_master table for identifier mapping (optional)
    
    Fallback strategy:
    - Mapping failures are logged and original values preserved
    - _mapping_failed column added to mark failed mappings
    
    Example:
        >>> aligner = DataAligner()
        >>> df = aligner.align_date(df, 'date_col')
        >>> df = aligner.align_identifier(df, 'code_col')
        >>> df = aligner.build_primary_key(df, ['trade_date', 'ts_code'])
    """
    
    # Exchange suffix mapping based on code patterns
    EXCHANGE_PATTERNS = {
        # Shanghai: 6xxxxx, 9xxxxx (B shares)
        r'^6\d{5}$': '.SH',
        r'^9\d{5}$': '.SH',
        # Shenzhen: 0xxxxx, 2xxxxx (B shares), 3xxxxx (ChiNext)
        r'^0\d{5}$': '.SZ',
        r'^2\d{5}$': '.SZ',
        r'^3\d{5}$': '.SZ',
        # Beijing: 4xxxxx, 8xxxxx
        r'^4\d{5}$': '.BJ',
        r'^8\d{5}$': '.BJ',
    }
    
    # Prefix patterns for identifier parsing
    PREFIX_PATTERNS = {
        r'^sh(\d{6})$': ('.SH', 1),  # sh600000 -> 600000.SH
        r'^sz(\d{6})$': ('.SZ', 1),  # sz000001 -> 000001.SZ
        r'^SH(\d{6})$': ('.SH', 1),  # SH600000 -> 600000.SH
        r'^SZ(\d{6})$': ('.SZ', 1),  # SZ000001 -> 000001.SZ
        r'^(\d{6})\.SH$': ('.SH', 1),  # Already in ts_code format
        r'^(\d{6})\.SZ$': ('.SZ', 1),  # Already in ts_code format
        r'^(\d{6})\.BJ$': ('.BJ', 1),  # Already in ts_code format
    }
    
    def __init__(self, security_master_loader: Optional[Callable] = None):
        """
        Initialize the DataAligner.
        
        Args:
            security_master_loader: Optional callable that returns a DataFrame
                with security master data for identifier mapping.
                Expected columns: symbol, ts_code
        """
        self._security_master_loader = security_master_loader
        self._security_master: Optional[pd.DataFrame] = None
    
    @property
    def security_master(self) -> Optional[pd.DataFrame]:
        """Lazy-load security master table."""
        if self._security_master is None and self._security_master_loader is not None:
            try:
                self._security_master = self._security_master_loader()
                logger.info(f"Loaded security master with {len(self._security_master)} records")
            except Exception as e:
                logger.warning(f"Failed to load security master: {e}")
        return self._security_master

    def align_date(
        self, 
        df: pd.DataFrame, 
        source_col: str,
        target_col: str = 'trade_date',
        output_format: str = 'int'
    ) -> pd.DataFrame:
        """
        Align date column to standard trade_date format.
        
        Supported source formats:
        - YYYY-MM-DD (string)
        - YYYYMMDD (string or int)
        - datetime/timestamp
        - pandas Timestamp
        
        Output format: YYYYMMDD (int) or datetime
        
        Constraints:
        - Does not change row order
        - Preserves all other columns
        
        Args:
            df: DataFrame to process.
            source_col: Name of the source date column.
            target_col: Name of the target date column (default: 'trade_date').
            output_format: Output format - 'int' for YYYYMMDD integer, 
                          'datetime' for pandas datetime (default: 'int').
            
        Returns:
            DataFrame with aligned date column.
            
        Raises:
            AlignmentError: When date parsing fails for all rows.
        """
        if df is None or df.empty:
            return df
        
        if source_col not in df.columns:
            raise AlignmentError(f"Source column '{source_col}' not found in DataFrame")
        
        result = df.copy()
        source_values = result[source_col]
        
        # Try to parse dates
        parsed_dates = self._parse_dates(source_values)
        
        if output_format == 'int':
            # Convert to YYYYMMDD integer
            result[target_col] = parsed_dates.apply(
                lambda x: int(x.strftime('%Y%m%d')) if pd.notna(x) else np.nan
            )
            # Convert to nullable integer type
            result[target_col] = result[target_col].astype('Int64')
        else:
            # Keep as datetime
            result[target_col] = parsed_dates
        
        # Log conversion
        valid_count = result[target_col].notna().sum()
        total_count = len(result)
        if valid_count < total_count:
            logger.warning(
                f"Date alignment: {valid_count}/{total_count} rows successfully converted "
                f"from '{source_col}' to '{target_col}'"
            )
        else:
            logger.info(
                f"Date alignment: {source_col} -> {target_col}, "
                f"format={output_format}, rows={total_count}"
            )
        
        return result
    
    def _parse_dates(self, series: pd.Series) -> pd.Series:
        """
        Parse a series of date values to datetime.
        
        Handles multiple input formats:
        - YYYY-MM-DD strings
        - YYYYMMDD strings or integers
        - datetime objects
        - pandas Timestamps
        
        Args:
            series: Series of date values.
            
        Returns:
            Series of datetime values.
        """
        # If already datetime, return as-is
        if pd.api.types.is_datetime64_any_dtype(series):
            return series
        
        # Try to convert
        result = pd.Series(index=series.index, dtype='datetime64[ns]')
        
        for idx, val in series.items():
            if pd.isna(val):
                result[idx] = pd.NaT
                continue
            
            try:
                if isinstance(val, (datetime, pd.Timestamp)):
                    result[idx] = pd.Timestamp(val)
                elif isinstance(val, (int, np.integer)):
                    # Assume YYYYMMDD format
                    result[idx] = pd.to_datetime(str(val), format='%Y%m%d')
                elif isinstance(val, str):
                    # Try multiple formats
                    result[idx] = self._parse_date_string(val)
                else:
                    result[idx] = pd.NaT
            except Exception:
                result[idx] = pd.NaT
        
        return result
    
    def _parse_date_string(self, val: str) -> pd.Timestamp:
        """
        Parse a date string trying multiple formats.
        
        Args:
            val: Date string to parse.
            
        Returns:
            Parsed Timestamp.
            
        Raises:
            ValueError: If no format matches.
        """
        formats = [
            '%Y-%m-%d',      # 2024-01-15
            '%Y%m%d',        # 20240115
            '%Y/%m/%d',      # 2024/01/15
            '%d-%m-%Y',      # 15-01-2024
            '%d/%m/%Y',      # 15/01/2024
            '%Y-%m-%d %H:%M:%S',  # 2024-01-15 10:30:00
        ]
        
        val = val.strip()
        
        for fmt in formats:
            try:
                return pd.to_datetime(val, format=fmt)
            except ValueError:
                continue
        
        # Last resort: let pandas infer
        return pd.to_datetime(val)

    def align_identifier(
        self, 
        df: pd.DataFrame, 
        source_col: str,
        target_col: str = 'ts_code',
        strict_mapping: bool = False
    ) -> pd.DataFrame:
        """
        Align security identifier column to ts_code format.
        
        Supported source formats:
        - 000001 -> 000001.SZ (6-digit code, exchange inferred from pattern)
        - sh600000 -> 600000.SH (prefixed format)
        - sz000001 -> 000001.SZ (prefixed format)
        - symbol -> query security_master table
        
        Output format: 000001.SZ (ts_code)
        
        Fallback strategy:
        - Default (strict_mapping=False): Allow write, mark _mapping_failed=True
        - Strict mode (strict_mapping=True): Block write, raise AlignmentError
        
        Args:
            df: DataFrame to process.
            source_col: Name of the source identifier column.
            target_col: Name of the target identifier column (default: 'ts_code').
            strict_mapping: If True, raise error on mapping failure.
            
        Returns:
            DataFrame with aligned identifier column and _mapping_failed column.
            
        Raises:
            AlignmentError: When strict_mapping=True and mapping fails.
        """
        if df is None or df.empty:
            return df
        
        if source_col not in df.columns:
            raise AlignmentError(f"Source column '{source_col}' not found in DataFrame")
        
        result = df.copy()
        source_values = result[source_col]
        
        # Initialize mapping result columns
        mapped_codes = pd.Series(index=result.index, dtype=object)
        mapping_failed = pd.Series(False, index=result.index)
        
        for idx, val in source_values.items():
            if pd.isna(val):
                mapped_codes[idx] = np.nan
                continue
            
            val_str = str(val).strip()
            ts_code = self._map_to_ts_code(val_str)
            
            if ts_code is not None:
                mapped_codes[idx] = ts_code
            else:
                # Mapping failed
                mapping_failed[idx] = True
                mapped_codes[idx] = val_str  # Preserve original value
        
        result[target_col] = mapped_codes
        result['_mapping_failed'] = mapping_failed
        
        # Log mapping results
        failed_count = mapping_failed.sum()
        total_count = len(result)
        
        if failed_count > 0:
            if strict_mapping:
                failed_indices = result.index[mapping_failed]
                raise AlignmentError(
                    f"Identifier mapping failed for {failed_count} rows",
                    failed_rows=failed_indices
                )
            else:
                logger.warning(
                    f"Identifier alignment: {failed_count}/{total_count} rows failed mapping, "
                    f"original values preserved with _mapping_failed=True"
                )
        else:
            logger.info(
                f"Identifier alignment: {source_col} -> {target_col}, "
                f"rows={total_count}, all successful"
            )
        
        return result
    
    def _map_to_ts_code(self, identifier: str) -> Optional[str]:
        """
        Map a single identifier to ts_code format.
        
        Args:
            identifier: The identifier to map.
            
        Returns:
            ts_code format string, or None if mapping fails.
        """
        if not identifier:
            return None
        
        # Check if already in ts_code format
        if re.match(r'^\d{6}\.[A-Z]{2}$', identifier):
            return identifier
        
        # Try prefix patterns (sh600000, sz000001, etc.)
        for pattern, (suffix, group) in self.PREFIX_PATTERNS.items():
            match = re.match(pattern, identifier)
            if match:
                code = match.group(group)
                return f"{code}{suffix}"
        
        # Try 6-digit code with exchange inference
        if re.match(r'^\d{6}$', identifier):
            for pattern, suffix in self.EXCHANGE_PATTERNS.items():
                if re.match(pattern, identifier):
                    return f"{identifier}{suffix}"
        
        # Try security master lookup
        if self.security_master is not None:
            ts_code = self._lookup_security_master(identifier)
            if ts_code is not None:
                return ts_code
        
        # Mapping failed
        return None
    
    def _lookup_security_master(self, identifier: str) -> Optional[str]:
        """
        Look up identifier in security master table.
        
        Args:
            identifier: The identifier to look up.
            
        Returns:
            ts_code if found, None otherwise.
        """
        if self.security_master is None:
            return None
        
        # Try to find by symbol column
        if 'symbol' in self.security_master.columns:
            matches = self.security_master[
                self.security_master['symbol'] == identifier
            ]
            if len(matches) > 0 and 'ts_code' in matches.columns:
                return matches.iloc[0]['ts_code']
        
        # Try to find by name or other columns
        for col in ['name', 'code', 'stock_code']:
            if col in self.security_master.columns:
                matches = self.security_master[
                    self.security_master[col] == identifier
                ]
                if len(matches) > 0 and 'ts_code' in matches.columns:
                    return matches.iloc[0]['ts_code']
        
        return None

    def build_primary_key(
        self, 
        df: pd.DataFrame, 
        keys: List[str],
        enforce_uniqueness: bool = True
    ) -> pd.DataFrame:
        """
        Build composite primary key and optionally enforce uniqueness.
        
        Creates a composite key from multiple columns and ensures uniqueness
        by removing duplicates (keeping the last occurrence).
        
        Args:
            df: DataFrame to process.
            keys: List of column names forming the primary key.
            enforce_uniqueness: If True, remove duplicates keeping last.
            
        Returns:
            DataFrame with unique primary keys (if enforce_uniqueness=True).
            
        Raises:
            AlignmentError: When key columns are missing.
        """
        if df is None or df.empty:
            return df
        
        # Check that all key columns exist
        missing_keys = [k for k in keys if k not in df.columns]
        if missing_keys:
            raise AlignmentError(f"Primary key columns not found: {missing_keys}")
        
        result = df.copy()
        
        if enforce_uniqueness:
            # Check for duplicates
            duplicate_mask = result.duplicated(subset=keys, keep='last')
            duplicate_count = duplicate_mask.sum()
            
            if duplicate_count > 0:
                logger.warning(
                    f"Primary key uniqueness: removing {duplicate_count} duplicate rows "
                    f"(keys: {keys})"
                )
                result = result[~duplicate_mask].copy()
        
        # Log result
        logger.info(
            f"Primary key built: {keys}, rows={len(result)}, "
            f"uniqueness_enforced={enforce_uniqueness}"
        )
        
        return result
    
    def align_all(
        self,
        df: pd.DataFrame,
        date_col: Optional[str] = None,
        identifier_col: Optional[str] = None,
        primary_keys: Optional[List[str]] = None,
        date_output_format: str = 'int',
        strict_mapping: bool = False
    ) -> pd.DataFrame:
        """
        Convenience method to perform all alignment operations.
        
        Args:
            df: DataFrame to process.
            date_col: Source date column name (optional).
            identifier_col: Source identifier column name (optional).
            primary_keys: List of primary key columns (optional).
            date_output_format: Output format for dates ('int' or 'datetime').
            strict_mapping: If True, raise error on identifier mapping failure.
            
        Returns:
            Fully aligned DataFrame.
        """
        result = df
        
        if date_col is not None:
            result = self.align_date(
                result, 
                date_col, 
                output_format=date_output_format
            )
        
        if identifier_col is not None:
            result = self.align_identifier(
                result, 
                identifier_col,
                strict_mapping=strict_mapping
            )
        
        if primary_keys is not None:
            result = self.build_primary_key(result, primary_keys)
        
        return result
