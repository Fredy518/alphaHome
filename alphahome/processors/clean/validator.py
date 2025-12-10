#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Data Validator for Clean Layer.

This module provides the DataValidator class for validating DataFrames
against TableSchema definitions. It implements the validation requirements
specified in the design document.
"""

from typing import Any, Dict, List, Optional, Tuple, Type
import numpy as np
import pandas as pd

from .schema import TableSchema, ValidationResult


class ValidationError(Exception):
    """
    Exception raised when data validation fails.
    
    Attributes:
        message: Human-readable error message.
        details: ValidationResult containing detailed validation information.
    """
    
    def __init__(self, message: str, details: Optional[ValidationResult] = None):
        super().__init__(message)
        self.message = message
        self.details = details
    
    def __str__(self):
        if self.details:
            return f"{self.message}: {self.details.get_error_summary()}"
        return self.message


class DataValidator:
    """
    Data validator for Clean Layer.
    
    Validates DataFrames against TableSchema definitions, checking:
    - Column types match expected types
    - Required columns are present
    - No null values in non-nullable required fields
    - Values are within specified ranges
    - No columns are silently dropped
    
    Key constraints:
    - Does not silently drop or rename columns
    - Schema configuration comes from TableSchema definition
    - Raises ValidationError when validation fails
    
    Example:
        >>> schema = TableSchema(
        ...     required_columns=['trade_date', 'ts_code', 'close'],
        ...     column_types={'trade_date': int, 'ts_code': str, 'close': float},
        ...     nullable_columns=[],
        ...     value_ranges={'close': (0.0, 1e10)}
        ... )
        >>> validator = DataValidator(schema)
        >>> result = validator.validate(df)
        >>> if not result.is_valid:
        ...     raise ValidationError("Validation failed", result)
    """
    
    def __init__(self, schema: TableSchema):
        """
        Initialize the DataValidator with a schema.
        
        Args:
            schema: TableSchema defining the expected structure and constraints.
        """
        self.schema = schema
    
    def validate(self, df: pd.DataFrame) -> ValidationResult:
        """
        Validate a DataFrame against the schema.
        
        Performs all validation checks and returns a ValidationResult
        containing detailed information about any failures.
        
        Args:
            df: DataFrame to validate.
            
        Returns:
            ValidationResult with validation status and details.
            
        Raises:
            ValidationError: When critical validation fails (missing columns, null in required).
        """
        result = ValidationResult()
        
        if df is None or df.empty:
            # Empty DataFrame is valid but may have warnings
            return result
        
        # Check for missing required columns
        missing = self.detect_missing_columns(df)
        if missing:
            result.missing_columns = missing
            result.is_valid = False
        
        # Check column types (only for columns that exist)
        type_errors = self.validate_column_types(df)
        if type_errors:
            result.type_errors = type_errors
            result.is_valid = False
        
        # Check for null values in non-nullable required columns
        null_fields = self.detect_nulls(
            df, 
            self.schema.get_non_nullable_required_columns()
        )
        if null_fields:
            result.null_fields = null_fields
            result.is_valid = False
        
        # Check for out-of-range values
        out_of_range = self.detect_out_of_range(df, self.schema.value_ranges)
        if len(out_of_range) > 0:
            result.out_of_range_rows = out_of_range
            # Note: out_of_range is a warning, not a failure
        
        return result
    
    def validate_column_types(self, df: pd.DataFrame) -> Dict[str, str]:
        """
        Validate column types against the schema.
        
        Checks that each column's dtype is compatible with the expected type.
        
        Args:
            df: DataFrame to validate.
            
        Returns:
            Dictionary mapping column names to error messages for type mismatches.
            Format: {column: "expected X, got Y"}
        """
        type_errors = {}
        
        for column, expected_type in self.schema.column_types.items():
            if column not in df.columns:
                # Skip columns that don't exist (handled by missing column check)
                continue
            
            actual_dtype = df[column].dtype
            
            if not self._is_type_compatible(actual_dtype, expected_type):
                type_errors[column] = (
                    f"expected {self._type_name(expected_type)}, "
                    f"got {actual_dtype}"
                )
        
        return type_errors
    
    def detect_missing_columns(self, df: pd.DataFrame) -> List[str]:
        """
        Detect required columns that are missing from the DataFrame.
        
        Args:
            df: DataFrame to check.
            
        Returns:
            List of missing required column names.
        """
        if df is None:
            return list(self.schema.required_columns)
        
        return [
            col for col in self.schema.required_columns 
            if col not in df.columns
        ]
    
    def detect_duplicates(
        self, 
        df: pd.DataFrame, 
        keys: List[str]
    ) -> pd.DataFrame:
        """
        Detect duplicate records by primary key.
        
        Args:
            df: DataFrame to check.
            keys: List of column names forming the primary key.
            
        Returns:
            DataFrame containing only the duplicate rows.
        """
        if df is None or df.empty:
            return pd.DataFrame()
        
        # Check that all key columns exist
        missing_keys = [k for k in keys if k not in df.columns]
        if missing_keys:
            raise ValueError(f"Key columns not found: {missing_keys}")
        
        # Find duplicates (keep='first' marks all but first occurrence as duplicates)
        duplicated_mask = df.duplicated(subset=keys, keep='first')
        return df[duplicated_mask]
    
    def detect_nulls(
        self, 
        df: pd.DataFrame, 
        required_cols: List[str]
    ) -> List[str]:
        """
        Detect null values in required columns.
        
        Args:
            df: DataFrame to check.
            required_cols: List of column names that must not contain nulls.
            
        Returns:
            List of column names that contain null values.
        """
        if df is None or df.empty:
            return []
        
        null_fields = []
        for col in required_cols:
            if col in df.columns and df[col].isna().any():
                null_fields.append(col)
        
        return null_fields
    
    def detect_out_of_range(
        self, 
        df: pd.DataFrame, 
        ranges: Dict[str, Tuple[float, float]]
    ) -> pd.Index:
        """
        Detect records with values outside valid ranges.
        
        Args:
            df: DataFrame to check.
            ranges: Dictionary mapping column names to (min, max) tuples.
            
        Returns:
            Index of rows containing out-of-range values.
        """
        if df is None or df.empty or not ranges:
            return pd.Index([])
        
        out_of_range_mask = pd.Series(False, index=df.index)
        
        for column, (min_val, max_val) in ranges.items():
            if column not in df.columns:
                continue
            
            # Only check numeric columns
            if not pd.api.types.is_numeric_dtype(df[column]):
                continue
            
            # Check for values outside range (excluding NaN)
            col_values = df[column]
            below_min = col_values < min_val
            above_max = col_values > max_val
            out_of_range_mask = out_of_range_mask | below_min | above_max
        
        return df.index[out_of_range_mask]
    
    def detect_dropped_columns(
        self, 
        input_cols: List[str], 
        output_cols: List[str]
    ) -> List[str]:
        """
        Detect columns that were dropped during processing.
        
        This method should return an empty list for compliant Clean Layer
        processing, as columns should never be silently dropped.
        
        Args:
            input_cols: List of column names in the input DataFrame.
            output_cols: List of column names in the output DataFrame.
            
        Returns:
            List of column names that were dropped (should be empty).
        """
        input_set = set(input_cols)
        output_set = set(output_cols)
        
        # Dropped columns are those in input but not in output
        dropped = input_set - output_set
        return list(dropped)
    
    def deduplicate(
        self, 
        df: pd.DataFrame, 
        keys: List[str],
        keep: str = 'last'
    ) -> Tuple[pd.DataFrame, int]:
        """
        Remove duplicate records, keeping the specified occurrence.
        
        Args:
            df: DataFrame to deduplicate.
            keys: List of column names forming the primary key.
            keep: Which duplicate to keep ('first', 'last').
            
        Returns:
            Tuple of (deduplicated DataFrame, count of removed duplicates).
        """
        if df is None or df.empty:
            return df, 0
        
        original_len = len(df)
        deduplicated = df.drop_duplicates(subset=keys, keep=keep)
        removed_count = original_len - len(deduplicated)
        
        return deduplicated, removed_count
    
    def add_validation_flag(
        self, 
        df: pd.DataFrame, 
        out_of_range_rows: pd.Index,
        flag_column: str = '_validation_flag'
    ) -> pd.DataFrame:
        """
        Add a validation flag column to mark out-of-range records.
        
        Args:
            df: DataFrame to modify.
            out_of_range_rows: Index of rows to flag.
            flag_column: Name of the flag column to add.
            
        Returns:
            DataFrame with validation flag column added.
        """
        result = df.copy()
        result[flag_column] = 0
        result.loc[out_of_range_rows, flag_column] = 1
        return result
    
    def _is_type_compatible(self, actual_dtype, expected_type: Type) -> bool:
        """
        Check if an actual dtype is compatible with an expected type.
        
        Args:
            actual_dtype: The actual pandas dtype.
            expected_type: The expected Python type.
            
        Returns:
            True if types are compatible, False otherwise.
        """
        if expected_type == str:
            return pd.api.types.is_string_dtype(actual_dtype) or actual_dtype == object
        
        elif expected_type == int:
            return pd.api.types.is_integer_dtype(actual_dtype)
        
        elif expected_type == float:
            return pd.api.types.is_float_dtype(actual_dtype)
        
        elif expected_type == bool:
            return pd.api.types.is_bool_dtype(actual_dtype)
        
        elif expected_type == 'datetime' or expected_type == pd.Timestamp:
            return pd.api.types.is_datetime64_any_dtype(actual_dtype)
        
        # For other types, do a basic check
        return True
    
    def _type_name(self, type_obj: Type) -> str:
        """Get a human-readable name for a type."""
        if type_obj == str:
            return 'str'
        elif type_obj == int:
            return 'int'
        elif type_obj == float:
            return 'float'
        elif type_obj == bool:
            return 'bool'
        elif type_obj == 'datetime' or type_obj == pd.Timestamp:
            return 'datetime'
        return str(type_obj)
