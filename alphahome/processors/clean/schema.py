#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Schema definitions for Clean Layer data validation.

This module defines the TableSchema and ValidationResult data classes
used by the DataValidator to validate incoming data against expected schemas.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Type, Union
import pandas as pd


@dataclass
class TableSchema:
    """
    Table schema definition for data validation.
    
    Defines the expected structure and constraints for a DataFrame,
    including required columns, column types, nullable columns, and value ranges.
    
    Attributes:
        required_columns: List of column names that must be present in the DataFrame.
        column_types: Mapping of column names to their expected Python types.
            Supported types: str, int, float, bool, 'datetime'
        nullable_columns: List of columns that are allowed to contain null values.
            Columns not in this list are considered non-nullable if they are required.
        value_ranges: Mapping of column names to (min, max) tuples defining valid ranges.
            Only applicable to numeric columns.
    
    Example:
        >>> schema = TableSchema(
        ...     required_columns=['trade_date', 'ts_code', 'close'],
        ...     column_types={'trade_date': int, 'ts_code': str, 'close': float},
        ...     nullable_columns=['vol'],
        ...     value_ranges={'close': (0.0, 1e10)}
        ... )
    """
    required_columns: List[str] = field(default_factory=list)
    column_types: Dict[str, Type] = field(default_factory=dict)
    nullable_columns: List[str] = field(default_factory=list)
    value_ranges: Dict[str, Tuple[float, float]] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate schema configuration after initialization."""
        # Ensure required_columns is a list
        if not isinstance(self.required_columns, list):
            self.required_columns = list(self.required_columns)
        
        # Ensure nullable_columns is a list
        if not isinstance(self.nullable_columns, list):
            self.nullable_columns = list(self.nullable_columns)
    
    def get_non_nullable_required_columns(self) -> List[str]:
        """
        Get required columns that are not nullable.
        
        Returns:
            List of column names that are required and must not contain nulls.
        """
        return [col for col in self.required_columns if col not in self.nullable_columns]
    
    def is_column_required(self, column: str) -> bool:
        """Check if a column is required."""
        return column in self.required_columns
    
    def is_column_nullable(self, column: str) -> bool:
        """Check if a column is nullable."""
        return column in self.nullable_columns
    
    def get_expected_type(self, column: str) -> Optional[Type]:
        """Get the expected type for a column, or None if not specified."""
        return self.column_types.get(column)
    
    def get_value_range(self, column: str) -> Optional[Tuple[float, float]]:
        """Get the valid value range for a column, or None if not specified."""
        return self.value_ranges.get(column)


@dataclass
class ValidationResult:
    """
    Result of data validation against a TableSchema.
    
    Contains detailed information about validation failures,
    including missing columns, type errors, null fields, and out-of-range values.
    
    Attributes:
        is_valid: True if all validations passed, False otherwise.
        missing_columns: List of required columns that are missing from the DataFrame.
        type_errors: Mapping of column names to error messages describing type mismatches.
            Format: {column: "expected X, got Y"}
        null_fields: List of non-nullable columns that contain null values.
        out_of_range_rows: Index of rows containing values outside valid ranges.
        dropped_columns: List of columns that were detected as dropped during processing.
            This should always be empty for compliant Clean Layer processing.
        duplicate_count: Number of duplicate records detected by primary key.
        validation_details: Additional validation details for debugging.
    
    Example:
        >>> result = ValidationResult(
        ...     is_valid=False,
        ...     missing_columns=['vol'],
        ...     type_errors={'close': 'expected float, got str'},
        ...     null_fields=['trade_date'],
        ...     out_of_range_rows=pd.Index([5, 10, 15]),
        ...     dropped_columns=[],
        ...     duplicate_count=3
        ... )
    """
    is_valid: bool = True
    missing_columns: List[str] = field(default_factory=list)
    type_errors: Dict[str, str] = field(default_factory=dict)
    null_fields: List[str] = field(default_factory=list)
    out_of_range_rows: pd.Index = field(default_factory=lambda: pd.Index([]))
    dropped_columns: List[str] = field(default_factory=list)
    duplicate_count: int = 0
    validation_details: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Update is_valid based on validation results."""
        self._update_validity()
    
    def _update_validity(self):
        """Recalculate is_valid based on current validation state."""
        self.is_valid = (
            len(self.missing_columns) == 0 and
            len(self.type_errors) == 0 and
            len(self.null_fields) == 0 and
            len(self.dropped_columns) == 0
            # Note: out_of_range_rows and duplicate_count are warnings, not failures
        )
    
    def has_missing_columns(self) -> bool:
        """Check if there are missing required columns."""
        return len(self.missing_columns) > 0
    
    def has_type_errors(self) -> bool:
        """Check if there are type validation errors."""
        return len(self.type_errors) > 0
    
    def has_null_fields(self) -> bool:
        """Check if there are null values in non-nullable fields."""
        return len(self.null_fields) > 0
    
    def has_out_of_range_values(self) -> bool:
        """Check if there are values outside valid ranges."""
        return len(self.out_of_range_rows) > 0
    
    def has_dropped_columns(self) -> bool:
        """Check if any columns were dropped during processing."""
        return len(self.dropped_columns) > 0
    
    def has_duplicates(self) -> bool:
        """Check if duplicate records were detected."""
        return self.duplicate_count > 0
    
    def get_error_summary(self) -> str:
        """
        Get a human-readable summary of validation errors.
        
        Returns:
            String describing all validation failures.
        """
        errors = []
        
        if self.missing_columns:
            errors.append(f"Missing columns: {self.missing_columns}")
        
        if self.type_errors:
            type_err_str = ", ".join(f"{k}: {v}" for k, v in self.type_errors.items())
            errors.append(f"Type errors: {type_err_str}")
        
        if self.null_fields:
            errors.append(f"Null values in required fields: {self.null_fields}")
        
        if self.dropped_columns:
            errors.append(f"Dropped columns detected: {self.dropped_columns}")
        
        if not errors:
            return "No validation errors"
        
        return "; ".join(errors)
    
    def get_warning_summary(self) -> str:
        """
        Get a human-readable summary of validation warnings.
        
        Returns:
            String describing all validation warnings (non-fatal issues).
        """
        warnings = []
        
        if self.has_out_of_range_values():
            warnings.append(f"Out of range values in {len(self.out_of_range_rows)} rows")
        
        if self.has_duplicates():
            warnings.append(f"Duplicate records: {self.duplicate_count}")
        
        if not warnings:
            return "No validation warnings"
        
        return "; ".join(warnings)
    
    def merge(self, other: 'ValidationResult') -> 'ValidationResult':
        """
        Merge another ValidationResult into this one.
        
        Useful for combining results from multiple validation steps.
        
        Args:
            other: Another ValidationResult to merge.
            
        Returns:
            A new ValidationResult combining both results.
        """
        return ValidationResult(
            is_valid=self.is_valid and other.is_valid,
            missing_columns=list(set(self.missing_columns + other.missing_columns)),
            type_errors={**self.type_errors, **other.type_errors},
            null_fields=list(set(self.null_fields + other.null_fields)),
            out_of_range_rows=self.out_of_range_rows.union(other.out_of_range_rows),
            dropped_columns=list(set(self.dropped_columns + other.dropped_columns)),
            duplicate_count=self.duplicate_count + other.duplicate_count,
            validation_details={**self.validation_details, **other.validation_details}
        )
