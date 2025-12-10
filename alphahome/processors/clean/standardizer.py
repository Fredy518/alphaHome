#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Data Standardizer for Clean Layer.

This module provides the DataStandardizer class for standardizing data units
and preserving original values during transformations.

Implements Requirements 3.1-3.5 from the design document:
- Convert monetary values to CNY (Yuan)
- Convert volume units to shares (股)
- Document price adjustment method in metadata
- Preserve original unadjusted price columns
"""

import logging
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class StandardizationError(Exception):
    """
    Exception raised when data standardization fails.
    
    Attributes:
        message: Human-readable error message.
        column: The column that failed standardization.
    """
    
    def __init__(self, message: str, column: Optional[str] = None):
        super().__init__(message)
        self.message = message
        self.column = column
    
    def __str__(self):
        if self.column:
            return f"{self.message} (column: {self.column})"
        return self.message


class DataStandardizer:
    """
    Data standardizer for Clean Layer.
    
    Standardizes data units and preserves original values:
    - Converts monetary values to CNY (Yuan)
    - Converts volume units to shares (股)
    - Preserves original unadjusted price columns with _unadj suffix
    
    Constraints:
    - Preserves original unit columns when preserve_original=True
    - Records conversion logs via logger.info
    
    Example:
        >>> standardizer = DataStandardizer()
        >>> df = standardizer.convert_monetary(df, 'amount', '万元')
        >>> df = standardizer.convert_volume(df, 'vol', '手')
        >>> df = standardizer.preserve_unadjusted(df, ['close', 'open'])
    """
    
    # Unit conversion factors
    UNIT_CONVERSIONS: Dict[str, float] = {
        '万元': 10000.0,
        '亿元': 100000000.0,
        '手': 100.0,
    }

    # Monetary unit aliases
    MONETARY_UNITS: Dict[str, str] = {
        '万元': '万元',
        '万': '万元',
        '10000': '万元',
        '亿元': '亿元',
        '亿': '亿元',
        '100000000': '亿元',
        '元': '元',  # No conversion needed
    }
    
    # Volume unit aliases
    VOLUME_UNITS: Dict[str, str] = {
        '手': '手',
        'lot': '手',
        'lots': '手',
        '股': '股',  # No conversion needed
        'share': '股',
        'shares': '股',
    }
    
    def __init__(self):
        """Initialize the DataStandardizer."""
        pass
    
    def convert_monetary(
        self, 
        df: pd.DataFrame, 
        col: str, 
        source_unit: str,
        preserve_original: bool = True,
    ) -> pd.DataFrame:
        """
        Convert monetary column to Yuan (元).
        
        Applies the appropriate conversion factor based on source unit
        and optionally preserves the original values.
        
        Args:
            df: DataFrame to process.
            col: Name of the column to convert.
            source_unit: Source unit ('万元', '亿元', etc.).
            preserve_original: If True, preserve original values in a new column
                with suffix indicating the original unit (default: True).
            
        Returns:
            DataFrame with converted monetary values.
            
        Side effects:
            - Logs conversion info: {col}: {source_unit} → 元, factor={factor}
            
        Raises:
            StandardizationError: When column is missing or unit is unknown.
        """
        if df is None or df.empty:
            return df
        
        if col not in df.columns:
            raise StandardizationError(f"Column '{col}' not found in DataFrame", col)
        
        # Normalize unit name
        normalized_unit = self.MONETARY_UNITS.get(source_unit, source_unit)
        
        # Get conversion factor
        factor = self.UNIT_CONVERSIONS.get(normalized_unit, 1.0)
        
        if factor == 1.0 and normalized_unit != '元':
            logger.warning(
                f"Unknown monetary unit '{source_unit}', no conversion applied"
            )
        
        result = df.copy()
        
        # Preserve original if requested
        if preserve_original and factor != 1.0:
            original_col = f"{col}_{normalized_unit}"
            result[original_col] = result[col]
        
        # Apply conversion
        result[col] = result[col] * factor
        
        # Log conversion
        logger.info(f"{col}: {source_unit} → 元, factor={factor}")
        
        return result
    
    def convert_volume(
        self, 
        df: pd.DataFrame, 
        col: str, 
        source_unit: str,
        preserve_original: bool = True,
    ) -> pd.DataFrame:
        """
        Convert volume column to shares (股).
        
        Applies the appropriate conversion factor based on source unit
        and optionally preserves the original values.
        
        Args:
            df: DataFrame to process.
            col: Name of the column to convert.
            source_unit: Source unit ('手', 'lot', etc.).
            preserve_original: If True, preserve original values in a new column
                with suffix indicating the original unit (default: True).
            
        Returns:
            DataFrame with converted volume values.
            
        Side effects:
            - Logs conversion info: {col}: {source_unit} → 股, factor={factor}
            
        Raises:
            StandardizationError: When column is missing or unit is unknown.
        """
        if df is None or df.empty:
            return df
        
        if col not in df.columns:
            raise StandardizationError(f"Column '{col}' not found in DataFrame", col)
        
        # Normalize unit name
        normalized_unit = self.VOLUME_UNITS.get(source_unit, source_unit)
        
        # Get conversion factor
        factor = self.UNIT_CONVERSIONS.get(normalized_unit, 1.0)
        
        if factor == 1.0 and normalized_unit != '股':
            logger.warning(
                f"Unknown volume unit '{source_unit}', no conversion applied"
            )
        
        result = df.copy()
        
        # Preserve original if requested
        if preserve_original and factor != 1.0:
            original_col = f"{col}_{normalized_unit}"
            result[original_col] = result[col]
        
        # Apply conversion
        result[col] = result[col] * factor
        
        # Log conversion
        logger.info(f"{col}: {source_unit} → 股, factor={factor}")
        
        return result

    
    def preserve_unadjusted(
        self, 
        df: pd.DataFrame, 
        price_cols: List[str]
    ) -> pd.DataFrame:
        """
        Preserve unadjusted price columns with _unadj suffix.
        
        Creates copies of price columns with '_unadj' suffix to preserve
        the original unadjusted values before any adjustment is applied.
        
        Args:
            df: DataFrame to process.
            price_cols: List of price column names to preserve.
            
        Returns:
            DataFrame with unadjusted price columns added.
            
        Side effects:
            - Logs preservation info for each column.
            
        Raises:
            StandardizationError: When a specified column is missing.
        """
        if df is None:
            return df
        
        # Handle empty price_cols list
        if not price_cols:
            return df.copy() if not df.empty else df
        
        result = df.copy()
        preserved_cols = []
        
        for col in price_cols:
            if col not in result.columns:
                raise StandardizationError(
                    f"Price column '{col}' not found in DataFrame", col
                )
            
            unadj_col = f"{col}_unadj"
            result[unadj_col] = result[col]
            preserved_cols.append(unadj_col)
        
        if preserved_cols:
            logger.info(
                f"Preserved unadjusted prices: {price_cols} → {preserved_cols}"
            )
        
        return result
    
    def standardize_all(
        self,
        df: pd.DataFrame,
        monetary_cols: Optional[Dict[str, str]] = None,
        volume_cols: Optional[Dict[str, str]] = None,
        price_cols: Optional[List[str]] = None,
        preserve_original: bool = True,
    ) -> pd.DataFrame:
        """
        Convenience method to perform all standardization operations.
        
        Args:
            df: DataFrame to process.
            monetary_cols: Dict mapping column names to their source units
                (e.g., {'amount': '万元', 'market_cap': '亿元'}).
            volume_cols: Dict mapping column names to their source units
                (e.g., {'vol': '手'}).
            price_cols: List of price columns to preserve as unadjusted.
            preserve_original: If True, preserve original values.
            
        Returns:
            Fully standardized DataFrame.
        """
        result = df
        
        # Convert monetary columns
        if monetary_cols:
            for col, unit in monetary_cols.items():
                result = self.convert_monetary(
                    result, col, unit, preserve_original=preserve_original
                )
        
        # Convert volume columns
        if volume_cols:
            for col, unit in volume_cols.items():
                result = self.convert_volume(
                    result, col, unit, preserve_original=preserve_original
                )
        
        # Preserve unadjusted prices
        if price_cols:
            result = self.preserve_unadjusted(result, price_cols)
        
        return result
    
    @staticmethod
    def get_conversion_factor(source_unit: str, target_unit: str = '元') -> float:
        """
        Get the conversion factor between two units.
        
        Args:
            source_unit: The source unit.
            target_unit: The target unit (default: '元').
            
        Returns:
            The conversion factor to multiply source values by.
        """
        source_factor = DataStandardizer.UNIT_CONVERSIONS.get(source_unit, 1.0)
        target_factor = DataStandardizer.UNIT_CONVERSIONS.get(target_unit, 1.0)
        
        return source_factor / target_factor
