#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Clean Layer Components

This module provides components for the Clean Layer of the data processing pipeline.
The Clean Layer is responsible for:
- Data validation (schema, types, nulls, ranges)
- Data alignment (dates, identifiers, primary keys)
- Data standardization (units, adjustments)
- Lineage tracking (source, timestamps, versions)
- Idempotent data writing (UPSERT)
"""

from .schema import TableSchema, ValidationResult
from .validator import DataValidator, ValidationError
from .aligner import DataAligner, AlignmentError
from .standardizer import DataStandardizer, StandardizationError
from .lineage import LineageTracker, LineageError
from .writer import CleanLayerWriter, WriteError
from .database.schema_manager import CleanSchemaManager, setup_clean_schema

__all__ = [
    # Schema definitions
    "TableSchema",
    "ValidationResult",
    # Validator
    "DataValidator",
    "ValidationError",
    # Aligner
    "DataAligner",
    "AlignmentError",
    # Standardizer
    "DataStandardizer",
    "StandardizationError",
    # Lineage
    "LineageTracker",
    "LineageError",
    # Writer
    "CleanLayerWriter",
    "WriteError",
    # Schema Manager
    "CleanSchemaManager",
    "setup_clean_schema",
]
