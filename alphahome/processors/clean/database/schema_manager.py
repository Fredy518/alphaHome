#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Clean Layer Schema Manager

Provides utilities for creating and managing the clean schema and its tables.
This module handles DDL execution for the clean layer database objects.

执行环境约束：
- 仅在 dev/staging 环境执行
- 生产环境需 DBA 审批

Usage:
    from alphahome.processors.clean.database.schema_manager import CleanSchemaManager
    
    # Async usage
    manager = CleanSchemaManager(db_manager)
    await manager.ensure_schema_exists()
    await manager.create_all_tables()
    
    # Or create specific tables
    await manager.create_table('index_valuation_base')
"""

import logging
import os
from pathlib import Path
from typing import List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from alphahome.common.db_manager import DBManager

logger = logging.getLogger(__name__)

# DDL files directory
DDL_DIR = Path(__file__).parent

# Table definitions with their DDL files
CLEAN_TABLES = {
    'index_valuation_base': 'create_index_valuation_base.sql',
    'index_volatility_base': 'create_index_volatility_base.sql',
    'industry_base': 'create_industry_base.sql',
    'money_flow_base': 'create_money_flow_base.sql',
    'market_technical_base': 'create_market_technical_base.sql',
}

SCHEMA_DDL_FILE = 'create_clean_schema.sql'


class CleanSchemaManager:
    """
    Manager for Clean Layer database schema operations.
    
    Handles creation and management of the clean schema and its tables.
    All operations are idempotent (safe to run multiple times).
    
    Attributes:
        db_manager: Database manager instance for executing SQL
        schema_name: Name of the clean schema (default: 'clean')
    """
    
    def __init__(self, db_manager: "DBManager", schema_name: str = "clean"):
        """
        Initialize the schema manager.
        
        Args:
            db_manager: Database manager instance
            schema_name: Name of the schema to manage (default: 'clean')
        """
        self.db_manager = db_manager
        self.schema_name = schema_name
        self._ddl_dir = DDL_DIR
    
    def _read_ddl_file(self, filename: str) -> str:
        """
        Read DDL content from a SQL file.
        
        Args:
            filename: Name of the SQL file
            
        Returns:
            SQL content as string
            
        Raises:
            FileNotFoundError: If the DDL file doesn't exist
        """
        filepath = self._ddl_dir / filename
        if not filepath.exists():
            raise FileNotFoundError(f"DDL file not found: {filepath}")
        
        with open(filepath, 'r', encoding='utf-8') as f:
            return f.read()
    
    async def ensure_schema_exists(self) -> bool:
        """
        Ensure the clean schema exists in the database.
        
        Returns:
            True if schema was created or already exists
            
        Raises:
            Exception: If schema creation fails
        """
        try:
            ddl = self._read_ddl_file(SCHEMA_DDL_FILE)
            await self.db_manager.execute(ddl)
            logger.info(f"Schema '{self.schema_name}' created or already exists.")
            return True
        except Exception as e:
            logger.error(f"Failed to create schema '{self.schema_name}': {e}")
            raise
    
    async def create_table(self, table_name: str) -> bool:
        """
        Create a specific table in the clean schema.
        
        Args:
            table_name: Name of the table to create (without schema prefix)
            
        Returns:
            True if table was created or already exists
            
        Raises:
            ValueError: If table_name is not recognized
            Exception: If table creation fails
        """
        if table_name not in CLEAN_TABLES:
            raise ValueError(
                f"Unknown table: {table_name}. "
                f"Valid tables: {list(CLEAN_TABLES.keys())}"
            )
        
        try:
            ddl_file = CLEAN_TABLES[table_name]
            ddl = self._read_ddl_file(ddl_file)
            await self.db_manager.execute(ddl)
            logger.info(f"Table '{self.schema_name}.{table_name}' created or already exists.")
            return True
        except Exception as e:
            logger.error(f"Failed to create table '{self.schema_name}.{table_name}': {e}")
            raise
    
    async def create_all_tables(self) -> List[str]:
        """
        Create all clean layer tables.
        
        Returns:
            List of table names that were created
            
        Raises:
            Exception: If any table creation fails
        """
        created_tables = []
        
        # First ensure schema exists
        await self.ensure_schema_exists()
        
        # Create each table
        for table_name in CLEAN_TABLES:
            await self.create_table(table_name)
            created_tables.append(table_name)
        
        logger.info(f"Created {len(created_tables)} tables in '{self.schema_name}' schema.")
        return created_tables
    
    async def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the clean schema.
        
        Args:
            table_name: Name of the table to check
            
        Returns:
            True if table exists, False otherwise
        """
        full_name = f'"{self.schema_name}"."{table_name}"'
        return await self.db_manager.table_exists(full_name)
    
    async def get_table_info(self, table_name: str) -> dict:
        """
        Get information about a table in the clean schema.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary with table information including columns and types
        """
        full_name = f'"{self.schema_name}"."{table_name}"'
        schema_info = await self.db_manager.get_table_schema(full_name)
        
        return {
            'schema': self.schema_name,
            'table': table_name,
            'columns': schema_info,
            'exists': len(schema_info) > 0
        }
    
    async def verify_all_tables(self) -> dict:
        """
        Verify that all expected tables exist and have correct structure.
        
        Returns:
            Dictionary with verification results for each table
        """
        results = {}
        
        for table_name in CLEAN_TABLES:
            exists = await self.table_exists(table_name)
            results[table_name] = {
                'exists': exists,
                'ddl_file': CLEAN_TABLES[table_name]
            }
            
            if exists:
                info = await self.get_table_info(table_name)
                results[table_name]['columns'] = len(info['columns'])
        
        return results


# Convenience function for quick setup
async def setup_clean_schema(db_manager: "DBManager") -> List[str]:
    """
    Convenience function to set up the entire clean schema.
    
    Args:
        db_manager: Database manager instance
        
    Returns:
        List of created table names
        
    Example:
        >>> from alphahome.common.db_manager import create_async_manager
        >>> db = create_async_manager(connection_string)
        >>> await db.connect()
        >>> tables = await setup_clean_schema(db)
        >>> print(f"Created tables: {tables}")
    """
    manager = CleanSchemaManager(db_manager)
    return await manager.create_all_tables()
