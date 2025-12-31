#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for barra.ddl module - DDL generation."""

import pytest

from alphahome.barra.ddl import BarraDDL


class TestBarraDDL:
    """Test BarraDDL class."""

    def test_default_schema(self):
        """Default schema should be 'barra'."""
        ddl = BarraDDL()
        assert ddl.schema == "barra"

    def test_custom_schema(self):
        """Should accept custom schema."""
        ddl = BarraDDL(schema="my_barra")
        assert ddl.schema == "my_barra"

    def test_create_schema_sql(self):
        """Should generate valid CREATE SCHEMA SQL."""
        ddl = BarraDDL()
        sql = ddl.create_schema_sql()
        
        assert "CREATE SCHEMA IF NOT EXISTS barra" in sql

    def test_create_pit_sw_view_sql(self):
        """Should generate PIT SW industry view SQL."""
        ddl = BarraDDL()
        sql = ddl.create_pit_sw_view_sql()
        
        assert "CREATE OR REPLACE VIEW" in sql
        assert "pit_sw_industry_member_mv" in sql
        assert "query_start_date" in sql
        assert "query_end_date" in sql
        assert "COALESCE(out_date" in sql
        assert "2099-12-31" in sql

    def test_create_industry_dim_table_sql(self):
        """Should generate industry dim table SQL."""
        ddl = BarraDDL()
        sql = ddl.create_industry_dim_table_sql()
        
        assert "CREATE TABLE IF NOT EXISTS" in sql
        assert "industry_l1_dim" in sql
        assert "l1_code TEXT PRIMARY KEY" in sql
        assert "column_name TEXT NOT NULL UNIQUE" in sql

    def test_create_partitioned_table_sql(self):
        """Should generate partitioned table SQL."""
        ddl = BarraDDL()
        sql = ddl.create_partitioned_table_sql(
            table_name="test_table",
            columns_sql="id INT, value DOUBLE PRECISION",
            pk_sql="PRIMARY KEY (id)",
        )
        
        assert "CREATE TABLE IF NOT EXISTS barra.test_table" in sql
        assert "PARTITION BY RANGE (trade_date)" in sql
        assert "id INT" in sql
        assert "PRIMARY KEY (id)" in sql

    def test_create_default_partition_sql(self):
        """Should generate default partition SQL."""
        ddl = BarraDDL()
        sql = ddl.create_default_partition_sql("test_table")
        
        assert "CREATE TABLE IF NOT EXISTS barra.test_table_default" in sql
        assert "PARTITION OF barra.test_table" in sql
        assert "DEFAULT" in sql
