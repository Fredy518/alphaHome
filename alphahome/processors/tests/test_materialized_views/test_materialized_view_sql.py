#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Property-based tests for MaterializedViewSQL.

Uses hypothesis library for property-based testing.

**Feature: materialized-views-system, Property 4: SQL template generation correctness**
**Validates: Requirements 5.1, 5.2, 5.3**
"""

import pytest
from hypothesis import given, strategies as st, settings, assume, HealthCheck
import re


# =============================================================================
# Custom Strategies for MaterializedViewSQL
# =============================================================================

def view_name_strategy():
    """Generate valid materialized view names."""
    return st.text(
        alphabet='abcdefghijklmnopqrstuvwxyz_',
        min_size=1,
        max_size=50
    ).filter(lambda x: not x.startswith('_') and x.isidentifier())


def table_name_strategy():
    """Generate valid table names."""
    return st.text(
        alphabet='abcdefghijklmnopqrstuvwxyz_',
        min_size=1,
        max_size=30
    ).filter(lambda x: not x.startswith('_') and x.isidentifier())


def column_name_strategy():
    """Generate valid column names."""
    return st.text(
        alphabet='abcdefghijklmnopqrstuvwxyz_',
        min_size=1,
        max_size=30
    ).filter(lambda x: not x.startswith('_') and x.isidentifier())


def source_table_strategy():
    """Generate valid source table references (rawdata.*)."""
    return st.builds(
        lambda t: f"rawdata.{t}",
        table_name_strategy()
    )


def key_columns_strategy():
    """Generate valid key column lists."""
    return st.lists(
        column_name_strategy(),
        min_size=1,
        max_size=3,
        unique=True
    )


def value_columns_strategy():
    """Generate valid value column lists."""
    return st.lists(
        column_name_strategy(),
        min_size=1,
        max_size=5,
        unique=True
    )


def time_columns_strategy():
    """Generate valid time column mappings."""
    return st.dictionaries(
        keys=column_name_strategy(),
        values=column_name_strategy(),
        min_size=1,
        max_size=3
    )


def group_by_columns_strategy():
    """Generate valid group by column lists."""
    return st.lists(
        column_name_strategy(),
        min_size=1,
        max_size=3,
        unique=True
    )


def aggregate_functions_strategy():
    """Generate valid aggregate function mappings."""
    func_names = st.sampled_from(['sum', 'avg', 'count', 'min', 'max', 'stddev'])
    return st.dictionaries(
        keys=func_names,
        values=st.lists(column_name_strategy(), min_size=1, max_size=3, unique=True),
        min_size=1,
        max_size=3
    )


# =============================================================================
# Property 4: SQL template generation correctness
# **Feature: materialized-views-system, Property 4: SQL template generation correctness**
# **Validates: Requirements 5.1, 5.2, 5.3**
# =============================================================================

class TestProperty4SQLTemplateGenerationCorrectness:
    """
    Property 4: SQL template generation correctness
    
    *For any* valid MaterializedViewSQL template configuration, the generated SQL SHALL:
    1. Be syntactically valid SQL
    2. Include all required components (SELECT, FROM, WHERE, ORDER BY)
    3. Include data lineage metadata (_source_table, _processed_at, _data_version)
    4. Include data quality checks (NOT NULL conditions)
    5. Support all three template types (PIT, aggregation, join)
    
    **Feature: materialized-views-system, Property 4: SQL template generation correctness**
    **Validates: Requirements 5.1, 5.2, 5.3**
    """
    
    @given(
        view_name=view_name_strategy(),
        source_table=source_table_strategy(),
        key_columns=key_columns_strategy(),
        time_columns=time_columns_strategy(),
        value_columns=value_columns_strategy()
    )
    @settings(
        max_examples=100,
        suppress_health_check=[HealthCheck.too_slow, HealthCheck.filter_too_much]
    )
    def test_pit_template_generates_valid_sql(
        self,
        view_name,
        source_table,
        key_columns,
        time_columns,
        value_columns
    ):
        """
        **Feature: materialized-views-system, Property 4: SQL template generation correctness**
        **Validates: Requirements 5.1, 5.2**
        
        For any valid PIT template parameters, the generated SQL SHALL:
        1. Contain CREATE MATERIALIZED VIEW statement
        2. Include the view name
        3. Include the source table
        4. Include all key columns
        5. Include time series expansion (query_start_date, query_end_date)
        6. Include all value columns
        7. Include data lineage metadata
        8. Include NOT NULL checks for all columns
        """
        from alphahome.processors.materialized_views import MaterializedViewSQL
        
        # Generate SQL
        sql = MaterializedViewSQL.pit_template(
            view_name=view_name,
            source_table=source_table,
            key_columns=key_columns,
            time_columns=time_columns,
            value_columns=value_columns
        )
        
        # Verify SQL is a string
        assert isinstance(sql, str), "Generated SQL should be a string"
        assert len(sql) > 0, "Generated SQL should not be empty"
        
        # Verify SQL contains required keywords
        sql_upper = sql.upper()
        assert "CREATE MATERIALIZED VIEW" in sql_upper, "SQL should contain CREATE MATERIALIZED VIEW"
        assert "SELECT" in sql_upper, "SQL should contain SELECT"
        assert "FROM" in sql_upper, "SQL should contain FROM"
        assert "WHERE" in sql_upper, "SQL should contain WHERE"
        assert "ORDER BY" in sql_upper, "SQL should contain ORDER BY"
        
        # Verify view name is in SQL
        assert view_name in sql, f"SQL should contain view name '{view_name}'"
        
        # Verify source table is in SQL
        assert source_table in sql, f"SQL should contain source table '{source_table}'"
        
        # Verify key columns are in SQL
        for col in key_columns:
            assert col in sql, f"SQL should contain key column '{col}'"
        
        # Verify time series expansion columns
        assert "query_start_date" in sql, "SQL should contain query_start_date"
        assert "query_end_date" in sql, "SQL should contain query_end_date"
        
        # Verify value columns are in SQL
        for col in value_columns:
            assert col in sql, f"SQL should contain value column '{col}'"
        
        # Verify data lineage metadata
        assert "_source_table" in sql, "SQL should contain _source_table metadata"
        assert "_processed_at" in sql, "SQL should contain _processed_at metadata"
        assert "_data_version" in sql, "SQL should contain _data_version metadata"
        
        # Verify NOT NULL checks
        assert "IS NOT NULL" in sql, "SQL should contain NOT NULL checks"

    @given(
        view_name=view_name_strategy(),
        source_table=source_table_strategy(),
        group_by_columns=group_by_columns_strategy(),
        aggregate_functions=aggregate_functions_strategy()
    )
    @settings(
        max_examples=100,
        suppress_health_check=[HealthCheck.too_slow, HealthCheck.filter_too_much]
    )
    def test_aggregation_template_generates_valid_sql(
        self,
        view_name,
        source_table,
        group_by_columns,
        aggregate_functions
    ):
        """
        **Feature: materialized-views-system, Property 4: SQL template generation correctness**
        **Validates: Requirements 5.1, 5.3**
        
        For any valid aggregation template parameters, the generated SQL SHALL:
        1. Contain CREATE MATERIALIZED VIEW statement
        2. Include the view name
        3. Include the source table
        4. Include all group by columns
        5. Include all aggregate functions
        6. Include GROUP BY clause
        7. Include data lineage metadata
        8. Include NOT NULL checks
        """
        from alphahome.processors.materialized_views import MaterializedViewSQL
        
        # Generate SQL
        sql = MaterializedViewSQL.aggregation_template(
            view_name=view_name,
            source_table=source_table,
            group_by_columns=group_by_columns,
            aggregate_functions=aggregate_functions
        )
        
        # Verify SQL is a string
        assert isinstance(sql, str), "Generated SQL should be a string"
        assert len(sql) > 0, "Generated SQL should not be empty"
        
        # Verify SQL contains required keywords
        sql_upper = sql.upper()
        assert "CREATE MATERIALIZED VIEW" in sql_upper, "SQL should contain CREATE MATERIALIZED VIEW"
        assert "SELECT" in sql_upper, "SQL should contain SELECT"
        assert "FROM" in sql_upper, "SQL should contain FROM"
        assert "WHERE" in sql_upper, "SQL should contain WHERE"
        assert "GROUP BY" in sql_upper, "SQL should contain GROUP BY"
        assert "ORDER BY" in sql_upper, "SQL should contain ORDER BY"
        
        # Verify view name is in SQL
        assert view_name in sql, f"SQL should contain view name '{view_name}'"
        
        # Verify source table is in SQL
        assert source_table in sql, f"SQL should contain source table '{source_table}'"
        
        # Verify group by columns are in SQL
        for col in group_by_columns:
            assert col in sql, f"SQL should contain group by column '{col}'"
        
        # Verify aggregate functions are in SQL
        for func_name in aggregate_functions.keys():
            func_upper = func_name.upper()
            assert func_upper in sql_upper, f"SQL should contain aggregate function '{func_name}'"
        
        # Verify data lineage metadata
        assert "_source_table" in sql, "SQL should contain _source_table metadata"
        assert "_processed_at" in sql, "SQL should contain _processed_at metadata"
        assert "_data_version" in sql, "SQL should contain _data_version metadata"
        
        # Verify NOT NULL checks
        assert "IS NOT NULL" in sql, "SQL should contain NOT NULL checks"

    @given(
        view_name=view_name_strategy(),
        select_columns=st.lists(
            column_name_strategy(),
            min_size=2,
            max_size=5,
            unique=True
        )
    )
    @settings(
        max_examples=100,
        suppress_health_check=[HealthCheck.too_slow, HealthCheck.filter_too_much]
    )
    def test_join_template_generates_valid_sql(
        self,
        view_name,
        select_columns
    ):
        """
        **Feature: materialized-views-system, Property 4: SQL template generation correctness**
        **Validates: Requirements 5.1, 5.3**
        
        For any valid join template parameters, the generated SQL SHALL:
        1. Contain CREATE MATERIALIZED VIEW statement
        2. Include the view name
        3. Include all source tables
        4. Include JOIN conditions
        5. Include all select columns
        6. Include data lineage metadata
        7. Include NOT NULL checks
        """
        from alphahome.processors.materialized_views import MaterializedViewSQL
        
        # Generate source tables and join conditions
        source_tables = [
            "rawdata.table1",
            "rawdata.table2"
        ]
        join_conditions = ["t1.id = t2.id"]
        
        # Generate SQL
        sql = MaterializedViewSQL.join_template(
            view_name=view_name,
            source_tables=source_tables,
            join_conditions=join_conditions,
            select_columns=select_columns
        )
        
        # Verify SQL is a string
        assert isinstance(sql, str), "Generated SQL should be a string"
        assert len(sql) > 0, "Generated SQL should not be empty"
        
        # Verify SQL contains required keywords
        sql_upper = sql.upper()
        assert "CREATE MATERIALIZED VIEW" in sql_upper, "SQL should contain CREATE MATERIALIZED VIEW"
        assert "SELECT" in sql_upper, "SQL should contain SELECT"
        assert "FROM" in sql_upper, "SQL should contain FROM"
        assert "JOIN" in sql_upper, "SQL should contain JOIN"
        assert "WHERE" in sql_upper, "SQL should contain WHERE"
        assert "ORDER BY" in sql_upper, "SQL should contain ORDER BY"
        
        # Verify view name is in SQL
        assert view_name in sql, f"SQL should contain view name '{view_name}'"
        
        # Verify source tables are in SQL
        for table in source_tables:
            assert table in sql, f"SQL should contain source table '{table}'"
        
        # Verify join conditions are in SQL
        for condition in join_conditions:
            assert condition in sql, f"SQL should contain join condition '{condition}'"
        
        # Verify select columns are in SQL
        for col in select_columns:
            assert col in sql, f"SQL should contain select column '{col}'"
        
        # Verify data lineage metadata
        assert "_source_tables" in sql, "SQL should contain _source_tables metadata"
        assert "_processed_at" in sql, "SQL should contain _processed_at metadata"
        assert "_data_version" in sql, "SQL should contain _data_version metadata"
        
        # Verify NOT NULL checks
        assert "IS NOT NULL" in sql, "SQL should contain NOT NULL checks"

    def test_pit_template_with_quality_checks(self):
        """
        **Feature: materialized-views-system, Property 4: SQL template generation correctness**
        **Validates: Requirements 5.2**
        
        For PIT template with quality checks, the generated SQL SHALL:
        1. Include outlier check conditions (BETWEEN clause)
        2. Include all quality check constraints
        """
        from alphahome.processors.materialized_views import MaterializedViewSQL
        
        quality_checks = {
            'outlier_check': {
                'columns': ['pe_ttm', 'pb'],
                'threshold': 3.0
            }
        }
        
        sql = MaterializedViewSQL.pit_template(
            view_name="test_mv",
            source_table="rawdata.test_table",
            key_columns=["ts_code"],
            time_columns={"ann_date": "announcement_date"},
            value_columns=["pe_ttm", "pb"],
            quality_checks=quality_checks
        )
        
        # Verify quality checks are included
        assert "BETWEEN" in sql, "SQL should include BETWEEN clause for outlier checks"

    def test_pit_template_raises_error_on_empty_key_columns(self):
        """
        **Feature: materialized-views-system, Property 4: SQL template generation correctness**
        **Validates: Requirements 5.1**
        
        For PIT template with empty key_columns, the method SHALL raise ValueError.
        """
        from alphahome.processors.materialized_views import MaterializedViewSQL
        
        with pytest.raises(ValueError, match="key_columns cannot be empty"):
            MaterializedViewSQL.pit_template(
                view_name="test_mv",
                source_table="rawdata.test_table",
                key_columns=[],
                time_columns={"ann_date": "announcement_date"},
                value_columns=["pe_ttm"]
            )

    def test_pit_template_raises_error_on_empty_time_columns(self):
        """
        **Feature: materialized-views-system, Property 4: SQL template generation correctness**
        **Validates: Requirements 5.1**
        
        For PIT template with empty time_columns, the method SHALL raise ValueError.
        """
        from alphahome.processors.materialized_views import MaterializedViewSQL
        
        with pytest.raises(ValueError, match="time_columns cannot be empty"):
            MaterializedViewSQL.pit_template(
                view_name="test_mv",
                source_table="rawdata.test_table",
                key_columns=["ts_code"],
                time_columns={},
                value_columns=["pe_ttm"]
            )

    def test_pit_template_raises_error_on_empty_value_columns(self):
        """
        **Feature: materialized-views-system, Property 4: SQL template generation correctness**
        **Validates: Requirements 5.1**
        
        For PIT template with empty value_columns, the method SHALL raise ValueError.
        """
        from alphahome.processors.materialized_views import MaterializedViewSQL
        
        with pytest.raises(ValueError, match="value_columns cannot be empty"):
            MaterializedViewSQL.pit_template(
                view_name="test_mv",
                source_table="rawdata.test_table",
                key_columns=["ts_code"],
                time_columns={"ann_date": "announcement_date"},
                value_columns=[]
            )

    def test_aggregation_template_raises_error_on_empty_group_by_columns(self):
        """
        **Feature: materialized-views-system, Property 4: SQL template generation correctness**
        **Validates: Requirements 5.1**
        
        For aggregation template with empty group_by_columns, the method SHALL raise ValueError.
        """
        from alphahome.processors.materialized_views import MaterializedViewSQL
        
        with pytest.raises(ValueError, match="group_by_columns cannot be empty"):
            MaterializedViewSQL.aggregation_template(
                view_name="test_mv",
                source_table="rawdata.test_table",
                group_by_columns=[],
                aggregate_functions={"sum": ["amount"]}
            )

    def test_aggregation_template_raises_error_on_empty_aggregate_functions(self):
        """
        **Feature: materialized-views-system, Property 4: SQL template generation correctness**
        **Validates: Requirements 5.1**
        
        For aggregation template with empty aggregate_functions, the method SHALL raise ValueError.
        """
        from alphahome.processors.materialized_views import MaterializedViewSQL
        
        with pytest.raises(ValueError, match="aggregate_functions cannot be empty"):
            MaterializedViewSQL.aggregation_template(
                view_name="test_mv",
                source_table="rawdata.test_table",
                group_by_columns=["trade_date"],
                aggregate_functions={}
            )

    def test_join_template_raises_error_on_insufficient_source_tables(self):
        """
        **Feature: materialized-views-system, Property 4: SQL template generation correctness**
        **Validates: Requirements 5.1**
        
        For join template with less than 2 source tables, the method SHALL raise ValueError.
        """
        from alphahome.processors.materialized_views import MaterializedViewSQL
        
        with pytest.raises(ValueError, match="source_tables must have at least 2 tables"):
            MaterializedViewSQL.join_template(
                view_name="test_mv",
                source_tables=["rawdata.table1"],
                join_conditions=["t1.id = t2.id"],
                select_columns=["t1.id"]
            )

    def test_join_template_raises_error_on_empty_join_conditions(self):
        """
        **Feature: materialized-views-system, Property 4: SQL template generation correctness**
        **Validates: Requirements 5.1**
        
        For join template with empty join_conditions, the method SHALL raise ValueError.
        """
        from alphahome.processors.materialized_views import MaterializedViewSQL
        
        with pytest.raises(ValueError, match="join_conditions cannot be empty"):
            MaterializedViewSQL.join_template(
                view_name="test_mv",
                source_tables=["rawdata.table1", "rawdata.table2"],
                join_conditions=[],
                select_columns=["t1.id"]
            )

    def test_join_template_raises_error_on_empty_select_columns(self):
        """
        **Feature: materialized-views-system, Property 4: SQL template generation correctness**
        **Validates: Requirements 5.1**
        
        For join template with empty select_columns, the method SHALL raise ValueError.
        """
        from alphahome.processors.materialized_views import MaterializedViewSQL
        
        with pytest.raises(ValueError, match="select_columns cannot be empty"):
            MaterializedViewSQL.join_template(
                view_name="test_mv",
                source_tables=["rawdata.table1", "rawdata.table2"],
                join_conditions=["t1.id = t2.id"],
                select_columns=[]
            )

    def test_pit_template_includes_cast_for_value_columns(self):
        """
        **Feature: materialized-views-system, Property 4: SQL template generation correctness**
        **Validates: Requirements 5.2**
        
        For PIT template, value columns SHALL be cast to DECIMAL for standardization.
        """
        from alphahome.processors.materialized_views import MaterializedViewSQL
        
        sql = MaterializedViewSQL.pit_template(
            view_name="test_mv",
            source_table="rawdata.test_table",
            key_columns=["ts_code"],
            time_columns={"ann_date": "announcement_date"},
            value_columns=["pe_ttm", "pb"]
        )
        
        # Verify CAST to DECIMAL
        assert "CAST" in sql, "SQL should include CAST for value columns"
        assert "DECIMAL" in sql, "SQL should cast to DECIMAL"

    def test_aggregation_template_includes_group_by_clause(self):
        """
        **Feature: materialized-views-system, Property 4: SQL template generation correctness**
        **Validates: Requirements 5.3**
        
        For aggregation template, GROUP BY clause SHALL include all group_by_columns.
        """
        from alphahome.processors.materialized_views import MaterializedViewSQL
        
        group_by_columns = ["trade_date", "industry"]
        sql = MaterializedViewSQL.aggregation_template(
            view_name="test_mv",
            source_table="rawdata.test_table",
            group_by_columns=group_by_columns,
            aggregate_functions={"sum": ["amount"]}
        )
        
        # Verify GROUP BY clause
        assert "GROUP BY" in sql, "SQL should include GROUP BY clause"
        for col in group_by_columns:
            assert col in sql, f"GROUP BY should include column '{col}'"

    def test_pit_template_includes_lead_window_function(self):
        """
        **Feature: materialized-views-system, Property 4: SQL template generation correctness**
        **Validates: Requirements 5.2**
        
        For PIT template, query_end_date SHALL use LEAD window function for time series expansion.
        """
        from alphahome.processors.materialized_views import MaterializedViewSQL
        
        sql = MaterializedViewSQL.pit_template(
            view_name="test_mv",
            source_table="rawdata.test_table",
            key_columns=["ts_code"],
            time_columns={"ann_date": "announcement_date"},
            value_columns=["pe_ttm"]
        )
        
        # Verify LEAD window function
        assert "LEAD" in sql, "SQL should use LEAD window function for time series expansion"
        assert "OVER" in sql, "SQL should include OVER clause for window function"
        assert "PARTITION BY" in sql, "SQL should partition by key columns"
        assert "ORDER BY" in sql, "SQL should order by time column"
