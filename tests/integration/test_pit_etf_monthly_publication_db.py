"""Real-PostgreSQL acceptance tests for scoped ETF monthly publication."""

from datetime import date

import pandas as pd
import psycopg2
import pytest

from alphahome.common.db_session import owned_sync_session
from alphahome.pit.pit_etf_index_a_share_proxy_fapi_manager import (
    PITETFIndexAShareProxyFAPIMonthlyManager,
)
from alphahome.pit.pit_etf_index_a_share_proxy_members_manager import (
    PITETFIndexAShareProxyMembersMonthlyManager,
)
from alphahome.pit.pit_etf_index_fapi_manager import PITETFIndexFAPIMonthlyManager
from alphahome.pit.pit_etf_index_members_manager import (
    PITETFIndexMembersMonthlyManager,
)
from alphahome.pit.schema import render_schema_sql


pytestmark = [pytest.mark.integration, pytest.mark.requires_db]

MANAGER_TYPES = (
    PITETFIndexMembersMonthlyManager,
    PITETFIndexFAPIMonthlyManager,
    PITETFIndexAShareProxyMembersMonthlyManager,
    PITETFIndexAShareProxyFAPIMonthlyManager,
)


@pytest.fixture
def pit_database(isolated_database_url):
    connection = psycopg2.connect(isolated_database_url)
    connection.autocommit = True
    with connection.cursor() as cursor:
        cursor.execute("SELECT to_regnamespace('pit')")
        assert cursor.fetchone()[0] is None
        cursor.execute(render_schema_sql())
    try:
        yield isolated_database_url, connection
    finally:
        with connection.cursor() as cursor:
            cursor.execute('DROP SCHEMA pit CASCADE')
        connection.close()


def _seed_members(cursor, manager, month, index_code):
    method_version = manager.calculator.METHOD_VERSION
    statement = """INSERT INTO pit.pit_etf_index_members_monthly
        (obs_date,index_code,index_name,ts_code,weight,raw_weight,
         weight_basis,weight_source,source_code,source_effective_date,
         source_available_date,source_staleness_days,source_member_count,
         source_weight_sum,source_coverage_rate,source_quality,is_eligible,
         method_version)
        VALUES (%s,%s,%s,'000001.SZ',1,100,'official_index_weight',
                'integration','IDX',%s,%s,0,1,100,1,'high',true,%s)"""
    cursor.execute(
        statement,
        (month, index_code, 'Seed Index', month, month, method_version),
    )
    cursor.execute(
        statement,
        (month, index_code, 'Guard Index', month, month, 'integration_guard_v1'),
    )
    return manager.calculator.OUTPUT_COLUMNS


def _seed_fapi(cursor, manager, month, index_code):
    method_version = manager.calculator.METHOD_VERSION
    statement = """INSERT INTO pit.pit_etf_index_fapi_monthly
        (obs_date,universe_type,index_code,index_name,member_weight_basis,
         member_weight_source,member_source_code,member_source_effective_date,
         member_source_available_date,member_source_staleness_days,
         member_source_coverage_rate,member_source_quality,benchmark_code,
         benchmark_name,equity_basis,method_version,org_weight_version,
         quality_rule_version)
        VALUES (%s,'etf_tracked_index',%s,%s,'official_index_weight',
                'integration','IDX',%s,%s,0,1,'high','000906.SH','CSI 800',
                'total_mv',%s,'integration','integration')"""
    cursor.execute(
        statement,
        (month, index_code, 'Seed Index', month, month, method_version),
    )
    cursor.execute(
        statement,
        (month, index_code, 'Guard Index', month, month, 'integration_guard_v1'),
    )
    return manager.calculator.INDEX_OUTPUT_COLUMNS


def _seed_etf_month(cursor, manager, month, index_code):
    columns = (
        _seed_members(cursor, manager, month, index_code)
        if manager.table_name == 'pit_etf_index_members_monthly'
        else _seed_fapi(cursor, manager, month, index_code)
    )
    projection = ','.join(f'"{column}"' for column in columns)
    cursor.execute(
        f"SELECT {projection} FROM pit.\"{manager.table_name}\" "
        "WHERE obs_date=%s AND index_code=%s AND method_version=%s",
        (month, index_code, manager.calculator.METHOD_VERSION),
    )
    return pd.DataFrame(cursor.fetchall(), columns=columns)


@pytest.mark.parametrize('manager_type', MANAGER_TYPES)
def test_scoped_publication_is_atomic_and_records_completed_month(
    pit_database, manager_type
):
    url, connection = pit_database
    month = date(2026, 8, 31)
    index_code = '931238.CSI'
    manager = manager_type()
    with connection.cursor() as cursor:
        replacement = _seed_etf_month(cursor, manager, month, index_code)
    replacement['index_name'] = 'Replacement Index'

    with owned_sync_session(url) as db, manager.bind_database(db_manager=db):
        assert manager._atomic_replace_scope(
            replacement, [month], [index_code]
        ) == 1
        assert manager._verified_replacement_months == [month]

    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT method_version,index_name FROM pit.\"{manager.table_name}\" "
            "WHERE obs_date=%s AND index_code=%s ORDER BY method_version",
            (month, index_code),
        )
        rows_after_success = cursor.fetchall()
    assert sorted(name for _, name in rows_after_success) == [
        'Guard Index',
        'Replacement Index',
    ]

    blocked_manager = manager_type()
    empty = pd.DataFrame(columns=replacement.columns)
    with owned_sync_session(url) as db, blocked_manager.bind_database(db_manager=db):
        with pytest.raises(ValueError, match='pit_incomplete_months'):
            blocked_manager._atomic_replace_scope(empty, [month], [index_code])

    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT method_version,index_name FROM pit.\"{manager.table_name}\" "
            "WHERE obs_date=%s AND index_code=%s ORDER BY method_version",
            (month, index_code),
        )
        assert cursor.fetchall() == rows_after_success
