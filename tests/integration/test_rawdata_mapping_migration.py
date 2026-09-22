"""Real-PostgreSQL acceptance tests for explicit rawdata mapping migration."""

from uuid import uuid4

import psycopg2
import pytest

from alphahome.common.maintenance_sql import rawdata_mapping_sql


pytestmark = [pytest.mark.integration, pytest.mark.requires_db]


def test_legacy_table_is_archived_before_mapping_view_is_created(
    isolated_database_url,
):
    suffix = uuid4().hex[:12]
    source_schema = f'mapping_source_{suffix}'
    source_table = f'macro_source_{suffix}'
    target = f'macro_target_{suffix}'
    archive = f'{target}_legacy'
    connection = psycopg2.connect(isolated_database_url)
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA "{source_schema}"')
            cursor.execute(
                f'CREATE TABLE "{source_schema}"."{source_table}" '
                '(id integer PRIMARY KEY, payload text NOT NULL)'
            )
            cursor.execute(
                f'INSERT INTO "{source_schema}"."{source_table}" VALUES (1,\'source\')'
            )
            cursor.execute('CREATE SCHEMA IF NOT EXISTS rawdata')
            cursor.execute(
                f'CREATE TABLE rawdata."{target}" '
                '(id integer PRIMARY KEY, payload text NOT NULL)'
            )
            cursor.execute(f'INSERT INTO rawdata."{target}" VALUES (9,\'legacy\')')

            sql = rawdata_mapping_sql(
                target,
                source_schema,
                source_table,
                archive_existing_table=archive,
            )
            cursor.execute(sql)
            cursor.execute(sql)
            cursor.execute(
                "SELECT c.relkind FROM pg_class c "
                "JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname='rawdata' AND c.relname=%s",
                (target,),
            )
            assert cursor.fetchone() == ('v',)
            cursor.execute(f'SELECT * FROM rawdata."{target}"')
            assert cursor.fetchall() == [(1, 'source')]
            cursor.execute(f'SELECT * FROM rawdata."{archive}"')
            assert cursor.fetchall() == [(9, 'legacy')]
    finally:
        with connection.cursor() as cursor:
            cursor.execute(f'DROP VIEW IF EXISTS rawdata."{target}"')
            cursor.execute(f'DROP TABLE IF EXISTS rawdata."{archive}"')
            cursor.execute(f'DROP SCHEMA IF EXISTS "{source_schema}" CASCADE')
        connection.close()
