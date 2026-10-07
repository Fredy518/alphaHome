"""Wide financial responses retain every typed field in the atomic archive."""
import json
import logging
from types import SimpleNamespace

import asyncpg
import pandas as pd
import pytest

from alphahome.common.db_components.database_operations_mixin import DatabaseOperationsMixin
from alphahome.common.source_observations import CREATE_SQL

pytestmark = [pytest.mark.integration, pytest.mark.requires_db]


@pytest.mark.asyncio
@pytest.mark.parametrize('field_count', [49, 51, 180])
async def test_wide_source_retains_all_columns_and_null_withdrawal(isolated_database_url, field_count):
    pool = await asyncpg.create_pool(isolated_database_url, min_size=1, max_size=1)
    columns = [f'amount_{n}' for n in range(field_count)]
    table = 'source_archive_wide_test'
    created_schema = created_source = created_archive = False

    class Harness(DatabaseOperationsMixin):
        def __init__(self):
            self.pool = pool
            self.resolver = SimpleNamespace(get_schema_and_table=lambda target: ('public', table))
            self.logger = logging.getLogger(__name__)

    try:
        async with pool.acquire() as conn:
            created_schema = await conn.fetchval("SELECT to_regnamespace('tushare')") is None
            await conn.execute('CREATE SCHEMA IF NOT EXISTS tushare')
            assert await conn.fetchval("SELECT to_regclass('tushare.financial_source_observation')") is None
            await conn.execute(CREATE_SQL)
            created_archive = True
            await conn.execute(f'CREATE TABLE public.{table} (id integer PRIMARY KEY,' + ','.join(f'{c} numeric' for c in columns) + ',update_time timestamp)')
            created_source = True
        harness = Harness()
        target = SimpleNamespace(archive_source_versions=True, schema_def={})
        frame = pd.DataFrame({'id': [1], **{c: [n] for n, c in enumerate(columns)}})
        options = dict(target=target, conflict_columns=['id'], update_columns=columns, timestamp_column='update_time')
        await harness.upsert(frame, **options)
        frame[columns[-1]] = None
        await harness.upsert(frame, **options)
        await harness.upsert(frame, **options)
        async with pool.acquire() as conn:
            rows = await conn.fetch('SELECT observation_role,payload::text AS payload FROM tushare.financial_source_observation ORDER BY observation_id')
            assert len(rows) == 3  # First insert; retained and incoming revision; unchanged replay adds none.
            payloads = [json.loads(r['payload']) for r in rows]
            assert all(set(p) == {'id', *columns} for p in payloads)
            assert all(p['amount_0'] == 0 for p in payloads)
            assert sorted(p[columns[-1]] for p in payloads if p[columns[-1]] is not None) == [field_count - 1] * 2
            assert sum(p[columns[-1]] is None for p in payloads) == 1
            assert await conn.fetchval(f'SELECT {columns[-1]} FROM public.{table} WHERE id=1') is None
    finally:
        async with pool.acquire() as conn:
            if created_source:
                await conn.execute(f'DROP TABLE public.{table}')
            if created_archive:
                await conn.execute('DROP TABLE tushare.financial_source_observation')
            if created_schema:
                await conn.execute('DROP SCHEMA tushare')
        await pool.close()
