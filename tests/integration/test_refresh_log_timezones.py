from datetime import datetime, timezone

import asyncpg
import pytest

from alphahome.features.storage.refresh_log import log_mv_refresh

pytestmark = [pytest.mark.integration, pytest.mark.requires_db]


@pytest.mark.asyncio
@pytest.mark.parametrize("server_timezone", ["UTC", "Asia/Shanghai"])
@pytest.mark.parametrize("explicit", [True, False])
async def test_refresh_instants_do_not_depend_on_server_timezone(isolated_database_url, server_timezone, explicit):
    connection = await asyncpg.connect(isolated_database_url)
    try:
        await connection.execute("SELECT set_config('TimeZone',$1,false)", server_timezone)
        # A temporary relation shadows the production name via a tiny adapter.
        await connection.execute("""CREATE TEMP TABLE closeout_refresh_log (
            view_name text, schema_name text, refresh_strategy text,
            started_at timestamptz, finished_at timestamptz,
            duration_seconds double precision, success boolean,
            error_message text, row_count bigint, details jsonb)""")

        class Adapter:
            async def execute(self, sql, *args):
                return await connection.execute(sql.replace("features.mv_refresh_log", "pg_temp.closeout_refresh_log"), *args)

        before = datetime.now(timezone.utc)
        options = {"started_at": datetime(2026,10,4,17,0,0), "finished_at": datetime(2026,10,4,17,0,7)} if explicit else {}
        assert await log_mv_refresh(Adapter(), view_name="sample", schema_name="features",
            refresh_strategy="full", success=True, duration_seconds=7, **options)
        after = datetime.now(timezone.utc)
        row = await connection.fetchrow("SELECT * FROM pg_temp.closeout_refresh_log")
        assert (row["finished_at"] - row["started_at"]).total_seconds() == 7
        if explicit:
            assert row["started_at"] == datetime(2026,10,4,9,0,0,tzinfo=timezone.utc)
            assert row["finished_at"] == datetime(2026,10,4,9,0,7,tzinfo=timezone.utc)
        else:
            assert before <= row["finished_at"] <= after
    finally:
        await connection.close()
