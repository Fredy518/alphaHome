import pytest
from psycopg2.errors import QueryCanceled, ReadOnlySqlTransaction

from alphahome.common.db_session import owned_sync_session, query_timeout
from alphahome.common.run_models import target_fingerprint


pytestmark = [pytest.mark.integration, pytest.mark.requires_db]


def test_owned_read_snapshot_rejects_writes_and_closes(isolated_database_url):
    with owned_sync_session(isolated_database_url, expected_target=target_fingerprint(isolated_database_url), readonly=True) as db:
        connection = db._get_sync_connection()
        assert db.fetch_val_sync("SHOW transaction_read_only") == "on"
        with pytest.raises(ReadOnlySqlTransaction):
            db.execute_sync("CREATE TABLE public.should_never_exist (id integer)")
    assert connection.closed


def test_query_budget_timeout_is_an_error_and_restores_setting(isolated_database_url):
    with owned_sync_session(isolated_database_url, readonly=True) as db:
        before = db.fetch_val_sync("SHOW statement_timeout")
        with pytest.raises(QueryCanceled):
            with query_timeout(db, 10):
                db.fetch_val_sync("SELECT pg_sleep(0.1)")
        assert db.fetch_val_sync("SHOW statement_timeout") == before


def test_target_drift_fails_before_opening_session(isolated_database_url):
    with pytest.raises(RuntimeError, match="target changed"):
        with owned_sync_session(isolated_database_url, expected_target="0" * 64):
            pytest.fail("unexpected connection")
