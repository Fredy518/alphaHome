from unittest.mock import Mock

import pytest

from db_safety import UnsafeTestDatabase, assert_connection_allowed, install_database_guard, test_target


SAFE_URL = "postgresql://test_owner:fixture-only@127.0.0.1:55439/alphahome_test_fixture"


@pytest.mark.parametrize("value", [
    None,
    "postgresql://user:fixture-only@127.0.0.1:5432/alphahome_test_fixture",
    "postgresql://user:fixture-only@127.0.0.1:55439/alphadb",
    "postgresql://user:fixture-only@database.example:55439/alphahome_test_fixture",
    "postgresql://user:fixture-only@127.0.0.1/alphahome_test_fixture",
    SAFE_URL + "?host=database.example",
    SAFE_URL + "?hostaddr=10.0.0.1",
    SAFE_URL + "?service=production",
    SAFE_URL + "#fragment",
])
def test_unsafe_targets_are_rejected_without_exposing_secrets(value):
    with pytest.raises(UnsafeTestDatabase) as error:
        test_target(value)
    assert "fixture-only" not in str(error.value)


def test_explicit_isolated_target_is_accepted():
    assert test_target(SAFE_URL) == ("127.0.0.1", 55439, "alphahome_test_fixture")
    assert_connection_allowed((SAFE_URL,), {}, configured_url=SAFE_URL)


@pytest.mark.parametrize("kwargs", [{"database": "alphadb"}, {"port": 5432}, {"hostaddr": "10.0.0.1"}])
def test_driver_target_overrides_are_rejected(kwargs):
    with pytest.raises(UnsafeTestDatabase):
        assert_connection_allowed((SAFE_URL,), kwargs, configured_url=SAFE_URL)


def test_all_database_drivers_reject_before_connect(monkeypatch):
    import asyncpg
    import psycopg2

    connection = Mock()
    pool = Mock()
    monkeypatch.delenv("ALPHAHOME_TEST_DATABASE_URL", raising=False)
    monkeypatch.setattr(psycopg2, "connect", connection)
    monkeypatch.setattr(asyncpg, "connect", connection)
    monkeypatch.setattr(asyncpg, "create_pool", pool)
    install_database_guard(monkeypatch)
    for connect in (psycopg2.connect, asyncpg.connect, asyncpg.create_pool):
        with pytest.raises(UnsafeTestDatabase):
            connect(SAFE_URL)
    connection.assert_not_called()
    pool.assert_not_called()
