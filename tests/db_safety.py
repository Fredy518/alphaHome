"""Database tests require an explicit disposable target before any connection."""

from __future__ import annotations

import os
import re
from urllib.parse import parse_qsl, unquote, urlsplit

from psycopg2.extensions import parse_dsn


TEST_DATABASE_ENV = "ALPHAHOME_TEST_DATABASE_URL"
_TEST_NAME = re.compile(r"alphahome_test_[a-z0-9_]+\Z")


class UnsafeTestDatabase(RuntimeError):
    pass


def test_target(database_url: str | None) -> tuple[str, int, str]:
    """Reject production, implicit libpq defaults and connection-override options."""
    if not database_url:
        raise UnsafeTestDatabase(f"Set {TEST_DATABASE_ENV} to an isolated database")
    try:
        parsed = urlsplit(database_url)
        if parsed.scheme not in {"postgres", "postgresql"}:
            raise ValueError("scheme")
        host, port, database = parsed.hostname, parsed.port, unquote(parsed.path[1:])
        if host not in {"127.0.0.1", "::1"} or port is None or port == 5432:
            raise ValueError("target")
        if not _TEST_NAME.fullmatch(database) or not parsed.username:
            raise ValueError("database")
        if parsed.fragment or any(
            key not in {"sslmode", "application_name", "connect_timeout"}
            for key, _ in parse_qsl(parsed.query, keep_blank_values=True)
        ):
            raise ValueError("override")
    except (ValueError, TypeError):
        # Never put URLs, driver exceptions or passwords in a test failure.
        raise UnsafeTestDatabase(
            "Database tests require a loopback address, explicit non-5432 port, "
            "alphahome_test_* database and no target overrides"
        ) from None
    return host, port, database


test_target.__test__ = False


def assert_connection_allowed(args, kwargs, *, configured_url=None):
    expected = test_target(configured_url or os.environ.get(TEST_DATABASE_ENV))
    if any(os.environ.get(key) for key in ("PGHOSTADDR", "PGSERVICE", "PGSERVICEFILE")):
        raise UnsafeTestDatabase("libpq environment target overrides are forbidden in tests")
    candidate = args[0] if args else kwargs.get("dsn", "")
    try:
        params = parse_dsn(candidate or "")
        params.update({key: value for key, value in kwargs.items() if key in {"host", "port", "dbname", "database", "hostaddr", "service"}})
        if params.get("hostaddr") or params.get("service"):
            raise ValueError("override")
        actual = (params.get("host"), int(params.get("port", 0)), params.get("database", params.get("dbname")))
    except Exception:
        raise UnsafeTestDatabase("Invalid explicit test connection target") from None
    if actual != expected:
        raise UnsafeTestDatabase("Connection target differs from the isolated test target")


def install_database_guard(monkeypatch):
    """Cover libpq C-extension connections as well as asyncpg pools."""
    import psycopg2
    import asyncpg

    def guarded(original):
        def connect(*args, **kwargs):
            assert_connection_allowed(args, kwargs)
            return original(*args, **kwargs)
        return connect

    monkeypatch.setattr(psycopg2, "connect", guarded(psycopg2.connect))
    monkeypatch.setattr(asyncpg, "connect", guarded(asyncpg.connect))
    monkeypatch.setattr(asyncpg, "create_pool", guarded(asyncpg.create_pool))
