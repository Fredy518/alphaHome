"""Owned synchronous sessions for worker-local domain execution."""

from contextlib import asynccontextmanager, contextmanager
from threading import get_ident

from .run_models import target_fingerprint


@contextmanager
def owned_sync_session(connection_string, *, expected_target=None, readonly=False):
    from .db_manager import DBManager

    actual_target = target_fingerprint(connection_string)
    if expected_target is not None and expected_target != actual_target:
        raise RuntimeError("Database target changed since planning")
    owner_thread = get_ident()
    db = DBManager(connection_string, mode="sync")
    try:
        if readonly:
            db._get_sync_connection().set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
        yield db
    finally:
        if get_ident() != owner_thread:
            raise RuntimeError("A database session crossed its worker thread boundary")
        db.close_sync()


@contextmanager
def query_timeout(db, timeout_ms=30000):
    """Bound a read-only inspection without leaving a session setting changed."""
    from psycopg2.extensions import TRANSACTION_STATUS_INERROR

    connection = db._get_sync_connection()
    with connection.cursor() as cursor:
        cursor.execute("SHOW statement_timeout")
        previous = cursor.fetchone()[0]
        cursor.execute("SELECT set_config('statement_timeout', %s, true)", (str(int(timeout_ms)),))
    try:
        yield
    finally:
        if connection.get_transaction_status() == TRANSACTION_STATUS_INERROR:
            connection.rollback()
        else:
            with connection.cursor() as cursor:
                cursor.execute("SELECT set_config('statement_timeout', %s, true)", (previous,))


class _AsyncSnapshot:
    def __init__(self, connection):
        self.connection = connection

    async def fetch(self, query, *args):
        return await self.connection.fetch(query, *args)

    async def fetch_one(self, query, *args):
        return await self.connection.fetchrow(query, *args)

    async def fetch_val(self, query, *args):
        return await self.connection.fetchval(query, *args)

    async def execute(self, query, *args):
        return await self.connection.execute(query, *args)


@asynccontextmanager
async def readonly_snapshot(db, timeout_ms=30000):
    """Inspect through one bounded PostgreSQL snapshot; test adapters stay injectable."""
    from .db_manager import DBManager

    if not isinstance(db, DBManager):
        yield db
        return
    await db.connect()
    async with db.pool.acquire() as connection:
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            await connection.execute("SELECT set_config('statement_timeout', $1, true)", str(int(timeout_ms)))
            yield _AsyncSnapshot(connection)
