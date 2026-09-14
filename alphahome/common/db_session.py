"""Owned synchronous sessions for worker-local domain execution."""

from contextlib import contextmanager
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
