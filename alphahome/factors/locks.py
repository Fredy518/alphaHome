"""All factor writers participate in the repair exclusion protocol."""

from contextlib import contextmanager


def snapshot_gate(cursor, *, exclusive=False):
    cursor.execute("SET LOCAL lock_timeout = '30s'")
    function = "pg_advisory_xact_lock" if exclusive else "pg_advisory_xact_lock_shared"
    cursor.execute(f"SELECT {function}(hashtext(%s), hashtext(%s))", ("alphahome.factors", "snapshots"))


@contextmanager
def repair_session(db):
    """Hold across date commits; the caller must own this synchronous session."""
    connection = db._get_sync_connection()
    locked = False
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL lock_timeout = '30s'")
            cursor.execute("SELECT pg_advisory_lock(hashtext(%s), hashtext(%s))", ("alphahome.factors", "snapshots"))
            locked = True
        connection.commit()
        yield
    finally:
        connection.rollback()
        if locked:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_unlock(hashtext(%s), hashtext(%s))", ("alphahome.factors", "snapshots"))
            connection.commit()
