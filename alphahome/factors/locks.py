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


@contextmanager
def compute_session(db):
    """Serialize whole P/G pipelines before reading any compute snapshot.

    Lock order: shared repair gate, exclusive pipeline lock, date transaction lock.
    Both session locks span all date commits and are released on cancellation.
    """
    connection = db._get_sync_connection()
    acquired = []
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL lock_timeout = '30s'")
            for key, shared in (("snapshots", True), ("compute_pipeline", False)):
                suffix = "_shared" if shared else ""
                cursor.execute(f"SELECT pg_advisory_lock{suffix}(hashtext(%s),hashtext(%s))", ("alphahome.factors", key))
                acquired.append((key, suffix))
        connection.commit()
        yield
    finally:
        connection.rollback()
        with connection.cursor() as cursor:
            for key, suffix in reversed(acquired):
                cursor.execute(f"SELECT pg_advisory_unlock{suffix}(hashtext(%s),hashtext(%s))", ("alphahome.factors", key))
        connection.commit()
