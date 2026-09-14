"""Transaction and lock shared by every ordinary feature-table refresh."""

from contextlib import asynccontextmanager
import re


def identifier(value: str) -> str:
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", value):
        raise ValueError("Invalid feature SQL identifier")
    return f'"{value}"'


@asynccontextmanager
async def table_refresh_transaction(connection, schema: str, table: str, *, lock_timeout_ms=30000):
    identifier(schema)
    identifier(table)
    if int(lock_timeout_ms) <= 0:
        raise ValueError("lock_timeout_ms must be positive")
    async with connection.transaction():
        await connection.execute("SELECT set_config('lock_timeout', $1, true)", f"{int(lock_timeout_ms)}ms")
        await connection.execute(
            "SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))",
            "alphahome.features", f"{schema}.{table}",
        )
        yield connection
