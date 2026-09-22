"""Refresh and validate a materialized view before committing its replacement."""

from time import monotonic

import asyncpg

from .atomic import table_refresh_transaction
from .quality import validate_quality
from .refresh_log import log_mv_refresh
from alphahome.common.plan_inspection import qualified_relation


async def refresh_validated_mv(recipe, strategy, allow_blocking_fallback=False):
    if strategy not in {'full', 'concurrent'}:
        raise ValueError('Unsupported materialized-view refresh strategy')
    connection = None
    started = monotonic()
    target = qualified_relation(recipe.full_name)
    effective, fallback = strategy, None
    try:
        connection = await asyncpg.connect(recipe._db_manager.connection_string, command_timeout=7200)
        async with table_refresh_transaction(connection, recipe.schema, recipe.view_name):
            row = await connection.fetchrow(
                """SELECT relkind::text, relispopulated, EXISTS(
                     SELECT 1 FROM pg_index i WHERE i.indrelid=c.oid AND i.indisvalid AND i.indisunique
                     AND i.indimmediate AND i.indpred IS NULL AND i.indexprs IS NULL) AS unique_index
                   FROM pg_class c WHERE c.oid=to_regclass($1)""", recipe.full_name,
            )
            if not row or row['relkind'] != 'm':
                raise RuntimeError('migration_required: expected a materialized view')
            if strategy == 'concurrent':
                fallback = 'unpopulated' if not row['relispopulated'] else 'missing_qualifying_unique_index' if not row['unique_index'] else None
                if fallback and not allow_blocking_fallback:
                    raise RuntimeError(f'Concurrent refresh unavailable: {fallback}; blocking fallback was not authorized')
                if fallback:
                    effective = 'full'
            old_count = await connection.fetchval(f'SELECT COUNT(*) FROM {target}') if row['relispopulated'] else 0
            modifier = 'CONCURRENTLY ' if effective == 'concurrent' else ''
            await connection.execute(f'REFRESH MATERIALIZED VIEW {modifier}{target}')
            count = await connection.fetchval(f'SELECT COUNT(*) FROM {target}')
            empty_reason = await recipe.expected_empty_view_reason(connection) if count == 0 else None
            quality = await validate_quality(connection, recipe.quality_checks, recipe.full_name, old_count,
                                             expected_empty=bool(empty_reason))
    except Exception as exc:
        await log_mv_refresh(recipe._db_manager, view_name=recipe.view_name, schema_name=recipe.schema,
                             refresh_strategy=effective, success=False, duration_seconds=monotonic()-started,
                             error_message=f'{type(exc).__name__}: {exc}')
        raise
    finally:
        if connection is not None:
            await connection.close()
    duration = monotonic()-started
    result = {'status': 'expected_no_data' if empty_reason else 'success', 'empty_reason': empty_reason,
              'view_name': recipe.view_name, 'view_schema': recipe.schema,
              'full_name': recipe.full_name, 'row_count': count, 'committed_rows': count,
              'duration_seconds': duration, 'requested_strategy': strategy,
              'effective_strategy': effective, 'refresh_strategy': effective, 'strategy': effective,
              'fallback_reason': fallback, 'quality': quality, 'source_consumption': 'unverified'}
    await log_mv_refresh(recipe._db_manager, view_name=recipe.view_name, schema_name=recipe.schema,
                         refresh_strategy=effective, success=True, duration_seconds=duration,
                         row_count=count, details=result)
    return result
