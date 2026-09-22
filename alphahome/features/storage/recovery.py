"""Bounded recovery for explicitly declared incremental source contracts.

Checkpoints are committed with the output table. They describe rebuilt ranges,
not certified historical availability of vendor data.
"""

import json
from hashlib import sha256
from datetime import date, timedelta

from alphahome.common.plan_inspection import qualified_relation
from .atomic import identifier


CREATE_CHECKPOINT_SQL = """
CREATE TABLE IF NOT EXISTS features.refresh_checkpoint (
    target TEXT PRIMARY KEY,
    covered_from DATE NOT NULL,
    covered_through DATE NOT NULL,
    source_since TIMESTAMPTZ NOT NULL,
    source_counts JSONB NOT NULL,
    contract_version TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (covered_from <= covered_through)
);
"""
VERSION = 'feature_recovery_v1'


def contract_version(recipe):
    contract = {'version': recipe.recovery_contract_version, 'sources': recipe.recovery_sources,
                'keys': recipe.primary_keys, 'date_column': recipe.date_column}
    return VERSION + ':' + sha256(json.dumps(contract, sort_keys=True).encode()).hexdigest()


async def incremental_window(recipe, end):
    from alphahome.common.async_worker import run_owned_worker
    from alphahome.common.db_session import owned_sync_session, query_timeout

    def inspect():
        with owned_sync_session(recipe._db_manager.connection_string, readonly=True) as db, query_timeout(db):
            return plan_recovery(db, recipe, 'incremental', end - timedelta(days=recipe.incremental_days), end)
    return await run_owned_worker(lambda cancelled: inspect())


def _source_query(relation, spec, since, cutoff):
    date_column, updated_column = spec
    relation_sql = qualified_relation(relation)
    updated = identifier(updated_column)
    changed = f'({updated} >= %s::timestamptz)'
    earliest = (f'MIN({identifier(date_column)}) FILTER (WHERE {changed} AND {identifier(date_column)} <= %s)'
                if date_column else f'CASE WHEN BOOL_OR({changed}) THEN DATE \'1900-01-01\' END')
    params = (since, cutoff) if date_column else (since,)
    return (f'SELECT COUNT(*) AS rows, COUNT(*) FILTER (WHERE {updated} IS NULL) AS missing_watermarks, '
            f'{earliest} AS changed_from FROM {relation_sql}', params)


def _decide(recipe, strategy, start, end, checkpoint, sources):
    if strategy == 'full':
        return start
    if not checkpoint or checkpoint['contract_version'] != contract_version(recipe):
        raise RuntimeError(f'feature_baseline_required: {recipe.full_name}; use an explicit full refresh')
    if end < checkpoint['covered_through']:
        raise RuntimeError('feature_historical_request: use an explicit full plan for an earlier cutoff')
    start = min(start, checkpoint['covered_through'] + timedelta(days=1))
    previous_counts = checkpoint['source_counts']
    if isinstance(previous_counts, str):
        previous_counts = json.loads(previous_counts)
    if set(previous_counts) != set(recipe.recovery_sources):
        raise RuntimeError('feature_backfill_required: source contract changed')
    for relation, observed in sources.items():
        if observed['missing_watermarks']:
            raise RuntimeError(f'feature_source_change_unverified: {relation} has null update timestamps; use full')
        if observed['rows'] < previous_counts[relation]:
            raise RuntimeError(f'feature_backfill_required: {relation} lost rows; deletion dates are unknown')
        dirty = observed['changed_from']
        if dirty:
            if recipe.recovery_sources[relation][0] is None:
                raise RuntimeError(f'feature_backfill_required: non-dated source changed: {relation}')
            start = min(start, dirty)
    if (end - start).days > recipe.max_incremental_recovery_days:
        raise RuntimeError(f'feature_backfill_required: {recipe.full_name} needs {start}..{end}; exceeds automatic recovery budget')
    return start


def plan_recovery(db, recipe, strategy, start, end):
    if not recipe.recovery_sources:
        if strategy == 'incremental':
            raise RuntimeError(f'feature_recovery_contract_required: {recipe.full_name}; use full or declare sources')
        return start
    checkpoint = db.fetch_one_sync('SELECT * FROM features.refresh_checkpoint WHERE target=%s', (recipe.full_name,))
    since = checkpoint['source_since'] - timedelta(days=1) if checkpoint else None
    sources = {relation: db.fetch_one_sync(*_source_query(relation, spec, since, end))
               for relation, spec in recipe.recovery_sources.items()}
    return _decide(recipe, strategy, start, end, checkpoint, sources)


async def begin_recovery(connection, recipe, strategy, start, end):
    if not recipe.recovery_sources:
        return None
    started = await connection.fetchval('SELECT clock_timestamp()')
    checkpoint = await connection.fetchrow('SELECT * FROM features.refresh_checkpoint WHERE target=$1', recipe.full_name)
    since = checkpoint['source_since'] - timedelta(days=1) if checkpoint else None
    sources = {}
    for relation, spec in recipe.recovery_sources.items():
        query, params = _source_query(relation, spec, since, end)
        for index in range(len(params)):
            query = query.replace('%s', f'${index+1}', 1)
        sources[relation] = dict(await connection.fetchrow(query, *params))
    required = _decide(recipe, strategy, start, end, checkpoint, sources)
    if required < start:
        raise RuntimeError(f'feature_plan_incomplete: rebuild the plan from {required}')
    return {
        'covered_from': min(start, checkpoint['covered_from']) if checkpoint and strategy != 'full' else start,
        'covered_through': end,
        'source_since': started,
        'source_counts': {relation: row['rows'] for relation, row in sources.items()},
        'initial_baseline': checkpoint is None,
    }


async def commit_checkpoint(connection, recipe, evidence):
    if evidence is None:
        return
    await connection.execute(
        """INSERT INTO features.refresh_checkpoint
           (target,covered_from,covered_through,source_since,source_counts,contract_version)
           VALUES ($1,$2,$3,$4,$5::jsonb,$6)
           ON CONFLICT(target) DO UPDATE SET covered_from=EXCLUDED.covered_from,
             covered_through=EXCLUDED.covered_through, source_since=EXCLUDED.source_since,
             source_counts=EXCLUDED.source_counts, contract_version=EXCLUDED.contract_version,
             updated_at=clock_timestamp()""",
        recipe.full_name, evidence['covered_from'], evidence['covered_through'],
        evidence['source_since'], json.dumps(evidence['source_counts']), contract_version(recipe),
    )
