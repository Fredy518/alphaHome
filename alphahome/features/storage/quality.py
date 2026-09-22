"""Executable publication checks; descriptive metadata remains explicitly advisory."""

from .atomic import identifier
from alphahome.common.plan_inspection import qualified_relation


async def comparable_row_counts(connection, target_relation, staged_relation, date_column, strategy, start, end):
    """Compare the same existing dates; catching up new dates is expected growth."""
    target, staged = qualified_relation(target_relation), qualified_relation(staged_relation)
    column = identifier(date_column)
    old = await connection.fetchrow(
        f'SELECT COUNT(*) AS rows, MAX({column}) AS last_date FROM {target}'
        + ('' if strategy == 'full' else f' WHERE {column} BETWEEN $1 AND $2'),
        *((start, end) if strategy != 'full' else ()),
    )
    comparable = await connection.fetchval(f'SELECT COUNT(*) FROM {staged} WHERE {column} <= $1', old['last_date'])
    return old['rows'], comparable


async def validate_quality(connection, checks, staged_relation, previous_count, *, expected_empty=False,
                           comparable_count=None, approved_initial_baseline_rows=None):
    target = qualified_relation(staged_relation)
    count = int(await connection.fetchval(f'SELECT COUNT(*) FROM {target}'))
    results = {}
    if not count and not expected_empty:
        raise ValueError('feature_quality_failed: empty result has no expected-no-data contract')
    rules = checks or {}
    null_rule = rules.get('null_check')
    required = rules.get('required_keys') or []
    for label, columns, threshold in (
        ('required_keys', required, 0.0),
        ('null_check', (null_rule or {}).get('columns', []), float((null_rule or {}).get('threshold', 0))),
    ):
        if not columns:
            continue
        if not 0 <= threshold <= 1:
            raise ValueError('Invalid null-rate threshold')
        condition = ' OR '.join(f'{identifier(column)} IS NULL' for column in columns)
        nulls = int(await connection.fetchval(f'SELECT COUNT(*) FROM {target} WHERE {condition}'))
        ratio = nulls / count if count else 0
        results[label] = {'null_rows': nulls, 'ratio': ratio, 'threshold': threshold}
        if ratio > threshold:
            raise ValueError(f'feature_quality_failed: {label} ratio {ratio:.6f} > {threshold}')
    if 'row_count_change' in rules:
        rule = rules['row_count_change']
        limit = float(rule['threshold'])
        if limit < 0:
            raise ValueError('Invalid row-count threshold')
        current = count if comparable_count is None else comparable_count
        change = abs(current - previous_count) / previous_count if previous_count else None
        approved_growth = approved_initial_baseline_rows is not None
        if approved_growth:
            approved = int(approved_initial_baseline_rows)
            if approved <= 0 or count != approved:
                raise ValueError(
                    f'feature_quality_failed: initial baseline rows {count} differ from approved {approved}'
                )
            if current < previous_count:
                raise ValueError('feature_quality_failed: initial baseline approval cannot authorize row loss')
        results['row_count_change'] = {'previous': previous_count, 'current': current, 'total_rows': count,
                                       'ratio': change, 'threshold': limit,
                                       'initial_baseline_growth_approved': approved_growth,
                                       'approved_total_rows': approved if approved_growth else None}
        if change is not None and change > limit and not expected_empty and not approved_growth:
            raise ValueError(f'feature_quality_failed: row_count_change {change:.6f} > {limit}')
    return {'executed': results, 'advisory_keys': sorted(set(rules) - {'null_check', 'required_keys', 'row_count_change'})}


async def validate_expected_keys(connection, recipe, staged_relation, start, end):
    sql = recipe.expected_keys_sql(start, end)
    if sql is None:
        return
    keys = ','.join(identifier(key) for key in recipe.primary_keys)
    target = qualified_relation(staged_relation)
    mismatch = await connection.fetchval(
        f'WITH expected AS ({sql}), actual AS (SELECT {keys} FROM {target}) '
        f'SELECT EXISTS((SELECT {keys} FROM expected EXCEPT SELECT {keys} FROM actual) '
        f'UNION ALL (SELECT {keys} FROM actual EXCEPT SELECT {keys} FROM expected))'
    )
    if mismatch:
        raise ValueError('feature_quality_failed: eligible business-key coverage differs from source')
