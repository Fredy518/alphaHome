"""New system observations; never retroactive public-release certification."""

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS tushare.financial_source_observation (
    observation_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_table text NOT NULL,
    business_key jsonb NOT NULL,
    received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    writer_timezone text NOT NULL,
    payload_hash text NOT NULL,
    payload jsonb NOT NULL,
    observation_role text NOT NULL CHECK (observation_role IN ('incoming_received','pre_change_retained_snapshot')),
    availability_basis text NOT NULL DEFAULT 'system_observed_not_publication',
    CHECK (availability_basis = 'system_observed_not_publication')
)
"""


def _payload_sql(alias, columns):
    """Keep PostgreSQL's 100-argument limit independent of source width."""
    pairs = [
        "'" + column.replace("'", "''") + "'," + alias + '."'
        + column.replace('"', '""') + '"'
        for column in columns
    ]
    chunks = [
        'jsonb_build_object(' + ', '.join(pairs[start:start + 50]) + ')'
        for start in range(0, len(pairs), 50)
    ]
    return '(' + ' || '.join(chunks) + ')' if chunks else "'{}'::jsonb"


async def record_financial_observations(connection, *, target, resolved_table, temp_table,
                                      columns, primary_keys, timestamp_column):
    """Archive changed source values in the SAME transaction as their upsert.

    Initial retained values are seen now, not assigned an earlier receipt. An
    absent archive table aborts instead of silently certifying missing history.
    No schema creation occurs during ingestion.
    """
    if not getattr(target, 'archive_source_versions', False):
        return
    if not primary_keys or not set(primary_keys).issubset(columns):
        raise ValueError('source_observation_key_required')
    join=' AND '.join(f'old."{key}"=incoming."{key}"' for key in primary_keys)
    payload_columns = [col for col in columns if col != timestamp_column]
    incoming = _payload_sql('incoming', payload_columns)
    retained = _payload_sql('old', payload_columns)
    key_sql = _payload_sql('incoming', primary_keys)
    sql=f"""
        WITH changed AS MATERIALIZED (
            SELECT {key_sql} AS business_key,{incoming} AS incoming_payload,
                   CASE WHEN old."{primary_keys[0]}" IS NOT NULL THEN {retained} END AS retained_payload
            FROM "{temp_table}" incoming LEFT JOIN {resolved_table} old ON {join}
            WHERE old."{primary_keys[0]}" IS NULL OR {retained} IS DISTINCT FROM {incoming}
        ), observations AS (
            SELECT business_key,incoming_payload AS payload,'incoming_received' AS role FROM changed
            UNION ALL
            SELECT business_key,retained_payload,'pre_change_retained_snapshot' FROM changed WHERE retained_payload IS NOT NULL
        )
        INSERT INTO tushare.financial_source_observation
            (source_table,business_key,writer_timezone,payload_hash,payload,observation_role)
        SELECT $1,business_key,current_setting('TimeZone'),md5(payload::text),payload,role FROM observations
    """
    await connection.execute(sql, resolved_table)
