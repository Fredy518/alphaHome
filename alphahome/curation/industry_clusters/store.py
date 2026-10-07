"""Append-only research results; old cluster_result and ETF pool tables are untouched."""

from __future__ import annotations

from contextlib import nullcontext

from psycopg2.extras import Json, execute_values

from .maintenance import stable_hash

CURRENT_VIEW_SQL = """
CREATE OR REPLACE VIEW fund_pool_on.industry_cluster_current AS
 SELECT b.universe_id,b.asof_date,b.algorithm_version,b.record_kind,b.recorded_at,
        b.config_hash,m.*,
        m.detail->>'selection_group_id' AS selection_group_id,
        m.detail->>'selection_method' AS selection_method,
        (m.detail->>'selection_eligible')::boolean AS selection_eligible,
        (m.detail->>'can_represent_selection_group')::boolean AS can_represent_selection_group,
        m.detail->>'selection_confidence' AS selection_confidence
 FROM fund_pool_on.industry_cluster_member m
 JOIN fund_pool_on.industry_cluster_latest_batch b USING(batch_id);
COMMENT ON VIEW fund_pool_on.industry_cluster_current IS
 'Industry minimax research clusters. asof_date is the business cutoff; recorded_at is actual availability. Not historical live holdings.';
"""

SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS fund_pool_on;
CREATE TABLE IF NOT EXISTS fund_pool_on.industry_cluster_universe (
 universe_id text PRIMARY KEY,
 source_description text NOT NULL,
 members jsonb NOT NULL,
 universe_hash text NOT NULL,
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS fund_pool_on.industry_cluster_batch (
 batch_id text PRIMARY KEY,
 universe_id text NOT NULL REFERENCES fund_pool_on.industry_cluster_universe(universe_id),
 asof_date date NOT NULL,
 algorithm_version text NOT NULL,
 config_hash text NOT NULL,
 config jsonb NOT NULL,
 input_hash text NOT NULL,
 result_hash text NOT NULL,
 previous_batch_id text REFERENCES fund_pool_on.industry_cluster_batch(batch_id),
 record_kind text NOT NULL CHECK (record_kind='historical_reconstruction'),
 summary jsonb NOT NULL,
 maintenance_state jsonb NOT NULL,
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS ix_industry_cluster_batch_asof
 ON fund_pool_on.industry_cluster_batch(universe_id,algorithm_version,asof_date,recorded_at);
CREATE TABLE IF NOT EXISTS fund_pool_on.industry_cluster_member (
 batch_id text NOT NULL REFERENCES fund_pool_on.industry_cluster_batch(batch_id),
 index_code text NOT NULL,
 cluster_id text NOT NULL,
 representative_index_code text NOT NULL,
 status text NOT NULL,
 price_ready boolean NOT NULL,
 can_represent_cluster boolean NOT NULL,
 is_central_representative boolean NOT NULL,
 detail jsonb NOT NULL,
 PRIMARY KEY(batch_id,index_code)
);
CREATE TABLE IF NOT EXISTS fund_pool_on.industry_cluster_group (
 batch_id text NOT NULL REFERENCES fund_pool_on.industry_cluster_batch(batch_id),
 cluster_id text NOT NULL,
 representative_index_code text NOT NULL,
 index_count integer NOT NULL CHECK(index_count>0),
 status text NOT NULL,
 quality jsonb NOT NULL,
 PRIMARY KEY(batch_id,cluster_id)
);
CREATE TABLE IF NOT EXISTS fund_pool_on.industry_cluster_event (
 batch_id text NOT NULL REFERENCES fund_pool_on.industry_cluster_batch(batch_id),
 event_no integer NOT NULL,
 event_type text NOT NULL,
 detail jsonb NOT NULL,
 PRIMARY KEY(batch_id,event_no)
);
CREATE OR REPLACE VIEW fund_pool_on.industry_cluster_latest_batch AS
 SELECT DISTINCT ON (universe_id,algorithm_version) *
 FROM fund_pool_on.industry_cluster_batch
 ORDER BY universe_id,algorithm_version,asof_date DESC,recorded_at DESC,batch_id DESC;
""" + CURRENT_VIEW_SQL


def publish_series(conn, payload: dict, *, manage_transaction: bool = True,
                   create_schema: bool = True) -> dict:
    """Publish a saved, hash-bound calculation in one transaction, idempotently."""
    universe = payload["universe"]
    universe_id = payload["universe_id"]
    universe_hash = stable_hash(universe)
    record_kind = payload.get("record_kind", "historical_reconstruction")
    if record_kind not in {"historical_reconstruction", "observed"}:
        raise ValueError("unknown cluster record kind")
    inserted = reused = 0
    with conn if manage_transaction else nullcontext():
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", ("industry_clusters:" + universe_id,))
            if create_schema:
                cur.execute(SCHEMA_SQL)
            cur.execute("SELECT universe_hash FROM fund_pool_on.industry_cluster_universe WHERE universe_id=%s", (universe_id,))
            old = cur.fetchone()
            if old and old[0] != universe_hash:
                raise ValueError("universe identity is immutable; use a new universe_id for revised members")
            cur.execute("""
                INSERT INTO fund_pool_on.industry_cluster_universe
                    (universe_id,source_description,members,universe_hash)
                VALUES(%s,%s,%s,%s) ON CONFLICT(universe_id) DO NOTHING
            """, (universe_id, payload["source_description"], Json(universe), universe_hash))
            previous_id = payload.get("previous_batch_id")
            for snapshot in payload["snapshots"]:
                hash_content = {k: snapshot[k] for k in ("state", "members", "groups", "events", "metrics")}
                result_hash = stable_hash(hash_content)
                config = snapshot["state"]["config"]
                config_hash = stable_hash(config)
                batch_id = "industry_" + stable_hash([universe_id, snapshot["state"]["asof"], config_hash,
                                                        payload["input_hash"], result_hash, previous_id])[:24]
                if record_kind == "observed":
                    batch_id = "industry_" + stable_hash([batch_id, record_kind])[:24]
                cur.execute("SELECT result_hash FROM fund_pool_on.industry_cluster_batch WHERE batch_id=%s", (batch_id,))
                existing = cur.fetchone()
                if existing:
                    if existing[0] != result_hash:
                        raise ValueError("immutable batch content mismatch")
                    reused += 1
                    previous_id = batch_id
                    continue
                cur.execute("""
                    INSERT INTO fund_pool_on.industry_cluster_batch
                    (batch_id,universe_id,asof_date,algorithm_version,config_hash,config,input_hash,
                     result_hash,previous_batch_id,record_kind,summary,maintenance_state)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (batch_id, universe_id, snapshot["state"]["asof"], config["version"], config_hash,
                      Json(config), payload["input_hash"], result_hash, previous_id, record_kind,
                      Json(snapshot["metrics"]), Json(snapshot["state"])))
                execute_values(cur, """
                    INSERT INTO fund_pool_on.industry_cluster_member
                    (batch_id,index_code,cluster_id,representative_index_code,status,price_ready,
                     can_represent_cluster,is_central_representative,detail) VALUES %s
                """, [(batch_id, r["index_code"], r["cluster_id"], r["representative_index_code"], r["status"],
                       r["price_ready"], r["can_represent_cluster"], r["is_central_representative"], Json(r))
                      for r in snapshot["members"]])
                execute_values(cur, """
                    INSERT INTO fund_pool_on.industry_cluster_group
                    (batch_id,cluster_id,representative_index_code,index_count,status,quality) VALUES %s
                """, [(batch_id, r["cluster_id"], r["representative_index_code"], r["index_count"], r["status"], Json(r))
                      for r in snapshot["groups"]])
                if snapshot["events"]:
                    execute_values(cur, """
                        INSERT INTO fund_pool_on.industry_cluster_event(batch_id,event_no,event_type,detail) VALUES %s
                    """, [(batch_id, i, r["event"], Json(r)) for i, r in enumerate(snapshot["events"])])
                inserted += 1
                previous_id = batch_id
    return {"universe_id": universe_id, "inserted_batches": inserted,
            "reused_batches": reused, "last_batch_id": previous_id}


def read_previous(conn, universe_id: str, before_date, algorithm_version: str) -> dict | None:
    """Explicitly read reconstructed history for computation, never live order authority."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT batch_id,maintenance_state FROM fund_pool_on.industry_cluster_batch
            WHERE universe_id=%s AND algorithm_version=%s AND asof_date<%s
            ORDER BY asof_date DESC,recorded_at DESC,batch_id DESC LIMIT 1
        """, (universe_id, algorithm_version, before_date))
        row = cur.fetchone()
    return {"batch_id": row[0], "state": row[1]} if row else None


def read_universe(conn, universe_id: str) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute("SELECT members FROM fund_pool_on.industry_cluster_universe WHERE universe_id=%s", (universe_id,))
        row = cur.fetchone()
    if not row:
        raise ValueError("unknown persistent universe")
    return row[0]
