"""Bounded read-only catalog evidence for plans; no schema installation."""

import re
from pathlib import Path
from hashlib import sha256


def package_fingerprint(directory):
    directory = Path(directory)
    return {path.relative_to(directory).as_posix(): sha256(path.read_bytes()).hexdigest()
            for path in sorted(directory.rglob("*")) if path.is_file() and path.suffix in {".py", ".sql"}}


def qualified_relation(value):
    if not re.fullmatch(r"[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*", value):
        raise ValueError("Invalid qualified relation")
    return '.'.join('"' + part + '"' for part in value.split('.'))


def inspect_relation(db, relation):
    qualified_relation(relation)
    rows = db.fetch_sync("""
        SELECT c.oid::bigint, c.relkind::text, c.relowner::bigint, c.relacl::text, c.relrowsecurity,
               CASE WHEN c.relkind IN ('v','m') THEN pg_get_viewdef(c.oid,true) ELSE NULL END AS view_definition,
               a.attname, format_type(a.atttypid,a.atttypmod) AS type,
               a.attnotnull, pg_get_expr(d.adbin,d.adrelid) AS default_expression
        FROM pg_class c JOIN pg_attribute a ON a.attrelid=c.oid
        LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
        WHERE c.oid=to_regclass(%s) AND a.attnum>0 AND NOT a.attisdropped
        ORDER BY a.attnum
    """, (relation,))
    indexes = db.fetch_sync("""
        SELECT pg_get_indexdef(indexrelid) AS definition, indisvalid, indisunique
        FROM pg_index WHERE indrelid=to_regclass(%s) ORDER BY indexrelid
    """, (relation,))
    constraints = db.fetch_sync("SELECT pg_get_constraintdef(oid) AS definition FROM pg_constraint WHERE conrelid=to_regclass(%s) ORDER BY oid", (relation,))
    triggers = db.fetch_sync("SELECT pg_get_triggerdef(oid) AS definition, tgenabled::text FROM pg_trigger WHERE tgrelid=to_regclass(%s) AND NOT tgisinternal ORDER BY oid", (relation,))
    return {"columns": rows or [], "indexes": indexes or [], "constraints": constraints or [], "triggers": triggers or []}


def observed_relation_boundary(db, relation, structure):
    """Observation only; this is never a certificate of completed consumption."""
    columns = {row["attname"] for row in structure["columns"]}
    time_column = next((key for key in ("updated_at", "update_time", "created_at") if key in columns), None)
    if not columns:
        return {"status": "missing"}
    maximum = None
    if time_column:
        maximum = db.fetch_val_sync(f'SELECT MAX("{time_column}") FROM {qualified_relation(relation)}')
    return {"observed_maximum": maximum, "time_column": time_column, "consumed": False}
