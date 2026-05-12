#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
AlphaDB 本机 -> NAS 逻辑复制与补同步脚本

目标：
1. 用 PostgreSQL logical replication 精确同步表级 INSERT/UPDATE/DELETE/TRUNCATE。
2. 允许笔记本离线办公；恢复到能连接 NAS 的网络后，再把两次同步之间的变更补齐。
3. 处理逻辑复制不覆盖的对象：
   - 无主键表：自动设置 REPLICA IDENTITY FULL
   - sequence：同步 last_value / is_called
   - materialized view：按需在 NAS 上刷新

说明：
- 逻辑复制不会自动同步 DDL。若本机变更了表结构，需先让 NAS 端 schema 对齐。
- 逻辑复制不会复制 materialized view，本脚本仅在显式开启时刷新 NAS 端 MV。
- 第一次 bootstrap 会把本机 PostgreSQL 调整为逻辑复制可用配置；若 wal_level 需要改动，
  PostgreSQL 必须重启一次后再继续。
"""

from __future__ import annotations

import argparse
import difflib
import json
import logging
import os
import secrets
import select
import socket
import socketserver
import sys
import threading
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Sequence
from urllib.parse import urlparse, urlunparse

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from alphahome.common.config_manager import get_database_url

LOGGER = logging.getLogger("alphadb_nas_logical_sync")

STATE_FILE = Path.home() / ".alphahome" / "nas_logical_sync.json"
HBA_BLOCK_BEGIN = "# BEGIN ALPHAHOME NAS LOGICAL SYNC"
HBA_BLOCK_END = "# END ALPHAHOME NAS LOGICAL SYNC"

DEFAULT_PUBLICATION = "alphadb_nas_pub"
DEFAULT_SUBSCRIPTION = "alphadb_nas_sub"
DEFAULT_SLOT = "alphadb_nas_slot"
DEFAULT_REPLICATION_USER = "alphadb_sync"
DEFAULT_PROXY_PORT = 15432

MIN_MAX_REPLICATION_SLOTS = 16
MIN_MAX_WAL_SENDERS = 16
SCHEMA_DIFF_PREVIEW_LINES = 40


@dataclass
class SyncState:
    publication_name: str = DEFAULT_PUBLICATION
    subscription_name: str = DEFAULT_SUBSCRIPTION
    slot_name: str = DEFAULT_SLOT
    replication_user: str = DEFAULT_REPLICATION_USER
    replication_password: str = ""
    publisher_host: str = ""


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def load_state() -> SyncState | None:
    if not STATE_FILE.exists():
        return None
    data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    return SyncState(**data)


def save_state(state: SyncState) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(
        json.dumps(asdict(state), indent=2, ensure_ascii=True),
        encoding="utf-8",
    )


def build_state(args: argparse.Namespace) -> SyncState:
    state = load_state() or SyncState()
    if not state.replication_password:
        state.replication_password = secrets.token_urlsafe(24)

    if getattr(args, "publication_name", None):
        state.publication_name = args.publication_name
    if getattr(args, "subscription_name", None):
        state.subscription_name = args.subscription_name
    if getattr(args, "slot_name", None):
        state.slot_name = args.slot_name
    if getattr(args, "replication_user", None):
        state.replication_user = args.replication_user
    if getattr(args, "publisher_host", None):
        state.publisher_host = args.publisher_host

    save_state(state)
    return state


def local_database_url() -> str:
    db_url = get_database_url()
    if not db_url:
        raise RuntimeError("未能从 ~/.alphahome/config.json 读取本地 DATABASE_URL")
    return db_url


def nas_database_url() -> str:
    db_url = os.environ.get("NASPG_URL")
    if not db_url:
        raise RuntimeError("环境变量 NASPG_URL 未设置")
    return db_url.strip()


def parsed_url(url: str):
    parsed = urlparse(url)
    if parsed.scheme not in {"postgresql", "postgres"}:
        raise ValueError(f"不支持的 PostgreSQL URL: {url}")
    if not parsed.path or parsed.path == "/":
        raise ValueError(f"URL 未包含数据库名: {url}")
    return parsed


def database_name_from_url(url: str) -> str:
    return parsed_url(url).path.lstrip("/")


def host_from_url(url: str) -> str:
    parsed = parsed_url(url)
    if not parsed.hostname:
        raise ValueError(f"URL 未包含主机名: {url}")
    return parsed.hostname


def port_from_url(url: str, default: int = 5432) -> int:
    parsed = parsed_url(url)
    return parsed.port or default


def replace_database_in_url(url: str, dbname: str) -> str:
    parsed = parsed_url(url)
    return urlunparse(parsed._replace(path=f"/{dbname}"))


def quote_conninfo_value(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def make_publisher_conninfo(
    local_ip: str,
    local_url: str,
    state: SyncState,
    *,
    publisher_port: int | None = None,
) -> str:
    target_port = publisher_port or port_from_url(local_url)
    return " ".join(
        [
            f"host={quote_conninfo_value(local_ip)}",
            f"port={quote_conninfo_value(str(target_port))}",
            f"dbname={quote_conninfo_value(database_name_from_url(local_url))}",
            f"user={quote_conninfo_value(state.replication_user)}",
            f"password={quote_conninfo_value(state.replication_password)}",
            f"application_name={quote_conninfo_value(state.subscription_name)}",
            "connect_timeout='5'",
            "keepalives='1'",
            "keepalives_idle='30'",
            "keepalives_interval='10'",
            "keepalives_count='3'",
        ]
    )


def open_connection(dsn: str, *, autocommit: bool = True):
    conn = psycopg2.connect(dsn)
    conn.autocommit = autocommit
    return conn


@contextmanager
def connection_scope(dsn: str, *, autocommit: bool = True):
    conn = open_connection(dsn, autocommit=autocommit)
    try:
        yield conn
    finally:
        conn.close()


def fetchall(
    conn,
    query: str,
    params: Sequence[Any] | None = None,
) -> list[dict[str, Any]]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
    return [dict(row) for row in rows]


def fetchone(
    conn,
    query: str,
    params: Sequence[Any] | None = None,
) -> dict[str, Any] | None:
    rows = fetchall(conn, query, params)
    return rows[0] if rows else None


def fetchval(conn, query: str, params: Sequence[Any] | None = None) -> Any:
    with conn.cursor() as cur:
        cur.execute(query, params)
        row = cur.fetchone()
    return row[0] if row else None


def exec_sql(
    conn,
    statement: sql.Composed | str,
    params: Sequence[Any] | None = None,
    *,
    dry_run: bool = False,
    label: str | None = None,
) -> None:
    if dry_run:
        rendered = statement if isinstance(statement, str) else statement.as_string(conn)
        LOGGER.info("[DRY-RUN] %s%s", f"{label}: " if label else "", rendered)
        return

    with conn.cursor() as cur:
        cur.execute(statement, params)


def detect_local_ip_for_target(target_host: str, target_port: int) -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.connect((target_host, target_port))
        return sock.getsockname()[0]


def resolve_publisher_host(
    state: SyncState,
    nas_host: str,
    nas_port: int,
) -> str:
    if state.publisher_host:
        return state.publisher_host
    return detect_local_ip_for_target(nas_host, nas_port)


def can_reach(host: str, port: int, timeout: float = 3.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


class ForwardingHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        upstream = socket.create_connection(
            (self.server.target_host, self.server.target_port),
            timeout=10.0,
        )
        try:
            sockets = [self.request, upstream]
            while True:
                readable, _, exceptional = select.select(sockets, [], sockets, 1.0)
                if exceptional:
                    return
                if not readable:
                    continue
                for current in readable:
                    peer = upstream if current is self.request else self.request
                    chunk = current.recv(65536)
                    if not chunk:
                        return
                    peer.sendall(chunk)
        finally:
            upstream.close()


@contextmanager
def local_tcp_proxy(
    *,
    bind_host: str,
    bind_port: int,
    target_host: str,
    target_port: int,
    enabled: bool,
):
    if not enabled:
        yield None
        return

    server = ThreadedTCPServer((bind_host, bind_port), ForwardingHandler)
    server.target_host = target_host
    server.target_port = target_port
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    LOGGER.info(
        "已启动本机 TCP 代理 %s:%s -> %s:%s",
        bind_host,
        bind_port,
        target_host,
        target_port,
    )
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        LOGGER.info("已停止本机 TCP 代理 %s:%s", bind_host, bind_port)


def load_file_text(path: Path) -> tuple[str, str]:
    text = path.read_text(encoding="utf-8")
    newline = "\r\n" if "\r\n" in text else "\n"
    return text, newline


def ensure_hba_block(
    hba_path: Path,
    lines: list[str],
    *,
    dry_run: bool,
) -> bool:
    current_text, newline = load_file_text(hba_path)
    block = newline.join([HBA_BLOCK_BEGIN, *lines, HBA_BLOCK_END]) + newline

    if HBA_BLOCK_BEGIN in current_text and HBA_BLOCK_END in current_text:
        prefix, rest = current_text.split(HBA_BLOCK_BEGIN, 1)
        _, suffix = rest.split(HBA_BLOCK_END, 1)
        new_text = prefix.rstrip("\r\n") + newline + block + suffix.lstrip("\r\n")
    else:
        base = current_text.rstrip("\r\n")
        new_text = base + newline * 2 + block

    if new_text == current_text:
        return False

    if dry_run:
        LOGGER.info("[DRY-RUN] 将更新 pg_hba.conf: %s", hba_path)
        for line in lines:
            LOGGER.info("[DRY-RUN] hba: %s", line)
        return True

    hba_path.write_text(new_text, encoding="utf-8")
    LOGGER.info("已更新 pg_hba.conf 受管区块: %s", hba_path)
    return True


def local_replication_hba_lines(
    nas_host: str,
    database_name: str,
    replication_user: str,
) -> list[str]:
    cidr = f"{nas_host}/32"
    return [
        f"host    replication     {replication_user:<20} {cidr:<22} scram-sha-256",
        f"host    {database_name:<15} {replication_user:<20} {cidr:<22} scram-sha-256",
    ]


def ensure_local_server_config(local_conn, *, dry_run: bool) -> bool:
    current = {
        row["name"]: row
        for row in fetchall(
            local_conn,
            """
            SELECT name, setting, pending_restart
            FROM pg_settings
            WHERE name IN (
                'wal_level',
                'max_replication_slots',
                'max_wal_senders',
                'listen_addresses',
                'max_slot_wal_keep_size'
            )
            """,
        )
    }

    restart_required = False
    desired = {
        "wal_level": "logical",
        "max_replication_slots": str(
            max(int(current["max_replication_slots"]["setting"]), MIN_MAX_REPLICATION_SLOTS)
        ),
        "max_wal_senders": str(
            max(int(current["max_wal_senders"]["setting"]), MIN_MAX_WAL_SENDERS)
        ),
    }
    if current["listen_addresses"]["setting"] != "*":
        desired["listen_addresses"] = "*"

    for key, value in desired.items():
        if current[key]["setting"] == value:
            continue
        statement = sql.SQL("ALTER SYSTEM SET {} = %s").format(sql.Identifier(key))
        exec_sql(
            local_conn,
            statement,
            [value],
            dry_run=dry_run,
            label=f"ALTER SYSTEM {key}",
        )
        restart_required = True

    wal_keep = current["max_slot_wal_keep_size"]["setting"]
    if wal_keep != "-1":
        LOGGER.warning(
            "当前 max_slot_wal_keep_size=%s。若离线期间 WAL 超过该上限，逻辑复制槽可能失效。",
            wal_keep,
        )

    if restart_required and not dry_run:
        fetchval(local_conn, "SELECT pg_reload_conf()")
        LOGGER.warning("已写入 postgresql.auto.conf。wal_level / listen_addresses 变更需要重启 PostgreSQL。")

    return restart_required


def ensure_replication_role(local_conn, state: SyncState, *, dry_run: bool) -> None:
    exists = fetchval(
        local_conn,
        "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = %s)",
        [state.replication_user],
    )
    if not exists:
        statement = sql.SQL(
            "CREATE ROLE {} WITH LOGIN REPLICATION PASSWORD %s"
        ).format(sql.Identifier(state.replication_user))
        exec_sql(
            local_conn,
            statement,
            [state.replication_password],
            dry_run=dry_run,
            label="CREATE ROLE",
        )
        return

    statement = sql.SQL(
        "ALTER ROLE {} WITH LOGIN REPLICATION PASSWORD %s"
    ).format(sql.Identifier(state.replication_user))
    exec_sql(
        local_conn,
        statement,
        [state.replication_password],
        dry_run=dry_run,
        label="ALTER ROLE",
    )


def missing_primary_key_tables(local_conn) -> list[tuple[str, str]]:
    rows = fetchall(
        local_conn,
        """
        SELECT n.nspname AS schema_name, c.relname AS table_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND NOT EXISTS (
              SELECT 1
              FROM pg_constraint con
              WHERE con.conrelid = c.oid
                AND con.contype = 'p'
          )
        ORDER BY 1, 2
        """,
    )
    return [(row["schema_name"], row["table_name"]) for row in rows]


def ensure_replica_identity_for_no_pk_tables(local_conn, *, dry_run: bool) -> list[str]:
    changed: list[str] = []
    rows = fetchall(
        local_conn,
        """
        SELECT n.nspname AS schema_name, c.relname AS table_name, c.relreplident
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND NOT EXISTS (
              SELECT 1
              FROM pg_constraint con
              WHERE con.conrelid = c.oid
                AND con.contype = 'p'
          )
        ORDER BY 1, 2
        """,
    )
    for row in rows:
        if row["relreplident"] == "f":
            continue
        statement = sql.SQL("ALTER TABLE {}.{} REPLICA IDENTITY FULL").format(
            sql.Identifier(row["schema_name"]),
            sql.Identifier(row["table_name"]),
        )
        exec_sql(
            local_conn,
            statement,
            dry_run=dry_run,
            label="ALTER TABLE REPLICA IDENTITY FULL",
        )
        changed.append(f'{row["schema_name"]}.{row["table_name"]}')
    return changed


def ensure_publication(local_conn, publication_name: str, *, dry_run: bool) -> None:
    row = fetchone(
        local_conn,
        "SELECT pubname, puballtables FROM pg_publication WHERE pubname = %s",
        [publication_name],
    )
    if row and row["puballtables"]:
        return
    if row and not row["puballtables"]:
        raise RuntimeError(
            f"本地 publication {publication_name!r} 已存在，但不是 FOR ALL TABLES。"
            "请先手动清理或换 publication 名称。"
        )
    statement = sql.SQL("CREATE PUBLICATION {} FOR ALL TABLES").format(
        sql.Identifier(publication_name)
    )
    exec_sql(
        local_conn,
        statement,
        dry_run=dry_run,
        label="CREATE PUBLICATION",
    )


def ensure_logical_slot(local_conn, slot_name: str, local_dbname: str, *, dry_run: bool) -> None:
    row = fetchone(
        local_conn,
        """
        SELECT slot_name, plugin, database
        FROM pg_replication_slots
        WHERE slot_name = %s
        """,
        [slot_name],
    )
    if row:
        if row["plugin"] != "pgoutput":
            raise RuntimeError(f"复制槽 {slot_name!r} 已存在，但 plugin={row['plugin']!r}，不是 pgoutput。")
        if row["database"] != local_dbname:
            raise RuntimeError(
                f"复制槽 {slot_name!r} 指向数据库 {row['database']!r}，不是 {local_dbname!r}。"
            )
        return
    exec_sql(
        local_conn,
        "SELECT * FROM pg_create_logical_replication_slot(%s, 'pgoutput')",
        [slot_name],
        dry_run=dry_run,
        label="CREATE LOGICAL REPLICATION SLOT",
    )


def schema_signature(conn) -> dict[tuple[str, str], list[str]]:
    rows = fetchall(
        conn,
        """
        SELECT
            c.table_schema,
            c.table_name,
            c.column_name,
            c.data_type,
            c.udt_name,
            COALESCE(c.character_maximum_length, -1) AS char_len,
            c.is_nullable
        FROM information_schema.columns c
        JOIN information_schema.tables t
          ON t.table_schema = c.table_schema
         AND t.table_name = c.table_name
        WHERE t.table_type = 'BASE TABLE'
          AND c.table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY c.table_schema, c.table_name, c.ordinal_position
        """,
    )
    signatures: dict[tuple[str, str], list[str]] = {}
    for row in rows:
        table_key = (row["table_schema"], row["table_name"])
        signatures.setdefault(table_key, []).append(
            "|".join(
                [
                    row["table_schema"],
                    row["table_name"],
                    row["column_name"],
                    row["data_type"],
                    row["udt_name"],
                    str(row["char_len"]),
                    row["is_nullable"],
                ]
            )
        )
    return signatures


def ensure_schema_compatible(local_conn, nas_conn) -> None:
    local_sig = schema_signature(local_conn)
    nas_sig = schema_signature(nas_conn)
    missing_tables = sorted(set(local_sig) - set(nas_sig))
    differing_tables = sorted(
        table_key
        for table_key in set(local_sig) & set(nas_sig)
        if local_sig[table_key] != nas_sig[table_key]
    )

    if not missing_tables and not differing_tables:
        extra_tables = sorted(set(nas_sig) - set(local_sig))
        if extra_tables:
            LOGGER.info("NAS 上存在 %s 张本机没有的额外表，已忽略。", len(extra_tables))
        return

    diff: list[str] = []
    if missing_tables:
        diff.append("NAS 缺少以下本机表:")
        diff.extend(f"  {schema_name}.{table_name}" for schema_name, table_name in missing_tables)

    for schema_name, table_name in differing_tables:
        diff.extend(
            difflib.unified_diff(
                local_sig[(schema_name, table_name)],
                nas_sig[(schema_name, table_name)],
                fromfile=f"local:{schema_name}.{table_name}",
                tofile=f"nas:{schema_name}.{table_name}",
                lineterm="",
                n=2,
            )
        )

    preview = "\n".join(diff[:SCHEMA_DIFF_PREVIEW_LINES])
    raise RuntimeError(
        "本机发布表与 NAS 表结构不兼容。逻辑复制不会自动同步 DDL，请先让 NAS schema 对齐。\n"
        f"{preview}"
    )


def ensure_subscription(
    nas_conn,
    *,
    state: SyncState,
    publication_conninfo: str,
    publication_name: str,
    dry_run: bool,
) -> None:
    row = fetchone(
        nas_conn,
        "SELECT subname FROM pg_subscription WHERE subname = %s",
        [state.subscription_name],
    )
    if not row:
        statement = sql.SQL(
            """
            CREATE SUBSCRIPTION {}
            CONNECTION %s
            PUBLICATION {}
            WITH (
                copy_data = false,
                create_slot = false,
                enabled = false,
                slot_name = %s
            )
            """
        ).format(
            sql.Identifier(state.subscription_name),
            sql.Identifier(publication_name),
        )
        exec_sql(
            nas_conn,
            statement,
            [publication_conninfo, state.slot_name],
            dry_run=dry_run,
            label="CREATE SUBSCRIPTION",
        )
        return

    disable_statement = sql.SQL("ALTER SUBSCRIPTION {} DISABLE").format(
        sql.Identifier(state.subscription_name)
    )
    exec_sql(
        nas_conn,
        disable_statement,
        dry_run=dry_run,
        label="ALTER SUBSCRIPTION DISABLE",
    )

    conn_statement = sql.SQL("ALTER SUBSCRIPTION {} CONNECTION %s").format(
        sql.Identifier(state.subscription_name)
    )
    exec_sql(
        nas_conn,
        conn_statement,
        [publication_conninfo],
        dry_run=dry_run,
        label="ALTER SUBSCRIPTION CONNECTION",
    )

    slot_statement = sql.SQL("ALTER SUBSCRIPTION {} SET (slot_name = %s)").format(
        sql.Identifier(state.subscription_name)
    )
    exec_sql(
        nas_conn,
        slot_statement,
        [state.slot_name],
        dry_run=dry_run,
        label="ALTER SUBSCRIPTION SET SLOT",
    )

def enable_subscription(nas_conn, subscription_name: str, *, dry_run: bool) -> None:
    statement = sql.SQL("ALTER SUBSCRIPTION {} ENABLE").format(
        sql.Identifier(subscription_name)
    )
    exec_sql(
        nas_conn,
        statement,
        dry_run=dry_run,
        label="ALTER SUBSCRIPTION ENABLE",
    )


def refresh_subscription_publication(
    nas_conn,
    subscription_name: str,
    *,
    dry_run: bool,
) -> None:
    statement = sql.SQL(
        "ALTER SUBSCRIPTION {} REFRESH PUBLICATION WITH (copy_data = false)"
    ).format(sql.Identifier(subscription_name))
    exec_sql(
        nas_conn,
        statement,
        dry_run=dry_run,
        label="ALTER SUBSCRIPTION REFRESH PUBLICATION",
    )


def disable_subscription(nas_conn, subscription_name: str, *, dry_run: bool) -> None:
    statement = sql.SQL("ALTER SUBSCRIPTION {} DISABLE").format(
        sql.Identifier(subscription_name)
    )
    exec_sql(
        nas_conn,
        statement,
        dry_run=dry_run,
        label="ALTER SUBSCRIPTION DISABLE",
    )


def wait_until_caught_up(
    local_conn,
    nas_conn,
    subscription_name: str,
    *,
    timeout_seconds: int,
) -> None:
    target_lsn = fetchval(local_conn, "SELECT pg_current_wal_lsn()")
    LOGGER.info("等待 NAS 订阅追平到本机目标 LSN: %s", target_lsn)

    started = time.time()
    while time.time() - started < timeout_seconds:
        row = fetchone(
            nas_conn,
            """
            SELECT
                sub.subenabled,
                stat.received_lsn::text AS received_lsn,
                stat.latest_end_lsn::text AS latest_end_lsn,
                COALESCE(stat.latest_end_lsn >= %s::pg_lsn, false) AS caught_up
            FROM pg_subscription sub
            LEFT JOIN pg_stat_subscription stat
              ON stat.subid = sub.oid
            WHERE sub.subname = %s
            """,
            [target_lsn, subscription_name],
        )
        if not row:
            raise RuntimeError(f"NAS 上找不到 subscription {subscription_name!r}")
        if row["caught_up"]:
            LOGGER.info(
                "订阅已追平，received_lsn=%s latest_end_lsn=%s",
                row["received_lsn"],
                row["latest_end_lsn"],
            )
            return
        LOGGER.info(
            "订阅尚未追平，received_lsn=%s latest_end_lsn=%s",
            row["received_lsn"],
            row["latest_end_lsn"],
        )
        time.sleep(3)

    raise TimeoutError(f"等待 subscription {subscription_name!r} 追平超时（{timeout_seconds}s）")


def user_sequences(conn) -> list[tuple[str, str]]:
    rows = fetchall(
        conn,
        """
        SELECT schemaname, sequencename
        FROM pg_sequences
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY schemaname, sequencename
        """,
    )
    return [(row["schemaname"], row["sequencename"]) for row in rows]


def read_sequence_state(conn, schema_name: str, sequence_name: str) -> tuple[int, bool]:
    statement = sql.SQL("SELECT last_value, is_called FROM {}.{}").format(
        sql.Identifier(schema_name),
        sql.Identifier(sequence_name),
    )
    with conn.cursor() as cur:
        cur.execute(statement)
        row = cur.fetchone()
    if not row:
        raise RuntimeError(f"读取 sequence 失败: {schema_name}.{sequence_name}")
    return int(row[0]), bool(row[1])


def sync_sequences(local_conn, nas_conn, *, dry_run: bool) -> None:
    for schema_name, sequence_name in user_sequences(local_conn):
        local_last_value, local_is_called = read_sequence_state(
            local_conn,
            schema_name,
            sequence_name,
        )
        exists = fetchval(
            nas_conn,
            """
            SELECT EXISTS (
                SELECT 1
                FROM pg_sequences
                WHERE schemaname = %s AND sequencename = %s
            )
            """,
            [schema_name, sequence_name],
        )
        if not exists:
            LOGGER.warning("NAS 缺少 sequence，跳过: %s.%s", schema_name, sequence_name)
            continue
        exec_sql(
            nas_conn,
            "SELECT setval(%s, %s, %s)",
            [f"{schema_name}.{sequence_name}", local_last_value, local_is_called],
            dry_run=dry_run,
            label=f"SETVAL {schema_name}.{sequence_name}",
        )


def refresh_materialized_views(nas_conn, *, dry_run: bool) -> None:
    rows = fetchall(
        nas_conn,
        """
        SELECT schemaname, matviewname
        FROM pg_matviews
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY schemaname, matviewname
        """,
    )
    if not rows:
        LOGGER.info("NAS 上没有物化视图，无需刷新")
        return
    exec_sql(
        nas_conn,
        "SET statement_timeout = 0",
        dry_run=dry_run,
        label="SET statement_timeout",
    )
    for row in rows:
        statement = sql.SQL("REFRESH MATERIALIZED VIEW {}.{}").format(
            sql.Identifier(row["schemaname"]),
            sql.Identifier(row["matviewname"]),
        )
        exec_sql(
            nas_conn,
            statement,
            dry_run=dry_run,
            label=f"REFRESH {row['schemaname']}.{row['matviewname']}",
        )


def slot_lag_bytes(local_conn, slot_name: str) -> int | None:
    row = fetchone(
        local_conn,
        """
        SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS lag_bytes
        FROM pg_replication_slots
        WHERE slot_name = %s
        """,
        [slot_name],
    )
    if not row or row["lag_bytes"] is None:
        return None
    return int(row["lag_bytes"])


def print_status(
    local_conn,
    nas_conn,
    state: SyncState,
    local_url: str,
    nas_url: str,
    *,
    publisher_port: int,
) -> None:
    nas_host = host_from_url(nas_url)
    nas_port = port_from_url(nas_url)

    print("== Local ==")
    print(f"database_url: {local_url}")
    print(f"wal_level: {fetchval(local_conn, 'SHOW wal_level')}")
    print(f"max_replication_slots: {fetchval(local_conn, 'SHOW max_replication_slots')}")
    print(f"max_wal_senders: {fetchval(local_conn, 'SHOW max_wal_senders')}")
    print(f"listen_addresses: {fetchval(local_conn, 'SHOW listen_addresses')}")
    print(f"tables_without_pk: {len(missing_primary_key_tables(local_conn))}")
    print(f"replication_slot_lag_bytes: {slot_lag_bytes(local_conn, state.slot_name)}")

    print()
    print("== NAS ==")
    print(f"nas_url: {nas_url}")
    print(f"reachable_from_local: {can_reach(nas_host, nas_port)}")
    try:
        print(f"publisher_host_effective: {resolve_publisher_host(state, nas_host, nas_port)}")
    except OSError:
        print("publisher_host_effective: <unavailable>")
    print(f"publisher_host_override: {state.publisher_host or '<auto>'}")
    print(f"publisher_port_effective: {publisher_port}")

    sub = fetchone(
        nas_conn,
        """
        SELECT
            sub.subname,
            sub.subenabled,
            stat.received_lsn::text AS received_lsn,
            stat.latest_end_lsn::text AS latest_end_lsn,
            stat.last_msg_send_time,
            stat.last_msg_receipt_time
        FROM pg_subscription sub
        LEFT JOIN pg_stat_subscription stat
          ON stat.subid = sub.oid
        WHERE sub.subname = %s
        """,
        [state.subscription_name],
    )
    if not sub:
        print(f"subscription: {state.subscription_name} (missing)")
    else:
        print(f"subscription: {sub['subname']}")
        print(f"  enabled: {sub['subenabled']}")
        print(f"  received_lsn: {sub['received_lsn']}")
        print(f"  latest_end_lsn: {sub['latest_end_lsn']}")
        print(f"  last_msg_send_time: {sub['last_msg_send_time']}")
        print(f"  last_msg_receipt_time: {sub['last_msg_receipt_time']}")

    print()
    print("== Managed State ==")
    print(json.dumps(asdict(state), indent=2, ensure_ascii=True))


def bootstrap(args: argparse.Namespace) -> int:
    state = build_state(args)
    local_url = local_database_url()
    nas_url = nas_database_url()
    nas_host = host_from_url(nas_url)
    nas_port = port_from_url(nas_url)
    publisher_port = args.proxy_port or port_from_url(local_url)
    local_dbname = database_name_from_url(local_url)

    with connection_scope(local_url) as local_conn:
        restart_required = ensure_local_server_config(local_conn, dry_run=args.dry_run)
        ensure_replication_role(local_conn, state, dry_run=args.dry_run)

        hba_path = Path(fetchval(local_conn, "SHOW hba_file"))
        changed = ensure_hba_block(
            hba_path,
            local_replication_hba_lines(
                nas_host,
                local_dbname,
                state.replication_user,
            ),
            dry_run=args.dry_run,
        )
        if changed and not args.dry_run:
            fetchval(local_conn, "SELECT pg_reload_conf()")
            LOGGER.info("已 reload PostgreSQL 配置，pg_hba.conf 变更生效")

        ensure_replica_identity_for_no_pk_tables(local_conn, dry_run=args.dry_run)
        ensure_publication(local_conn, state.publication_name, dry_run=args.dry_run)

        if restart_required:
            LOGGER.warning(
                "bootstrap 已完成可在线步骤，但 PostgreSQL 必须先重启一次才能启用 wal_level=logical。"
            )
            return 2

        ensure_logical_slot(
            local_conn,
            state.slot_name,
            local_dbname=local_dbname,
            dry_run=args.dry_run,
        )

    if not can_reach(nas_host, nas_port):
        LOGGER.warning(
            "当前无法连接 NAS %s:%s。bootstrap 已完成本地配置，等能连 NAS 后再执行 sync-now。",
            nas_host,
            nas_port,
        )
        return 0

    local_ip = resolve_publisher_host(state, nas_host, nas_port)
    publication_conninfo = make_publisher_conninfo(
        local_ip,
        local_url,
        state,
        publisher_port=publisher_port,
    )

    with local_tcp_proxy(
        bind_host="0.0.0.0",
        bind_port=args.proxy_port,
        target_host="127.0.0.1",
        target_port=port_from_url(local_url),
        enabled=bool(args.proxy_port) and not args.dry_run,
    ):
        with connection_scope(local_url) as local_conn, connection_scope(nas_url) as nas_conn:
            ensure_schema_compatible(local_conn, nas_conn)
            ensure_subscription(
                nas_conn,
                state=state,
                publication_conninfo=publication_conninfo,
                publication_name=state.publication_name,
                dry_run=args.dry_run,
            )

    LOGGER.info("bootstrap 完成。建议下一步执行 sync-now 验证追平。")
    return 0


def sync_now(args: argparse.Namespace) -> int:
    state = build_state(args)
    local_url = local_database_url()
    nas_url = nas_database_url()
    nas_host = host_from_url(nas_url)
    nas_port = port_from_url(nas_url)
    publisher_port = args.proxy_port or port_from_url(local_url)
    local_dbname = database_name_from_url(local_url)

    if not can_reach(nas_host, nas_port):
        LOGGER.warning("当前无法连接 NAS %s:%s，跳过本次同步。", nas_host, nas_port)
        return 0

    local_ip = resolve_publisher_host(state, nas_host, nas_port)
    publication_conninfo = make_publisher_conninfo(
        local_ip,
        local_url,
        state,
        publisher_port=publisher_port,
    )
    LOGGER.info("检测到当前本机对 NAS 的可达 IP: %s", local_ip)

    with local_tcp_proxy(
        bind_host="0.0.0.0",
        bind_port=args.proxy_port,
        target_host="127.0.0.1",
        target_port=port_from_url(local_url),
        enabled=bool(args.proxy_port) and not args.dry_run,
    ):
        with connection_scope(local_url) as local_conn, connection_scope(nas_url) as nas_conn:
            if fetchval(local_conn, "SHOW wal_level") != "logical":
                raise RuntimeError(
                    "本地 PostgreSQL wal_level 不是 logical，请先执行 bootstrap 并重启 PostgreSQL"
                )

            ensure_schema_compatible(local_conn, nas_conn)
            ensure_replication_role(local_conn, state, dry_run=args.dry_run)
            ensure_replica_identity_for_no_pk_tables(local_conn, dry_run=args.dry_run)
            ensure_publication(local_conn, state.publication_name, dry_run=args.dry_run)
            ensure_logical_slot(
                local_conn,
                state.slot_name,
                local_dbname=local_dbname,
                dry_run=args.dry_run,
            )
            ensure_subscription(
                nas_conn,
                state=state,
                publication_conninfo=publication_conninfo,
                publication_name=state.publication_name,
                dry_run=args.dry_run,
            )
            enable_subscription(
                nas_conn,
                state.subscription_name,
                dry_run=args.dry_run,
            )
            refresh_subscription_publication(
                nas_conn,
                state.subscription_name,
                dry_run=args.dry_run,
            )

            if args.dry_run:
                LOGGER.info("[DRY-RUN] 跳过等待追平、sequence 补齐和物化视图刷新")
                return 0

            try:
                wait_until_caught_up(
                    local_conn,
                    nas_conn,
                    state.subscription_name,
                    timeout_seconds=args.wait_seconds,
                )
                if not args.skip_sequences:
                    sync_sequences(local_conn, nas_conn, dry_run=False)
                if args.refresh_materialized_views:
                    refresh_materialized_views(nas_conn, dry_run=False)
            finally:
                disable_subscription(nas_conn, state.subscription_name, dry_run=False)

    LOGGER.info("本次 sync-now 完成")
    return 0


def status(args: argparse.Namespace) -> int:
    state = build_state(args)
    local_url = local_database_url()
    nas_url = nas_database_url()
    with connection_scope(local_url) as local_conn, connection_scope(nas_url) as nas_conn:
        print_status(
            local_conn,
            nas_conn,
            state,
            local_url,
            nas_url,
            publisher_port=args.proxy_port or port_from_url(local_url),
        )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="AlphaDB 本机 -> NAS 可持续逻辑同步脚本",
    )
    parser.add_argument("--verbose", action="store_true", help="输出调试日志")

    subparsers = parser.add_subparsers(dest="command", required=True)

    bootstrap_parser = subparsers.add_parser(
        "bootstrap",
        help="一次性配置本地 publisher + NAS subscriber",
    )
    bootstrap_parser.add_argument("--dry-run", action="store_true", help="仅打印将执行的动作")
    bootstrap_parser.add_argument("--publication-name", default=DEFAULT_PUBLICATION)
    bootstrap_parser.add_argument("--subscription-name", default=DEFAULT_SUBSCRIPTION)
    bootstrap_parser.add_argument("--slot-name", default=DEFAULT_SLOT)
    bootstrap_parser.add_argument("--replication-user", default=DEFAULT_REPLICATION_USER)
    bootstrap_parser.add_argument(
        "--publisher-host",
        default="",
        help="显式指定 NAS 回连本机时使用的主机/IP，默认自动探测",
    )
    bootstrap_parser.add_argument(
        "--proxy-port",
        type=int,
        default=DEFAULT_PROXY_PORT,
        help="若大于 0，则在本机启动 Python TCP 代理供 NAS 回连；设为 0 则直接连接 PostgreSQL 端口",
    )
    bootstrap_parser.set_defaults(handler=bootstrap)

    sync_parser = subparsers.add_parser(
        "sync-now",
        help="补齐两次同步期间的 DML 变更",
    )
    sync_parser.add_argument("--dry-run", action="store_true", help="仅打印将执行的动作")
    sync_parser.add_argument("--publication-name", default=DEFAULT_PUBLICATION)
    sync_parser.add_argument("--subscription-name", default=DEFAULT_SUBSCRIPTION)
    sync_parser.add_argument("--slot-name", default=DEFAULT_SLOT)
    sync_parser.add_argument("--replication-user", default=DEFAULT_REPLICATION_USER)
    sync_parser.add_argument(
        "--publisher-host",
        default="",
        help="显式指定 NAS 回连本机时使用的主机/IP，默认自动探测",
    )
    sync_parser.add_argument(
        "--proxy-port",
        type=int,
        default=DEFAULT_PROXY_PORT,
        help="若大于 0，则在本机启动 Python TCP 代理供 NAS 回连；设为 0 则直接连接 PostgreSQL 端口",
    )
    sync_parser.add_argument(
        "--wait-seconds",
        type=int,
        default=1800,
        help="等待 NAS 追平的最长秒数",
    )
    sync_parser.add_argument(
        "--skip-sequences",
        action="store_true",
        help="不补 sequence 值（不推荐）",
    )
    sync_parser.add_argument(
        "--refresh-materialized-views",
        action="store_true",
        help="追平表数据后，在 NAS 端刷新全部物化视图",
    )
    sync_parser.set_defaults(handler=sync_now)

    status_parser = subparsers.add_parser(
        "status",
        help="查看逻辑同步当前状态",
    )
    status_parser.add_argument("--publication-name", default=DEFAULT_PUBLICATION)
    status_parser.add_argument("--subscription-name", default=DEFAULT_SUBSCRIPTION)
    status_parser.add_argument("--slot-name", default=DEFAULT_SLOT)
    status_parser.add_argument("--replication-user", default=DEFAULT_REPLICATION_USER)
    status_parser.add_argument(
        "--publisher-host",
        default="",
        help="显式指定 NAS 回连本机时使用的主机/IP，默认自动探测",
    )
    status_parser.add_argument(
        "--proxy-port",
        type=int,
        default=DEFAULT_PROXY_PORT,
        help="显示当前计划给 NAS 使用的发布端口；大于 0 时表示走本机 Python TCP 代理",
    )
    status_parser.set_defaults(handler=status)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    setup_logging(args.verbose)

    try:
        return int(args.handler(args))
    except KeyboardInterrupt:
        LOGGER.warning("收到中断信号，退出")
        return 130
    except Exception as exc:
        LOGGER.error("执行失败: %s", exc, exc_info=args.verbose)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
