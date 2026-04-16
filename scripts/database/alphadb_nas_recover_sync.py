#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
恢复 NAS alphadb 到可持续逻辑同步状态。

用途：
1. 继续补齐 NAS 上尚未完成的一次性基线数据回灌（当前主要是 tushare 尾段大表）。
2. 回灌完成后，重新创建 logical replication slot / subscription。
3. 执行一次 sync-now，把当前快照后的少量增量追平。

说明：
- 该脚本设计成可重入。若中途被终止，再次执行会从仍然 count 不一致的表继续。
- 当前恢复范围聚焦于上次被中断的 tushare 尾段表；已成功回灌的表不会重跑。
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import psycopg2

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from alphahome.common.config_manager import get_database_url

LOGGER = logging.getLogger("alphadb_nas_recover_sync")

DEFAULT_TAIL_SCHEMA = "tushare"
DEFAULT_START_TABLE = "fund_nav"
DEFAULT_SUBSCRIPTION = "alphadb_nas_sub"
DEFAULT_SLOT = "alphadb_nas_slot"


def setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def open_connection(dsn: str):
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    return conn


def local_database_url() -> str:
    url = get_database_url()
    if not url:
        raise RuntimeError("未能从 ~/.alphahome/config.json 读取本地 DATABASE_URL")
    return url


def nas_database_url() -> str:
    url = os.environ.get("NASPG_URL")
    if not url:
        raise RuntimeError("环境变量 NASPG_URL 未设置")
    return url.strip()


def host_from_url(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.hostname:
        raise RuntimeError(f"URL 未包含主机名: {url}")
    return parsed.hostname


def detect_local_ip_for_target(target_host: str, target_port: int) -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.connect((target_host, target_port))
        return sock.getsockname()[0]


def resolve_publisher_host(explicit_host: str, nas_url: str) -> str:
    if explicit_host:
        return explicit_host
    parsed = urlparse(nas_url)
    if not parsed.hostname:
        raise RuntimeError(f"NAS URL 未包含主机名: {nas_url}")
    return detect_local_ip_for_target(parsed.hostname, parsed.port or 5432)


def find_pg_binary(name: str) -> str:
    found = shutil.which(name)
    if found:
        return found

    fallback = Path("E:/PostgreSQL/17/bin") / f"{name}.exe"
    if fallback.exists():
        return str(fallback)
    raise RuntimeError(f"未找到 PostgreSQL 可执行文件: {name}")


def fetch_count(conn, schema_name: str, table_name: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f'SELECT count(*) FROM "{schema_name}"."{table_name}"')
        return int(cur.fetchone()[0])


def tail_tables(local_conn, *, schema_name: str, start_table: str) -> list[str]:
    with local_conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type = 'BASE TABLE'
              AND table_name >= %s
            ORDER BY table_name
            """,
            [schema_name, start_table],
        )
        return [row[0] for row in cur.fetchall()]


def mismatched_tables(
    local_conn,
    nas_conn,
    *,
    schema_name: str,
    start_table: str,
) -> list[tuple[str, int, int]]:
    rows: list[tuple[str, int, int]] = []
    for table_name in tail_tables(local_conn, schema_name=schema_name, start_table=start_table):
        local_count = fetch_count(local_conn, schema_name, table_name)
        nas_count = fetch_count(nas_conn, schema_name, table_name)
        if local_count != nas_count:
            rows.append((table_name, local_count, nas_count))
    return rows


def wal_stable(local_conn, *, seconds: int = 5) -> bool:
    with local_conn.cursor() as cur:
        cur.execute("SELECT pg_current_wal_lsn()::text")
        first = cur.fetchone()[0]
        time.sleep(seconds)
        cur.execute("SELECT pg_current_wal_lsn()::text")
        second = cur.fetchone()[0]
    return first == second


def run_copy_table(
    *,
    pg_dump_bin: str,
    psql_bin: str,
    local_url: str,
    nas_url: str,
    schema_name: str,
    table_name: str,
) -> None:
    table_ref = f"{schema_name}.{table_name}"
    dump_cmd = [
        pg_dump_bin,
        "--data-only",
        "--disable-triggers",
        "--no-owner",
        "--no-privileges",
        f"--table={table_ref}",
        "--dbname",
        local_url,
    ]
    restore_cmd = [
        psql_bin,
        "-v",
        "ON_ERROR_STOP=1",
        nas_url,
    ]

    LOGGER.info("开始回灌表: %s", table_ref)
    dump_proc = subprocess.Popen(dump_cmd, stdout=subprocess.PIPE)
    try:
        assert dump_proc.stdout is not None
        restore_proc = subprocess.Popen(restore_cmd, stdin=dump_proc.stdout)
        dump_proc.stdout.close()
        restore_rc = restore_proc.wait()
        dump_rc = dump_proc.wait()
    except Exception:
        dump_proc.kill()
        dump_proc.wait()
        raise

    if dump_rc != 0 or restore_rc != 0:
        raise RuntimeError(f"表 {table_ref} 回灌失败: pg_dump={dump_rc}, psql={restore_rc}")
    LOGGER.info("表回灌完成: %s", table_ref)


def drop_stale_replication(
    *,
    local_url: str,
    nas_url: str,
    subscription_name: str,
    slot_name: str,
) -> None:
    with open_connection(nas_url) as nas_conn:
        with nas_conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_subscription WHERE subname = %s",
                [subscription_name],
            )
            if cur.fetchone():
                LOGGER.info("删除旧 subscription: %s", subscription_name)
                cur.execute(f'ALTER SUBSCRIPTION "{subscription_name}" DISABLE')
                cur.execute(f'ALTER SUBSCRIPTION "{subscription_name}" SET (slot_name = NONE)')
                cur.execute(f'DROP SUBSCRIPTION "{subscription_name}"')

    with open_connection(local_url) as local_conn:
        with local_conn.cursor() as cur:
            cur.execute(
                """
                SELECT active
                FROM pg_replication_slots
                WHERE slot_name = %s
                """,
                [slot_name],
            )
            row = cur.fetchone()
            if row:
                if row[0]:
                    raise RuntimeError(f"复制槽 {slot_name!r} 当前仍为 active，无法删除")
                LOGGER.info("删除旧 logical slot: %s", slot_name)
                cur.execute("SELECT pg_drop_replication_slot(%s)", [slot_name])


def run_main_sync_script(args: list[str]) -> None:
    cmd = [sys.executable, str(PROJECT_ROOT / "scripts" / "database" / "alphadb_nas_logical_sync.py"), *args]
    LOGGER.info("执行: %s", " ".join(args))
    subprocess.run(cmd, check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="恢复 NAS alphadb 同步")
    parser.add_argument("--publisher-host", default="", help="显式指定 NAS 回连本机时使用的主机/IP")
    parser.add_argument("--schema", default=DEFAULT_TAIL_SCHEMA, help="要恢复的 schema，默认 tushare")
    parser.add_argument("--start-table", default=DEFAULT_START_TABLE, help="从该表名开始按字典序恢复")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    setup_logging(args.verbose)
    local_url = local_database_url()
    nas_url = nas_database_url()
    publisher_host = resolve_publisher_host(args.publisher_host, nas_url)

    if not publisher_host:
        raise RuntimeError("未能解析 publisher_host")

    pg_dump_bin = find_pg_binary("pg_dump")
    psql_bin = find_pg_binary("psql")

    with open_connection(local_url) as local_conn:
        if not wal_stable(local_conn, seconds=5):
            LOGGER.warning("检测到本机 WAL 在变化。恢复期间若继续写入 alphadb，需在完成后再次核对。")

    drop_stale_replication(
        local_url=local_url,
        nas_url=nas_url,
        subscription_name=DEFAULT_SUBSCRIPTION,
        slot_name=DEFAULT_SLOT,
    )

    with open_connection(local_url) as local_conn, open_connection(nas_url) as nas_conn:
        todo = mismatched_tables(
            local_conn,
            nas_conn,
            schema_name=args.schema,
            start_table=args.start_table,
        )

    if not todo:
        LOGGER.info("未发现待恢复表，直接重建逻辑复制并追平增量")
    else:
        LOGGER.info("待恢复表数量: %s", len(todo))
        for idx, (table_name, local_count, nas_count) in enumerate(todo, start=1):
            LOGGER.info("[%s/%s] %s.%s local=%s nas=%s", idx, len(todo), args.schema, table_name, local_count, nas_count)
            run_copy_table(
                pg_dump_bin=pg_dump_bin,
                psql_bin=psql_bin,
                local_url=local_url,
                nas_url=nas_url,
                schema_name=args.schema,
                table_name=table_name,
            )
            with open_connection(local_url) as local_conn, open_connection(nas_url) as nas_conn:
                verify_local = fetch_count(local_conn, args.schema, table_name)
                verify_nas = fetch_count(nas_conn, args.schema, table_name)
            if verify_local != verify_nas:
                raise RuntimeError(
                    f"表 {args.schema}.{table_name} 校验失败: local={verify_local}, nas={verify_nas}"
                )
            LOGGER.info("校验通过: %s.%s (%s 行)", args.schema, table_name, verify_local)

    run_main_sync_script(["bootstrap", "--publisher-host", publisher_host])
    run_main_sync_script(["sync-now", "--publisher-host", publisher_host])
    LOGGER.info("恢复完成")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
