#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
AlphaDB 本机 -> NAS 并行基线重建脚本。

流程：
1. 在本机创建 logical replication slot，并导出与该 slot 对齐的 snapshot。
2. 使用 pg_dump directory format + 并行 worker 导出该 snapshot 的整库基线。
3. 在 NAS 上 drop/recreate alphadb，并使用 pg_restore 并行恢复。
4. 复用现有 logical sync 脚本创建 subscription，并追平 snapshot 之后的增量。

设计目标：
- 比串行 pg_dump | psql 更快。
- 不再出现“全量快照”和“后续增量”之间的缺口。
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import socket
import subprocess
import sys
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import psycopg2
from psycopg2 import sql

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from alphahome.common.config_manager import get_database_url

LOGGER = logging.getLogger("alphadb_nas_parallel_rebuild")

DEFAULT_SLOT = "alphadb_nas_slot"
DEFAULT_SUBSCRIPTION = "alphadb_nas_sub"
DEFAULT_DUMP_JOBS = 8
DEFAULT_RESTORE_JOBS = 6
DEFAULT_COMPRESS_LEVEL = 0
DEFAULT_PROXY_PORT = 15432
DEFAULT_DUMP_DIR = Path("D:/alphadb_nas_parallel_dump")
META_FILE = "codex_parallel_rebuild_meta.json"


def setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def open_connection(dsn: str):
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    return conn


@contextmanager
def connection_scope(dsn: str):
    # psycopg2 在 `with conn:` 下会重新进入事务块；这里显式 close，保留 autocommit 语义。
    conn = open_connection(dsn)
    try:
        yield conn
    finally:
        conn.close()


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


def replace_database_in_url(url: str, dbname: str) -> str:
    parsed = urlparse(url)
    if not parsed.path or parsed.path == "/":
        raise RuntimeError(f"URL 未包含数据库名: {url}")
    return urlunparse(parsed._replace(path=f"/{dbname}"))


def host_from_url(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.hostname:
        raise RuntimeError(f"URL 未包含主机名: {url}")
    return parsed.hostname


def port_from_url(url: str, default: int = 5432) -> int:
    parsed = urlparse(url)
    return parsed.port or default


def with_replication_param(url: str) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["replication"] = "database"
    return urlunparse(parsed._replace(query=urlencode(query)))


def can_reach(host: str, port: int, timeout: float = 3.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def detect_local_ip_for_target(target_host: str, target_port: int) -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.connect((target_host, target_port))
        return sock.getsockname()[0]


def resolve_publisher_host(explicit_host: str, nas_url: str) -> str:
    if explicit_host:
        return explicit_host
    return detect_local_ip_for_target(host_from_url(nas_url), port_from_url(nas_url))


def find_pg_binary(name: str) -> str:
    found = shutil.which(name)
    if found:
        return found
    fallback = Path("E:/PostgreSQL/17/bin") / f"{name}.exe"
    if fallback.exists():
        return str(fallback)
    raise RuntimeError(f"未找到 PostgreSQL 可执行文件: {name}")


def run_subprocess(cmd: list[str], *, label: str) -> None:
    LOGGER.info("开始执行 %s", label)
    completed = subprocess.run(cmd, check=False)
    if completed.returncode != 0:
        raise RuntimeError(f"{label} 失败，退出码={completed.returncode}")
    LOGGER.info("%s 完成", label)


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def write_meta(dump_dir: Path, meta: dict) -> None:
    (dump_dir / META_FILE).write_text(
        json.dumps(meta, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


def read_meta(dump_dir: Path) -> dict:
    path = dump_dir / META_FILE
    if not path.exists():
        raise RuntimeError(f"缺少 dump 元数据文件: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def get_database_settings(dsn: str) -> dict:
    with connection_scope(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    datname,
                    pg_encoding_to_char(encoding) AS encoding,
                    datcollate,
                    datctype,
                    datlocprovider,
                    datlocale,
                    daticurules
                FROM pg_database
                WHERE datname = current_database()
                """
            )
            row = cur.fetchone()
    if not row:
        raise RuntimeError(f"无法读取数据库设置: {dsn}")
    return {
        "datname": row[0],
        "encoding": row[1],
        "datcollate": row[2],
        "datctype": row[3],
        "datlocprovider": row[4],
        "datlocale": row[5],
        "daticurules": row[6],
    }


def database_exists(dsn: str) -> bool:
    try:
        with connection_scope(dsn):
            return True
    except psycopg2.OperationalError as exc:
        if 'database "' in str(exc) and '" does not exist' in str(exc):
            return False
        raise


def is_locale_compat_error(exc: psycopg2.Error) -> bool:
    message = str(exc)
    return any(
        needle in message
        for needle in (
            "invalid LC_COLLATE locale name",
            "invalid locale name",
            "invalid ICU locale",
        )
    )


def ensure_slot_absent(local_url: str, slot_name: str) -> None:
    with connection_scope(local_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT active
                FROM pg_replication_slots
                WHERE slot_name = %s
                """,
                [slot_name],
            )
            row = cur.fetchone()
            if not row:
                return
            if row[0]:
                raise RuntimeError(f"复制槽 {slot_name!r} 当前为 active，无法重建")
            cur.execute("SELECT pg_drop_replication_slot(%s)", [slot_name])
            LOGGER.info("已删除旧 logical slot: %s", slot_name)


def drop_subscription_if_exists(nas_db_url: str, subscription_name: str) -> None:
    if not database_exists(nas_db_url):
        LOGGER.info("NAS 目标数据库不存在，跳过删除旧 subscription: %s", nas_db_url)
        return
    with connection_scope(nas_db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_subscription WHERE subname = %s",
                [subscription_name],
            )
            if not cur.fetchone():
                return
            cur.execute(f'ALTER SUBSCRIPTION "{subscription_name}" DISABLE')
            cur.execute(f'ALTER SUBSCRIPTION "{subscription_name}" SET (slot_name = NONE)')
            cur.execute(f'DROP SUBSCRIPTION "{subscription_name}"')
            LOGGER.info("已删除旧 subscription: %s", subscription_name)


def create_slot_and_dump(
    *,
    local_url: str,
    dump_dir: Path,
    slot_name: str,
    dump_jobs: int,
    compress_level: int,
    pg_dump_bin: str,
    verbose: bool,
) -> dict:
    if dump_dir.exists():
        raise RuntimeError(f"dump 目录已存在，请更换 --dump-dir 或先清理: {dump_dir}")

    repl_conn = open_connection(with_replication_param(local_url))
    try:
        with repl_conn.cursor() as cur:
            cur.execute(
                f"CREATE_REPLICATION_SLOT {slot_name} LOGICAL pgoutput (SNAPSHOT 'export')"
            )
            slot_name_res, consistent_point, snapshot_name, plugin = cur.fetchone()
        meta = {
            "created_at": datetime.now().isoformat(),
            "slot_name": slot_name_res,
            "consistent_point": consistent_point,
            "snapshot_name": snapshot_name,
            "plugin": plugin,
            "dump_jobs": dump_jobs,
            "compress_level": compress_level,
            "local_url": local_url,
        }
        cmd = [
            pg_dump_bin,
            "-Fd",
            "-j",
            str(dump_jobs),
            f"--compress={compress_level}",
            f"--snapshot={snapshot_name}",
            "--no-owner",
            "--no-privileges",
            "--no-publications",
            "--no-subscriptions",
            "--file",
            str(dump_dir),
            "--dbname",
            local_url,
        ]
        if verbose:
            cmd.append("--verbose")
        run_subprocess(cmd, label="并行 pg_dump")
        write_meta(dump_dir, meta)
        return meta
    finally:
        repl_conn.close()


def ensure_reuse_slot_exists(local_url: str, slot_name: str) -> None:
    with connection_scope(local_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT active
                FROM pg_replication_slots
                WHERE slot_name = %s
                """,
                [slot_name],
            )
            row = cur.fetchone()
            if not row:
                raise RuntimeError(f"dump 对应的逻辑复制槽 {slot_name!r} 不存在，不能复用 dump")
            if row[0]:
                raise RuntimeError(f"逻辑复制槽 {slot_name!r} 当前为 active，不能复用 dump")


def recreate_nas_database(nas_admin_url: str, dbname: str, *, psql_bin: str) -> None:
    source_settings = get_database_settings(local_database_url())
    locale = source_settings["datlocale"] or source_settings["datcollate"]
    provider = source_settings["datlocprovider"]
    db_ident = quote_ident(dbname)
    encoding = quote_literal(source_settings["encoding"])
    locale_lit = quote_literal(locale)
    terminate_sql = (
        "SELECT pg_terminate_backend(pid) "
        "FROM pg_stat_activity "
        f"WHERE datname = {quote_literal(dbname)} "
        "AND pid <> pg_backend_pid()"
    )
    drop_sql = f"DROP DATABASE IF EXISTS {db_ident}"
    create_with_libc = (
        f"CREATE DATABASE {db_ident} WITH TEMPLATE = template0 "
        f"ENCODING = {encoding} LOCALE_PROVIDER = libc LOCALE = {locale_lit}"
    )
    create_with_icu = (
        f"CREATE DATABASE {db_ident} WITH TEMPLATE = template0 "
        f"ENCODING = {encoding} LOCALE_PROVIDER = icu ICU_LOCALE = {locale_lit}"
    )
    create_with_default_locale = (
        f"CREATE DATABASE {db_ident} WITH TEMPLATE = template0 ENCODING = {encoding}"
    )
    create_attempts: list[tuple[str, str]] = []
    if provider == "i":
        create_attempts.append((f"ICU locale={locale}", create_with_icu))
    else:
        create_attempts.append((f"libc locale={locale}", create_with_libc))
        create_attempts.append((f"ICU locale={locale}", create_with_icu))
    create_attempts.append(("NAS 默认 locale", create_with_default_locale))

    run_subprocess(
        [psql_bin, nas_admin_url, "-v", "ON_ERROR_STOP=1", "-c", terminate_sql],
        label="终止 NAS 目标库连接",
    )
    run_subprocess(
        [psql_bin, nas_admin_url, "-v", "ON_ERROR_STOP=1", "-c", drop_sql],
        label="删除 NAS 数据库",
    )

    last_error: RuntimeError | None = None
    for idx, (description, statement) in enumerate(create_attempts):
        try:
            run_subprocess(
                [psql_bin, nas_admin_url, "-v", "ON_ERROR_STOP=1", "-c", statement],
                label=f"创建 NAS 数据库 ({description})",
            )
            LOGGER.info("已创建 NAS 数据库: %s (%s)", dbname, description)
            return
        except RuntimeError as exc:
            last_error = exc
            is_last_attempt = idx == len(create_attempts) - 1
            if is_last_attempt:
                raise
            LOGGER.warning("按 %s 创建 NAS 数据库失败，继续回退；错误=%s", description, exc)

    if last_error:
        raise last_error
    raise RuntimeError(f"创建 NAS 数据库失败: {dbname}")


def terminate_nas_database_connections(nas_db_url: str) -> None:
    with connection_scope(nas_db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = current_database()
                  AND pid <> pg_backend_pid()
                """
            )
    LOGGER.info("已终止 NAS 目标库上的其他连接")


def restore_dump(
    *,
    pg_restore_bin: str,
    dump_dir: Path,
    nas_db_url: str,
    restore_jobs: int,
    verbose: bool,
) -> None:
    cmd = [
        pg_restore_bin,
        "-j",
        str(restore_jobs),
        "--clean",
        "--if-exists",
        "--no-owner",
        "--no-privileges",
        "--dbname",
        nas_db_url,
        str(dump_dir),
    ]
    if verbose:
        cmd.append("--verbose")
    run_subprocess(cmd, label="并行 pg_restore")


def run_logical_sync(
    *,
    publisher_host: str,
    proxy_port: int,
    refresh_materialized_views: bool,
    verbose: bool,
) -> None:
    script = PROJECT_ROOT / "scripts" / "database" / "alphadb_nas_logical_sync.py"
    bootstrap_cmd = [
        sys.executable,
        str(script),
        "bootstrap",
        "--publisher-host",
        publisher_host,
        "--proxy-port",
        str(proxy_port),
    ]
    sync_cmd = [
        sys.executable,
        str(script),
        "sync-now",
        "--publisher-host",
        publisher_host,
        "--proxy-port",
        str(proxy_port),
    ]
    if verbose:
        bootstrap_cmd.append("--verbose")
        sync_cmd.append("--verbose")
    if refresh_materialized_views:
        sync_cmd.append("--refresh-materialized-views")
    run_subprocess(bootstrap_cmd, label="bootstrap 逻辑复制")
    run_subprocess(sync_cmd, label="sync-now 追平增量")


def maybe_cleanup_dump(dump_dir: Path, cleanup: bool) -> None:
    if not cleanup:
        return
    shutil.rmtree(dump_dir)
    LOGGER.info("已删除 dump 目录: %s", dump_dir)


def main() -> int:
    parser = argparse.ArgumentParser(description="AlphaDB -> NAS 并行基线重建")
    parser.add_argument("--publisher-host", default="", help="显式指定 NAS 回连本机时使用的主机/IP")
    parser.add_argument("--slot-name", default=DEFAULT_SLOT)
    parser.add_argument("--subscription-name", default=DEFAULT_SUBSCRIPTION)
    parser.add_argument("--dump-dir", default=str(DEFAULT_DUMP_DIR))
    parser.add_argument("--dump-jobs", type=int, default=DEFAULT_DUMP_JOBS)
    parser.add_argument("--restore-jobs", type=int, default=DEFAULT_RESTORE_JOBS)
    parser.add_argument("--compress-level", type=int, default=DEFAULT_COMPRESS_LEVEL)
    parser.add_argument("--proxy-port", type=int, default=DEFAULT_PROXY_PORT)
    parser.add_argument("--reuse-dump", action="store_true", help="复用已有 directory dump，不重新 pg_dump")
    parser.add_argument(
        "--refresh-materialized-views",
        action="store_true",
        help="在最终 sync-now 后刷新 NAS 全部 materialized view",
    )
    parser.add_argument("--cleanup-dump", action="store_true", help="成功后删除 dump 目录")
    parser.add_argument(
        "--preserve-nas-extra-objects",
        action="store_true",
        help="不删除 NAS 数据库，仅 clean/restore dump 中对象，以保留 NAS-only schema/table",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    setup_logging(args.verbose)
    local_url = local_database_url()
    nas_url = nas_database_url()
    nas_admin_url = replace_database_in_url(nas_url, "postgres")
    dbname = urlparse(nas_url).path.lstrip("/")
    publisher_host = resolve_publisher_host(args.publisher_host, nas_url)

    if not can_reach(host_from_url(nas_url), port_from_url(nas_url)):
        raise RuntimeError(f"当前无法连接 NAS: {nas_url}")

    dump_dir = Path(args.dump_dir)
    pg_dump_bin = find_pg_binary("pg_dump")
    pg_restore_bin = find_pg_binary("pg_restore")
    psql_bin = find_pg_binary("psql")

    drop_subscription_if_exists(nas_url, args.subscription_name)

    if args.reuse_dump:
        meta = read_meta(dump_dir)
        ensure_reuse_slot_exists(local_url, meta["slot_name"])
        LOGGER.info("复用已有 dump 目录: %s", dump_dir)
    else:
        ensure_slot_absent(local_url, args.slot_name)
        meta = create_slot_and_dump(
            local_url=local_url,
            dump_dir=dump_dir,
            slot_name=args.slot_name,
            dump_jobs=args.dump_jobs,
            compress_level=args.compress_level,
            pg_dump_bin=pg_dump_bin,
            verbose=args.verbose,
        )
        LOGGER.info(
            "并行 dump 完成，slot=%s consistent_point=%s snapshot=%s",
            meta["slot_name"],
            meta["consistent_point"],
            meta["snapshot_name"],
        )

    if args.preserve_nas_extra_objects:
        terminate_nas_database_connections(nas_url)
        LOGGER.info("保留 NAS 数据库及 dump 外对象，仅覆盖本机 dump 中的对象")
    else:
        recreate_nas_database(nas_admin_url, dbname, psql_bin=psql_bin)

    restore_dump(
        pg_restore_bin=pg_restore_bin,
        dump_dir=dump_dir,
        nas_db_url=nas_url,
        restore_jobs=args.restore_jobs,
        verbose=args.verbose,
    )
    run_logical_sync(
        publisher_host=publisher_host,
        proxy_port=args.proxy_port,
        refresh_materialized_views=args.refresh_materialized_views,
        verbose=args.verbose,
    )
    maybe_cleanup_dump(dump_dir, args.cleanup_dump)
    LOGGER.info("并行基线重建完成")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
