"""Resumable, serial Massive US baseline with a frozen plan and daily receipts.

Preview writes plan.json; execute requires its hash. Run in one process because
all three collectors share the provider's free account request allowance.
"""

from __future__ import annotations

import argparse
import asyncio
import gzip
import hashlib
import json
import logging
import os
from pathlib import Path
import shutil
import subprocess
import sys
from datetime import date, datetime, timezone

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from alphahome.common.config_manager import get_database_url
from alphahome.common.constants import UpdateTypes
from alphahome.common.db_manager import DBManager
from alphahome.fetchers.tasks.stock.massive_stock_us_basic import (
    MassiveStockUsBasicTask,
)
from alphahome.fetchers.tasks.stock.massive_stock_us_daily import (
    MassiveStockUsDailyTask,
)
from alphahome.fetchers.tasks.stock.massive_stock_us_split import (
    MassiveStockUsSplitTask,
)

TASKS = {
    "daily": MassiveStockUsDailyTask,
    "basic": MassiveStockUsBasicTask,
    "split": MassiveStockUsSplitTask,
}
LOCK_NAME = "alphahome.massive_us_history_backfill.v1"


def now():
    return datetime.now(timezone.utc).isoformat()


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, default=str, ensure_ascii=False).encode()
    ).hexdigest()


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, indent=2, ensure_ascii=False, default=str), encoding="utf-8"
    )
    temporary.replace(path)


def code_hashes():
    files = [Path(__file__)]
    files += sorted((ROOT / "alphahome/fetchers/sources/massive").glob("*.py"))
    files += sorted((ROOT / "alphahome/fetchers/tasks/stock").glob("massive_*.py"))
    return {
        p.relative_to(ROOT).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in files
    }


def invalid_sql(kind):
    keys = "ticker IS NULL OR btrim(ticker) = '' OR observed_at IS NULL"
    if kind == "daily":
        prices = " OR ".join(
            f'NOT ("{c}" > 0 AND "{c}" < \'Infinity\'::float8) OR "{c}" IS NULL'
            for c in ("open", "high", "low", "close")
        )
        return (
            f"{keys} OR {prices} OR adjusted IS DISTINCT FROM false "
            "OR volume IS NULL OR NOT (volume >= 0 AND volume < 'Infinity'::float8) "
            "OR low > LEAST(open, close) OR high < GREATEST(open, close) "
            "OR (vwap IS NOT NULL AND NOT (vwap > 0 AND vwap < 'Infinity'::float8)) "
            "OR transactions < 0 OR bar_timestamp IS NULL "
            "OR (to_timestamp(bar_timestamp / 1000.0) AT TIME ZONE 'America/New_York')::date <> trade_date"
        )
    if kind == "basic":
        return (
            f"{keys} OR active IS DISTINCT FROM true OR name IS NULL OR btrim(name) = '' "
            "OR security_type IS NULL OR btrim(security_type) = '' "
            "OR primary_exchange IS NULL OR btrim(primary_exchange) = ''"
        )
    return (
        f"{keys} OR event_id IS NULL OR btrim(event_id) = '' "
        "OR split_from IS NULL OR NOT (split_from > 0 AND split_from < 'Infinity'::float8) "
        "OR split_to IS NULL OR NOT (split_to > 0 AND split_to < 'Infinity'::float8) "
        "OR adjustment_type IS NULL "
        "OR adjustment_type NOT IN ('forward_split', 'reverse_split', 'stock_dividend')"
    )


async def coverage(db, kind):
    cls = TASKS[kind]
    key = "event_id" if kind == "split" else "ticker"
    rows = await db.fetch(
        f"SELECT {cls.date_column} AS day, count(*) AS rows, "
        f"count(*) - count(DISTINCT {key}) AS duplicate_keys, "
        f"count(*) FILTER (WHERE {invalid_sql(kind)}) AS invalid_rows "
        f"FROM massive.{cls.table_name} GROUP BY 1 ORDER BY 1"
    )
    return {str(r["day"]): dict(r) for r in rows}


async def outside_fingerprints(db, start, end):
    result = {}
    for kind, cls in TASKS.items():
        row = await db.fetch_one(
            "SELECT count(*) AS rows, md5(COALESCE(string_agg("
            f"md5(to_jsonb(t)::text), '' ORDER BY {', '.join(cls.primary_keys)}), '')) AS hash "
            f"FROM massive.{cls.table_name} t WHERE {cls.date_column} NOT BETWEEN $1 AND $2",
            date.fromisoformat(start),
            date.fromisoformat(end),
        )
        result[kind] = dict(row)
    return result


def completed_dates(rows, sessions):
    return {
        day
        for day, r in rows.items()
        if day in sessions
        and r["rows"] >= 1000
        and r["invalid_rows"] == 0
        and r["duplicate_keys"] == 0
    }


async def preflight(db):
    identity = dict(
        await db.fetch_one(
            "SELECT current_database() AS database, inet_server_addr()::text AS address, "
            "inet_server_port() AS port, current_setting('data_directory') AS data_directory"
        )
    )
    for cls in TASKS.values():
        relation = await db.fetch_one(
            "SELECT relkind::text AS relkind FROM pg_class WHERE oid = to_regclass($1)",
            f"massive.{cls.table_name}",
        )
        if not relation or relation["relkind"] != "r":
            raise RuntimeError(f"migration_required: massive.{cls.table_name}")
        task = cls(db)
        await db.ensure_table_schema_compatible(task)
        await task._create_rawdata_view_if_needed()
    writers = await db.fetch(
        "SELECT pid FROM pg_stat_activity WHERE pid <> pg_backend_pid() "
        "AND state <> 'idle' AND query ILIKE '%massive.%' "
        "AND query NOT ILIKE '%pg_stat_activity%'"
    )
    if writers:
        raise RuntimeError(
            "Other active Massive database sessions; retry after they finish"
        )
    local_data = Path(identity["data_directory"])
    identity["disk_free_bytes"] = (
        shutil.disk_usage(local_data).free if local_data.exists() else None
    )
    if (
        identity["disk_free_bytes"] is not None
        and identity["disk_free_bytes"] < 10 * 1024**3
    ):
        raise RuntimeError("Database volume has less than 10 GiB free")
    return identity


def identity_hash(identity):
    return digest({k: v for k, v in identity.items() if k != "disk_free_bytes"})


async def make_plan(db, args):
    identity = await preflight(db)
    daily = MassiveStockUsDailyTask(db)
    start = (
        date.fromisoformat(args.start_date)
        if args.start_date
        else daily.history_floor()
    )
    end = (
        date.fromisoformat(args.end_date)
        if args.end_date
        else daily.latest_complete_session()
    )
    if (
        start < daily.history_floor()
        or end > daily.latest_complete_session()
        or start > end
    ):
        raise RuntimeError("Requested window is outside completed, entitled history")
    sessions = [d.isoformat() for d in daily.sessions(start, end)]
    if not sessions:
        raise RuntimeError("No completed US sessions in requested window")
    initial = {kind: await coverage(db, kind) for kind in TASKS}
    plan = {
        "version": 1,
        "created_at": now(),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "sessions": sessions,
        "database": identity,
        "database_fingerprint": identity_hash(identity),
        "code_hashes": code_hashes(),
        "initial_coverage": initial,
        "outside_window": await outside_fingerprints(
            db, start.isoformat(), end.isoformat()
        ),
        "repository_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "repository_status": subprocess.check_output(
            ["git", "status", "--short"], cwd=ROOT, text=True
        ),
        "interpreter": sys.executable,
        "request_interval_seconds": 13,
        "order": "splits, oldest missing security snapshot, daily bars, remaining security snapshots",
    }
    plan["plan_hash"] = digest(plan)
    return plan


def validate_plan(plan, expected_hash):
    unsigned = {k: v for k, v in plan.items() if k != "plan_hash"}
    if (
        not expected_hash
        or digest(unsigned) != plan.get("plan_hash")
        or expected_hash != plan["plan_hash"]
    ):
        raise RuntimeError("plan_hash_mismatch")
    if code_hashes() != plan["code_hashes"]:
        raise RuntimeError("source_changed: prepare a new plan")


async def backup_targets(db, run_dir, plan):
    manifest_path = run_dir / "backup_manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest["plan_hash"] != plan["plan_hash"]:
            raise RuntimeError("backup_plan_mismatch")
        for filename, info in manifest["files"].items():
            if (
                hashlib.sha256((run_dir / filename).read_bytes()).hexdigest()
                != info["sha256"]
            ):
                raise RuntimeError("backup_checksum_mismatch")
        return
    if (run_dir / "receipts.jsonl").exists():
        raise RuntimeError("Backup missing after collection has begun")
    current = {kind: await coverage(db, kind) for kind in TASKS}
    if digest(current) != digest(plan["initial_coverage"]):
        raise RuntimeError("target_coverage_changed_since_preview")
    if shutil.disk_usage(run_dir).free < 1024**3:
        raise RuntimeError("Insufficient free space for selective backup")
    manifest = {"plan_hash": plan["plan_hash"], "created_at": now(), "files": {}}
    async with db.pool.acquire() as conn:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            for cls in TASKS.values():
                filename = f"before_{cls.table_name}.csv.gz"
                with gzip.open(run_dir / filename, "wb") as output:
                    copied = await conn.copy_from_query(
                        f'SELECT * FROM massive.{cls.table_name} ORDER BY {", ".join(cls.primary_keys)}',
                        output=output,
                        format="csv",
                        header=True,
                    )
                manifest["files"][filename] = {
                    "copy_result": copied,
                    "sha256": hashlib.sha256(
                        (run_dir / filename).read_bytes()
                    ).hexdigest(),
                }
    write_json(manifest_path, manifest)


async def collect_one(db, kind, start, end):
    task = TASKS[kind](
        db,
        update_type=UpdateTypes.MANUAL,
        start_date=start.replace("-", ""),
        end_date=end.replace("-", ""),
    )

    class PaginationProgress:
        def info(self, message, *values):
            # Only the client's page/count progress, never URLs or credentials.
            print(
                json.dumps(
                    {
                        "at": now(),
                        "kind": kind,
                        "start": start,
                        "progress": message % values,
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )

    task.api.logger = PaginationProgress()
    result = await task.execute()
    empty_split = (
        kind == "split"
        and result.get("status") == "no_data"
        and getattr(task, "_smart_skip_reason", None)
        == "Massive 成功返回该窗口无拆股事件"
    )
    if result.get("status") != "success" and not empty_split:
        raise RuntimeError(f"{task.name} failed: {result}")
    count = await db.fetch_val(
        f"SELECT count(*) FROM massive.{task.table_name} "
        f"WHERE {task.date_column} BETWEEN $1 AND $2",
        date.fromisoformat(start),
        date.fromisoformat(end),
    )
    if kind != "split" and (count < 1000 or count != result["rows"]):
        raise RuntimeError(f"Committed row count mismatch: {task.name}, {start}")
    receipt = {
        "kind": kind,
        "start": start,
        "end": end,
        "rows": count,
        "finished_at": now(),
        "result": result,
    }
    if kind == "basic":
        receipt["unknown_security_types"] = await db.fetch_val(
            "SELECT count(*) FROM massive.stock_us_basic "
            "WHERE snapshot_date=$1 AND security_type='UNKNOWN'",
            date.fromisoformat(start),
        )
    return receipt


async def audit(db, plan):
    result = {"as_of": now(), "plan_hash": plan["plan_hash"], "tables": {}}
    failures = []
    for kind, cls in TASKS.items():
        rows = await coverage(db, kind)
        missing = (
            []
            if kind == "split"
            else sorted(set(plan["sessions"]) - completed_dates(rows, plan["sessions"]))
        )
        invalid = sum(r["invalid_rows"] + r["duplicate_keys"] for r in rows.values())
        source_count = sum(r["rows"] for r in rows.values())
        view_count = await db.fetch_val(
            f"SELECT count(*) FROM rawdata.{cls.table_name}"
        )
        result["tables"][kind] = {
            "rows": source_count,
            "view_rows": view_count,
            "missing_sessions": missing,
            "invalid_or_duplicate_rows": invalid,
            "per_date": rows,
        }
        if missing or invalid or view_count != source_count:
            failures.append(kind)
        # Only source tables owned by this run; no broad maintenance.
        await db.execute(f"ANALYZE massive.{cls.table_name}")
    pairs = await db.fetch(
        "SELECT b.snapshot_date, count(*) AS common_stocks, "
        "count(d.ticker) AS common_stocks_with_bars "
        "FROM massive.stock_us_basic b LEFT JOIN massive.stock_us_daily d "
        "ON d.ticker=b.ticker AND d.trade_date=b.snapshot_date "
        "WHERE b.security_type='CS' AND b.snapshot_date BETWEEN $1 AND $2 "
        "GROUP BY 1 ORDER BY 1",
        date.fromisoformat(plan["sessions"][0]),
        date.fromisoformat(plan["end_date"]),
    )
    result["common_stock_coverage"] = [dict(r) for r in pairs]
    unknown = await db.fetch(
        "SELECT snapshot_date, count(*) AS unknown_security_types "
        "FROM massive.stock_us_basic WHERE security_type='UNKNOWN' "
        "AND snapshot_date BETWEEN $1 AND $2 GROUP BY 1 ORDER BY 1",
        date.fromisoformat(plan["sessions"][0]),
        date.fromisoformat(plan["end_date"]),
    )
    result["unknown_security_types"] = [dict(r) for r in unknown]
    result["outside_window"] = await outside_fingerprints(
        db, plan["start_date"], plan["end_date"]
    )
    if result["outside_window"] != plan["outside_window"]:
        failures.append("outside_window_changed")
    result["failures"] = failures
    result["status"] = "passed" if not failures else "failed"
    return result


async def execute_plan(db, plan, run_dir):
    identity = await preflight(db)
    if identity_hash(identity) != plan["database_fingerprint"]:
        raise RuntimeError("database_identity_changed")
    state = {
        "status": "running",
        "pid": os.getpid(),
        "started_at": now(),
        "plan_hash": plan["plan_hash"],
        "total_sessions": len(plan["sessions"]),
        "start_date": plan["start_date"],
        "end_date": plan["end_date"],
        "phase": "backup",
        "completed": {},
    }
    status_path = run_dir / "status.json"

    def checkpoint():
        state["updated_at"] = now()
        write_json(status_path, state)

    async def heartbeat():
        while True:
            checkpoint()
            await asyncio.sleep(30)

    # A session lock excludes another backfill runner, including another folder.
    async with db.pool.acquire() as lock:
        locked = await lock.fetchval(
            "SELECT pg_try_advisory_lock(hashtext($1))", LOCK_NAME
        )
        if not locked:
            raise RuntimeError("another_massive_backfill_is_running")
        pulse = asyncio.create_task(heartbeat())
        try:
            await backup_targets(db, run_dir, plan)
            current = {k: await coverage(db, k) for k in ("daily", "basic")}
            done = {k: completed_dates(v, plan["sessions"]) for k, v in current.items()}
            state["completed"] = {k: len(v) for k, v in done.items()}

            async def collect(kind, start, end):
                state.update(phase=kind, current_start=start, current_end=end)
                checkpoint()
                receipt = await collect_one(db, kind, start, end)
                with (run_dir / "receipts.jsonl").open("a", encoding="utf-8") as output:
                    output.write(
                        json.dumps(receipt, ensure_ascii=False, default=str) + "\n"
                    )
                    output.flush()
                    os.fsync(output.fileno())
                if kind in done:
                    done[kind].add(start)
                    state["completed"][kind] = len(done[kind])
                state["last_receipt"] = receipt
                checkpoint()

            split_receipt = run_dir / "split_completed.json"
            if split_receipt.exists():
                saved = json.loads(split_receipt.read_text(encoding="utf-8"))
                if saved.get("plan_hash") != plan["plan_hash"]:
                    raise RuntimeError("split_receipt_plan_mismatch")
                current_split = await coverage(db, "split")
                if digest(current_split) != saved["coverage_hash"]:
                    raise RuntimeError("split_data_changed_since_receipt")
            else:
                await collect("split", plan["start_date"], plan["end_date"])
                write_json(
                    split_receipt,
                    {
                        "plan_hash": plan["plan_hash"],
                        "coverage_hash": digest(await coverage(db, "split")),
                        "finished_at": now(),
                    },
                )
            # Probe historical classifications early, before the long daily phase.
            oldest_basic = next(
                (d for d in plan["sessions"] if d not in done["basic"]), None
            )
            if oldest_basic:
                await collect("basic", oldest_basic, oldest_basic)
            for kind in ("daily", "basic"):
                for day in plan["sessions"]:
                    if day not in done[kind]:
                        await collect(kind, day, day)
            state["phase"] = "audit"
            checkpoint()
            report = await audit(db, plan)
            write_json(run_dir / "audit.json", report)
            if report["status"] != "passed":
                raise RuntimeError("final_audit_failed")
            state.update(status="completed", phase="completed", finished_at=now())
        except BaseException as exc:
            state.update(
                status="failed", error_type=type(exc).__name__, finished_at=now()
            )
            # Collector error messages are sanitized by MassiveAPI; arbitrary
            # connection exception strings must not expose connection credentials.
            if type(exc) is RuntimeError:
                state["error"] = str(exc)
            raise
        finally:
            pulse.cancel()
            try:
                await pulse
            except asyncio.CancelledError:
                pass
            checkpoint()
            await lock.execute("SELECT pg_advisory_unlock(hashtext($1))", LOCK_NAME)


async def main(args):
    run_dir = Path(args.run_dir).resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    plan_path = run_dir / "plan.json"
    plan = None
    if args.execute:
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        validate_plan(plan, args.expected_plan_hash)
    elif plan_path.exists():
        raise RuntimeError(
            "Plan already exists; use another run directory for a new preview"
        )
    db = DBManager(get_database_url())
    await db.connect()
    try:
        if args.execute:
            await execute_plan(db, plan, run_dir)
        else:
            plan = await make_plan(db, args)
            write_json(plan_path, plan)
            print(
                json.dumps(
                    {
                        "plan_hash": plan["plan_hash"],
                        "sessions": len(plan["sessions"]),
                        "start": plan["start_date"],
                        "end": plan["end_date"],
                        "run_dir": str(run_dir),
                    },
                    ensure_ascii=False,
                )
            )
    finally:
        await db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--expected-plan-hash")
    args = parser.parse_args()
    # Keep progress in structured local files. Avoid unrelated framework logs
    # containing connection exceptions or verbose per-day progress displays.
    logging.disable(logging.CRITICAL)
    try:
        asyncio.run(main(args))
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "error_type": type(exc).__name__,
                    "error": (
                        str(exc) if type(exc) is RuntimeError else "See status.json"
                    ),
                },
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        sys.exit(1)
