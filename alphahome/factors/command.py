"""Command-line interface for governed factor operations."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from typing import Any, Iterable, List, Optional
from datetime import date

from alphahome.common.config_manager import ConfigManager
from alphahome.common.db_manager import DBManager

from .audit_service import FactorAuditService
from .coordinator import FactorCoordinator
from .governance import FactorGovernanceStore, MIGRATION_HINT
from .repair import FactorRepairService


def _task_names(values: Iterable[str]) -> List[str]:
    mapping = {"p": "factor_p", "g": "factor_g"}
    return [mapping.get(value.lower(), value) for value in values]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m alphahome.factors")
    subparsers = parser.add_subparsers(dest="command", required=True)

    schema = subparsers.add_parser("schema", help="只读检查因子治理结构；显式--apply建表")
    schema.add_argument("--apply", action="store_true", help="执行治理表DDL（维护操作）")

    run = subparsers.add_parser("run", help="运行或预览P/G因子任务")
    run.add_argument("--tasks", nargs="+", default=["p", "g"])
    run.add_argument("--mode", choices=["smart", "manual", "full"], default="smart")
    run.add_argument("--start-date")
    run.add_argument("--end-date")
    run.add_argument("--max-automatic-dates", type=int, default=26)
    run.add_argument("--dry-run", action="store_true")
    run.add_argument("--expected-plan-hash")
    run.add_argument("--as-of-date", type=date.fromisoformat)

    audit = subparsers.add_parser("audit", help="审计因子实时表")
    audit.add_argument("--tasks", nargs="+", default=["p", "g"])
    audit.add_argument("--no-persist", action="store_true")

    diagnose = subparsers.add_parser("diagnose", help="诊断因子日期或股票")
    diagnose.add_argument("--task", choices=["p", "g", "factor_p", "factor_g"])
    diagnose.add_argument("--date")
    diagnose.add_argument("--stock")

    repair = subparsers.add_parser("repair", help="预览或执行可回滚历史修复")
    repair.add_argument("--cutoff-date")
    repair.add_argument("--apply", action="store_true")
    repair.add_argument("--rollback")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    db_url = os.environ.get("ALPHAHOME_DATABASE_URL") or ConfigManager().get_database_url()
    if not db_url:
        raise SystemExit("数据库连接未配置")
    if args.command == "schema":
        db = DBManager(db_url, mode="sync")
        try:
            store = FactorGovernanceStore(db)
            if args.apply:
                store.ensure_schema()
            issues = store.schema_issues()
            _print_json({
                "status": "migration_required" if issues else "ready",
                "applied": args.apply,
                "issues": issues,
                "message": MIGRATION_HINT if issues else "治理表列契约检查通过",
            })
            return 2 if issues else 0
        finally:
            db.close_sync()
    if args.command == "run":
        db = DBManager(db_url, mode="sync")
        try:
            coordinator = FactorCoordinator(
                db, max_automatic_dates=args.max_automatic_dates
            )
            if args.dry_run:
                payload = coordinator.plan(
                    _task_names(args.tasks),
                    mode=args.mode,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    batch_started_at=args.as_of_date,
                ).to_dict()
            else:
                payload = coordinator.run(
                    _task_names(args.tasks),
                    mode=args.mode,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    batch_started_at=args.as_of_date,
                    expected_plan_hash=args.expected_plan_hash,
                ).to_dict()
            _print_json(payload)
            return 0 if payload.get("status") in {"ready", "success"} else 2
        finally:
            db.close_sync()
    if args.command == "repair":
        db = DBManager(db_url, mode="sync")
        try:
            service = FactorRepairService(db)
            if args.rollback:
                payload = service.rollback(args.rollback)
            elif args.apply:
                payload = service.apply(args.cutoff_date)
            else:
                payload = service.plan(args.cutoff_date)
            _print_json(payload)
            return 0
        finally:
            db.close_sync()
    return asyncio.run(_run_async_command(db_url, args))


async def _run_async_command(db_url: str, args: argparse.Namespace) -> int:
    db = DBManager(db_url, mode="async")
    await db.connect()
    try:
        service = FactorAuditService(db)
        if args.command == "audit":
            selected = set(_task_names(args.tasks))
            results = [
                item
                for item in await service.audit_all(persist=not args.no_persist)
                if item["task_name"] in selected
            ]
            _print_json(results)
            return 0 if all(item.get("status") == "healthy" for item in results) else 2
        if args.command == "diagnose":
            if bool(args.stock) == bool(args.date):
                raise SystemExit("diagnose必须且只能提供--stock或--date")
            if args.stock:
                payload = await service.diagnose_stock(args.stock)
            else:
                if not args.task:
                    raise SystemExit("日期诊断必须提供--task")
                payload = await service.diagnose_date(
                    _task_names([args.task])[0], args.date
                )
            _print_json(payload)
            return 0
        raise SystemExit(f"未知命令: {args.command}")
    finally:
        await db.close()


def _print_json(payload: Any) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))


__all__ = ["build_parser", "main"]
