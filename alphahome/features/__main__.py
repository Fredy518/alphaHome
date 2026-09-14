"""Explicit Features planning, refresh and object creation commands."""

import argparse
import asyncio
import json
import os
from pathlib import Path

from alphahome.common.run_models import canonical_json
from .coordinator import FeatureCoordinator, execute_feature_request


def main(argv=None):
    parser = argparse.ArgumentParser(description="Features domain entrypoint; schema prints SQL without executing it")
    parser.add_argument("operation", choices=("list", "schema", "refresh", "create"))
    parser.add_argument("--task", action="append", default=[])
    parser.add_argument("--strategy", choices=("default", "full", "incremental", "concurrent"), default="default")
    parser.add_argument("--as-of-date")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--plan-file", type=Path)
    parser.add_argument("--expected-plan-hash")
    parser.add_argument("--allow-blocking-fallback", action="store_true")
    args = parser.parse_args(argv)
    if args.operation == "list":
        from .registry import FeatureRegistry
        print(canonical_json([{ "name": cls.name, "target": cls().full_name,
                                "sources": cls.source_tables, "strategies": cls.supported_strategies}
                              for cls in FeatureRegistry.discover()]))
        return 0
    if args.operation == "schema":
        from .storage.database_init import CREATE_SCHEMA_SQL, CREATE_MV_METADATA_TABLE_SQL, CREATE_MV_REFRESH_LOG_TABLE_SQL
        print("BEGIN;\nSET LOCAL lock_timeout = '5s';\n" + "\n".join((CREATE_SCHEMA_SQL, CREATE_MV_METADATA_TABLE_SQL, CREATE_MV_REFRESH_LOG_TABLE_SQL)) + "\nCOMMIT;")
        return 0
    if not args.task:
        parser.error("refresh/create require at least one --task")

    async def execute():
        from alphahome.common.config_manager import get_database_url
        from alphahome.common.db_manager import DBManager
        connection_string = os.getenv("ALPHAHOME_DATABASE_URL") or get_database_url()
        if not connection_string:
            raise ValueError("An explicit database target is required")
        db = DBManager(connection_string, mode="async")
        try:
            if args.dry_run:
                plan = await FeatureCoordinator(db).plan(args.task, args.strategy, operation=args.operation,
                    as_of_date=args.as_of_date, allow_blocking_fallback=args.allow_blocking_fallback)
                print(canonical_json(plan.to_dict()))
                return 1 if plan.blockers else 0
            submitted = json.loads(args.plan_file.read_text(encoding="utf-8")) if args.plan_file else None
            result = await execute_feature_request(db, args.task, args.strategy, operation=args.operation,
                submitted_plan=submitted, expected_plan_hash=args.expected_plan_hash, as_of_date=args.as_of_date,
                allow_blocking_fallback=args.allow_blocking_fallback)
            print(canonical_json(result))
            return 0 if result["status"] == "success" else 1
        finally:
            await db.close()

    try:
        return asyncio.run(execute())
    except Exception as error:
        # Connection exceptions may contain connection strings. Keep stdout credential-free.
        print(canonical_json({"status": "error", "error_type": type(error).__name__}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
