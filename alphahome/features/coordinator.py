"""Plan-first Features lifecycle. Refresh never installs or replaces schema objects."""

import asyncio
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import asyncpg

from alphahome.common.async_worker import run_owned_worker
from alphahome.common.db_session import owned_sync_session, query_timeout
from alphahome.common.plan_inspection import (
    inspect_relation, observed_relation_boundary, package_fingerprint, qualified_relation,
)
from alphahome.common.run_models import RunPlan, RunRequest, RunUnit, canonical_json, fingerprint, target_fingerprint
from .registry import FeatureRegistry
from .storage.atomic import identifier, table_refresh_transaction
from .storage.incremental_view import IncrementalTableView
from .storage.python_feature import PythonFeatureTable


GOOD = {"success", "no_op", "expected_no_data"}


def _summarize_feature_results(plan_hash, expected_task_count, results):
    """Keep user cancellation separate from execution failures."""

    statuses = [item.get("status") for item in results.values()]
    good = sum(status in GOOD for status in statuses)
    cancelled = sum(status == "cancelled" for status in statuses)
    missing = max(int(expected_task_count) - len(statuses), 0)
    failed = sum(
        status not in GOOD and status != "cancelled" for status in statuses
    ) + missing
    if cancelled:
        status = "cancelled"
    elif good == expected_task_count:
        status = "success"
    elif good:
        status = "partial_success"
    else:
        status = "error"
    return {
        "status": status,
        "plan_hash": plan_hash,
        "results": results,
        "success_count": good,
        "fail_count": failed,
        "cancelled_count": cancelled,
        "source_consumption": "unverified",
    }


def _recipes():
    return {recipe.name: recipe for recipe in FeatureRegistry.discover()}


def _ordered_recipes(names, recipes):
    outputs = {}
    for name, cls in recipes.items():
        relation = cls().full_name
        if relation in outputs:
            raise ValueError(f"Multiple feature recipes own {relation}")
        outputs[relation] = name
    dependencies = {name: tuple(sorted({outputs[source] for source in cls.source_tables if source in outputs}))
                    for name, cls in recipes.items()}
    ordered, visiting = [], set()

    def visit(name):
        if name not in recipes:
            raise ValueError(f"Unknown feature recipe: {name}")
        if name in visiting:
            raise ValueError("Feature dependency cycle")
        if name in ordered:
            return
        visiting.add(name)
        for dependency in dependencies[name]:
            visit(dependency)
        visiting.remove(name)
        ordered.append(name)

    for name in sorted(set(names)):
        visit(name)
    return ordered, dependencies


def build_feature_plan(connection_string, names, strategy="default", *, operation="refresh", as_of_date=None,
                       allow_blocking_fallback=False):
    today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
    cutoff = date.fromisoformat(as_of_date) if isinstance(as_of_date, str) else as_of_date or today
    if cutoff > today:
        raise ValueError("Feature plan cutoff cannot be in the future")
    if operation not in {"refresh", "create"} or strategy not in {"default", "full", "incremental", "concurrent"}:
        raise ValueError("Unsupported feature operation or strategy")
    if operation == "create" and (strategy != "default" or allow_blocking_fallback):
        raise ValueError("Create does not accept refresh options")
    request = RunRequest("features", tuple(names), operation + ":" + strategy, target_fingerprint(connection_string), as_of_date=cutoff)
    recipes = _recipes()
    ordered, dependencies = _ordered_recipes(request.tasks, recipes)
    structures, boundaries, units, blockers = {}, {}, [], []
    targets = {recipes[name]().full_name for name in ordered}
    with owned_sync_session(connection_string, readonly=True) as db, query_timeout(db):
        snapshot = db.fetch_val_sync("SELECT pg_current_snapshot()::text")
        for relation in sorted(targets | {source for name in ordered for source in recipes[name].source_tables}
                               | {"features.mv_metadata", "features.mv_refresh_log"}):
            structures[relation] = inspect_relation(db, relation)
            boundaries[relation] = observed_relation_boundary(db, relation, structures[relation])
            if not structures[relation]["columns"] and not (operation == "create" and relation in targets):
                blockers.append(f"migration_required: {relation} is missing")
        for name in ordered:
            recipe = recipes[name]()
            table = isinstance(recipe, (IncrementalTableView, PythonFeatureTable))
            columns = structures[recipe.full_name]["columns"]
            exists = bool(columns)
            if exists and columns[0]["relkind"] != ("r" if table else "m"):
                blockers.append(f"migration_required: {recipe.full_name} has an incompatible storage kind")
            keys = tuple(recipe.primary_keys) if table else ()
            if table and exists:
                key_columns = {row["attname"]: row["attnotnull"] for row in columns}
                unique = db.fetch_val_sync("""SELECT EXISTS(SELECT 1 FROM pg_index i
                    WHERE i.indrelid=to_regclass(%s) AND i.indisunique AND i.indisvalid AND i.indimmediate
                      AND i.indpred IS NULL AND i.indexprs IS NULL
                      AND ARRAY(SELECT a.attname::text FROM unnest(i.indkey) WITH ORDINALITY k(attnum,n)
                          JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum
                          WHERE k.n<=i.indnkeyatts ORDER BY a.attname)=%s::text[])""", (recipe.full_name, sorted(keys)))
                if not keys or recipe.date_column not in keys or not all(key_columns.get(key) for key in keys) or not unique:
                    blockers.append(f"migration_required: {recipe.full_name} requires its declared NOT NULL unique key")
            actual = recipe.refresh_strategy if strategy == "default" else strategy
            start, end, count, fallback = None, None, None, None
            if operation == "refresh":
                if actual not in recipe.supported_strategies:
                    blockers.append(f"{name}: unsupported strategy {actual}")
                if table:
                    end = cutoff
                    start = date(1900, 1, 1) if actual == "full" else end - timedelta(days=recipe.incremental_days)
                    if not keys or recipe.date_column not in keys or not set(keys) <= {row["attname"] for row in columns}:
                        blockers.append(f"migration_required: {recipe.full_name} lacks its declared date/key columns")
                    elif exists:
                        where = "" if actual == "full" else f" WHERE {identifier(recipe.date_column)} BETWEEN %s AND %s"
                        count = db.fetch_val_sync(f"SELECT COUNT(*) FROM {qualified_relation(recipe.full_name)}{where}",
                                                  () if actual == "full" else (start, end))
                elif actual == "concurrent" and exists:
                    capability = db.fetch_one_sync("""SELECT relispopulated,
                        EXISTS(SELECT 1 FROM pg_index i WHERE i.indrelid=c.oid AND i.indisunique
                          AND i.indisvalid AND i.indimmediate AND i.indpred IS NULL AND i.indexprs IS NULL) AS unique_index
                        FROM pg_class c WHERE c.oid=to_regclass(%s)""", (recipe.full_name,))
                    fallback = "unpopulated" if not capability["relispopulated"] else "missing_unique_index" if not capability["unique_index"] else None
                    if fallback and not allow_blocking_fallback:
                        blockers.append(f"{name}: concurrent refresh unavailable ({fallback})")
            parameters = {"operation": operation, "target": recipe.full_name, "exists": exists,
                          "requested_strategy": actual, "effective_strategy": "full" if fallback else actual,
                          "fallback_reason": fallback, "allow_blocking_fallback": bool(allow_blocking_fallback),
                          "scope": "date_window" if start else "all_rows", "source_consumption": "unverified"}
            units.append(RunUnit(name, dependencies=dependencies[name], start_date=start, end_date=end,
                                 existing_rows_to_replace=count, parameters_json=canonical_json(parameters)))
        return RunPlan.build(request, units, cutoff, schema=structures,
                             sources={"snapshot": snapshot, "observations": boundaries},
                             config={"implementation": package_fingerprint(Path(__file__).parent),
                                     "allow_blocking_fallback": bool(allow_blocking_fallback)}, blockers=blockers)


class _CreationSession:
    """Record SQL failure even when a legacy metadata helper catches the exception."""
    def __init__(self, connection):
        self.connection = connection
        self.failed = False

    async def execute(self, query, *args):
        try:
            return await self.connection.execute(query, *args)
        except Exception:
            self.failed = True
            raise


class FeatureCoordinator:
    def __init__(self, db_manager):
        self.db = db_manager
        self.last_result = None

    async def plan(self, names, strategy="default", **options):
        return await run_owned_worker(lambda cancelled: build_feature_plan(self.db.connection_string, names, strategy, **options))

    async def _create(self, recipe, exists):
        if exists:
            return {"status": "no_op", "committed_rows": 0}
        connection = await asyncpg.connect(self.db.connection_string, command_timeout=7200)
        try:
            async with table_refresh_transaction(connection, recipe.schema, recipe.view_name):
                adapter = _CreationSession(connection)
                await adapter.execute(recipe.get_create_sql())
                for sql in recipe.get_post_create_sqls() or ():
                    if isinstance(sql, str) and sql.strip():
                        await adapter.execute(sql)
                if isinstance(recipe, (IncrementalTableView, PythonFeatureTable)):
                    if not recipe.primary_keys or recipe.date_column not in recipe.primary_keys:
                        raise ValueError("New feature table must declare a date-containing unique key")
                    target = qualified_relation(recipe.full_name)
                    for key in recipe.primary_keys:
                        await adapter.execute(f"ALTER TABLE {target} ALTER COLUMN {identifier(key)} SET NOT NULL")
                    index = identifier("ah_feature_key_" + fingerprint(recipe.full_name)[:16])
                    keys = ",".join(identifier(key) for key in recipe.primary_keys)
                    await adapter.execute(f"CREATE UNIQUE INDEX {index} ON {target} ({keys})")
                recipe.set_db_manager(adapter)
                await recipe._upsert_metadata()
                if adapter.failed:
                    raise RuntimeError("Feature creation metadata failed; all creation changes rolled back")
                count = await connection.fetchval(f"SELECT COUNT(*) FROM {qualified_relation(recipe.full_name)}")
            return {"status": "success", "committed_rows": count}
        finally:
            await connection.close()

    async def run(self, plan, *, expected_plan_hash=None, stop_event=None):
        plan = RunPlan.from_dict(plan) if isinstance(plan, dict) else plan
        if plan.request.domain != "features" or plan.request.target_fingerprint != target_fingerprint(self.db.connection_string):
            raise ValueError("Feature execution target or domain differs from the plan")
        expected = expected_plan_hash or plan.plan_hash
        plan.require_matching(expected)
        operation, strategy = plan.request.mode.split(":", 1)
        options = json.loads(plan.units[0].parameters_json)
        connection = await asyncpg.connect(self.db.connection_string, command_timeout=7200)
        results = {}
        try:
            await connection.execute("SET lock_timeout = '30s'")
            # Hold a session lock across the whole dependency chain, including commits.
            await connection.execute("SELECT pg_advisory_lock(hashtext('alphahome.features'), hashtext('pipeline'))")
            current = await self.plan(plan.request.tasks, strategy, operation=operation, as_of_date=plan.effective_cutoff,
                                      allow_blocking_fallback=options["allow_blocking_fallback"])
            current.require_matching(expected)
            recipes = _recipes()
            for unit in plan.units:
                if stop_event is not None and stop_event.is_set():
                    results[unit.task_name] = {"status": "cancelled", "committed_rows": 0}
                    continue
                if any(results[dep]["status"] not in GOOD for dep in unit.dependencies):
                    results[unit.task_name] = {"status": "blocked", "committed_rows": 0, "error_code": "dependency_failed"}
                    continue
                recipe = recipes[unit.task_name](db_manager=self.db)
                params = json.loads(unit.parameters_json)
                try:
                    if operation == "create":
                        result = await self._create(recipe, params["exists"])
                    elif isinstance(recipe, PythonFeatureTable):
                        result = await recipe._refresh_window(params["requested_strategy"], unit.start_date.strftime("%Y%m%d"), unit.end_date.strftime("%Y%m%d"))
                    elif isinstance(recipe, IncrementalTableView):
                        result = await recipe._refresh_table_window(params["requested_strategy"], unit.start_date.strftime("%Y%m%d"), unit.end_date.strftime("%Y%m%d"))
                    else:
                        result = await recipe.refresh(strategy=params["requested_strategy"], allow_blocking_fallback=params["allow_blocking_fallback"])
                    if result.get("status") not in GOOD or result.get("error_count", 0):
                        result = {**result, "status": "error"}
                    results[unit.task_name] = result
                except asyncio.CancelledError:
                    results[unit.task_name] = {"status": "cancelled", "committed_rows": None}
                    raise
                except Exception as error:
                    results[unit.task_name] = {"status": "error", "error_message": str(error), "committed_rows": None}
        except asyncio.CancelledError:
            for unit in plan.units:
                results.setdefault(unit.task_name, {"status": "cancelled", "committed_rows": 0})
            raise
        finally:
            await connection.close()  # Closing releases the pipeline lock, including on cancellation.
            self.last_result = _summarize_feature_results(
                plan.plan_hash,
                len(plan.units),
                results,
            )
        return self.last_result


async def execute_feature_request(db_manager, names, strategy="default", *, operation="refresh", submitted_plan=None,
                                  expected_plan_hash=None, as_of_date=None, allow_blocking_fallback=False,
                                  stop_event=None):
    coordinator = FeatureCoordinator(db_manager)
    plan = submitted_plan or await coordinator.plan(names, strategy, operation=operation, as_of_date=as_of_date,
                                                    allow_blocking_fallback=allow_blocking_fallback)
    plan = RunPlan.from_dict(plan) if isinstance(plan, dict) else plan
    if plan.request.tasks != tuple(sorted(set(names))) or plan.request.mode != operation + ":" + strategy:
        raise ValueError("Feature request differs from submitted plan")
    return await coordinator.run(
        plan,
        expected_plan_hash=expected_plan_hash,
        stop_event=stop_event,
    )
