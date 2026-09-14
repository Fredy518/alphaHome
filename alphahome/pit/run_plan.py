"""One immutable PIT plan shared by GUI, CLI and compatibility coordinators."""

from datetime import date, timedelta
import inspect
from pathlib import Path

from alphahome.common.db_session import owned_sync_session, query_timeout
from alphahome.common.plan_inspection import inspect_relation, observed_relation_boundary, qualified_relation, package_fingerprint
from alphahome.common.run_models import RunPlan, RunRequest, RunUnit, canonical_json, target_fingerprint
from .base.monthly_snapshot_manager import PITMonthlySnapshotManager
from .base.pit_config import PITConfig
from .planning_time import business_date, frozen_pit_time


def build_pit_plan(connection_string, task_names, mode, *, cutoff=None, start_date=None, end_date=None):
    from .pit_data_update_production import PITDataUpdateCoordinator

    cutoff = date.fromisoformat(cutoff) if isinstance(cutoff, str) else cutoff or business_date()
    start_date = date.fromisoformat(start_date) if isinstance(start_date, str) else start_date
    end_date = date.fromisoformat(end_date) if isinstance(end_date, str) else end_date
    if cutoff > business_date() or (end_date and end_date > cutoff):
        raise ValueError("PIT plan cannot exceed the frozen current business date")
    mode = {"smart": "incremental", "full": "full_backfill", "manual": "manual_range"}.get(mode, mode)
    if mode not in {"incremental", "full_backfill", "manual_range"}:
        raise ValueError("Unsupported PIT plan mode")
    if mode == "manual_range" and not (start_date and end_date):
        raise ValueError("Manual PIT plans require both dates")
    contracts = PITDataUpdateCoordinator._registered_contracts()
    selected = PITDataUpdateCoordinator._expand_dependency_closure(task_names, contracts)
    layers = PITDataUpdateCoordinator._topological_layers(selected, contracts)
    request = RunRequest("pit", tuple(task_names), mode, target_fingerprint(connection_string), start_date, end_date)
    units, blockers, structures, boundaries = [], [], {}, {}
    with owned_sync_session(connection_string, readonly=True) as db, query_timeout(db), frozen_pit_time(cutoff):
        # A strict snapshot token also invalidates a preview after a late transaction
        # commits. It may conservatively invalidate after an unrelated DB write.
        snapshot = db.fetch_val_sync("SELECT pg_current_snapshot()::text")
        for name in [item for layer in layers for item in layer]:
            contract = contracts[name]
            if mode not in contract.supported_modes:
                blockers.append(f"{name}: unsupported mode {mode}")
            for relation in (contract.output_table, *contract.source_tables):
                if relation not in structures:
                    structures[relation] = inspect_relation(db, relation)
                    if not structures[relation]["columns"]:
                        blockers.append(f"migration_required: {relation} is missing")
                    boundaries[relation] = observed_relation_boundary(db, relation, structures[relation])
            dates, start, end, parameters, replacement_count = (), start_date, end_date or cutoff, {}, None
            manager = contract.resolve_manager_class()().bind_database(db_manager=db)
            if all(structures[relation]["columns"] for relation in (contract.output_table, *contract.source_tables)):
                with manager:
                    manager._ensure_table_exists()
                    manager._require_unique_keys(contract.primary_keys)
                    if isinstance(manager, PITMonthlySnapshotManager):
                        if mode == "incremental":
                            dates = tuple(manager.plan_incremental_months(cutoff_date=cutoff.replace(day=1)-timedelta(days=1)))
                        else:
                            dates = tuple(manager.plan_backfill_months(start_date=start, end_date=end))
                        parameters["planned_months"] = [value.isoformat() for value in dates]
                        start, end = (dates[0], dates[-1]) if dates else (None, None)
                    elif contract.pit_time_key == "obs_date":
                        complete = PITMonthlySnapshotManager.latest_complete_month(cutoff)
                        end = min(end, complete)
                        if mode == "incremental":
                            start = PITMonthlySnapshotManager.incremental_months(3, end_date=end)[0].replace(day=1)
                            changes = manager._detect_industry_changes(start.isoformat(), end_date=end)
                            dates = tuple(manager._get_affected_months(start.isoformat(), cutoff_date=end)) if changes['has_changes'] else ()
                        else:
                            start = start or date.fromisoformat(PITConfig.DEFAULT_DATE_RANGES["backfill_start"])
                            dates = tuple(manager._find_missing_months(start.isoformat(), end.isoformat()))
                        dates = tuple(manager._get_month_end_date(date.fromisoformat(str(value)[:10])) for value in dates)
                        parameters["planned_months"] = [value.isoformat() for value in dates]
                    else:
                        if mode == "incremental":
                            default_days = inspect.signature(manager.incremental_update).parameters.get("days")
                            days = default_days.default if default_days else None
                            days = days if isinstance(days, int) else None
                            start_s, end_s = manager.plan_incremental_range(days)
                            start, end = date.fromisoformat(start_s), date.fromisoformat(end_s)
                            parameters["planned_date_range"] = [start.isoformat(), end.isoformat()]
                        else:
                            default_start = getattr(manager, "DEFAULT_FULL_START", PITConfig.DEFAULT_DATE_RANGES["backfill_start"])
                            start = start or date.fromisoformat(str(default_start)[:10])
                    if start is not None and end is not None:
                        replacement_count = int(db.fetch_val_sync(
                            f'SELECT COUNT(*) FROM {qualified_relation(contract.output_table)} WHERE "{contract.pit_time_key}" BETWEEN %s AND %s', (start, end),
                        ))
            units.append(RunUnit(name, dates, tuple(contract.dependencies), existing_rows_to_replace=replacement_count,
                                 start_date=start, end_date=end, parameters_json=canonical_json(parameters)))
    return RunPlan.build(request, units, cutoff, schema=structures,
                         sources={"planning_snapshot": snapshot, "observations": boundaries, "consumed": False},
                         config={"contracts": {name: contracts[name].to_dict() for name in sorted(selected)},
                                 "defaults": PITConfig.DEFAULT_DATE_RANGES, "implementation": package_fingerprint(Path(__file__).parent)}, blockers=blockers)
