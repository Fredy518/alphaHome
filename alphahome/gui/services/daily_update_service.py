"""User-oriented daily update planning and execution for the desktop GUI."""

from __future__ import annotations

import asyncio
import importlib.util
import sys
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional
from zoneinfo import ZoneInfo

from ...common.logging_utils import get_logger
from ...common.run_models import fingerprint
from ...common.task_system import UnifiedTaskFactory
from ...curation import etf_candidate_monthly_maintenance as candidate_maintenance
from ...features import FeatureRegistry
from ...factors import tasks as factor_tasks
from ...fetchers import tasks as fetcher_tasks
from ...pit import tasks as pit_tasks
from . import (
    factor_service,
    feature_service,
    fundpos_service,
    pit_service,
    task_registry_service,
)

logger = get_logger(__name__)

SHANGHAI = ZoneInfo("Asia/Shanghai")
GOOD_TASK_STATUSES = {"success", "no_op", "expected_no_data", "skipped"}
NON_DAILY_FEATURE_NAMES = {
    "stock_fina_indicator",
    "stock_sharefloat_schedule",
    "stock_shareholder_concentration",
}

_send_response_callback: Optional[Callable] = None
_cached_plan: Dict[str, Any] = {}
_stop_event: Optional[asyncio.Event] = None
_is_running = False


def initialize_daily_update_service(response_callback: Callable) -> None:
    global _send_response_callback
    _send_response_callback = response_callback


@lru_cache(maxsize=1)
def _load_collection_updater_class() -> type:
    """Load the production collector without relying on the process CWD.

    The installed GUI exposes only the ``alphahome`` package.  On Windows,
    ``scripts`` may instead resolve to pywin32's namespace package, so importing
    ``scripts.production`` is not a stable way to reuse the repository entrypoint.
    """

    module_path = (
        Path(__file__).resolve().parents[3]
        / "scripts"
        / "production"
        / "data_updaters"
        / "tushare"
        / "data_collection_smart_update_production.py"
    )
    if not module_path.is_file():
        raise RuntimeError(f"数据采集生产入口不存在: {module_path}")

    spec = importlib.util.spec_from_file_location(
        "_alphahome_data_collection_production",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载数据采集生产入口: {module_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        if sys.modules.get(spec.name) is module:
            sys.modules.pop(spec.name, None)
        raise
    updater_class = getattr(module, "DataCollectionProductionUpdater", None)
    if updater_class is None:
        raise RuntimeError(f"数据采集生产入口缺少更新器: {module_path}")
    return updater_class


def _as_date(value: Optional[str | date]) -> date:
    if value is None:
        return datetime.now(SHANGHAI).date()
    return date.fromisoformat(value) if isinstance(value, str) else value


async def _is_exchange_workday(db_manager: Any, day: date) -> tuple[bool, str]:
    """Resolve workday from the canonical SSE calendar, failing closed."""

    if day.weekday() >= 5:
        return False, "自然周末"
    if db_manager is None:
        return True, "交易日历不可用；按工作日处理并跳过非日常任务"
    try:
        row = await db_manager.fetch_one(
            """
            SELECT is_open
            FROM rawdata.others_calendar
            WHERE exchange = 'SSE' AND cal_date = $1
            LIMIT 1
            """,
            day,
        )
        if row is None:
            return True, "交易日历缺少当日记录；按工作日处理并跳过非日常任务"
        if isinstance(row, Mapping):
            is_open = row.get("is_open")
        else:
            try:
                is_open = row["is_open"]
            except (KeyError, TypeError):
                is_open = row[0]
        return bool(int(is_open or 0)), "rawdata.others_calendar（SSE）"
    except Exception as exc:  # noqa: BLE001 - safe fallback is part of policy
        logger.warning("读取交易日历失败，按工作日安全降级: %s", exc)
        return True, "交易日历查询失败；按工作日处理并跳过非日常任务"


def _collection_cadence(task_class: type, task_name: str = "") -> str:
    if task_name and task_registry_service._is_hidden_from_collection_gui(
        task_name, task_class
    ):
        return "manual_only"
    explicit = getattr(task_class, "daily_update_cadence", None)
    if explicit in {"daily", "non_workday", "manual_only"}:
        return explicit
    interval = getattr(task_class, "smart_refresh_interval_days", None)
    try:
        return "non_workday" if interval is not None and int(interval) > 1 else "daily"
    except (TypeError, ValueError):
        return "daily"


def _pit_cadence(task_class: type) -> str:
    explicit = getattr(task_class, "daily_update_cadence", None)
    if explicit in {"daily", "non_workday"}:
        return explicit
    contract = getattr(task_class, "contract", None)
    return (
        "non_workday"
        if getattr(contract, "pit_time_key", None) == "obs_date"
        else "daily"
    )


def _pit_cadences(task_classes: Mapping[str, type]) -> Dict[str, str]:
    """Propagate low-frequency cadence through PIT dependencies."""

    cadences = {
        name: _pit_cadence(task_class) for name, task_class in task_classes.items()
    }
    changed = True
    while changed:
        changed = False
        for name, task_class in task_classes.items():
            if cadences[name] == "non_workday":
                continue
            contract = getattr(task_class, "contract", None)
            dependencies = tuple(getattr(contract, "dependencies", ()) or ())
            if any(
                cadences.get(dependency) == "non_workday" for dependency in dependencies
            ):
                cadences[name] = "non_workday"
                changed = True
    return cadences


def _feature_cadence(recipe_class: type) -> str:
    explicit = getattr(recipe_class, "daily_update_cadence", None)
    if explicit in {"daily", "non_workday"}:
        return explicit
    name = str(getattr(recipe_class, "name", "")).lower()
    if name in NON_DAILY_FEATURE_NAMES or any(
        token in name for token in ("_weekly", "_monthly", "_quarterly")
    ):
        return "non_workday"
    supported = tuple(getattr(recipe_class, "supported_strategies", ()))
    if getattr(recipe_class, "is_python_feature", False) and supported == ("full",):
        return "non_workday"
    return "daily"


def _feature_cadences(recipe_classes: Iterable[type]) -> Dict[str, str]:
    """Propagate low-frequency cadence through Feature output dependencies."""

    classes = {str(recipe.name): recipe for recipe in recipe_classes}
    cadences = {name: _feature_cadence(recipe) for name, recipe in classes.items()}
    output_owners: Dict[str, str] = {}
    dependencies: Dict[str, tuple[str, ...]] = {}
    for name, recipe_class in classes.items():
        recipe = recipe_class()
        output = getattr(recipe, "full_name", None) or f"features.{name}"
        output_owners[str(output)] = name
        dependencies[name] = tuple(
            str(source) for source in (getattr(recipe, "source_tables", ()) or ())
        )

    changed = True
    while changed:
        changed = False
        for name, sources in dependencies.items():
            if cadences[name] == "non_workday":
                continue
            if any(
                cadences.get(output_owners.get(source, "")) == "non_workday"
                for source in sources
            ):
                cadences[name] = "non_workday"
                changed = True
    return cadences


def _make_group(
    *,
    key: str,
    label: str,
    order: int,
    items: Iterable[tuple[str, str]],
    is_workday: bool,
    execution_mode: str,
    description: str,
    depends_on: Iterable[str] = (),
) -> Dict[str, Any]:
    ordered = sorted(items)
    selected = [
        name
        for name, cadence in ordered
        if cadence == "daily" or (cadence == "non_workday" and not is_workday)
    ]
    skipped = [name for name, _cadence in ordered if name not in selected]
    manual_only = [name for name, cadence in ordered if cadence == "manual_only"]
    if selected and skipped:
        action = f"更新 {len(selected)}，自动跳过 {len(skipped)}"
    elif selected:
        action = f"更新 {len(selected)}"
    else:
        action = f"自动跳过 {len(skipped)}"
    return {
        "key": key,
        "label": label,
        "order": order,
        "task_count": len(ordered),
        "run_count": len(selected),
        "skip_count": len(skipped),
        "task_names": selected,
        "skipped_task_names": skipped,
        "manual_only_task_names": manual_only,
        "execution_mode": execution_mode,
        "schedule": "每日；低频仅非工作日；手工任务始终跳过",
        "action": action,
        "description": description,
        "depends_on": list(depends_on),
        "status": "ready" if selected else "skipped_policy",
        "result": None,
    }


def _database_url(db_manager: Any) -> str | None:
    value = getattr(db_manager, "connection_string", None)
    return str(value) if value else None


async def _build_candidate_monthly_group(
    db_manager: Any,
    day: date,
    *,
    order: int,
) -> Dict[str, Any]:
    """Build the ETF candidate row without exposing credentials to the GUI."""

    task_name = candidate_maintenance.MONTHLY_TASK_NAME
    base = {
        "key": "etf_candidate_monthly",
        "label": "ETF候选池月度维护",
        "order": order,
        "task_count": 1,
        "execution_mode": "AlphaHome 内部计算 + DeepSeek确认",
        "schedule": "每月5日起检查；每月最多成功一次",
        "description": (
            "在产品事实刷新后重算候选快照；保护人工确认/拒绝，"
            "AI标签不赋予资金或下单权限"
        ),
        "depends_on": ["features"],
        "manual_only_task_names": [],
        "result": None,
    }
    database_url = _database_url(db_manager)
    if not database_url:
        return {
            **base,
            "run_count": 0,
            "skip_count": 0,
            "task_names": [],
            "skipped_task_names": [],
            "action": "配置阻断：数据库连接不可用",
            "status": "blocked",
        }

    try:
        preview = await asyncio.to_thread(
            candidate_maintenance.build_candidate_monthly_maintenance_plan,
            database_url,
            run_date=day,
        )
    except Exception as exc:  # noqa: BLE001 - isolate this optional monthly domain
        logger.error("生成 ETF 候选池月度计划失败: %s", exc, exc_info=True)
        return {
            **base,
            "run_count": 0,
            "skip_count": 0,
            "task_names": [],
            "skipped_task_names": [],
            "action": f"配置阻断：{exc}",
            "description": f"{base['description']}；计划生成失败：{exc}",
            "status": "blocked",
        }

    preview_status = preview.get("status")
    common = {
        **base,
        "candidate_plan": preview,
        "plan_hash": preview.get("plan_hash"),
    }
    if preview_status == "deferred_before_monthly_window":
        return {
            **common,
            "run_count": 0,
            "skip_count": 1,
            "task_names": [],
            "skipped_task_names": [task_name],
            "action": f"本月{preview['not_before_day']}日前，按规则跳过",
            "status": "skipped_policy",
        }
    if preview_status == "skipped_already_succeeded":
        return {
            **common,
            "run_count": 0,
            "skip_count": 1,
            "task_names": [],
            "skipped_task_names": [task_name],
            "action": (
                "本月已成功维护，自动跳过"
                + (
                    f"（{preview.get('output_snapshot_id')}）"
                    if preview.get("output_snapshot_id")
                    else ""
                )
            ),
            "status": "skipped_policy",
        }
    if preview_status == "blocked_missing_api_key":
        return {
            **common,
            "run_count": 0,
            "skip_count": 0,
            "task_names": [],
            "skipped_task_names": [],
            "action": "配置阻断：需要 DEEPSEEK_API_KEY",
            "description": f"{base['description']}；当前有模型目标但未读取到密钥",
            "status": "blocked",
        }

    target_count = int(preview.get("llm_target_count") or 0)
    new_count = int(preview.get("new_product_count") or 0)
    if preview_status == "ready_after_upstream_refresh":
        action = "上游刷新后重算门禁并执行月度维护"
    else:
        action = f"月度维护待执行：模型目标 {target_count}，新产品 {new_count}"
    return {
        **common,
        "run_count": 1,
        "skip_count": 0,
        "task_names": [task_name],
        "skipped_task_names": [],
        "action": action,
        "status": "ready",
    }


async def build_daily_update_plan(
    db_manager: Any, as_of_date: Optional[str | date] = None
) -> Dict[str, Any]:
    """Build a read-only policy plan without pre-planning downstream DB writes."""

    day = _as_date(as_of_date)
    today = datetime.now(SHANGHAI).date()
    if day > today:
        raise ValueError("日常更新日期不能晚于今天")

    fetcher_tasks.discover_tasks()
    pit_tasks.discover_tasks()
    factor_tasks.discover_tasks()
    recipes = FeatureRegistry.discover()
    feature_cadence = _feature_cadences(recipes)
    is_workday, calendar_source = await _is_exchange_workday(db_manager, day)
    pit_task_classes = UnifiedTaskFactory.get_tasks_by_type("pit")
    pit_cadence = _pit_cadences(pit_task_classes)
    fetch_task_classes = UnifiedTaskFactory.get_tasks_by_type("fetch")
    processor_task_classes = UnifiedTaskFactory.get_tasks_by_type("processor")

    groups = [
        _make_group(
            key="collection",
            label="数据采集",
            order=1,
            items=[
                *(
                    (name, _collection_cadence(task_class, name))
                    for name, task_class in fetch_task_classes.items()
                ),
                *((name, "manual_only") for name in processor_task_classes),
            ],
            is_workday=is_workday,
            execution_mode="智能增量",
            description="全部已注册采集任务；隐藏、分钟线和兼容处理任务仅允许手工显式运行",
            depends_on=(),
        ),
        _make_group(
            key="pit",
            label="PIT",
            order=2,
            items=((name, pit_cadence[name]) for name in pit_task_classes),
            is_workday=is_workday,
            execution_mode="增量计划",
            description="工作日更新公告日任务；月末快照仅非工作日运行",
            depends_on=("collection",),
        ),
        _make_group(
            key="factors",
            label="因子",
            order=3,
            items=(
                (name, "non_workday")
                for name in UnifiedTaskFactory.get_tasks_by_type("factor")
            ),
            is_workday=is_workday,
            execution_mode="智能增量",
            description="P/G 周五快照在非工作日补齐",
            depends_on=("pit",),
        ),
        _make_group(
            key="features",
            label="Features",
            order=4,
            items=((recipe.name, feature_cadence[recipe.name]) for recipe in recipes),
            is_workday=is_workday,
            execution_mode="默认策略",
            description="增量型按窗口更新；全量型仅按其声明策略刷新",
            depends_on=("factors",),
        ),
    ]

    groups.append(await _build_candidate_monthly_group(db_manager, day, order=5))

    fundpos = fundpos_service.get_fundpos_snapshot()
    if fundpos.get("status") == "ready":
        groups.append(
            _make_group(
                key="fundpos",
                label="FundPos 估算",
                order=6,
                items=((row["name"], "daily") for row in fundpos.get("families", [])),
                is_workday=is_workday,
                execution_mode="影子估算",
                description="估算并勾稽入库，不更新正式发布指针",
                depends_on=("features",),
            )
        )
    else:
        groups.append(
            {
                "key": "fundpos",
                "label": "FundPos 估算",
                "order": 6,
                "task_count": 0,
                "run_count": 0,
                "skip_count": 0,
                "task_names": [],
                "skipped_task_names": [],
                "execution_mode": "影子估算",
                "schedule": "每日",
                "action": "配置阻断",
                "description": fundpos.get("error", "FundPos 配置不可用"),
                "depends_on": ["features"],
                "status": "blocked",
                "result": None,
            }
        )

    policy_payload = {
        "as_of_date": day.isoformat(),
        "is_workday": is_workday,
        "groups": [
            {
                "key": group["key"],
                "tasks": group["task_names"],
                "skipped": group["skipped_task_names"],
                "mode": group["execution_mode"],
                "depends_on": group.get("depends_on", []),
                "plan_hash": group.get("plan_hash"),
            }
            for group in groups
        ],
    }
    blocked = any(group["status"] == "blocked" for group in groups)
    return {
        "status": "ready_with_blockers" if blocked else "ready",
        "as_of_date": day.isoformat(),
        "created_at": datetime.now(SHANGHAI).isoformat(),
        "is_workday": is_workday,
        "day_type": "交易日" if is_workday else "非交易日",
        "calendar_source": calendar_source,
        "policy_hash": fingerprint(policy_payload),
        "groups": groups,
        "publication_note": (
            "ETF候选池仅研究候选且无资金/下单权限；"
            "FundPos 固定影子估算；一键更新不执行正式发布"
        ),
    }


async def _run_collection(
    db_manager: Any,
    names: List[str],
    stop_event: asyncio.Event,
    group: Dict[str, Any],
) -> Dict[str, Any]:
    DataCollectionProductionUpdater = _load_collection_updater_class()

    progress = {
        "completed": 0,
        "success": 0,
        "failed": 0,
        "skipped": 0,
        "cancelled": 0,
    }

    def report_progress(result: Dict[str, Any]) -> None:
        status = result.get("status", "unknown")
        progress["completed"] += 1
        if status in {"success", "expected_no_data"}:
            progress["success"] += 1
        elif status in {"expected_skip", "skipped", "skipped_dry_run"}:
            progress["skipped"] += 1
        elif status == "cancelled":
            progress["cancelled"] += 1
        else:
            progress["failed"] += 1
        group["progress"] = dict(progress)
        group["action"] = (
            f"已完成 {progress['completed']}/{len(names)}："
            f"成功 {progress['success']}，失败 {progress['failed']}，"
            f"跳过 {progress['skipped']}，已停止 {progress['cancelled']}"
        )
        _send("DAILY_UPDATE_STAGE_UPDATE", dict(group))

    updater = DataCollectionProductionUpdater(
        max_workers=3,
        max_retries=3,
        retry_delay=5,
        task_names=names,
        db_manager=db_manager,
        manage_factory_lifecycle=False,
        stop_event=stop_event,
        progress_callback=report_progress,
    )
    success = await updater.run_production_update()
    cancelled = stop_event.is_set() or any(
        result.get("status") == "cancelled" for result in updater.last_results
    )
    return {
        "status": "cancelled" if cancelled else "success" if success else "error",
        "total": updater.stats["total_tasks"],
        "success_count": updater.stats["successful_tasks"],
        "fail_count": updater.stats["failed_tasks"],
        "skip_count": updater.stats["skipped_tasks"],
        "cancelled_count": updater.stats.get("cancelled_tasks", 0),
        "batch_outcome": updater.batch_outcome,
    }


async def _run_pit(
    db_manager: Any,
    names: List[str],
    stop_event: asyncio.Event,
    day: date,
) -> Dict[str, Any]:
    plan = await pit_service.plan_pit_execution(
        db_manager, names, "incremental", cutoff=day.isoformat()
    )
    _log(f"PIT 执行计划 {plan.plan_hash}，任务单元 {len(plan.units)} 个")
    results = await pit_service.execute_pit_plan(
        db_manager, plan, stop_event=stop_event
    )
    cancelled = [item for item in results if item.get("status") == "cancelled"]
    failures = [
        item
        for item in results
        if item.get("status") not in GOOD_TASK_STATUSES | {"cancelled"}
    ]
    return {
        "status": (
            "cancelled"
            if cancelled
            else "success" if not failures else "partial_success"
        ),
        "plan_hash": plan.plan_hash,
        "success_count": len(results) - len(failures) - len(cancelled),
        "fail_count": len(failures),
        "cancelled_count": len(cancelled),
        "results": results,
    }


async def _run_factors(
    db_manager: Any,
    names: List[str],
    stop_event: asyncio.Event,
    day: date,
) -> Dict[str, Any]:
    return await factor_service.run_factor_execution(
        db_manager,
        names,
        "smart",
        end_date=day.isoformat(),
        stop_event=stop_event,
    )


async def _run_features(
    names: List[str], stop_event: asyncio.Event, day: date
) -> Dict[str, Any]:
    return await feature_service.handle_refresh_features(
        names,
        strategy="default",
        as_of_date=day.isoformat(),
        stop_event=stop_event,
    )


async def _run_fundpos(names: List[str], day: date) -> Dict[str, Any]:
    families = [name.split(".", 1)[1] for name in names]
    return await fundpos_service.execute_fundpos(
        mode="shadow", families=families, cutoff=day.isoformat()
    )


async def _run_candidate_monthly(
    db_manager: Any,
    stop_event: asyncio.Event,
    day: date,
) -> Dict[str, Any]:
    if stop_event.is_set():
        return {"status": "cancelled", "cancelled_count": 1}
    database_url = _database_url(db_manager)
    if not database_url:
        raise RuntimeError("AlphaHome 数据库连接不可用")
    result = await asyncio.to_thread(
        candidate_maintenance.execute_candidate_monthly_maintenance,
        database_url,
        run_date=day,
    )
    if result.get('status') in {'succeeded', 'skipped_already_succeeded'}:
        candidate_status = result['status']
        # This product depends on the newly committed candidate set, not merely
        # on the earlier Features phase. A retry can repair it without another LLM call.
        try:
            refresh = await feature_service.handle_refresh_features(
                ['etf_exposure_technical_current_universe_daily'], strategy='full',
                as_of_date=day.isoformat(), stop_event=stop_event,
            )
        except Exception as exc:
            refresh = {'status': 'error', 'error': str(exc), 'fail_count': 1}
        result = {**result, 'candidate_status': candidate_status, 'dependent_features': refresh}
        if refresh.get('status') not in GOOD_TASK_STATUSES:
            result['status'] = 'cancelled' if refresh.get('status') == 'cancelled' else 'partial_success'
    return result


async def _execute_group(
    group: Dict[str, Any],
    db_manager: Any,
    stop_event: asyncio.Event,
    day: date,
) -> Dict[str, Any]:
    names = list(group["task_names"])
    if group["key"] == "collection":
        return await _run_collection(db_manager, names, stop_event, group)
    if group["key"] == "pit":
        return await _run_pit(db_manager, names, stop_event, day)
    if group["key"] == "factors":
        return await _run_factors(db_manager, names, stop_event, day)
    if group["key"] == "features":
        return await _run_features(names, stop_event, day)
    if group["key"] == "etf_candidate_monthly":
        return await _run_candidate_monthly(db_manager, stop_event, day)
    if group["key"] == "fundpos":
        return await _run_fundpos(names, day)
    raise ValueError(f"未知日常更新域: {group['key']}")


def _result_is_success(result: Dict[str, Any]) -> bool:
    return result.get("status") in {
        "success",
        "passed",
        "no_op",
        "expected_no_data",
        "succeeded",
        "skipped_already_succeeded",
        "deferred_before_monthly_window",
    }


def _failed_dependency_labels(
    group: Dict[str, Any],
    groups_by_key: Dict[str, Dict[str, Any]],
    failed_domains: Dict[str, str],
) -> List[str]:
    """策略跳过不切断依赖链；独立分支只受自身祖先的失败影响。"""

    dependencies = group.get("depends_on")
    if dependencies is None:
        return list(failed_domains.values())
    failures: List[str] = []
    visited = set()

    def visit(key: str) -> None:
        if key in visited:
            return
        visited.add(key)
        if key in failed_domains:
            failures.append(failed_domains[key])
            return
        for dependency in groups_by_key.get(key, {}).get("depends_on") or ():
            visit(dependency)

    for key in dependencies:
        visit(key)
    return failures


async def handle_get_daily_update_plan(
    db_manager: Any, as_of_date: Optional[str] = None
) -> Dict[str, Any]:
    global _cached_plan
    try:
        _cached_plan = await build_daily_update_plan(db_manager, as_of_date)
        _send("DAILY_UPDATE_PLAN_UPDATE", _cached_plan)
        _send(
            "DAILY_UPDATE_PLAN_COMPLETE",
            {"success": True, "policy_hash": _cached_plan["policy_hash"]},
        )
        return _cached_plan
    except Exception as exc:  # noqa: BLE001 - return actionable GUI error
        logger.error("生成日常更新计划失败: %s", exc, exc_info=True)
        _send("DAILY_UPDATE_PLAN_COMPLETE", {"success": False, "error": str(exc)})
        return {"status": "error", "error": str(exc), "groups": []}


async def handle_run_daily_update(
    db_manager: Any, as_of_date: Optional[str] = None
) -> Dict[str, Any]:
    global _cached_plan, _is_running, _stop_event
    if _is_running:
        result = {"status": "busy", "error": "日常更新已经在运行"}
        _send("DAILY_UPDATE_COMPLETE", {"success": False, "result": result})
        return result

    _is_running = True
    _stop_event = asyncio.Event()
    started = datetime.now(SHANGHAI)
    completed_groups = 0
    failed_groups = 0
    failed_domains: Dict[str, str] = {}
    try:
        _cached_plan = await build_daily_update_plan(db_manager, as_of_date)
        _send("DAILY_UPDATE_PLAN_UPDATE", _cached_plan)
        failed_groups = sum(
            group.get("status") == "blocked" for group in _cached_plan["groups"]
        )
        day = date.fromisoformat(_cached_plan["as_of_date"])
        _log(
            f"一键智能增量更新开始：{day.isoformat()} " f"({_cached_plan['day_type']})"
        )

        total_groups = len(_cached_plan["groups"])
        groups_by_key = {group["key"]: group for group in _cached_plan["groups"]}
        for group in _cached_plan["groups"]:
            if group["status"] in {"blocked", "skipped_policy"}:
                if group["status"] == "blocked":
                    failed_domains[group["key"]] = group["label"]
                _send("DAILY_UPDATE_STAGE_UPDATE", group)
                continue
            if _stop_event.is_set():
                group["status"] = "cancelled"
                group["action"] = "收到停止信号，未启动"
                _send("DAILY_UPDATE_STAGE_UPDATE", group)
                continue
            dependency_failures = _failed_dependency_labels(
                group, groups_by_key, failed_domains
            )
            if dependency_failures:
                group["status"] = "blocked"
                group["blocked_by"] = dependency_failures
                group["action"] = (
                    f"前置更新域未成功（{', '.join(dependency_failures)}），"
                    "为避免使用旧数据，本域未启动"
                )
                failed_groups += 1
                failed_domains[group["key"]] = group["label"]
                _send("DAILY_UPDATE_STAGE_UPDATE", group)
                continue

            group["status"] = "running"
            group["action"] = f"正在更新 {group['run_count']} 个任务"
            _send("DAILY_UPDATE_STAGE_UPDATE", group)
            _log(f"[{group['order']}/{total_groups}] {group['label']} 开始")
            try:
                result = await _execute_group(group, db_manager, _stop_event, day)
                group["result"] = result
                if _result_is_success(result):
                    group["status"] = "success"
                    completed_groups += 1
                elif result.get("status") == "cancelled":
                    group["status"] = "cancelled"
                else:
                    group["status"] = "error"
                    failed_groups += 1
                    failed_domains[group["key"]] = group["label"]
                group["action"] = _format_result(group, result)
            except Exception as exc:  # noqa: BLE001 - continue independent domains
                logger.error("%s 更新失败: %s", group["label"], exc, exc_info=True)
                group["status"] = "error"
                group["result"] = {"status": "error", "error": str(exc)}
                group["action"] = f"失败：{exc}"
                failed_groups += 1
                failed_domains[group["key"]] = group["label"]
            _send("DAILY_UPDATE_STAGE_UPDATE", group)

        stopped = _stop_event.is_set()
        overall = (
            "cancelled"
            if stopped
            else (
                "partial_success"
                if failed_groups and completed_groups
                else "error" if failed_groups else "success"
            )
        )
        result = {
            "status": overall,
            "started_at": started.isoformat(),
            "finished_at": datetime.now(SHANGHAI).isoformat(),
            "completed_groups": completed_groups,
            "failed_groups": failed_groups,
            "policy_hash": _cached_plan["policy_hash"],
            "plan": _cached_plan,
        }
        _send(
            "DAILY_UPDATE_COMPLETE",
            {"success": overall == "success", "result": result},
        )
        return result
    except Exception as exc:  # noqa: BLE001 - always release and restore the GUI
        logger.error("一键日常更新启动失败: %s", exc, exc_info=True)
        result = {
            "status": "error",
            "started_at": started.isoformat(),
            "finished_at": datetime.now(SHANGHAI).isoformat(),
            "completed_groups": completed_groups,
            "failed_groups": max(failed_groups, 1),
            "error": str(exc),
        }
        _send("DAILY_UPDATE_COMPLETE", {"success": False, "result": result})
        return result
    finally:
        _is_running = False
        _stop_event = None


def stop_daily_update() -> None:
    if _stop_event is None:
        _log("当前没有正在运行的一键更新", level="warning")
        return
    _stop_event.set()
    _log("已发送停止信号；当前任务将在安全点结束", level="warning")


def is_daily_update_running() -> bool:
    return _is_running


def _format_result(group: Dict[str, Any], result: Dict[str, Any]) -> str:
    status = result.get("status", "unknown")
    status_label = {
        "success": "成功",
        "succeeded": "成功",
        "passed": "成功",
        "skipped_already_succeeded": "本月已完成",
        "deferred_before_monthly_window": "未到月度窗口",
        "partial_success": "部分成功",
        "error": "失败",
        "cancelled": "已停止",
        "busy": "被占用",
    }.get(status, status)
    error = result.get("error") or result.get("error_message")
    if error:
        return f"{status_label}：{str(error)[:180]}；策略跳过 {group['skip_count']}"
    success_count = result.get("success_count")
    fail_count = result.get("fail_count")
    skip_count = result.get("skip_count")
    cancelled_count = result.get("cancelled_count")
    counts = []
    if success_count is not None:
        counts.append(f"成功 {success_count}")
    if fail_count:
        counts.append(f"失败 {fail_count}")
    if skip_count:
        counts.append(f"跳过 {skip_count}")
    if cancelled_count:
        counts.append(f"未执行 {cancelled_count}")
    suffix = f"（{'，'.join(counts)}）" if counts else ""
    return f"{status_label}{suffix}；策略跳过 {group['skip_count']}"


def _log(message: str, *, level: str = "info") -> None:
    logger_method = getattr(logger, level, logger.info)
    logger_method(message)
    _send("LOG", {"level": level, "message": message})


def _send(command: str, payload: Any) -> None:
    if _send_response_callback:
        _send_response_callback(command, payload)


__all__ = [
    "build_daily_update_plan",
    "handle_get_daily_update_plan",
    "handle_run_daily_update",
    "initialize_daily_update_service",
    "is_daily_update_running",
    "stop_daily_update",
]
