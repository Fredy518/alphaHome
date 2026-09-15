"""GUI-facing FundPos discovery, status, and shadow execution service."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional

from ...common.logging_utils import get_logger
from ...integrations.fundpos import FundposProductionRunner, load_production_config

logger = get_logger(__name__)

DEFAULT_CONFIG_PATH = Path.home() / ".alphahome" / "fundpos_production.json"
FAMILY_LABELS = {
    "fixed_income_plus": "固收+",
    "enhanced_index": "指数增强",
    "convertible_dominant": "转债主导",
}

_send_response_callback: Optional[Callable] = None
_is_running = False


def initialize_fundpos_service(response_callback: Callable) -> None:
    global _send_response_callback
    _send_response_callback = response_callback


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    return payload if isinstance(payload, dict) else {}


def _latest_family_payloads(
    state_dir: Path,
    family_names: Iterable[str],
) -> Dict[str, tuple[Dict[str, Any], Dict[str, Any]]]:
    """Find each family's latest result, even after a check or partial run."""

    wanted = set(family_names)
    found: Dict[str, tuple[Dict[str, Any], Dict[str, Any]]] = {}
    candidates = [state_dir / "latest.json"]
    runs_dir = state_dir / "runs"
    if runs_dir.exists():
        candidates.extend(sorted(runs_dir.glob("*.json"), reverse=True))
    seen_paths: set[Path] = set()
    for path in candidates:
        resolved = path.resolve()
        if resolved in seen_paths:
            continue
        seen_paths.add(resolved)
        payload = _read_json(path)
        for item in payload.get("families", []):
            if not isinstance(item, dict):
                continue
            family = item.get("family")
            if family in wanted and family not in found:
                found[family] = (item, payload)
        if set(found) == wanted:
            break
    return found


def get_fundpos_snapshot(
    config_path: Path = DEFAULT_CONFIG_PATH,
) -> Dict[str, Any]:
    """Return enabled FundPos families and the latest auditable run state."""

    try:
        config = load_production_config(config_path)
        latest = _read_json(config.state_dir / "latest.json")
        by_family = _latest_family_payloads(config.state_dir, config.families)
        rows: List[Dict[str, Any]] = []
        for family in config.families:
            item, family_run = by_family.get(family, ({}, {}))
            assessment = item.get("assessment") or {}
            reconciliation = item.get("reconciliation") or {}
            coverage = assessment.get("count_coverage")
            rows.append(
                {
                    "name": f"fundpos.{family}",
                    "family": family,
                    "display_name": FAMILY_LABELS.get(family, family),
                    "scope": config.expected_scope_versions.get(family, config.scope),
                    "expected_count": config.expected_universe_counts.get(family),
                    "valuation_date": assessment.get("valuation_date"),
                    "coverage": coverage,
                    "run_id": item.get("run_id"),
                    "status": (
                        "成功"
                        if family_run.get("status") == "passed"
                        and reconciliation.get("status") == "passed"
                        else (
                            "失败" if family_run.get("status") == "failed" else "未运行"
                        )
                    ),
                    "finished_at": family_run.get("finished_at"),
                    "selected": True,
                }
            )
        observation = _read_json(config.state_dir / "observation.json")
        if not observation:
            observation = latest.get("observation") or {}
        return {
            "status": "ready",
            "config_path": str(config_path),
            "mode": "shadow",
            "scope": config.scope,
            "families": rows,
            "latest_run_status": latest.get("status", "未运行"),
            "latest_run_mode": latest.get("mode"),
            "latest_finished_at": latest.get("finished_at"),
            "latest_error": latest.get("error"),
            "observation_days": observation.get(
                "successful_distinct_valuation_days", 0
            ),
            "required_observation_days": observation.get("required_shadow_days", 10),
            "publication_note": "影子估算，不更新正式发布指针",
        }
    except Exception as exc:  # noqa: BLE001 - surface configuration errors in GUI
        logger.error("读取 FundPos 状态失败: %s", exc, exc_info=True)
        return {
            "status": "blocked",
            "config_path": str(config_path),
            "families": [],
            "error": str(exc),
            "publication_note": "FundPos 配置不可用，未执行任何估算",
        }


async def execute_fundpos(
    *,
    mode: str,
    families: Optional[Iterable[str]] = None,
    cutoff: Optional[str] = None,
    config_path: Path = DEFAULT_CONFIG_PATH,
) -> Dict[str, Any]:
    """Run only safe GUI modes; publication stays outside the GUI."""

    global _is_running
    if mode not in {"check", "shadow"}:
        raise ValueError("GUI 仅允许 FundPos 检查或影子估算，不允许正式发布")
    if _is_running:
        return {"status": "busy", "error": "已有 FundPos 任务正在运行"}

    _is_running = True
    try:
        config = load_production_config(config_path)
        selected = tuple(families or config.families)
        unknown = sorted(set(selected) - set(config.families))
        if unknown:
            raise ValueError(f"未启用的 FundPos 估算类型: {unknown}")
        runner = FundposProductionRunner(config)
        return await asyncio.to_thread(
            runner.run,
            mode=mode,
            date="latest",
            cutoff=cutoff,
            families=selected,
        )
    finally:
        _is_running = False


async def handle_get_fundpos_tasks() -> None:
    snapshot = get_fundpos_snapshot()
    _send("FUNDPOS_TASK_LIST_UPDATE", snapshot)
    _send(
        "FUNDPOS_REFRESH_COMPLETE",
        {"success": snapshot.get("status") == "ready"},
    )


async def handle_run_fundpos(
    mode: str, families: Optional[Iterable[str]] = None
) -> Dict[str, Any]:
    selected = list(families or [])
    _send(
        "LOG",
        {
            "level": "info",
            "message": f"FundPos {mode} 已启动，估算类型: {', '.join(selected) or '全部'}",
        },
    )
    try:
        result = await execute_fundpos(mode=mode, families=selected or None)
    except Exception as exc:  # noqa: BLE001 - convert worker failure to GUI result
        logger.error("FundPos %s 失败: %s", mode, exc, exc_info=True)
        result = {"status": "failed", "error": str(exc)}

    success = result.get("status") == "passed"
    _send("FUNDPOS_RUN_COMPLETE", {"success": success, "result": result})
    await handle_get_fundpos_tasks()
    return result


def is_fundpos_running() -> bool:
    return _is_running


def _send(command: str, payload: Any) -> None:
    if _send_response_callback:
        _send_response_callback(command, payload)


__all__ = [
    "DEFAULT_CONFIG_PATH",
    "execute_fundpos",
    "get_fundpos_snapshot",
    "handle_get_fundpos_tasks",
    "handle_run_fundpos",
    "initialize_fundpos_service",
    "is_fundpos_running",
]
