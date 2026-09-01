"""UI handlers for the PIT management tab."""

from __future__ import annotations

import asyncio
import tkinter as tk
from datetime import datetime
from tkinter import messagebox, ttk
from typing import Any, Dict, List, Optional

from ...common.constants import UpdateTypes
from ...common.logging_utils import get_logger
from .. import controller
from ..utils.common import format_datetime_for_display

logger = get_logger(__name__)

_full_pit_task_list: List[Dict[str, Any]] = []
_current_detail_task_name: Optional[str] = None


def handle_refresh_pit_tasks(widgets: Dict[str, tk.Widget]):
    button = widgets.get("pit_refresh_button")
    if button:
        button.config(state=tk.DISABLED)
    _set_status(widgets, "正在刷新PIT任务列表...")
    controller.request_pit_tasks()


def handle_select_all_pit(widgets: Dict[str, tk.Widget]):
    for task in _full_pit_task_list:
        task["selected"] = True
    _update_pit_task_display(widgets)


def handle_deselect_all_pit(widgets: Dict[str, tk.Widget]):
    for task in _full_pit_task_list:
        task["selected"] = False
    _update_pit_task_display(widgets)


def handle_pit_task_tree_click(event: tk.Event, widgets: Dict[str, tk.Widget]):
    tree = widgets.get("pit_task_tree")
    if not isinstance(tree, ttk.Treeview):
        return
    if tree.identify_region(event.x, event.y) != "cell":
        return
    item_id = tree.identify_row(event.y)
    if not item_id:
        return
    values = tree.item(item_id, "values")
    if not values:
        return
    task_name = values[2]
    if tree.identify_column(event.x) == "#1":
        task = _find_task(task_name)
        if task:
            task["selected"] = not task.get("selected", False)
            _update_pit_task_display(widgets)
    _show_task_detail(widgets, task_name)


def handle_pit_tree_select(event: tk.Event, widgets: Dict[str, tk.Widget]):
    tree = widgets.get("pit_task_tree")
    if not isinstance(tree, ttk.Treeview):
        return
    selected = tree.selection()
    if not selected:
        return
    values = tree.item(selected[0], "values")
    if values:
        _show_task_detail(widgets, values[2])


def handle_incremental_selected_pit(widgets: Dict[str, tk.Widget]):
    _run_selected_pit_tasks(widgets, UpdateTypes.SMART_DISPLAY)


def handle_full_backfill_selected_pit(widgets: Dict[str, tk.Widget]):
    selected = get_selected_pit_tasks()
    if not selected:
        messagebox.showwarning("提示", "请先选择要回填的PIT任务。")
        return
    if not messagebox.askyesno("确认全量回填", f"将对 {len(selected)} 个PIT任务执行全量回填，可能需要较长时间。\n\n是否继续？"):
        return
    _run_selected_pit_tasks(widgets, UpdateTypes.FULL_DISPLAY)


def handle_audit_selected_pit(widgets: Dict[str, tk.Widget]):
    selected_names = [task["task_name"] for task in get_selected_pit_tasks()]
    if not selected_names:
        messagebox.showwarning("提示", "请先选择要审计的PIT任务。")
        return
    _set_status(widgets, f"正在审计 {len(selected_names)} 个PIT任务...")
    controller.request_audit_pit_tasks(selected_names)


def handle_view_pit_gaps(widgets: Dict[str, tk.Widget]):
    _set_status(widgets, "正在生成PIT覆盖率矩阵...")
    controller.request_pit_coverage_matrix()


def handle_stock_diagnosis(widgets: Dict[str, tk.Widget]):
    entry = widgets.get("pit_stock_entry")
    ts_code = entry.get().strip() if hasattr(entry, "get") else ""
    if not ts_code:
        messagebox.showwarning("提示", "请输入股票代码，例如 002549.SZ。")
        return
    _set_status(widgets, f"正在诊断 {ts_code} 的PIT覆盖...")
    controller.request_pit_stock_diagnosis(ts_code)


def update_pit_task_list_ui(widgets: Dict[str, tk.Widget], task_list: List[Dict[str, Any]]):
    global _full_pit_task_list
    _full_pit_task_list = task_list
    _update_pit_task_display(widgets)
    button = widgets.get("pit_refresh_button")
    if button:
        button.config(state=tk.NORMAL)
    _set_status(widgets, f"PIT任务列表已更新 ({len(task_list)}个任务)。")
    if _current_detail_task_name:
        _show_task_detail(widgets, _current_detail_task_name)


def handle_pit_refresh_complete(widgets: Dict[str, tk.Widget], data: Dict[str, Any]):
    button = widgets.get("pit_refresh_button")
    if button:
        button.config(state=tk.NORMAL)
    if not data.get("success"):
        _set_status(widgets, "PIT任务列表刷新失败")


def handle_pit_audit_complete(widgets: Dict[str, tk.Widget], data: Dict[str, Any]):
    if data.get("success"):
        results = data.get("results") or []
        failed = [item for item in results if item.get("status") == "error"]
        _set_status(widgets, f"PIT审计完成: {len(results) - len(failed)} 成功, {len(failed)} 失败")
        _write_detail_text(widgets, _format_audit_results(results))
    else:
        _set_status(widgets, f"PIT审计失败: {data.get('error')}")


def update_pit_coverage_matrix_ui(widgets: Dict[str, tk.Widget], matrix: Dict[str, Any]):
    _set_status(widgets, "PIT覆盖率矩阵已生成")
    lines = [
        "PIT财务覆盖率矩阵",
        f"当前上市股票数: {matrix.get('listed_stock_count', 0)}",
        "",
    ]
    for row in matrix.get("rows", []):
        lines.append(
            "{task} | {period} | 覆盖 {coverage}/{listed} | 覆盖率 {rate} | 缺口 {gap}".format(
                task=row.get("task_name"),
                period=_fmt(row.get("report_period")),
                coverage=row.get("coverage_count"),
                listed=row.get("listed_stock_count"),
                rate=_format_rate(row.get("coverage_rate")),
                gap=row.get("gap_count"),
            )
        )
    _write_detail_text(widgets, "\n".join(lines))


def update_pit_stock_diagnosis_ui(widgets: Dict[str, tk.Widget], diagnosis: Dict[str, Any]):
    if diagnosis.get("status") != "success":
        _set_status(widgets, f"单股诊断失败: {diagnosis.get('error')}")
        return
    ts_code = diagnosis.get("ts_code")
    _set_status(widgets, f"{ts_code} PIT诊断完成")
    _write_detail_text(widgets, _format_stock_diagnosis(diagnosis))


def _format_stock_diagnosis(diagnosis: Dict[str, Any]) -> str:
    ts_code = diagnosis.get("ts_code")
    lines = [f"{ts_code} PIT单股诊断", ""]
    for item in diagnosis.get("tasks", []):
        lines.append(f"[{item.get('task_name')}] {item.get('status')}")
        if item.get("missing_expected_periods") is not None:
            missing_expected = item.get("missing_expected_periods") or []
            lines.append(
                "  三表预期缺口: "
                + (", ".join(map(_fmt, missing_expected[:12])) if missing_expected else "无")
            )
        gap_diagnosis = item.get("gap_diagnosis") or []
        if gap_diagnosis:
            lines.append("  缺口原因:")
            for gap in gap_diagnosis[:8]:
                lines.append(
                    f"    {_fmt(gap.get('period'))}: {_format_gap_reason(gap.get('reason'))}"
                )
                for check in (gap.get("source_checks") or [])[:4]:
                    lines.append(f"      {_format_source_check(check)}")
        if item.get("raw_missing_in_pit") is not None:
            missing = item.get("raw_missing_in_pit") or []
            lines.append(f"  raw有但PIT缺: {', '.join(map(_fmt, missing[:12])) if missing else '无'}")
        periods = item.get("pit_periods") or []
        if periods:
            lines.append(f"  PIT最近期: {', '.join(map(_fmt, periods[:8]))}")
        latest_rows = item.get("latest_rows") or []
        if latest_rows:
            lines.append(f"  最近记录: {latest_rows[:3]}")
        lines.append("")
    return "\n".join(lines)


def get_selected_pit_tasks() -> List[Dict[str, Any]]:
    selected = []
    for task in _full_pit_task_list:
        if task.get("selected"):
            task_info = {
                "task_name": task["name"],
                "task_type": "pit",
                "description": task.get("description", ""),
                "data_source": "pit",
                "dependencies": list(task.get("dependencies") or []),
            }
            if task.get("pit_time_key") is not None:
                task_info["pit_time_key"] = task["pit_time_key"]
            selected.append(task_info)
    return selected


def _run_selected_pit_tasks(widgets: Dict[str, tk.Widget], exec_mode: str):
    selected = get_selected_pit_tasks()
    if not selected:
        messagebox.showwarning("提示", "请先选择要运行的PIT任务。")
        return
    _set_status(widgets, f"已提交 {len(selected)} 个PIT任务")
    asyncio.create_task(
        controller.handle_request(
            "RUN_TASKS",
            {
                "tasks_to_run": selected,
                "start_date": None,
                "end_date": None,
                "exec_mode": exec_mode,
                "use_insert_mode": False,
            },
        )
    )


def _update_pit_task_display(widgets: Dict[str, tk.Widget]):
    tree = widgets.get("pit_task_tree")
    if not isinstance(tree, ttk.Treeview):
        return
    tree.delete(*tree.get_children())
    for task in _full_pit_task_list:
        values = (
            "✓" if task.get("selected") else "",
            task.get("domain", ""),
            task.get("name", ""),
            task.get("output_table", ""),
            task.get("pit_time_key", ""),
            ", ".join(task.get("dependencies") or []),
            _fmt(task.get("latest_date")),
            task.get("row_count", 0),
            _format_rate(task.get("coverage_rate")),
            "" if task.get("gap_count") is None else task.get("gap_count"),
            task.get("last_execution_status", ""),
            _fmt(task.get("last_execution_time")),
            _fmt(task.get("last_audit_time")),
        )
        tags = ("selected",) if task.get("selected") else ()
        if (
            task.get("last_execution_status") == "error"
            or task.get("live_status") in ("error", "missing_table")
        ):
            tags = tags + ("error",)
        tree.insert("", tk.END, values=values, tags=tags)
    tree.tag_configure("selected", background="#e8f4fd")
    tree.tag_configure("error", foreground="red")


def _show_task_detail(widgets: Dict[str, tk.Widget], task_name: str):
    global _current_detail_task_name
    _current_detail_task_name = task_name
    task = _find_task(task_name)
    if not task:
        return
    _write_detail_text(widgets, _format_task_detail(task))


def _format_task_detail(task: Dict[str, Any]) -> str:
    lines = [
        f"任务: {task.get('name')}",
        f"域: {task.get('domain')}",
        f"描述: {task.get('description')}",
        f"输出表: {task.get('output_table')}",
        f"PIT时间键: {task.get('pit_time_key')}",
        f"主键: {', '.join(task.get('primary_keys') or [])}",
        f"输入来源: {', '.join(task.get('source_tables') or [])}",
        f"依赖: {', '.join(task.get('dependencies') or []) or '无'}",
        f"支持模式: {', '.join(task.get('supported_modes') or [])}",
        "",
        "当前实时表状态",
        f"最新日期: {_fmt(task.get('latest_date'))}",
        f"行数: {task.get('row_count', 0)}",
        f"覆盖率: {_format_rate(task.get('coverage_rate'))}",
        f"缺口数: {task.get('gap_count')}",
        f"实时表状态: {task.get('live_status')}",
        "",
        "最近执行记录",
        f"执行状态: {task.get('last_execution_status')}",
        f"执行时间: {_fmt(task.get('last_execution_time'))}",
        f"执行详情: {task.get('last_execution_details') or '无'}",
        "",
        "最近审计快照",
        f"审计状态: {task.get('audit_status')}",
        f"审计时间: {_fmt(task.get('last_audit_time'))}",
        f"审计时最新日期: {_fmt(task.get('audited_latest_date'))}",
        f"审计时行数: {task.get('audited_row_count')}",
        f"审计时覆盖率: {_format_rate(task.get('audited_coverage_rate'))}",
        f"审计时缺口数: {task.get('audited_gap_count')}",
    ]
    return "\n".join(lines)


def _find_task(task_name: str) -> Optional[Dict[str, Any]]:
    return next((task for task in _full_pit_task_list if task.get("name") == task_name), None)


def _format_audit_results(results: List[Dict[str, Any]]) -> str:
    lines = ["PIT审计结果", ""]
    for item in results:
        lines.append(
            "{task} | {status} | rows={rows} | latest={latest} | coverage={coverage} | gaps={gaps}".format(
                task=item.get("task_name"),
                status=item.get("status"),
                rows=item.get("row_count"),
                latest=_fmt(item.get("latest_pit_time")),
                coverage=_format_rate(item.get("coverage_rate")),
                gaps=item.get("gap_count"),
            )
        )
        raw_gap = ((item.get("details") or {}).get("raw_vs_pit") or {})
        if raw_gap:
            lines.append(f"  raw-vs-pit: {raw_gap}")
    return "\n".join(lines)


def _format_gap_reason(reason: Any) -> str:
    labels = {
        "source_relation_missing": "源表不存在",
        "source_missing": "源表无该报告期",
        "source_not_eligible": "源表有记录但不符合PIT口径",
        "source_empty_after_field_filter": "源表有记录但核心字段全空",
        "pit_build_gap": "源表有效但PIT未落库",
        "missing_relation": "源表不存在",
        "unsupported_columns": "字段不支持",
    }
    return labels.get(str(reason), str(reason))


def _format_source_check(check: Dict[str, Any]) -> str:
    source = check.get("source_table", "unknown")
    status = check.get("status")
    if status != "ok":
        return f"{source}: {_format_gap_reason(status)}"
    return (
        f"{source}: raw={check.get('raw_rows', 0)}, "
        f"eligible={check.get('eligible_rows', 0)}, "
        f"valid={check.get('valid_rows', 0)}"
    )


def _write_detail_text(widgets: Dict[str, tk.Widget], text: str):
    detail = widgets.get("pit_detail_text")
    if not isinstance(detail, tk.Text):
        return
    detail.config(state=tk.NORMAL)
    detail.delete("1.0", tk.END)
    detail.insert(tk.END, text)
    detail.config(state=tk.DISABLED)


def _set_status(widgets: Dict[str, tk.Widget], message: str):
    label = widgets.get("pit_status_label")
    if label:
        label.config(text=message)


def _format_rate(value: Any) -> str:
    if value is None or value == "":
        return "N/A"
    try:
        return f"{float(value) * 100:.2f}%"
    except (TypeError, ValueError):
        return str(value)


def _fmt(value: Any) -> str:
    if value is None:
        return "N/A"
    if isinstance(value, datetime):
        return format_datetime_for_display(value)
    return str(value)[:19]
