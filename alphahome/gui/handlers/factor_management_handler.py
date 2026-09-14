"""Handlers for the independent factor management tab."""

from __future__ import annotations

import asyncio
import tkinter as tk
from datetime import datetime
from tkinter import messagebox, ttk
from typing import Any, Dict, List, Optional

from ...common.constants import UpdateTypes
from .. import controller
from ..utils.common import format_datetime_for_display


_full_factor_task_list: List[Dict[str, Any]] = []
_current_detail_task_name: Optional[str] = None
_pending_run: Optional[Dict[str, Any]] = None


def handle_refresh_factor_tasks(widgets: Dict[str, tk.Widget]) -> None:
    _set_status(widgets, "正在刷新因子任务...")
    _set_button(widgets, "factor_refresh_button", tk.DISABLED)
    controller.request_factor_tasks()


def handle_select_all_factor(widgets: Dict[str, tk.Widget]) -> None:
    for task in _full_factor_task_list:
        task["selected"] = True
    _update_display(widgets)


def handle_deselect_all_factor(widgets: Dict[str, tk.Widget]) -> None:
    for task in _full_factor_task_list:
        task["selected"] = False
    _update_display(widgets)


def handle_factor_tree_click(event: tk.Event, widgets: Dict[str, tk.Widget]) -> None:
    tree = widgets.get("factor_task_tree")
    if not isinstance(tree, ttk.Treeview):
        return
    item_id = tree.identify_row(event.y)
    if not item_id:
        return
    values = tree.item(item_id, "values")
    if not values:
        return
    task_name = values[2]
    if (
        tree.identify_region(event.x, event.y) == "cell"
        and tree.identify_column(event.x) == "#1"
    ):
        task = _find_task(task_name)
        if task:
            task["selected"] = not task.get("selected", False)
            _update_display(widgets)
    _show_task_detail(widgets, task_name)


def handle_factor_tree_select(event: tk.Event, widgets: Dict[str, tk.Widget]) -> None:
    tree = widgets.get("factor_task_tree")
    if not isinstance(tree, ttk.Treeview):
        return
    selected = tree.selection()
    if selected:
        values = tree.item(selected[0], "values")
        if values:
            _show_task_detail(widgets, values[2])


def handle_smart_selected_factor(widgets: Dict[str, tk.Widget]) -> None:
    _request_preflight(widgets, "smart")


def handle_manual_selected_factor(widgets: Dict[str, tk.Widget]) -> None:
    start = _entry(widgets, "factor_start_date_entry")
    end = _entry(widgets, "factor_end_date_entry")
    if not start or not end:
        messagebox.showwarning("提示", "指定日期回补必须填写开始和结束日期。")
        return
    try:
        datetime.strptime(start, "%Y-%m-%d")
        datetime.strptime(end, "%Y-%m-%d")
    except ValueError:
        messagebox.showwarning("提示", "日期格式必须为 YYYY-MM-DD。")
        return
    _request_preflight(widgets, "manual", start, end)


def handle_full_selected_factor(widgets: Dict[str, tk.Widget]) -> None:
    start = _entry(widgets, "factor_start_date_entry") or None
    end = _entry(widgets, "factor_end_date_entry") or None
    _request_preflight(widgets, "full", start, end)


def handle_audit_selected_factor(widgets: Dict[str, tk.Widget]) -> None:
    names = [item["task_name"] for item in get_selected_factor_tasks()]
    if not names:
        messagebox.showwarning("提示", "请先选择因子任务。")
        return
    _set_status(widgets, f"正在审计 {len(names)} 个因子任务...")
    controller.request_audit_factor_tasks(names)


def handle_view_factor_gaps(widgets: Dict[str, tk.Widget]) -> None:
    names = [item["task_name"] for item in get_selected_factor_tasks()]
    controller.request_factor_gaps(names)
    _set_status(widgets, "正在获取因子日期缺口...")


def handle_factor_date_diagnosis(widgets: Dict[str, tk.Widget]) -> None:
    task_name = _current_detail_task_name
    calc_date = _entry(widgets, "factor_diagnose_date_entry")
    if not task_name or not calc_date:
        messagebox.showwarning("提示", "请选择一个任务并填写诊断日期。")
        return
    controller.request_factor_date_diagnosis(task_name, calc_date)
    _set_status(widgets, f"正在诊断 {task_name} / {calc_date}...")


def handle_factor_stock_diagnosis(widgets: Dict[str, tk.Widget]) -> None:
    ts_code = _entry(widgets, "factor_stock_entry")
    if not ts_code:
        messagebox.showwarning("提示", "请输入股票代码。")
        return
    controller.request_factor_stock_diagnosis(ts_code)
    _set_status(widgets, f"正在诊断 {ts_code} 的因子历史...")


def _request_preflight(
    widgets: Dict[str, tk.Widget],
    mode: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> None:
    global _pending_run
    selected = get_selected_factor_tasks()
    if not selected:
        messagebox.showwarning("提示", "请先选择因子任务。")
        return
    _pending_run = {
        "mode": mode,
        "start_date": start_date,
        "end_date": end_date,
    }
    _set_status(widgets, "正在生成依赖与日期预检...")
    controller.request_factor_preflight(
        [item["task_name"] for item in selected], mode, start_date, end_date
    )


def handle_factor_preflight_complete(
    widgets: Dict[str, tk.Widget], data: Dict[str, Any]
) -> None:
    global _pending_run
    if not data.get("success"):
        _set_status(widgets, f"预检失败: {data.get('error')}")
        _pending_run = None
        return
    plan = data.get("plan") or {}
    _write_detail(widgets, _format_preflight(plan))
    if plan.get("status") != "ready":
        _set_status(widgets, f"预检未通过: {plan.get('status')}")
        messagebox.showwarning(
            "因子预检未通过", plan.get("message") or plan.get("status")
        )
        _pending_run = None
        return
    pending = _pending_run or {}
    task_names = plan.get("task_names") or []
    total_dates = int(plan.get("total_dates") or 0)
    prompt = (
        f"任务依赖顺序: {', '.join(task_names)}\n"
        f"计划处理任务日期数: {total_dates}\n"
        f"截止日: {plan.get('effective_cutoff_date')}\n\n是否执行？"
    )
    if not messagebox.askyesno("确认因子运行", prompt):
        _set_status(widgets, "已取消因子运行")
        _pending_run = None
        return
    tasks = [_task_info_for_name(name) for name in task_names]
    tasks = [task for task in tasks if task]
    mode = pending.get("mode", "smart")
    exec_modes = {
        "smart": UpdateTypes.SMART_DISPLAY,
        "manual": UpdateTypes.MANUAL_DISPLAY,
        "full": UpdateTypes.FULL_DISPLAY,
    }
    _set_status(widgets, f"已提交 {len(tasks)} 个因子任务")
    asyncio.create_task(
        controller.handle_request(
            "RUN_TASKS",
            {
                "tasks_to_run": tasks,
                "start_date": pending.get("start_date"),
                "end_date": pending.get("end_date"),
                "exec_mode": exec_modes[mode],
                "use_insert_mode": False,
            },
        )
    )
    _pending_run = None


def get_selected_factor_tasks() -> List[Dict[str, Any]]:
    return [
        task
        for item in _full_factor_task_list
        if item.get("selected")
        for task in [_task_info_for_name(item["name"])]
        if task
    ]


def _task_info_for_name(task_name: str) -> Optional[Dict[str, Any]]:
    item = _find_task(task_name)
    if not item:
        return None
    return {
        "task_name": task_name,
        "task_type": "factor",
        "description": item.get("description", ""),
        "data_source": "factors",
        "dependencies": list(item.get("dependencies") or []),
        "task_config": {"factor_expand_dependencies": False},
    }


def update_factor_task_list_ui(
    widgets: Dict[str, tk.Widget], task_list: List[Dict[str, Any]]
) -> None:
    global _full_factor_task_list
    _full_factor_task_list = task_list
    _update_display(widgets)
    _set_button(widgets, "factor_refresh_button", tk.NORMAL)
    _set_status(widgets, f"因子任务列表已更新 ({len(task_list)}个任务)")


def handle_factor_refresh_complete(
    widgets: Dict[str, tk.Widget], data: Dict[str, Any]
) -> None:
    _set_button(widgets, "factor_refresh_button", tk.NORMAL)
    if not data.get("success"):
        _set_status(widgets, "因子任务刷新失败")


def handle_factor_audit_complete(
    widgets: Dict[str, tk.Widget], data: Dict[str, Any]
) -> None:
    if not data.get("success"):
        _set_status(widgets, f"因子审计失败: {data.get('error')}")
        return
    results = data.get("results") or []
    _write_detail(widgets, _format_audit(results))
    _set_status(widgets, f"因子审计完成 ({len(results)}个任务)")


def update_factor_gaps_ui(widgets: Dict[str, tk.Widget], data: Dict[str, Any]) -> None:
    lines = ["因子日期缺口", ""]
    for row in data.get("rows") or []:
        missing = row.get("missing_dates") or []
        lines.append(
            f"{row.get('task_name')}: missing={len(missing)}, "
            f"non_friday={row.get('nonstandard_date_count')}"
        )
        if missing:
            lines.append("  " + ", ".join(_fmt(value) for value in missing[:30]))
    _write_detail(widgets, "\n".join(lines))
    _set_status(widgets, "因子日期缺口已生成")


def update_factor_diagnosis_ui(
    widgets: Dict[str, tk.Widget], data: Dict[str, Any]
) -> None:
    _write_detail(widgets, _format_diagnosis(data))
    _set_status(widgets, "因子诊断完成")


def _update_display(widgets: Dict[str, tk.Widget]) -> None:
    tree = widgets.get("factor_task_tree")
    if not isinstance(tree, ttk.Treeview):
        return
    tree.delete(*tree.get_children())
    for task in _full_factor_task_list:
        values = (
            "✓" if task.get("selected") else "",
            task.get("domain", ""),
            task.get("name", ""),
            task.get("output_table", ""),
            task.get("cadence", ""),
            task.get("formula_version", ""),
            ", ".join(task.get("dependencies") or []),
            _fmt(task.get("expected_latest_date")),
            _fmt(task.get("actual_latest_date")),
            task.get("missing_date_count", 0),
            task.get("nonstandard_date_count", 0),
            task.get("row_count", 0),
            _rate(task.get("coverage_rate")),
            task.get("last_execution_status", ""),
            _fmt(task.get("last_execution_time")),
            _fmt(task.get("last_audit_time")),
        )
        tags = []
        if task.get("selected"):
            tags.append("selected")
        if task.get("live_status") not in {"healthy", None}:
            tags.append("warning")
        tree.insert("", tk.END, values=values, tags=tuple(tags))
    tree.tag_configure("selected", background="#e8f4fd")
    tree.tag_configure("warning", foreground="#b45309")


def _show_task_detail(widgets: Dict[str, tk.Widget], task_name: str) -> None:
    global _current_detail_task_name
    _current_detail_task_name = task_name
    task = _find_task(task_name)
    if task:
        _write_detail(widgets, _format_task_detail(task))


def _format_task_detail(task: Dict[str, Any]) -> str:
    return "\n".join(
        [
            f"任务: {task.get('name')}",
            f"描述: {task.get('description')}",
            f"输出: {task.get('output_table')}",
            f"周期: {task.get('cadence')}",
            f"公式: {task.get('formula_version')}",
            f"来源: {', '.join(task.get('source_tables') or [])}",
            f"运行依赖: {', '.join(task.get('dependencies') or []) or '无'}",
            f"PIT就绪依赖: {', '.join(task.get('readiness_dependencies') or []) or '无'}",
            "",
            "当前实时表状态",
            f"预期最新: {_fmt(task.get('expected_latest_date'))}",
            f"实际最新: {_fmt(task.get('actual_latest_date'))}",
            f"缺失日期: {task.get('missing_date_count')}",
            f"非周五日期: {task.get('nonstandard_date_count')}",
            f"行数: {task.get('row_count')}",
            f"覆盖率: {_rate(task.get('coverage_rate'))}",
            f"状态: {task.get('live_status')}",
            "",
            "最近执行",
            f"状态: {task.get('last_execution_status')}",
            f"时间: {_fmt(task.get('last_execution_time'))}",
            f"详情: {task.get('last_execution_details') or '无'}",
            "",
            "最近审计",
            f"时间: {_fmt(task.get('last_audit_time'))}",
            f"审计时最新: {_fmt(task.get('audited_latest_date'))}",
            f"审计时行数: {task.get('audited_row_count')}",
            f"审计时缺口: {task.get('audited_gap_count')}",
        ]
    )


def _format_preflight(plan: Dict[str, Any]) -> str:
    lines = [
        "因子运行预检",
        f"状态: {plan.get('status')}",
        f"模式: {plan.get('mode')}",
        f"截止日: {plan.get('effective_cutoff_date')}",
        f"依赖顺序: {', '.join(plan.get('task_names') or [])}",
        f"任务日期总数: {plan.get('total_dates')}",
    ]
    if plan.get("message"):
        lines.append(f"提示: {plan.get('message')}")
    lines.append("")
    for item in plan.get("task_plans") or []:
        dates = item.get("dates") or []
        lines.append(
            f"[{item.get('task_name')}] dates={len(dates)}, "
            f"blockers={item.get('blockers') or '无'}"
        )
        if dates:
            lines.append(f"  {dates[0]} ~ {dates[-1]}")
    return "\n".join(lines)


def _format_audit(results: List[Dict[str, Any]]) -> str:
    lines = ["因子审计结果", ""]
    for item in results:
        lines.append(
            f"{item.get('task_name')} | {item.get('status')} | "
            f"latest={_fmt(item.get('actual_latest_date'))} | "
            f"missing={item.get('missing_date_count')} | "
            f"non_friday={item.get('nonstandard_date_count')}"
        )
    return "\n".join(lines)


def _format_diagnosis(data: Dict[str, Any]) -> str:
    if "calc_date" in data:
        return "\n".join(f"{key}: {_fmt(value)}" for key, value in data.items())
    lines = [f"{data.get('ts_code')} 因子历史诊断", ""]
    for task in data.get("tasks") or []:
        lines.append(f"[{task.get('task_name')}] rows={len(task.get('rows') or [])}")
        for row in (task.get("rows") or [])[:5]:
            lines.append(f"  {row}")
    return "\n".join(lines)


def _find_task(task_name: str) -> Optional[Dict[str, Any]]:
    return next(
        (item for item in _full_factor_task_list if item.get("name") == task_name),
        None,
    )


def _write_detail(widgets: Dict[str, tk.Widget], text: str) -> None:
    widget = widgets.get("factor_detail_text")
    if not isinstance(widget, tk.Text):
        return
    widget.config(state=tk.NORMAL)
    widget.delete("1.0", tk.END)
    widget.insert(tk.END, text)
    widget.config(state=tk.DISABLED)


def _set_status(widgets: Dict[str, tk.Widget], text: str) -> None:
    label = widgets.get("factor_status_label")
    if label:
        label.config(text=text)


def _set_button(widgets: Dict[str, tk.Widget], key: str, state: str) -> None:
    button = widgets.get(key)
    if button:
        button.config(state=state)


def _entry(widgets: Dict[str, tk.Widget], key: str) -> str:
    widget = widgets.get(key)
    return widget.get().strip() if hasattr(widget, "get") else ""


def _fmt(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return format_datetime_for_display(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _rate(value: Any) -> str:
    try:
        return f"{float(value):.2%}"
    except (TypeError, ValueError):
        return ""


__all__ = [
    name
    for name in globals()
    if name.startswith("handle_") or name.startswith("update_")
]
