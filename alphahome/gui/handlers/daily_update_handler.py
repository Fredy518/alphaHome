"""UI handlers for the one-click daily update page."""

from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Any, Dict, List, Optional

from .. import controller

_plan: Dict[str, Any] = {}
_groups: List[Dict[str, Any]] = []
_selected_group_key: Optional[str] = None
_running = False

STATUS_LABELS = {
    "ready": "待运行",
    "ready_with_blockers": "部分可运行",
    "skipped_policy": "按规则跳过",
    "blocked": "配置阻断",
    "running": "运行中",
    "success": "成功",
    "error": "失败",
    "partial_success": "部分成功",
    "cancelled": "已停止",
    "busy": "已有任务运行",
}


def handle_preview_daily_update(widgets: Dict[str, tk.Widget]) -> None:
    button = widgets.get("daily_update_preview_button")
    if button:
        button.config(state=tk.DISABLED)
    _set_status(widgets, "正在识别交易日并生成今日计划...")
    controller.request_daily_update_plan()


def handle_run_daily_update(widgets: Dict[str, tk.Widget]) -> None:
    global _running
    if _running:
        return
    _running = True
    _set_controls(widgets, running=True)
    _set_status(widgets, "一键更新已启动，正在重新核对今日计划...")
    controller.request_run_daily_update()


def handle_stop_daily_update(widgets: Dict[str, tk.Widget]) -> None:
    _set_status(widgets, "已发送停止请求；当前任务将在安全点结束...")
    controller.request_stop_daily_update()


def handle_daily_update_tree_select(
    _event: tk.Event,
    widgets: Dict[str, tk.Widget],
) -> None:
    global _selected_group_key
    tree = widgets.get("daily_update_tree")
    if not isinstance(tree, ttk.Treeview):
        return
    selected = tree.selection()
    if not selected:
        return
    values = tree.item(selected[0], "values")
    if len(values) < 2:
        return
    label = values[1]
    group = next((item for item in _groups if item.get("label") == label), None)
    if group:
        _selected_group_key = group.get("key")
        _write_detail(widgets, _format_group_detail(group))


def update_daily_update_plan_ui(
    widgets: Dict[str, tk.Widget],
    plan: Dict[str, Any],
) -> None:
    global _plan, _groups
    _plan = plan
    _groups = [dict(group) for group in plan.get("groups", [])]
    day_label = widgets.get("daily_update_day_label")
    if day_label:
        day_label.config(
            text=f"日期类型: {plan.get('as_of_date', '--')} · {plan.get('day_type', '--')}"
        )
    policy_label = widgets.get("daily_update_policy_label")
    if policy_label:
        policy_hash = str(plan.get("policy_hash") or "")
        policy_label.config(text=f"计划: {policy_hash[:12] or '生成失败'}")
    _render_groups(widgets)
    _write_detail(widgets, _format_plan_detail(plan))
    runnable = sum(group.get("run_count", 0) for group in _groups)
    if not _running:
        run_button = widgets.get("daily_update_run_button")
        if run_button:
            run_button.config(
                text=f"一键智能增量更新（{runnable}）",
                state=tk.NORMAL if runnable else tk.DISABLED,
            )


def handle_daily_update_plan_complete(
    widgets: Dict[str, tk.Widget],
    data: Dict[str, Any],
) -> None:
    preview = widgets.get("daily_update_preview_button")
    if preview and not _running:
        preview.config(state=tk.NORMAL)
    if data.get("success"):
        if not _running:
            runnable = sum(group.get("run_count", 0) for group in _groups)
            skipped = sum(group.get("skip_count", 0) for group in _groups)
            _set_status(
                widgets,
                f"今日计划已生成：运行 {runnable} 个任务，自动跳过 {skipped} 个。",
            )
    else:
        _set_status(widgets, f"今日计划生成失败：{data.get('error', '未知错误')}")


def update_daily_update_stage_ui(
    widgets: Dict[str, tk.Widget],
    group: Dict[str, Any],
) -> None:
    global _groups
    key = group.get("key")
    replaced = False
    for index, current in enumerate(_groups):
        if current.get("key") == key:
            _groups[index] = dict(group)
            replaced = True
            break
    if not replaced:
        _groups.append(dict(group))
    _render_groups(widgets)
    _set_status(
        widgets,
        f"{group.get('label', '任务')}：{group.get('action', '')}",
    )
    if _selected_group_key == key:
        _write_detail(widgets, _format_group_detail(group))


def handle_daily_update_complete(
    widgets: Dict[str, tk.Widget],
    data: Dict[str, Any],
) -> None:
    global _running
    _running = False
    _set_controls(widgets, running=False)
    result = data.get("result") or {}
    status = result.get("status", "error")
    completed = result.get("completed_groups", 0)
    failed = result.get("failed_groups", 0)
    if status == "success":
        message = f"一键更新完成：{completed} 个更新域成功。"
    elif status == "cancelled":
        message = f"一键更新已停止：停止前完成 {completed} 个更新域。"
    elif status == "busy":
        message = f"一键更新未启动：{result.get('error', '已有任务正在运行')}"
    else:
        message = f"一键更新{STATUS_LABELS.get(status, status)}：成功 {completed}，失败/阻断 {failed}。"
    _set_status(widgets, message)


def _render_groups(widgets: Dict[str, tk.Widget]) -> None:
    tree = widgets.get("daily_update_tree")
    if not isinstance(tree, ttk.Treeview):
        return
    tree.delete(*tree.get_children())
    for group in sorted(_groups, key=lambda item: item.get("order", 99)):
        status = str(group.get("status", ""))
        tag = (
            "error"
            if status in {"error", "blocked"}
            else (
                "success"
                if status == "success"
                else (
                    "skipped"
                    if status in {"skipped_policy", "cancelled"}
                    else "running" if status == "running" else ""
                )
            )
        )
        tree.insert(
            "",
            tk.END,
            values=(
                group.get("order", ""),
                group.get("label", ""),
                group.get("schedule", ""),
                group.get("execution_mode", ""),
                group.get("task_count", 0),
                group.get("run_count", 0),
                group.get("skip_count", 0),
                group.get("action", ""),
                STATUS_LABELS.get(status, status),
            ),
            tags=(tag,) if tag else (),
        )
    tree.tag_configure("error", foreground="#b42318")
    tree.tag_configure("success", foreground="#067647")
    tree.tag_configure("skipped", foreground="#667085")
    tree.tag_configure("running", background="#fff4cc")


def _format_plan_detail(plan: Dict[str, Any]) -> str:
    lines = [
        f"日期：{plan.get('as_of_date', '--')}（{plan.get('day_type', '--')}）",
        f"日期判断来源：{plan.get('calendar_source', '--')}",
        f"计划指纹：{plan.get('policy_hash', '--')}",
        f"发布边界：{plan.get('publication_note', '--')}",
        "",
        "点击上方任一更新域，可查看本次运行和自动跳过的具体任务。",
    ]
    blocked = [
        group for group in plan.get("groups", []) if group.get("status") == "blocked"
    ]
    if blocked:
        lines.extend(["", "当前阻断："])
        lines.extend(
            f"- {group.get('label')}：{group.get('description')}" for group in blocked
        )
    return "\n".join(lines)


def _format_group_detail(group: Dict[str, Any]) -> str:
    running = group.get("task_names") or []
    skipped = group.get("skipped_task_names") or []
    manual_only = set(group.get("manual_only_task_names") or [])
    policy_skipped = [name for name in skipped if name not in manual_only]
    lines = [
        str(group.get("label", "")),
        f"说明：{group.get('description', '--')}",
        f"执行方式：{group.get('execution_mode', '--')}",
        f"当前状态：{STATUS_LABELS.get(group.get('status'), group.get('status', '--'))}",
        "",
        f"本次运行（{len(running)}）：",
        *(f"- {name}" for name in running),
        "",
        f"工作日按频率自动跳过（{len(policy_skipped)}）：",
        *(f"- {name}" for name in policy_skipped),
    ]
    progress = group.get("progress") or {}
    if progress:
        lines[4:4] = [
            (
                "实时进度："
                f"{progress.get('completed', 0)}/{group.get('run_count', 0)}，"
                f"成功 {progress.get('success', 0)}，"
                f"失败 {progress.get('failed', 0)}，"
                f"跳过 {progress.get('skipped', 0)}，"
                f"已停止 {progress.get('cancelled', 0)}"
            ),
            "",
        ]
    if manual_only:
        lines.extend(
            [
                "",
                f"仅允许手工显式运行（{len(manual_only)}）：",
                *(f"- {name}" for name in sorted(manual_only)),
            ]
        )
    result = group.get("result")
    if result:
        lines.extend(["", f"执行结果：{result}"])
    return "\n".join(lines)


def _set_controls(widgets: Dict[str, tk.Widget], *, running: bool) -> None:
    normal = tk.DISABLED if running else tk.NORMAL
    for key in ("daily_update_preview_button", "daily_update_run_button"):
        widget = widgets.get(key)
        if widget:
            widget.config(state=normal)
    stop = widgets.get("daily_update_stop_button")
    if stop:
        stop.config(state=tk.NORMAL if running else tk.DISABLED)


def _set_status(widgets: Dict[str, tk.Widget], message: str) -> None:
    label = widgets.get("daily_update_status_label")
    if label:
        label.config(text=message)


def _write_detail(widgets: Dict[str, tk.Widget], text: str) -> None:
    widget = widgets.get("daily_update_detail_text")
    if not isinstance(widget, tk.Text):
        return
    widget.config(state=tk.NORMAL)
    widget.delete("1.0", tk.END)
    widget.insert("1.0", text)
    widget.config(state=tk.DISABLED)


__all__ = [
    "handle_daily_update_complete",
    "handle_daily_update_plan_complete",
    "handle_daily_update_tree_select",
    "handle_preview_daily_update",
    "handle_run_daily_update",
    "handle_stop_daily_update",
    "update_daily_update_plan_ui",
    "update_daily_update_stage_ui",
]
