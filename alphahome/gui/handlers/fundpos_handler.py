"""UI handlers for FundPos estimation tasks."""

from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk
from typing import Any, Dict, List, Optional

from .. import controller

_snapshot: Dict[str, Any] = {}
_families: List[Dict[str, Any]] = []
_detail_family: Optional[str] = None


def handle_refresh_fundpos(widgets: Dict[str, tk.Widget]) -> None:
    button = widgets.get("fundpos_refresh_button")
    if button:
        button.config(state=tk.DISABLED)
    _set_status(widgets, "正在刷新 FundPos 状态...")
    controller.request_fundpos_tasks()


def handle_select_all_fundpos(widgets: Dict[str, tk.Widget]) -> None:
    for family in _families:
        family["selected"] = True
    _render(widgets)


def handle_deselect_all_fundpos(widgets: Dict[str, tk.Widget]) -> None:
    for family in _families:
        family["selected"] = False
    _render(widgets)


def handle_fundpos_tree_click(
    event: tk.Event,
    widgets: Dict[str, tk.Widget],
) -> None:
    tree = widgets.get("fundpos_task_tree")
    if not isinstance(tree, ttk.Treeview):
        return
    item_id = tree.identify_row(event.y)
    if not item_id:
        return
    values = tree.item(item_id, "values")
    if len(values) < 3:
        return
    family_name = values[2]
    family = next(
        (item for item in _families if item.get("family") == family_name), None
    )
    if not family:
        return
    if (
        tree.identify_region(event.x, event.y) == "cell"
        and tree.identify_column(event.x) == "#1"
    ):
        family["selected"] = not family.get("selected", False)
        _render(widgets)
    _show_detail(widgets, family)


def handle_fundpos_tree_select(
    _event: tk.Event,
    widgets: Dict[str, tk.Widget],
) -> None:
    tree = widgets.get("fundpos_task_tree")
    if not isinstance(tree, ttk.Treeview):
        return
    selected = tree.selection()
    if not selected:
        return
    values = tree.item(selected[0], "values")
    if len(values) < 3:
        return
    family = next((item for item in _families if item.get("family") == values[2]), None)
    if family:
        _show_detail(widgets, family)


def handle_check_fundpos(widgets: Dict[str, tk.Widget]) -> None:
    _run_selected(widgets, "check")


def handle_shadow_fundpos(widgets: Dict[str, tk.Widget]) -> None:
    _run_selected(widgets, "shadow")


def update_fundpos_task_list_ui(
    widgets: Dict[str, tk.Widget],
    snapshot: Dict[str, Any],
) -> None:
    global _snapshot, _families
    old_selection = {
        item.get("family"): item.get("selected", False) for item in _families
    }
    _snapshot = snapshot
    _families = [dict(item) for item in snapshot.get("families", [])]
    for item in _families:
        item["selected"] = old_selection.get(
            item.get("family"), item.get("selected", True)
        )
    scope = widgets.get("fundpos_scope_label")
    if scope:
        scope.config(text=f"范围: {snapshot.get('scope', '--')} · 影子模式")
    observation = widgets.get("fundpos_observation_label")
    if observation:
        observation.config(
            text=(
                f"影子观察: {snapshot.get('observation_days', 0)}/"
                f"{snapshot.get('required_observation_days', '--')} 天"
            )
        )
    _render(widgets)
    if snapshot.get("status") == "ready":
        if snapshot.get("latest_run_status") == "failed":
            _set_status(
                widgets,
                "FundPos 最近一次"
                f"{snapshot.get('latest_run_mode') or '运行'}失败："
                f"{snapshot.get('latest_error') or '请查看任务日志'}",
            )
        else:
            _set_status(widgets, f"FundPos 已加载：{len(_families)} 个估算任务。")
        if _detail_family:
            family = next(
                (item for item in _families if item.get("family") == _detail_family),
                None,
            )
            if family:
                _show_detail(widgets, family)
        elif _families:
            _show_detail(widgets, _families[0])
    else:
        _set_status(widgets, f"FundPos 配置阻断：{snapshot.get('error', '未知错误')}")
        _write_detail(
            widgets,
            f"配置文件：{snapshot.get('config_path', '--')}\n"
            f"阻断原因：{snapshot.get('error', '未知错误')}\n\n"
            f"{snapshot.get('publication_note', '')}",
        )
    _set_run_controls(widgets, enabled=snapshot.get("status") == "ready")


def handle_fundpos_refresh_complete(
    widgets: Dict[str, tk.Widget],
    data: Dict[str, Any],
) -> None:
    button = widgets.get("fundpos_refresh_button")
    if button:
        button.config(state=tk.NORMAL)
    if not data.get("success") and _snapshot.get("status") != "blocked":
        _set_status(widgets, "FundPos 状态刷新失败。")


def handle_fundpos_run_complete(
    widgets: Dict[str, tk.Widget],
    data: Dict[str, Any],
) -> None:
    _set_run_controls(widgets, enabled=True)
    result = data.get("result") or {}
    if data.get("success"):
        observation = result.get("observation") or {}
        if observation.get("status") == "partial_smoke_not_counted":
            _set_status(
                widgets,
                "FundPos 影子估算成功；因未运行全部类型，本次不计入 10 日观察期。",
            )
        else:
            _set_status(widgets, "FundPos 运行成功，正在刷新最近状态...")
    else:
        _set_status(
            widgets,
            f"FundPos 运行失败：{result.get('error', result.get('status', '未知错误'))}",
        )


def _run_selected(widgets: Dict[str, tk.Widget], mode: str) -> None:
    selected = [item.get("family") for item in _families if item.get("selected")]
    if not selected:
        messagebox.showwarning("提示", "请先选择至少一个 FundPos 估算任务。")
        return
    _set_run_controls(widgets, enabled=False)
    label = "环境检查" if mode == "check" else "影子估算"
    _set_status(widgets, f"FundPos {label}已启动（{len(selected)} 个任务）...")
    controller.request_run_fundpos(mode, selected)


def _render(widgets: Dict[str, tk.Widget]) -> None:
    tree = widgets.get("fundpos_task_tree")
    if not isinstance(tree, ttk.Treeview):
        return
    tree.delete(*tree.get_children())
    for family in _families:
        status = family.get("status", "未运行")
        tag = "error" if status == "失败" else "success" if status == "成功" else ""
        tree.insert(
            "",
            tk.END,
            values=(
                "✓" if family.get("selected") else "",
                family.get("display_name", ""),
                family.get("family", ""),
                family.get("scope", ""),
                family.get("valuation_date") or "--",
                _format_coverage(family.get("coverage")),
                status,
                _format_datetime(family.get("finished_at")),
                family.get("run_id") or "--",
            ),
            tags=(tag,) if tag else (),
        )
    tree.tag_configure("error", foreground="#b42318")
    tree.tag_configure("success", foreground="#067647")


def _show_detail(widgets: Dict[str, tk.Widget], family: Dict[str, Any]) -> None:
    global _detail_family
    _detail_family = family.get("family")
    lines = [
        f"{family.get('display_name')}（{family.get('family')}）",
        f"任务标识：{family.get('name')}",
        f"估算范围：{family.get('scope')}",
        f"预期产品数：{family.get('expected_count', '--')}",
        f"最近估值日：{family.get('valuation_date') or '--'}",
        f"最近覆盖率：{_format_coverage(family.get('coverage'))}",
        f"最近状态：{family.get('status', '--')}",
        f"最近完成时间：{_format_datetime(family.get('finished_at'))}",
        f"最近运行号：{family.get('run_id') or '--'}",
        f"全局最近运行：{_snapshot.get('latest_run_mode') or '--'} / {_snapshot.get('latest_run_status') or '--'}",
        "",
        f"运行边界：{_snapshot.get('publication_note', '影子估算，不正式发布')}",
        f"配置文件：{_snapshot.get('config_path', '--')}",
    ]
    if _snapshot.get("latest_error"):
        lines.append(f"全局最近错误：{_snapshot['latest_error']}")
    _write_detail(widgets, "\n".join(lines))


def _set_run_controls(widgets: Dict[str, tk.Widget], *, enabled: bool) -> None:
    state = tk.NORMAL if enabled else tk.DISABLED
    for key in (
        "fundpos_check_button",
        "fundpos_shadow_button",
        "fundpos_select_all_button",
        "fundpos_deselect_all_button",
    ):
        widget = widgets.get(key)
        if widget:
            widget.config(state=state)


def _format_coverage(value: Any) -> str:
    if value is None:
        return "--"
    try:
        return f"{float(value):.1%}"
    except (TypeError, ValueError):
        return str(value)


def _format_datetime(value: Any) -> str:
    if not value:
        return "--"
    return str(value).replace("T", " ")[:19]


def _set_status(widgets: Dict[str, tk.Widget], message: str) -> None:
    label = widgets.get("fundpos_status_label")
    if label:
        label.config(text=message)


def _write_detail(widgets: Dict[str, tk.Widget], text: str) -> None:
    widget = widgets.get("fundpos_detail_text")
    if not isinstance(widget, tk.Text):
        return
    widget.config(state=tk.NORMAL)
    widget.delete("1.0", tk.END)
    widget.insert("1.0", text)
    widget.config(state=tk.DISABLED)


__all__ = [
    "handle_check_fundpos",
    "handle_deselect_all_fundpos",
    "handle_fundpos_refresh_complete",
    "handle_fundpos_run_complete",
    "handle_fundpos_tree_click",
    "handle_fundpos_tree_select",
    "handle_refresh_fundpos",
    "handle_select_all_fundpos",
    "handle_shadow_fundpos",
    "update_fundpos_task_list_ui",
]
