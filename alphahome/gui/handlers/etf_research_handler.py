"""UI handlers for the ETF research-foundation tab."""

from __future__ import annotations

import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Any, Dict

from ...common.logging_utils import get_logger
from .. import controller

logger = get_logger(__name__)


def handle_choose_snapshot(widgets: Dict[str, tk.Widget]) -> None:
    selected = filedialog.askopenfilename(
        title="选择ETF候选母表标准快照",
        filetypes=[("JSON 快照", "*.json"), ("所有文件", "*.*")],
    )
    if selected:
        variable = widgets.get("etf_research_snapshot_var")
        if variable is not None and hasattr(variable, "set"):
            variable.set(str(Path(selected)))


def handle_refresh_status(widgets: Dict[str, tk.Widget]) -> None:
    button = widgets.get("etf_research_refresh_button")
    if button:
        button.config(state=tk.DISABLED)
    _set_status(widgets, "正在读取ETF研究底座状态...")
    controller.request_etf_research_status()


def handle_run_update(widgets: Dict[str, tk.Widget]) -> None:
    variable = widgets.get("etf_research_snapshot_var")
    snapshot_path = variable.get().strip() if hasattr(variable, "get") else ""
    if not snapshot_path:
        messagebox.showwarning("提示", "请先选择标准化候选母表 JSON 快照。")
        return
    if Path(snapshot_path).suffix.lower() != ".json":
        messagebox.showwarning("提示", "候选母表维护入口只接受标准化 JSON 快照。")
        return
    if not messagebox.askyesno(
        "确认ETF研究底座维护",
        "将校验来源哈希、刷新四类原子事实并幂等载入候选母表。\n\n"
        "该操作不产生资金或下单权限。是否继续？",
    ):
        return

    _set_action_state(widgets, tk.DISABLED)
    _set_status(widgets, "已提交ETF研究底座维护任务...")
    controller.request_update_etf_research_foundation(snapshot_path)


def update_status_ui(widgets: Dict[str, tk.Widget], data: Dict[str, Any]) -> None:
    _set_action_state(widgets, tk.NORMAL)
    if data.get("status") != "success":
        message = data.get("error") or "状态读取失败"
        _set_status(widgets, f"ETF研究底座状态读取失败: {message}")
        _write_detail(widgets, str(message))
        return

    tree = widgets.get("etf_research_tree")
    if isinstance(tree, ttk.Treeview):
        tree.delete(*tree.get_children())
        batch = data.get("candidate_batch") or {}
        current = data.get("candidate_current") or {}
        tree.insert(
            "",
            tk.END,
            values=(
                "候选母表版本",
                "fund_pool_on.etf_candidate_master_current_enriched",
                "已载入" if batch else "未载入",
                current.get("row_count", batch.get("row_count", 0)),
                batch.get("product_facts_as_of", "N/A"),
                "人工身份版本化；最新产品事实只并列对照",
            ),
        )
        for fact in data.get("facts") or []:
            tree.insert(
                "",
                tk.END,
                values=(
                    fact.get("label", ""),
                    fact.get("relation", ""),
                    "已创建" if fact.get("exists") else "未创建",
                    _fmt(fact.get("row_count")),
                    _fmt(fact.get("watermark")),
                    fact.get("boundary", ""),
                ),
            )
        coverage = data.get("index_coverage") or {}
        tree.insert(
            "",
            tk.END,
            values=(
                "候选指数覆盖",
                "fund_pool_on.etf_candidate_index_coverage_current",
                "已生成" if coverage else "未生成",
                coverage.get("tracking_index_count", 0),
                _fmt(coverage.get("technical_latest_date")),
                _format_coverage(coverage),
            ),
        )

    _write_detail(widgets, _format_status_detail(data))
    _set_status(widgets, "ETF研究底座状态已更新")


def handle_progress(widgets: Dict[str, tk.Widget], data: Dict[str, Any]) -> None:
    message = data.get("message") or data.get("stage") or "正在维护"
    row_count = data.get("row_count")
    if row_count is not None:
        message = f"{message}（{row_count} 行）"
    _set_status(widgets, message)


def handle_update_complete(widgets: Dict[str, tk.Widget], data: Dict[str, Any]) -> None:
    _set_action_state(widgets, tk.NORMAL)
    if data.get("status") == "success":
        update_status_ui(widgets, data)
        snapshot_id = data.get("snapshot_id", "")
        _set_status(widgets, f"ETF研究底座维护完成: {snapshot_id}")
        controller.request_etf_research_status()
    else:
        error = data.get("error") or "未知错误"
        _set_status(widgets, f"ETF研究底座维护失败: {error}")
        messagebox.showerror("ETF研究底座维护失败", str(error))


def _format_status_detail(data: Dict[str, Any]) -> str:
    batch = data.get("candidate_batch") or {}
    current = data.get("candidate_current") or {}
    coverage = data.get("index_coverage") or {}
    authority = data.get("authority") or {}
    return "\n".join(
        [
            "ETF研究底座当前状态",
            "",
            f"候选版本: {batch.get('snapshot_id', '未载入')}",
            f"来源文件: {batch.get('source_file_name', 'N/A')}",
            f"来源SHA-256: {batch.get('source_file_sha256', 'N/A')}",
            f"工作簿生成日: {_fmt(batch.get('workbook_generated_on'))}",
            f"候选事实日: {_fmt(batch.get('product_facts_as_of'))}",
            f"产品/暴露: {current.get('row_count', 0)} / {current.get('exposure_count', 0)}",
            "候选状态: 正式 {formal} / 条件 {conditional} / 观察 {watch}".format(
                formal=current.get("formal_candidate_count", 0),
                conditional=current.get("conditional_candidate_count", 0),
                watch=current.get("watch_count", 0),
            ),
            "最新产品事实: {covered}/{total}，完整 {complete}，辅助状态变化 {changed}".format(
                covered=current.get("live_product_fact_count", 0),
                total=current.get("row_count", 0),
                complete=current.get("live_complete_count", 0),
                changed=current.get("live_auxiliary_state_change_count", 0),
            ),
            f"候选指数覆盖: {_format_coverage(coverage)}",
            "",
            "权限边界",
            f"资金权限: {_yes_no(authority.get('capital_authority'))}",
            f"下单权限: {_yes_no(authority.get('order_authority'))}",
            "本页维护研究事实和候选版本，不生成策略晋级、账户仓位或订单。",
        ]
    )


def _format_coverage(coverage: Dict[str, Any]) -> str:
    total = coverage.get("tracking_index_count", 0)
    technical = coverage.get("technical_index_count", 0)
    valuation = coverage.get("direct_valuation_index_count", 0)
    return f"技术 {technical}/{total}；直接估值 {valuation}/{total}"


def _yes_no(value: Any) -> str:
    return "是" if value is True else "否"


def _fmt(value: Any) -> str:
    return "N/A" if value in (None, "") else str(value)[:19]


def _set_action_state(widgets: Dict[str, tk.Widget], state: str) -> None:
    for key in (
        "etf_research_refresh_button",
        "etf_research_choose_button",
        "etf_research_run_button",
    ):
        widget = widgets.get(key)
        if widget:
            widget.config(state=state)


def _set_status(widgets: Dict[str, tk.Widget], message: str) -> None:
    label = widgets.get("etf_research_status_label")
    if label:
        label.config(text=message)


def _write_detail(widgets: Dict[str, tk.Widget], text: str) -> None:
    detail = widgets.get("etf_research_detail_text")
    if not isinstance(detail, tk.Text):
        return
    detail.config(state=tk.NORMAL)
    detail.delete("1.0", tk.END)
    detail.insert(tk.END, text)
    detail.config(state=tk.DISABLED)


__all__ = [
    "handle_choose_snapshot",
    "handle_progress",
    "handle_refresh_status",
    "handle_run_update",
    "handle_update_complete",
    "update_status_ui",
]
