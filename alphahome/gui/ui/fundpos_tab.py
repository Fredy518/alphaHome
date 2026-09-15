"""FundPos estimation task tab."""

from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Dict

from ..utils.dpi_aware_ui import get_ui_factory


def create_fundpos_tab(parent: ttk.Frame) -> Dict[str, tk.Widget]:
    widgets: Dict[str, tk.Widget] = {}
    ui_factory = get_ui_factory()

    notice = ttk.LabelFrame(parent, text="运行边界", padding=10)
    notice.pack(side=tk.TOP, fill=tk.X, pady=(0, 8))
    notice_label = ui_factory.create_label(
        notice,
        text=(
            "此页面仅提供环境检查和影子估算。影子结果会勾稽入库，"
            "但不会更新正式发布指针，也不会产生资金或订单动作。"
        ),
    )
    notice_label.pack(side=tk.LEFT)

    top = ui_factory.create_frame(parent)
    top.pack(side=tk.TOP, fill=tk.X, pady=(0, 8))

    refresh_button = ui_factory.create_button(top, text="刷新状态")
    refresh_button.pack(side=tk.LEFT, padx=(0, 5))
    widgets["fundpos_refresh_button"] = refresh_button

    select_all_button = ui_factory.create_button(top, text="全选")
    select_all_button.pack(side=tk.LEFT, padx=(0, 5))
    widgets["fundpos_select_all_button"] = select_all_button

    deselect_all_button = ui_factory.create_button(top, text="取消全选")
    deselect_all_button.pack(side=tk.LEFT, padx=(0, 14))
    widgets["fundpos_deselect_all_button"] = deselect_all_button

    check_button = ui_factory.create_button(top, text="只检查")
    check_button.pack(side=tk.LEFT, padx=(0, 5))
    widgets["fundpos_check_button"] = check_button

    shadow_button = ui_factory.create_button(top, text="运行影子估算（不发布）")
    shadow_button.pack(side=tk.LEFT, padx=(0, 18))
    widgets["fundpos_shadow_button"] = shadow_button

    scope_label = ui_factory.create_label(top, text="范围: 正在读取...")
    scope_label.pack(side=tk.LEFT, padx=(0, 18))
    widgets["fundpos_scope_label"] = scope_label

    observation_label = ui_factory.create_label(top, text="影子观察: --/-- 天")
    observation_label.pack(side=tk.LEFT)
    widgets["fundpos_observation_label"] = observation_label

    paned = ttk.PanedWindow(parent, orient=tk.VERTICAL)
    paned.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

    table_frame = ui_factory.create_frame(paned)
    paned.add(table_frame, weight=3)
    columns = (
        "selected",
        "display_name",
        "family",
        "scope",
        "valuation_date",
        "coverage",
        "status",
        "finished_at",
        "run_id",
    )
    tree = ui_factory.create_treeview(table_frame, columns=columns, show="headings")
    headings = {
        "selected": "选择",
        "display_name": "估算类型",
        "family": "任务标识",
        "scope": "估算范围",
        "valuation_date": "估值日",
        "coverage": "覆盖率",
        "status": "最近状态",
        "finished_at": "最近完成时间",
        "run_id": "最近运行号",
    }
    for column, text in headings.items():
        tree.heading(column, text=text)
    tree.column("selected", width=55, minwidth=45, anchor=tk.CENTER, stretch=False)
    tree.column("display_name", width=120, minwidth=100, stretch=False)
    tree.column("family", width=190, minwidth=160, stretch=False)
    tree.column("scope", width=300, minwidth=210, stretch=False)
    tree.column(
        "valuation_date", width=120, minwidth=105, anchor=tk.CENTER, stretch=False
    )
    tree.column("coverage", width=100, minwidth=85, anchor=tk.CENTER, stretch=False)
    tree.column("status", width=110, minwidth=90, anchor=tk.CENTER, stretch=False)
    tree.column("finished_at", width=210, minwidth=175, anchor=tk.CENTER, stretch=False)
    tree.column("run_id", width=410, minwidth=260, stretch=True)

    vsb = ttk.Scrollbar(table_frame, orient="vertical", command=tree.yview)
    hsb = ttk.Scrollbar(table_frame, orient="horizontal", command=tree.xview)
    tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
    tree.grid(row=0, column=0, sticky="nsew")
    vsb.grid(row=0, column=1, sticky="ns")
    hsb.grid(row=1, column=0, sticky="ew")
    table_frame.grid_rowconfigure(0, weight=1)
    table_frame.grid_columnconfigure(0, weight=1)
    widgets["fundpos_task_tree"] = tree
    tree.insert(
        "",
        tk.END,
        values=("", "正在读取 FundPos 状态...", "", "", "", "", "", "", ""),
    )

    detail_frame = ttk.LabelFrame(paned, text="任务详情", padding=8)
    paned.add(detail_frame, weight=2)
    detail_text = ui_factory.create_text(
        detail_frame,
        wrap=tk.WORD,
        state=tk.DISABLED,
        height=9,
    )
    detail_vsb = ttk.Scrollbar(
        detail_frame,
        orient="vertical",
        command=detail_text.yview,
    )
    detail_text.configure(yscrollcommand=detail_vsb.set)
    detail_text.grid(row=0, column=0, sticky="nsew")
    detail_vsb.grid(row=0, column=1, sticky="ns")
    detail_frame.grid_rowconfigure(0, weight=1)
    detail_frame.grid_columnconfigure(0, weight=1)
    widgets["fundpos_detail_text"] = detail_text

    bottom = ui_factory.create_frame(parent)
    bottom.pack(side=tk.BOTTOM, fill=tk.X, pady=(6, 0))
    status_label = ui_factory.create_label(bottom, text="正在加载 FundPos 状态...")
    status_label.pack(side=tk.LEFT)
    widgets["fundpos_status_label"] = status_label

    return widgets


__all__ = ["create_fundpos_tab"]
