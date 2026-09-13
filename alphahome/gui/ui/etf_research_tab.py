"""ETF research-foundation maintenance tab UI."""

from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Dict

from ..utils.dpi_aware_ui import get_ui_factory


def create_etf_research_tab(parent: ttk.Frame) -> Dict[str, tk.Widget]:
    widgets: Dict[str, tk.Widget] = {}
    ui_factory = get_ui_factory()

    title = ui_factory.create_label(
        parent,
        text="ETF研究底座：候选版本、产品事实、指数技术、直接估值与行业盈利",
    )
    title.pack(side=tk.TOP, anchor=tk.W, pady=(0, 4))
    boundary = ui_factory.create_label(
        parent,
        text=("仅维护研究事实和候选池版本；不生成策略晋级、资金权限、账户仓位或订单。"),
    )
    boundary.pack(side=tk.TOP, anchor=tk.W, pady=(0, 8))

    action_frame = ttk.LabelFrame(parent, text="统一维护入口", padding=8)
    action_frame.pack(side=tk.TOP, fill=tk.X, pady=(0, 8))

    path_label = ui_factory.create_label(action_frame, text="标准快照 JSON:")
    path_label.grid(row=0, column=0, padx=(0, 6), sticky=tk.W)
    snapshot_var = tk.StringVar(value="")
    snapshot_entry = ui_factory.create_entry(action_frame, width=100)
    snapshot_entry.config(textvariable=snapshot_var)
    snapshot_entry.grid(row=0, column=1, padx=(0, 6), sticky="ew")
    action_frame.grid_columnconfigure(1, weight=1)
    widgets["etf_research_snapshot_var"] = snapshot_var
    widgets["etf_research_snapshot_entry"] = snapshot_entry

    choose_button = ui_factory.create_button(action_frame, text="选择快照")
    choose_button.grid(row=0, column=2, padx=(0, 6))
    widgets["etf_research_choose_button"] = choose_button

    run_button = ui_factory.create_button(action_frame, text="校验并统一维护")
    run_button.grid(row=0, column=3, padx=(0, 6))
    widgets["etf_research_run_button"] = run_button

    refresh_button = ui_factory.create_button(action_frame, text="刷新状态")
    refresh_button.grid(row=0, column=4)
    widgets["etf_research_refresh_button"] = refresh_button

    hint = ui_factory.create_label(
        action_frame,
        text=(
            "先在研究工作区导出标准化 JSON；GUI 会复核源工作簿哈希，并按固定顺序刷新和入库。"
        ),
    )
    hint.grid(row=1, column=0, columnspan=5, pady=(6, 0), sticky=tk.W)

    paned = ttk.PanedWindow(parent, orient=tk.VERTICAL)
    paned.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

    table_frame = ui_factory.create_frame(paned)
    paned.add(table_frame, weight=3)
    columns = ("object", "relation", "status", "row_count", "watermark", "boundary")
    tree = ui_factory.create_treeview(table_frame, columns=columns, show="headings")
    headings = {
        "object": "维护对象",
        "relation": "AlphaDB对象",
        "status": "状态",
        "row_count": "行数/对象数",
        "watermark": "数据截至",
        "boundary": "口径与边界",
    }
    for column, text in headings.items():
        tree.heading(column, text=text)
    tree.column("object", width=180, minwidth=140, stretch=False)
    tree.column("relation", width=410, minwidth=260, stretch=True)
    tree.column("status", width=100, minwidth=80, anchor=tk.CENTER, stretch=False)
    tree.column("row_count", width=120, minwidth=90, anchor=tk.E, stretch=False)
    tree.column("watermark", width=150, minwidth=120, anchor=tk.CENTER, stretch=False)
    tree.column("boundary", width=360, minwidth=220, stretch=True)

    vsb = ttk.Scrollbar(table_frame, orient="vertical", command=tree.yview)
    hsb = ttk.Scrollbar(table_frame, orient="horizontal", command=tree.xview)
    tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
    tree.grid(row=0, column=0, sticky="nsew")
    vsb.grid(row=0, column=1, sticky="ns")
    hsb.grid(row=1, column=0, sticky="ew")
    table_frame.grid_rowconfigure(0, weight=1)
    table_frame.grid_columnconfigure(0, weight=1)
    widgets["etf_research_tree"] = tree

    detail_frame = ttk.LabelFrame(paned, text="候选版本与覆盖摘要", padding=8)
    paned.add(detail_frame, weight=2)
    detail_text = ui_factory.create_text(
        detail_frame,
        wrap=tk.WORD,
        state=tk.DISABLED,
        height=10,
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
    widgets["etf_research_detail_text"] = detail_text

    bottom_frame = ui_factory.create_frame(parent)
    bottom_frame.pack(side=tk.BOTTOM, fill=tk.X, pady=(5, 0))
    status_label = ui_factory.create_label(bottom_frame, text="正在读取当前状态...")
    status_label.pack(side=tk.LEFT)
    widgets["etf_research_status_label"] = status_label

    return widgets


__all__ = ["create_etf_research_tab"]
