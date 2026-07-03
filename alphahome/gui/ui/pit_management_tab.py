"""PIT management tab UI."""

from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Dict

from ..utils.dpi_aware_ui import get_ui_factory


def create_pit_management_tab(parent: ttk.Frame) -> Dict[str, tk.Widget]:
    widgets: Dict[str, tk.Widget] = {}
    ui_factory = get_ui_factory()

    top_frame = ui_factory.create_frame(parent)
    top_frame.pack(side=tk.TOP, fill=tk.X, pady=(0, 5))

    refresh_button = ui_factory.create_button(top_frame, text="刷新")
    refresh_button.pack(side=tk.LEFT, padx=(0, 5))
    widgets["pit_refresh_button"] = refresh_button

    select_all_button = ui_factory.create_button(top_frame, text="全选")
    select_all_button.pack(side=tk.LEFT, padx=(0, 5))
    widgets["pit_select_all_button"] = select_all_button

    deselect_all_button = ui_factory.create_button(top_frame, text="取消全选")
    deselect_all_button.pack(side=tk.LEFT, padx=(0, 12))
    widgets["pit_deselect_all_button"] = deselect_all_button

    incremental_button = ui_factory.create_button(top_frame, text="增量更新")
    incremental_button.pack(side=tk.LEFT, padx=(0, 5))
    widgets["pit_incremental_button"] = incremental_button

    full_button = ui_factory.create_button(top_frame, text="全量回填")
    full_button.pack(side=tk.LEFT, padx=(0, 5))
    widgets["pit_full_backfill_button"] = full_button

    audit_button = ui_factory.create_button(top_frame, text="只审计")
    audit_button.pack(side=tk.LEFT, padx=(0, 5))
    widgets["pit_audit_button"] = audit_button

    gaps_button = ui_factory.create_button(top_frame, text="查看缺口")
    gaps_button.pack(side=tk.LEFT, padx=(0, 12))
    widgets["pit_view_gaps_button"] = gaps_button

    stock_label = ui_factory.create_label(top_frame, text="股票诊断:")
    stock_label.pack(side=tk.LEFT, padx=(0, 4))
    stock_entry = ui_factory.create_entry(top_frame, width=14)
    stock_entry.insert(0, "002549.SZ")
    stock_entry.pack(side=tk.LEFT, padx=(0, 5))
    widgets["pit_stock_entry"] = stock_entry

    diagnose_button = ui_factory.create_button(top_frame, text="单股诊断")
    diagnose_button.pack(side=tk.LEFT)
    widgets["pit_stock_diagnosis_button"] = diagnose_button

    paned = ttk.PanedWindow(parent, orient=tk.VERTICAL)
    paned.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

    table_frame = ui_factory.create_frame(paned)
    paned.add(table_frame, weight=3)

    columns = (
        "selected",
        "domain",
        "name",
        "output_table",
        "pit_time_key",
        "dependencies",
        "latest_date",
        "row_count",
        "coverage_rate",
        "gap_count",
        "recent_status",
        "last_run_time",
    )
    tree = ui_factory.create_treeview(table_frame, columns=columns, show="headings")
    headings = {
        "selected": "选择",
        "domain": "域",
        "name": "任务名称",
        "output_table": "输出表",
        "pit_time_key": "PIT时间键",
        "dependencies": "依赖",
        "latest_date": "最新日期",
        "row_count": "行数",
        "coverage_rate": "覆盖率",
        "gap_count": "缺口数",
        "recent_status": "最近状态",
        "last_run_time": "最近运行时间",
    }
    for column, text in headings.items():
        tree.heading(column, text=text)

    tree.column("selected", width=48, minwidth=44, anchor=tk.CENTER, stretch=False)
    tree.column("domain", width=100, minwidth=80, anchor=tk.CENTER, stretch=False)
    tree.column("name", width=210, minwidth=160, stretch=False)
    tree.column("output_table", width=230, minwidth=180, stretch=False)
    tree.column("pit_time_key", width=100, minwidth=80, anchor=tk.CENTER, stretch=False)
    tree.column("dependencies", width=210, minwidth=160, stretch=False)
    tree.column("latest_date", width=140, minwidth=120, anchor=tk.CENTER, stretch=False)
    tree.column("row_count", width=110, minwidth=90, anchor=tk.E, stretch=False)
    tree.column("coverage_rate", width=100, minwidth=80, anchor=tk.CENTER, stretch=False)
    tree.column("gap_count", width=90, minwidth=80, anchor=tk.E, stretch=False)
    tree.column("recent_status", width=110, minwidth=90, anchor=tk.CENTER, stretch=False)
    tree.column("last_run_time", width=170, minwidth=150, anchor=tk.CENTER, stretch=False)

    vsb = ttk.Scrollbar(table_frame, orient="vertical", command=tree.yview)
    hsb = ttk.Scrollbar(table_frame, orient="horizontal", command=tree.xview)
    tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
    tree.grid(row=0, column=0, sticky="nsew")
    vsb.grid(row=0, column=1, sticky="ns")
    hsb.grid(row=1, column=0, sticky="ew")
    table_frame.grid_rowconfigure(0, weight=1)
    table_frame.grid_columnconfigure(0, weight=1)
    widgets["pit_task_tree"] = tree
    tree.insert("", tk.END, values=("", "", "正在加载, 请稍候...", "", "", "", "", "", "", "", "", ""))

    detail_frame = ttk.LabelFrame(paned, text="任务详情 / 审计 / 诊断", padding=8)
    paned.add(detail_frame, weight=2)
    detail_text = ui_factory.create_text(detail_frame, wrap=tk.WORD, state=tk.DISABLED, height=10)
    detail_vsb = ttk.Scrollbar(detail_frame, orient="vertical", command=detail_text.yview)
    detail_text.configure(yscrollcommand=detail_vsb.set)
    detail_text.grid(row=0, column=0, sticky="nsew")
    detail_vsb.grid(row=0, column=1, sticky="ns")
    detail_frame.grid_rowconfigure(0, weight=1)
    detail_frame.grid_columnconfigure(0, weight=1)
    widgets["pit_detail_text"] = detail_text

    bottom_frame = ui_factory.create_frame(parent)
    bottom_frame.pack(side=tk.BOTTOM, fill=tk.X, pady=(5, 0))
    status_label = ui_factory.create_label(bottom_frame, text="就绪")
    status_label.pack(side=tk.LEFT)
    widgets["pit_status_label"] = status_label

    return widgets
