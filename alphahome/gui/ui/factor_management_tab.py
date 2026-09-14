"""Independent factor management tab."""

from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Dict

from ..utils.dpi_aware_ui import get_ui_factory


def create_factor_management_tab(parent: ttk.Frame) -> Dict[str, tk.Widget]:
    widgets: Dict[str, tk.Widget] = {}
    ui = get_ui_factory()

    top = ui.create_frame(parent)
    top.pack(side=tk.TOP, fill=tk.X, pady=(0, 5))
    buttons = (
        ("factor_refresh_button", "刷新"),
        ("factor_select_all_button", "全选"),
        ("factor_deselect_all_button", "取消全选"),
        ("factor_smart_button", "智能增量"),
        ("factor_manual_button", "指定日期回补"),
        ("factor_full_button", "全量回算"),
        ("factor_audit_button", "只审计"),
        ("factor_gaps_button", "日期缺口"),
    )
    for key, label in buttons:
        button = ui.create_button(top, text=label)
        button.pack(side=tk.LEFT, padx=(0, 5))
        widgets[key] = button

    range_frame = ui.create_frame(parent)
    range_frame.pack(side=tk.TOP, fill=tk.X, pady=(0, 5))
    ui.create_label(range_frame, text="开始日期:").pack(side=tk.LEFT)
    start_entry = ui.create_entry(range_frame, width=12)
    start_entry.pack(side=tk.LEFT, padx=(3, 8))
    widgets["factor_start_date_entry"] = start_entry
    ui.create_label(range_frame, text="结束日期:").pack(side=tk.LEFT)
    end_entry = ui.create_entry(range_frame, width=12)
    end_entry.pack(side=tk.LEFT, padx=(3, 12))
    widgets["factor_end_date_entry"] = end_entry
    ui.create_label(range_frame, text="诊断日期:").pack(side=tk.LEFT)
    date_entry = ui.create_entry(range_frame, width=12)
    date_entry.pack(side=tk.LEFT, padx=(3, 5))
    widgets["factor_diagnose_date_entry"] = date_entry
    date_button = ui.create_button(range_frame, text="日期诊断")
    date_button.pack(side=tk.LEFT, padx=(0, 12))
    widgets["factor_date_diagnosis_button"] = date_button
    ui.create_label(range_frame, text="股票:").pack(side=tk.LEFT)
    stock_entry = ui.create_entry(range_frame, width=14)
    stock_entry.insert(0, "000001.SZ")
    stock_entry.pack(side=tk.LEFT, padx=(3, 5))
    widgets["factor_stock_entry"] = stock_entry
    stock_button = ui.create_button(range_frame, text="单股诊断")
    stock_button.pack(side=tk.LEFT)
    widgets["factor_stock_diagnosis_button"] = stock_button

    paned = ttk.PanedWindow(parent, orient=tk.VERTICAL)
    paned.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
    table_frame = ui.create_frame(paned)
    paned.add(table_frame, weight=3)
    columns = (
        "selected",
        "domain",
        "name",
        "output_table",
        "cadence",
        "formula_version",
        "dependencies",
        "expected_latest_date",
        "actual_latest_date",
        "missing_date_count",
        "nonstandard_date_count",
        "row_count",
        "coverage_rate",
        "last_execution_status",
        "last_execution_time",
        "last_audit_time",
    )
    tree = ui.create_treeview(table_frame, columns=columns, show="headings")
    headings = {
        "selected": "选择",
        "domain": "域",
        "name": "任务",
        "output_table": "输出表",
        "cadence": "周期",
        "formula_version": "公式版本",
        "dependencies": "依赖",
        "expected_latest_date": "预期最新",
        "actual_latest_date": "实际最新",
        "missing_date_count": "缺失",
        "nonstandard_date_count": "非周五",
        "row_count": "行数",
        "coverage_rate": "覆盖率",
        "last_execution_status": "最近执行",
        "last_execution_time": "执行时间",
        "last_audit_time": "审计时间",
    }
    for column, label in headings.items():
        tree.heading(column, text=label)
        tree.column(column, width=110, minwidth=70, anchor=tk.CENTER, stretch=False)
    tree.column("selected", width=48, minwidth=44)
    tree.column("name", width=120)
    tree.column("output_table", width=170)
    tree.column("dependencies", width=120)
    tree.column("row_count", width=100, anchor=tk.E)
    tree.column("last_execution_time", width=165)
    tree.column("last_audit_time", width=165)
    vsb = ttk.Scrollbar(table_frame, orient="vertical", command=tree.yview)
    hsb = ttk.Scrollbar(table_frame, orient="horizontal", command=tree.xview)
    tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
    tree.grid(row=0, column=0, sticky="nsew")
    vsb.grid(row=0, column=1, sticky="ns")
    hsb.grid(row=1, column=0, sticky="ew")
    table_frame.grid_rowconfigure(0, weight=1)
    table_frame.grid_columnconfigure(0, weight=1)
    widgets["factor_task_tree"] = tree
    tree.insert("", tk.END, values=("", "", "正在加载, 请稍候..."))

    detail_frame = ttk.LabelFrame(
        paned, text="任务详情 / 预检 / 审计 / 诊断", padding=8
    )
    paned.add(detail_frame, weight=2)
    detail = ui.create_text(detail_frame, wrap=tk.WORD, state=tk.DISABLED, height=10)
    detail_vsb = ttk.Scrollbar(detail_frame, orient="vertical", command=detail.yview)
    detail.configure(yscrollcommand=detail_vsb.set)
    detail.grid(row=0, column=0, sticky="nsew")
    detail_vsb.grid(row=0, column=1, sticky="ns")
    detail_frame.grid_rowconfigure(0, weight=1)
    detail_frame.grid_columnconfigure(0, weight=1)
    widgets["factor_detail_text"] = detail

    bottom = ui.create_frame(parent)
    bottom.pack(side=tk.BOTTOM, fill=tk.X, pady=(5, 0))
    status = ui.create_label(bottom, text="就绪")
    status.pack(side=tk.LEFT)
    widgets["factor_status_label"] = status
    return widgets


__all__ = ["create_factor_management_tab"]
