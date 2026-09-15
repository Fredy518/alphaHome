"""User-oriented one-click daily update tab."""

from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Dict

from ..utils.dpi_aware_ui import get_ui_factory


def create_daily_update_tab(parent: ttk.Frame) -> Dict[str, tk.Widget]:
    widgets: Dict[str, tk.Widget] = {}
    ui_factory = get_ui_factory()

    intro = ttk.LabelFrame(parent, text="今天需要做什么", padding=10)
    intro.pack(side=tk.TOP, fill=tk.X, pady=(0, 8))
    intro.columnconfigure(1, weight=1)

    title = ui_factory.create_label(
        intro,
        text="一键智能增量更新",
    )
    title.grid(row=0, column=0, sticky="w", padx=(0, 16))
    explanation = ui_factory.create_label(
        intro,
        text=(
            "按依赖顺序更新数据采集、PIT、因子、Features 和 FundPos。"
            "工作日自动跳过周/月/季度等低频任务，非交易日自动纳入。"
        ),
    )
    explanation.grid(row=0, column=1, sticky="w")

    button_frame = ui_factory.create_frame(parent)
    button_frame.pack(side=tk.TOP, fill=tk.X, pady=(0, 8))

    preview_button = ui_factory.create_button(button_frame, text="预览今日计划")
    preview_button.pack(side=tk.LEFT, padx=(0, 6))
    widgets["daily_update_preview_button"] = preview_button

    run_button = ui_factory.create_button(
        button_frame,
        text="一键智能增量更新",
    )
    run_button.pack(side=tk.LEFT, padx=(0, 6))
    widgets["daily_update_run_button"] = run_button

    stop_button = ui_factory.create_button(button_frame, text="停止", state=tk.DISABLED)
    stop_button.pack(side=tk.LEFT, padx=(0, 18))
    widgets["daily_update_stop_button"] = stop_button

    day_label = ui_factory.create_label(button_frame, text="日期类型: 正在识别...")
    day_label.pack(side=tk.LEFT, padx=(0, 18))
    widgets["daily_update_day_label"] = day_label

    policy_label = ui_factory.create_label(button_frame, text="计划: 尚未生成")
    policy_label.pack(side=tk.LEFT)
    widgets["daily_update_policy_label"] = policy_label

    paned = ttk.PanedWindow(parent, orient=tk.VERTICAL)
    paned.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

    table_frame = ui_factory.create_frame(paned)
    paned.add(table_frame, weight=3)
    columns = (
        "order",
        "domain",
        "schedule",
        "mode",
        "total",
        "run",
        "skip",
        "action",
        "status",
    )
    tree = ui_factory.create_treeview(table_frame, columns=columns, show="headings")
    headings = {
        "order": "顺序",
        "domain": "更新域",
        "schedule": "调度规则",
        "mode": "执行方式",
        "total": "任务数",
        "run": "本次运行",
        "skip": "自动跳过",
        "action": "今日动作",
        "status": "状态",
    }
    for column, text in headings.items():
        tree.heading(column, text=text)
    tree.column("order", width=60, minwidth=50, anchor=tk.CENTER, stretch=False)
    tree.column("domain", width=150, minwidth=120, stretch=False)
    tree.column("schedule", width=250, minwidth=190, stretch=False)
    tree.column("mode", width=140, minwidth=110, anchor=tk.CENTER, stretch=False)
    tree.column("total", width=85, minwidth=70, anchor=tk.E, stretch=False)
    tree.column("run", width=95, minwidth=80, anchor=tk.E, stretch=False)
    tree.column("skip", width=105, minwidth=90, anchor=tk.E, stretch=False)
    tree.column("action", width=420, minwidth=250, stretch=True)
    tree.column("status", width=120, minwidth=100, anchor=tk.CENTER, stretch=False)

    vsb = ttk.Scrollbar(table_frame, orient="vertical", command=tree.yview)
    hsb = ttk.Scrollbar(table_frame, orient="horizontal", command=tree.xview)
    tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
    tree.grid(row=0, column=0, sticky="nsew")
    vsb.grid(row=0, column=1, sticky="ns")
    hsb.grid(row=1, column=0, sticky="ew")
    table_frame.grid_rowconfigure(0, weight=1)
    table_frame.grid_columnconfigure(0, weight=1)
    widgets["daily_update_tree"] = tree
    tree.insert(
        "",
        tk.END,
        values=("", "正在生成今日计划...", "", "", "", "", "", "", ""),
    )

    detail_frame = ttk.LabelFrame(paned, text="计划说明", padding=8)
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
    widgets["daily_update_detail_text"] = detail_text

    bottom = ui_factory.create_frame(parent)
    bottom.pack(side=tk.BOTTOM, fill=tk.X, pady=(6, 0))
    status_label = ui_factory.create_label(bottom, text="正在准备今日计划...")
    status_label.pack(side=tk.LEFT)
    widgets["daily_update_status_label"] = status_label

    return widgets


__all__ = ["create_daily_update_tab"]
