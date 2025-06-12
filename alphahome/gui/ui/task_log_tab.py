"""
任务日志标签页 UI

负责创建"任务日志"标签页的全部Tkinter控件。
"""
import tkinter as tk
from tkinter import font as tkFont
from tkinter import ttk, scrolledtext
from typing import Dict, Callable


def create_task_log_tab(
    parent: ttk.Frame, handlers: Dict[str, Callable]
) -> Dict[str, tk.Widget]:
    """
    Creates and populates the Task Log tab.

    Args:
        parent: The parent widget (a ttk.Frame in the notebook).
        handlers: A dictionary mapping handler names to callable functions.

    Returns:
        A dictionary of the created UI widgets for this tab.
    """
    widgets = {}

    # Main frame for the log display and controls
    log_frame = ttk.Frame(parent, padding="10")
    log_frame.pack(expand=True, fill="both")
    log_frame.grid_rowconfigure(0, weight=1)
    log_frame.grid_columnconfigure(0, weight=1)

    # ScrolledText for log output
    log_text = scrolledtext.ScrolledText(
        log_frame, wrap=tk.WORD, state=tk.DISABLED, font=("Consolas", 9)
    )
    log_text.grid(row=0, column=0, columnspan=2, sticky="nsew")
    widgets["task_log_text"] = log_text

    # --- Controls ---
    controls_frame = ttk.Frame(log_frame)
    controls_frame.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(5, 0))

    clear_button = ttk.Button(
        controls_frame,
        text="Clear Log",
        command=handlers.get("handle_clear_log"),
    )
    clear_button.pack(side="right")
    widgets["clear_log_button"] = clear_button

    return widgets 