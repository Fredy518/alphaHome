"""
任务日志UI事件处理器

负责处理"任务日志"标签页上的用户交互事件。
"""
import tkinter as tk
from tkinter import scrolledtext
from typing import Dict, Union
from datetime import datetime

from ...common.logging_utils import get_logger

logger = get_logger(__name__)


def update_task_log(ui_elements: Dict[str, tk.Widget], log_data: Union[str, Dict]):
    """Appends new log data to the task log text widget."""
    log_text_widget = ui_elements.get("task_log_text")
    if log_text_widget and isinstance(log_text_widget, scrolledtext.ScrolledText):
        # Store current state
        is_disabled = log_text_widget.cget("state") == tk.DISABLED

        # Enable widget to modify content
        if is_disabled:
            log_text_widget.config(state=tk.NORMAL)

        # 格式化日志数据
        if isinstance(log_data, dict):
            timestamp = datetime.now().strftime("%H:%M:%S")
            level = log_data.get("level", "INFO").upper()
            message = log_data.get("message", str(log_data))
            formatted_log = f"[{timestamp}] {level}: {message}\n"
        else:
            formatted_log = str(log_data) + "\n" if not str(log_data).endswith("\n") else str(log_data)

        log_text_widget.insert(tk.END, formatted_log)
        log_text_widget.see(tk.END)  # Auto-scroll to the end

        # Restore original state
        if is_disabled:
            log_text_widget.config(state=tk.DISABLED)
    else:
        logger.warning("Task log widget not found or is of incorrect type.")


def handle_clear_log(ui_elements: Dict[str, tk.Widget]):
    """Clears all text from the task log widget."""
    log_text_widget = ui_elements.get("task_log_text")
    if log_text_widget and isinstance(log_text_widget, scrolledtext.ScrolledText):
        logger.info("Clearing task log display.")
        # Store current state
        is_disabled = log_text_widget.cget("state") == tk.DISABLED

        # Enable widget to modify content
        if is_disabled:
            log_text_widget.config(state=tk.NORMAL)

        log_text_widget.delete("1.0", tk.END)

        # Restore original state
        if is_disabled:
            log_text_widget.config(state=tk.DISABLED)
    else:
        logger.warning("Task log widget not found for clearing.") 