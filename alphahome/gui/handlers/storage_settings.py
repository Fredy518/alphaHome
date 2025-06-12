"""
存储设置UI事件处理器

负责为"存储设置"标签页提供UI更新和数据提取的辅助函数。
"""
import tkinter as tk
from tkinter import messagebox, ttk
import urllib.parse
from typing import Dict, Any

from async_tkinter_loop import async_handler
from ..controller_logic import storage_settings as storage_logic

# This module no longer sends requests directly to the controller.
# It provides helper functions for the main_window to call.
# The main_window is responsible for dispatching requests.

def update_storage_settings_display(widgets: Dict[str, tk.Widget], settings: Dict[str, Any]):
    """根据从控制器接收的设置数据，填充存储设置标签页的各个字段。"""
    try:
        db_config = settings.get("database", {})
        api_config = settings.get("api", {})
        db_url = db_config.get("url", "")
        tushare_token = api_config.get("tushare_token", "")

        # 解析URL并填充UI
        _populate_db_fields_from_url(widgets, db_url)

        # 填充Token
        token_entry = widgets.get("tushare_token")
        if token_entry:
            token_entry.delete(0, tk.END)
            token_entry.insert(0, tushare_token)

        _update_status(widgets, "设置已成功加载。")

    except Exception as e:
        messagebox.showerror("UI 更新错误", f"更新设置界面时发生意外错误: {e}")
        _update_status(widgets, f"UI 更新错误: {e}")

def get_settings_from_ui(widgets: Dict[str, tk.Widget]) -> Dict[str, Any]:
    """Helper to extract current settings from UI widgets."""
    # 从UI收集数据库信息
    db_values = {
        "db_host": widgets["db_host"].get(),
        "db_port": widgets["db_port"].get(),
        "db_name": widgets["db_name"].get(),
        "db_user": widgets["db_user"].get(),
        "db_password": widgets["db_password"].get()
    }
    
    # 构建URL
    db_url = ""
    # Only build URL if all user/host/db fields are filled
    if all(db_values[k] for k in ["db_host", "db_name", "db_user"]): 
        db_url = urllib.parse.urlunparse(
            (
                "postgresql",
                f"{db_values['db_user']}:{db_values['db_password']}@{db_values['db_host']}:{db_values['db_port'] or '5432'}",
                f"/{db_values['db_name']}",
                "", "", ""
            )
        )
    
    # 收集Tushare Token
    tushare_token = widgets.get("tushare_token", tk.Entry()).get()

    return {"db_url": db_url, "tushare_token": tushare_token}

async def handle_test_db_connection(widgets: Dict[str, tk.Widget]):
    """处理测试数据库连接按钮点击事件。"""
    # 从UI收集数据库信息 to build a temporary URL for testing
    settings = get_settings_from_ui(widgets)
    db_url = settings.get("db_url")

    info_label = widgets.get("settings_info_label")
    if info_label:
        info_label.config(text="正在测试数据库连接...")
    
    result = await storage_logic.test_database_connection(db_url)
    
    if result["status"] == "success":
        messagebox.showinfo("连接成功", result["message"])
    else:
        messagebox.showerror("连接失败", result["message"])
        
    if info_label:
        info_label.config(text=result["message"])

def _populate_db_fields_from_url(widgets: Dict[str, tk.Widget], db_url: str):
    """根据数据库URL填充UI输入框。"""
    db_keys = ["db_host", "db_port", "db_name", "db_user", "db_password"]
    
    # 清空现有值
    for key in db_keys:
        if widgets.get(key):
            widgets[key].delete(0, tk.END)

    if not db_url:
        return

    try:
        parsed_url = urllib.parse.urlparse(db_url)
        widgets["db_host"].insert(0, parsed_url.hostname or "")
        widgets["db_port"].insert(0, str(parsed_url.port or ""))
        widgets["db_user"].insert(0, parsed_url.username or "")
        widgets["db_password"].insert(0, parsed_url.password or "")
        widgets["db_name"].insert(0, parsed_url.path.lstrip("/") or "")
    except Exception as e:
        messagebox.showwarning("URL解析错误", f"无法解析数据库URL: {db_url}\n{e}")

def _update_status(widgets: Dict[str, tk.Widget], message: str):
    """更新状态栏或信息标签。"""
    statusbar = widgets.get("settings_info_label") # Corrected to use the info label
    if statusbar:
        statusbar.config(text=message) 