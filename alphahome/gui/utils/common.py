"""
GUI通用工具函数模块

包含GUI模块中各个组件共用的工具函数，
如数据格式化、状态转换、UI辅助函数等。
"""

from typing import Any, Dict, List
from datetime import datetime

# --- 状态映射 ---
STATUS_MAP_CN = {
    "PENDING": "排队中",
    "RUNNING": "运行中", 
    "SUCCESS": "成功",
    "FAILED": "失败",
    "CANCELED": "已取消",
    "SKIPPED": "已跳过",
    "WARNING": "部分成功",
}


def format_status_chinese(status: str) -> str:
    """将英文状态转换为中文显示"""
    return STATUS_MAP_CN.get(status, status)


def format_datetime_for_display(dt: datetime) -> str:
    """格式化日期时间用于界面显示，正确处理时区转换"""
    if dt is None:
        return ""
    
    # 如果datetime对象有时区信息，转换为本地时区
    if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
        dt = dt.astimezone()  # 转换为本地时区
    
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def safe_get_dict_value(data: Dict[str, Any], key: str, default: Any = "") -> Any:
    """安全获取字典值，避免KeyError"""
    return data.get(key, default)


def validate_date_string(date_str: str) -> bool:
    """验证日期字符串格式是否正确"""
    if not date_str:
        return True  # 空字符串视为有效（可选日期）
    
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def truncate_text(text: str, max_length: int = 50) -> str:
    """截断过长的文本，添加省略号"""
    if len(text) <= max_length:
        return text
    return text[:max_length-3] + "..." 