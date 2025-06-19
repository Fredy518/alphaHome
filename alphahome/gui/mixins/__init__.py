"""
GUI Mixins Module

包含用于拆分MainWindow功能的Mixin类，以提高代码的可维护性和可读性。

各Mixin职责：
- WindowEventsMixin: UI事件绑定管理
- WindowDpiMixin: DPI和显示设置管理  
- WindowLayoutMixin: UI组件创建和布局管理
"""

from .window_events_mixin import WindowEventsMixin
from .window_dpi_mixin import WindowDpiMixin  
from .window_layout_mixin import WindowLayoutMixin

__all__ = [
    "WindowEventsMixin",
    "WindowDpiMixin", 
    "WindowLayoutMixin",
]