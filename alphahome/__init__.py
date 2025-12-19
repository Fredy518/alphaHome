"""
alphahome 包初始化

保持此文件“轻量化”，避免在 import alphahome 时引入大量子包副作用（耗时、日志初始化等）。
子模块请按需显式导入，例如：`import alphahome.gui` 或 `from alphahome import gui`。
"""

__all__ = ["gui", "fetchers", "processors", "factors"]
