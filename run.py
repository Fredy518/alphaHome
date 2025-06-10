#!/usr/bin/env python
# -*- coding: utf-8 -*-

from alphahome.common.logging_utils import setup_logging
from alphahome.gui.main_window import run_gui

if __name__ == "__main__":
    # 在应用程序入口点初始化日志系统
    setup_logging(log_level="INFO", log_to_file=True)
    run_gui()
