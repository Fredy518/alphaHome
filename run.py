#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from pathlib import Path

# 将项目根目录添加到sys.path
# 这确保了无论从哪里运行run.py，模块导入都能正常工作
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))

from alphahome.gui.main_window import run_gui
from research import initialize_research_environment

if __name__ == "__main__":
    try:
        # 在启动时初始化投研工作台环境
        initialize_research_environment()
        
        run_gui()
    except KeyboardInterrupt:
        print("应用程序被用户中断。")
    except Exception as e:
        # 在这里可以添加更复杂的日志记录或错误对话框
        print(f"应用程序遇到未处理的错误: {e}")
        # 在GUI未启动时，这可能是唯一的错误反馈方式
        # 如果有GUI，应该在GUI层面处理
