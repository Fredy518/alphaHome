"""
AlphaHome GUI模块

本模块提供AlphaHome智能量化投研系统的图形用户界面，采用Tkinter实现。
GUI模块负责用户交互、数据展示、任务配置和系统设置等功能。

## 模块架构

### 核心组件
- `main_window.py`: 主窗口类，应用程序入口，负责窗口初始化和事件循环管理
- `controller.py`: UI控制器，负责前后端通信和业务逻辑调度

### 功能模块
- `handlers/`: 业务逻辑处理器，处理各个功能域的用户交互和数据操作
  - `data_collection_handler.py`: 数据采集任务管理
  - `data_processing_handler.py`: 数据处理任务管理  
  - `task_execution_handler.py`: 任务执行控制
  - `storage_settings_handler.py`: 存储设置管理
  - `task_log_handler.py`: 任务日志处理

- `ui/`: UI组件创建模块，负责各个标签页的界面构建
  - `data_collection_tab.py`: 数据采集标签页
  - `data_processing_tab.py`: 数据处理标签页
  - `task_execution_tab.py`: 任务执行标签页
  - `storage_settings_tab.py`: 存储设置标签页
  - `task_log_tab.py`: 任务日志标签页

- `services/`: GUI业务服务，提供数据处理和业务逻辑支持
  - `task_registry_service.py`: 任务注册和发现服务
  - `task_execution_service.py`: 任务执行引擎服务
  - `configuration_service.py`: 配置管理服务

### 支撑组件
- `mixins/`: 功能混入类，用于分离主窗口的职责
  - `WindowEventsMixin`: UI事件绑定管理
  - `WindowDpiMixin`: DPI感知和显示设置
  - `WindowLayoutMixin`: UI布局和组件创建

- `utils/`: 工具类和辅助函数
  - `dpi_manager.py`: 高DPI环境检测和适配
  - `dpi_aware_ui.py`: DPI感知的UI组件工厂
  - `layout_manager.py`: 表格列布局管理
  - `screen_utils.py`: 屏幕信息和窗口定位
  - `common.py`: 通用工具函数

## 技术特性

### 高DPI支持
- 自动检测系统DPI环境
- 动态调整UI组件尺寸和间距
- 支持4K显示器优化

### 异步架构
- 前后端分离设计
- 异步任务执行，避免界面冻结
- 响应式用户交互

### 模块化设计
- 职责清晰的模块划分
- 松耦合的组件关系
- 便于扩展和维护

## 使用方式

```python
from alphahome.gui.main_window import run_gui

# 启动GUI应用
run_gui()
```

## 依赖要求
- Python 3.8+
- tkinter (Python标准库)
- async-tkinter-loop (异步事件循环支持)
"""

from . import controller, main_window
