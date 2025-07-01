"""
窗口DPI和显示设置Mixin

负责处理MainWindow的DPI感知和显示设置功能，包括：
- 窗口大小变化处理
- 表格列宽重新计算
- DPI模式切换刷新
- 显示设置应用
- 应用程序重启
- 显示信息更新
"""

from ...common.logging_utils import get_logger
from ..utils.dpi_manager import DisplayMode


class WindowDpiMixin:
    """窗口DPI和显示设置Mixin类"""
    
    def _on_window_configure(self, event):
        """处理窗口大小变化事件，触发表格列宽重新计算"""
        # 只处理主窗口的configure事件，避免子组件事件干扰
        if event.widget == self:
            # 延迟执行以避免频繁调用，并确保布局稳定
            self.after_idle(self._reconfigure_all_tables)
            
    def _reconfigure_all_tables(self):
        """重新配置所有表格的列宽"""
        try:
            # 重新配置数据采集表格
            collection_tree = self.ui_elements.get("collection_task_tree")
            if collection_tree and hasattr(collection_tree, '_column_manager'):
                collection_tree._column_manager.configure_columns()
                
            # 重新配置任务状态表格
            status_tree = self.ui_elements.get("task_status_tree")
            if status_tree and hasattr(status_tree, '_column_manager'):
                status_tree._column_manager.configure_columns()
                
        except Exception as e:
            logger = get_logger("main_window")
            logger.warning(f"重新配置表格列宽时出错: {e}")
    
    def refresh_for_dpi_change(self):
        """DPI模式切换时刷新所有UI元素"""
        logger = get_logger("main_window")
        logger.info("开始刷新UI以适配DPI变化")
        
        try:
            # 刷新UI工厂
            from ..utils.dpi_aware_ui import refresh_ui_factory
            refresh_ui_factory()
            
            # 刷新所有表格的列管理器
            collection_tree = self.ui_elements.get("collection_task_tree")
            if collection_tree and hasattr(collection_tree, '_column_manager'):
                collection_tree._column_manager.refresh_for_dpi_change()
                
            status_tree = self.ui_elements.get("task_status_tree")
            if status_tree and hasattr(status_tree, '_column_manager'):
                status_tree._column_manager.refresh_for_dpi_change()
            
            # 强制重新布局
            self.update_idletasks()
            self._reconfigure_all_tables()
            
            logger.info("DPI适配刷新完成")
            
        except Exception as e:
            logger.error(f"DPI适配刷新失败: {e}")
    
    def apply_display_settings(self):
        """应用显示设置"""
        logger = get_logger("main_window")
        
        try:
            # 获取选择的显示模式
            mode_combo = self.ui_elements.get("display_mode_combo")
            mode_values = self.ui_elements.get("display_mode_values")
            
            if not mode_combo or not mode_values:
                logger.warning("显示设置控件未找到")
                return
            
            selected_display_name = mode_combo.get()
            selected_mode_value = None
            
            # 查找对应的模式值
            for display_name, mode_value in mode_values:
                if display_name == selected_display_name:
                    selected_mode_value = mode_value
                    break
            
            if selected_mode_value is None:
                logger.warning(f"无效的显示模式选择: {selected_display_name}")
                return
            
            # 转换为DisplayMode枚举
            try:
                new_mode = DisplayMode(selected_mode_value)
            except ValueError:
                logger.error(f"无效的显示模式值: {selected_mode_value}")
                return
            
            # 应用新的显示模式
            current_mode = self.dpi_manager.current_mode
            if new_mode != current_mode:
                logger.info(f"切换显示模式: {current_mode.value} -> {new_mode.value}")
                
                # 设置新模式
                self.dpi_manager.set_display_mode(new_mode)
                
                # 刷新UI
                self.refresh_for_dpi_change()
                
                # 更新显示信息
                self.update_display_info()
                
                # 强制重新绘制整个窗口
                self.update()
                self.update_idletasks()
                
                # 显示成功消息
                from tkinter import messagebox
                messagebox.showinfo(
                    "显示设置", 
                    f"显示模式已切换为: {selected_display_name}\n\n"
                    f"界面已自动调整以适配新的显示模式。\n"
                    f"如果部分元素显示异常，请重启应用程序。"
                )
            else:
                logger.info("显示模式未变化，无需切换")
                
        except Exception as e:
            logger.error(f"应用显示设置失败: {e}")
            from tkinter import messagebox
            messagebox.showerror("错误", f"应用显示设置时发生错误:\n{e}")
    
    def restart_application(self):
        """重启应用程序"""
        from tkinter import messagebox
        import sys
        import os
        import subprocess

        logger = get_logger("main_window")

        # 确认重启
        if messagebox.askyesno(
            "重启应用",
            "重启应用程序将关闭当前窗口并重新启动。\n\n确定要继续吗？"
        ):
            try:
                logger.info("用户确认重启应用程序")

                # 获取当前Python可执行文件和脚本路径
                python_exe = sys.executable
                script_path = os.path.abspath(sys.argv[0])

                logger.info(f"原始脚本路径: {script_path}")

                # 尝试多种方式找到正确的启动脚本
                run_script_found = False

                # 方法1: 如果是通过模块运行的，查找run.py
                if script_path.endswith('__main__.py') or 'alphahome' in os.path.basename(script_path):
                    # 从当前文件位置向上查找项目根目录
                    current_dir = os.path.dirname(os.path.abspath(__file__))
                    # window_dpi_mixin.py -> mixins -> gui -> alphahome -> 项目根目录
                    project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
                    run_script = os.path.join(project_root, 'run.py')

                    logger.info(f"计算的项目根目录: {project_root}")
                    logger.info(f"查找run.py路径: {run_script}")

                    if os.path.exists(run_script):
                        script_path = run_script
                        run_script_found = True
                        logger.info(f"找到run.py: {run_script}")
                    else:
                        logger.warning(f"方法1未找到run.py: {run_script}")

                # 方法2: 如果方法1失败，尝试从sys.path查找
                if not run_script_found:
                    for path in sys.path:
                        if path and os.path.isdir(path):
                            potential_run = os.path.join(path, 'run.py')
                            if os.path.exists(potential_run):
                                # 验证这是正确的run.py（包含alphahome导入）
                                try:
                                    with open(potential_run, 'r', encoding='utf-8') as f:
                                        content = f.read()
                                        if 'alphahome' in content and 'run_gui' in content:
                                            script_path = potential_run
                                            run_script_found = True
                                            logger.info(f"通过sys.path找到run.py: {potential_run}")
                                            break
                                except Exception:
                                    continue

                # 方法3: 如果仍未找到，尝试使用模块启动方式
                if not run_script_found:
                    logger.warning("未找到run.py，尝试使用模块启动方式")
                    # 使用 python -m alphahome 的方式启动
                    try:
                        subprocess.Popen([python_exe, '-m', 'alphahome'],
                                       creationflags=subprocess.CREATE_NEW_CONSOLE if os.name == 'nt' else 0)
                        logger.info("使用模块方式启动新进程成功，关闭当前应用")
                        self.destroy()
                        return
                    except Exception as module_error:
                        logger.error(f"模块启动方式失败: {module_error}")
                        # 继续使用原始脚本路径
                        logger.warning(f"回退到原始脚本路径: {script_path}")

                logger.info(f"最终重启命令: {python_exe} {script_path}")

                # 启动新进程
                cwd = os.path.dirname(script_path) if os.path.isfile(script_path) else os.getcwd()
                subprocess.Popen([python_exe, script_path],
                               cwd=cwd,
                               creationflags=subprocess.CREATE_NEW_CONSOLE if os.name == 'nt' else 0)

                # 关闭当前应用
                logger.info("启动新进程成功，关闭当前应用")
                self.destroy()

            except Exception as e:
                logger.error(f"重启应用失败: {e}")
                messagebox.showerror("重启失败", f"重启应用程序时发生错误:\n{e}")
    
    def update_display_info(self):
        """更新显示信息标签"""
        try:
            display_info_label = self.ui_elements.get("display_info_label")
            if display_info_label:
                info_text = f"当前分辨率: {self.dpi_manager.dpi_info.logical_resolution[0]}x{self.dpi_manager.dpi_info.logical_resolution[1]}\n"
                info_text += f"DPI缩放: {self.dpi_manager.dpi_info.scale_factor:.0%}\n"
                info_text += f"高DPI环境: {'是' if self.dpi_manager.dpi_info.is_high_dpi else '否'}"
                display_info_label.config(text=info_text)
        except Exception as e:
            logger = get_logger("main_window")
            logger.warning(f"更新显示信息失败: {e}") 