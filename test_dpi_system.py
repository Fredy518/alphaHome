#!/usr/bin/env python
"""
高DPI适配系统测试脚本

测试DPI管理器、UI工厂、动态列管理器等组件的功能
"""
import sys
import os
import tkinter as tk
from tkinter import ttk

# 添加项目根目录到sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_dpi_system():
    """测试高DPI适配系统"""
    print("=== 高DPI适配系统测试 ===")
    
    # 创建测试窗口
    root = tk.Tk()
    root.title("高DPI适配系统测试")
    root.geometry("1000x700")
    
    try:
        # 测试DPI管理器
        print("\n1. 测试DPI管理器...")
        from alphahome.gui.utils.dpi_manager import initialize_dpi_manager, DisplayMode
        dpi_manager = initialize_dpi_manager()
        
        print(f"   ✓ DPI管理器初始化成功")
        print(f"   - 逻辑分辨率: {dpi_manager.dpi_info.logical_resolution}")
        print(f"   - 物理分辨率: {dpi_manager.dpi_info.physical_resolution}")
        print(f"   - DPI缩放比例: {dpi_manager.dpi_info.scale_factor:.2f}")
        print(f"   - 当前显示模式: {dpi_manager.current_mode.value}")
        print(f"   - 推荐显示模式: {dpi_manager.recommend_display_mode().value}")
        
        # 测试UI工厂
        print("\n2. 测试DPI感知UI工厂...")
        from alphahome.gui.utils.dpi_aware_ui import initialize_ui_factory
        ui_factory = initialize_ui_factory()
        
        print(f"   ✓ UI工厂初始化成功")
        print(f"   - 字体缩放因子: {ui_factory.dpi_manager.scale_factors.font_scale:.2f}")
        print(f"   - 行高缩放因子: {ui_factory.dpi_manager.scale_factors.row_height_scale:.2f}")
        print(f"   - 列宽缩放因子: {ui_factory.dpi_manager.scale_factors.column_width_scale:.2f}")
        
        # 创建测试界面
        print("\n3. 创建测试界面...")
        
        # 主框架
        main_frame = ui_factory.create_frame(root, padding=10)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # 信息显示区域
        info_frame = ui_factory.create_labelframe(main_frame, text="DPI信息", padding=10)
        info_frame.pack(fill=tk.X, pady=(0, 10))
        
        info_text = f"分辨率: {dpi_manager.dpi_info.logical_resolution[0]}x{dpi_manager.dpi_info.logical_resolution[1]}\n"
        info_text += f"DPI缩放: {dpi_manager.dpi_info.scale_factor:.0%}\n"
        info_text += f"显示模式: {dpi_manager.current_mode.value}"
        
        info_label = ui_factory.create_label(info_frame, text=info_text, justify=tk.LEFT)
        info_label.pack(anchor="w")
        
        # 控制区域
        control_frame = ui_factory.create_frame(main_frame)
        control_frame.pack(fill=tk.X, pady=(0, 10))
        
        # 显示模式切换
        mode_label = ui_factory.create_label(control_frame, text="显示模式:")
        mode_label.pack(side=tk.LEFT, padx=(0, 5))
        
        mode_values = ["自动检测", "标准模式", "高DPI模式", "4K优化模式"]
        mode_combo = ui_factory.create_combobox(control_frame, values=mode_values, state="readonly", width=15)
        mode_combo.set(mode_values[0])  # 默认自动检测
        mode_combo.pack(side=tk.LEFT, padx=(0, 10))
        
        def switch_mode():
            selected = mode_combo.get()
            mode_map = {
                "自动检测": DisplayMode.AUTO,
                "标准模式": DisplayMode.STANDARD,
                "高DPI模式": DisplayMode.HIGH_DPI,
                "4K优化模式": DisplayMode.UHD_4K
            }
            
            new_mode = mode_map.get(selected)
            if new_mode:
                print(f"\n   切换到显示模式: {new_mode.value}")
                dpi_manager.set_display_mode(new_mode)
                
                # 刷新UI工厂
                from alphahome.gui.utils.dpi_aware_ui import refresh_ui_factory
                refresh_ui_factory()
                
                # 更新信息显示
                new_info_text = f"分辨率: {dpi_manager.dpi_info.logical_resolution[0]}x{dpi_manager.dpi_info.logical_resolution[1]}\n"
                new_info_text += f"DPI缩放: {dpi_manager.dpi_info.scale_factor:.0%}\n"
                new_info_text += f"显示模式: {dpi_manager.current_mode.value}"
                info_label.config(text=new_info_text)
                
                # 刷新表格
                if hasattr(tree, '_column_manager'):
                    tree._column_manager.refresh_for_dpi_change()
                
                print(f"   ✓ 模式切换完成")
        
        apply_button = ui_factory.create_button(control_frame, text="应用", command=switch_mode)
        apply_button.pack(side=tk.LEFT)
        
        # 测试表格
        print("\n4. 测试动态列管理器...")
        table_frame = ui_factory.create_labelframe(main_frame, text="测试表格", padding=10)
        table_frame.pack(fill=tk.BOTH, expand=True)
        
        columns = ("col1", "col2", "col3", "col4")
        tree = ui_factory.create_treeview(table_frame, columns=columns, show="headings")
        
        # 设置列标题
        tree.heading("col1", text="列1")
        tree.heading("col2", text="列2")
        tree.heading("col3", text="列3")
        tree.heading("col4", text="列4")
        
        # 创建动态列管理器
        from alphahome.gui.utils.layout_manager import DynamicColumnManager, ColumnConfig
        manager = DynamicColumnManager(tree)
        
        # 添加列配置
        configs = [
            ColumnConfig("col1", "列1", min_width=80, preferred_width=120, weight=1.0, stretch=False),
            ColumnConfig("col2", "列2", min_width=150, preferred_width=250, weight=2.0, stretch=True),
            ColumnConfig("col3", "列3", min_width=100, preferred_width=150, weight=1.0, stretch=False),
            ColumnConfig("col4", "列4", min_width=200, preferred_width=300, weight=3.0, stretch=True),
        ]
        
        for config in configs:
            manager.add_column(config)
        
        tree._column_manager = manager
        
        # 添加测试数据
        for i in range(10):
            tree.insert("", "end", values=(
                f"数据{i}",
                f"这是一个测试数据项目{i}，用来测试DPI适配效果",
                f"状态{i}",
                f"详细信息{i} - 在不同DPI模式下测试显示效果和文本重叠问题"
            ))
        
        # 添加滚动条
        scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=scrollbar.set)
        
        tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # 配置列宽和绑定事件
        manager.bind_resize_event()
        tree.after_idle(manager.configure_columns)
        
        print(f"   ✓ 动态列管理器配置完成")
        
        # 测试按钮
        test_frame = ui_factory.create_frame(main_frame)
        test_frame.pack(fill=tk.X, pady=(10, 0))
        
        def test_column_info():
            info = manager.get_column_info()
            print("\n   当前列信息:")
            for col_info in info:
                print(f"     {col_info['column_id']}: 宽度={col_info['current_width']}, 最小={col_info['min_width']}")
        
        def test_reconfigure():
            print("\n   重新配置列宽...")
            manager.configure_columns()
            print("   ✓ 重新配置完成")
        
        info_button = ui_factory.create_button(test_frame, text="显示列信息", command=test_column_info)
        info_button.pack(side=tk.LEFT, padx=(0, 5))
        
        reconfig_button = ui_factory.create_button(test_frame, text="重新配置", command=test_reconfigure)
        reconfig_button.pack(side=tk.LEFT, padx=(0, 5))
        
        def quit_test():
            print("\n=== 测试完成 ===")
            root.quit()
            root.destroy()
        
        quit_button = ui_factory.create_button(test_frame, text="退出测试", command=quit_test)
        quit_button.pack(side=tk.RIGHT)
        
        print(f"   ✓ 测试界面创建完成")
        
        print("\n=== 测试界面已启动 ===")
        print("请测试以下功能:")
        print("1. 切换不同的显示模式")
        print("2. 调整窗口大小观察表格适配")
        print("3. 检查文字是否清晰，列宽是否合理")
        print("4. 验证4K优化模式下是否解决文本重叠")
        
        # 启动GUI
        root.mainloop()
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        root.destroy()

if __name__ == "__main__":
    test_dpi_system() 