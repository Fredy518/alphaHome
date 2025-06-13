#!/usr/bin/env python
"""
4K屏幕显示问题诊断脚本

专门用于诊断4K高DPI屏幕上的GUI显示问题
"""
import tkinter as tk
from tkinter import ttk
import sys
import os

# 添加项目根目录到sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def create_diagnostic_window():
    """创建诊断窗口"""
    root = tk.Tk()
    root.title("4K屏幕诊断工具")
    
    # 启用DPI感知
    try:
        import ctypes
        ctypes.windll.shcore.SetProcessDpiAwarenessContext(ctypes.c_ssize_t(-4))
        print("✓ DPI感知已启用")
    except:
        print("❌ DPI感知启用失败")
    
    # 获取屏幕信息
    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()
    dpi = root.winfo_fpixels('1i')
    
    print(f"原始屏幕信息: {screen_width}x{screen_height}, DPI: {dpi:.1f}")
    
    # 测试我们的屏幕工具
    try:
        from alphahome.gui.utils.screen_utils import get_screen_info, get_optimal_window_size
        our_info = get_screen_info(root)
        our_size = get_optimal_window_size(root)
        print(f"我们的屏幕工具: {our_info}")
        print(f"推荐窗口尺寸: {our_size}")
    except Exception as e:
        print(f"❌ 屏幕工具测试失败: {e}")
    
    # 设置窗口尺寸 - 使用较小的测试尺寸
    root.geometry("1200x800")
    
    # 创建界面
    main_frame = ttk.Frame(root, padding="10")
    main_frame.pack(fill=tk.BOTH, expand=True)
    
    # 信息显示
    info_frame = ttk.LabelFrame(main_frame, text="屏幕信息", padding="10")
    info_frame.pack(fill=tk.X, pady=(0, 10))
    
    ttk.Label(info_frame, text=f"物理分辨率: {screen_width} x {screen_height}").pack(anchor="w")
    ttk.Label(info_frame, text=f"DPI: {dpi:.1f}").pack(anchor="w")
    ttk.Label(info_frame, text=f"DPI缩放比例: {dpi/96:.2f}x").pack(anchor="w")
    
    # 测试表格
    table_frame = ttk.LabelFrame(main_frame, text="测试表格", padding="10")
    table_frame.pack(fill=tk.BOTH, expand=True)
    
    # 创建测试表格
    columns = ("col1", "col2", "col3", "col4")
    tree = ttk.Treeview(table_frame, columns=columns, show="headings", height=10)
    
    # 设置列标题
    tree.heading("col1", text="列1")
    tree.heading("col2", text="列2")
    tree.heading("col3", text="列3")
    tree.heading("col4", text="列4")
    
    # 设置列宽
    tree.column("col1", width=100, stretch=False)
    tree.column("col2", width=200, stretch=True)
    tree.column("col3", width=150, stretch=False)
    tree.column("col4", width=250, stretch=True)
    
    # 添加测试数据
    for i in range(10):
        tree.insert("", "end", values=(
            f"数据{i}",
            f"这是一个很长的测试数据项目{i}，用来测试列宽显示",
            f"状态{i}",
            f"详细信息{i} - 更多内容测试显示效果"
        ))
    
    # 添加滚动条
    scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=tree.yview)
    tree.configure(yscrollcommand=scrollbar.set)
    
    tree.pack(side="left", fill="both", expand=True)
    scrollbar.pack(side="right", fill="y")
    
    # 测试动态列宽管理器
    def test_column_manager():
        try:
            from alphahome.gui.utils.layout_manager import DynamicColumnManager, ColumnConfig
            
            manager = DynamicColumnManager(tree)
            
            # 添加列配置
            configs = [
                ColumnConfig("col1", "列1", min_width=80, preferred_width=100, weight=0.5, stretch=False),
                ColumnConfig("col2", "列2", min_width=150, preferred_width=200, weight=2.0, stretch=True),
                ColumnConfig("col3", "列3", min_width=100, preferred_width=150, weight=0.5, stretch=False),
                ColumnConfig("col4", "列4", min_width=200, preferred_width=250, weight=3.0, stretch=True),
            ]
            
            for config in configs:
                manager.add_column(config)
            
            # 配置列宽
            manager.configure_columns()
            
            # 绑定事件
            manager.bind_resize_event()
            
            print("✓ 动态列宽管理器测试成功")
            
        except Exception as e:
            print(f"❌ 动态列宽管理器测试失败: {e}")
            import traceback
            traceback.print_exc()
    
    # 添加测试按钮
    button_frame = ttk.Frame(main_frame)
    button_frame.pack(fill=tk.X, pady=(10, 0))
    
    ttk.Button(button_frame, text="测试动态列宽管理器", command=test_column_manager).pack(side="left", padx=(0, 10))
    
    def get_current_sizes():
        root.update_idletasks()
        window_width = root.winfo_width()
        window_height = root.winfo_height()
        tree_width = tree.winfo_width()
        tree_height = tree.winfo_height()
        
        print(f"当前窗口尺寸: {window_width}x{window_height}")
        print(f"当前表格尺寸: {tree_width}x{tree_height}")
        
        # 获取列宽
        for col in columns:
            width = tree.column(col, "width")
            print(f"列 {col} 宽度: {width}")
    
    ttk.Button(button_frame, text="获取当前尺寸", command=get_current_sizes).pack(side="left", padx=(0, 10))
    
    def quit_app():
        root.quit()
        root.destroy()
    
    ttk.Button(button_frame, text="退出", command=quit_app).pack(side="right")
    
    # 延迟测试
    root.after(1000, test_column_manager)
    
    return root

def main():
    """主函数"""
    print("=== 4K屏幕诊断工具 ===")
    print("这个工具将帮助诊断4K屏幕上的GUI显示问题")
    print()
    
    root = create_diagnostic_window()
    
    print("诊断窗口已创建，请观察:")
    print("1. 窗口是否正确显示")
    print("2. 表格列宽是否合理")
    print("3. 文字是否清晰")
    print("4. 尝试调整窗口大小观察变化")
    print()
    
    try:
        root.mainloop()
    except KeyboardInterrupt:
        print("用户中断")
    except Exception as e:
        print(f"运行时错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 