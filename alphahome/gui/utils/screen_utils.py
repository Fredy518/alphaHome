"""
屏幕检测和窗口尺寸适配工具

提供屏幕信息获取、最佳窗口尺寸计算等功能，
用于实现GUI界面的智能屏幕适配。
"""
import tkinter as tk
from typing import Tuple, Dict, Any
from ...common.logging_utils import get_logger

logger = get_logger("screen_utils")


def get_screen_info(root: tk.Tk = None) -> Dict[str, Any]:
    """
    获取屏幕信息
    
    Returns:
        Dict包含屏幕宽度、高度、DPI等信息
    """
    if root is None:
        # 创建临时根窗口获取屏幕信息
        temp_root = tk.Tk()
        temp_root.withdraw()  # 隐藏窗口
        try:
            screen_width = temp_root.winfo_screenwidth()
            screen_height = temp_root.winfo_screenheight()
            dpi = temp_root.winfo_fpixels('1i')
        finally:
            temp_root.destroy()
    else:
        screen_width = root.winfo_screenwidth()
        screen_height = root.winfo_screenheight()
        dpi = root.winfo_fpixels('1i')
    
    info = {
        'width': screen_width,
        'height': screen_height,
        'dpi': dpi,
        'aspect_ratio': screen_width / screen_height if screen_height > 0 else 1.0
    }
    
    logger.info(f"屏幕信息: {screen_width}x{screen_height}, DPI: {dpi:.1f}")
    return info


def get_dpi_aware_window_size(root: tk.Tk = None) -> Tuple[int, int]:
    """
    获取DPI感知的最佳窗口尺寸
    
    针对不同DPI环境优化窗口大小：
    - 标准DPI (96): 使用传统尺寸
    - 高DPI (120-150): 适度增大
    - 4K高DPI (192+): 充分利用分辨率优势
    
    Args:
        root: Tkinter根窗口实例
        
    Returns:
        (width, height) 元组
    """
    screen_info = get_screen_info(root)
    screen_width = screen_info['width']
    screen_height = screen_info['height']
    dpi = screen_info['dpi']
    
    # 计算DPI缩放比例
    dpi_scale = dpi / 96.0
    
    # 尝试从DPI管理器获取更准确的缩放信息
    try:
        from .dpi_manager import get_dpi_manager
        dpi_manager = get_dpi_manager()
        if dpi_manager and dpi_manager.dpi_info.scale_factor > 1.0:
            # 使用DPI管理器检测到的缩放比例
            dpi_scale = dpi_manager.dpi_info.scale_factor
            logger.info(f"使用DPI管理器检测到的缩放比例: {dpi_scale:.2f}")
    except:
        # 如果DPI管理器不可用，使用基于DPI的计算
        pass
    
    # 根据DPI环境确定基础尺寸和比例策略
    if dpi_scale >= 2.0:  # 4K高DPI环境 (200%缩放)
        # 4K环境：充分利用分辨率优势
        base_width = 1600
        base_height = 1000
        width_ratio = 0.95  # 使用更大比例
        height_ratio = 0.90
        logger.info("检测到4K高DPI环境，使用大窗口策略")
        
    elif dpi_scale >= 1.5:  # 高DPI环境 (150%缩放)
        # 高DPI环境：适度增大
        base_width = 1400
        base_height = 900
        width_ratio = 0.90
        height_ratio = 0.85
        logger.info("检测到高DPI环境，使用中等窗口策略")
        
    elif dpi_scale >= 1.25:  # 中等DPI环境 (125%缩放)
        base_width = 1300
        base_height = 850
        width_ratio = 0.85
        height_ratio = 0.80
        logger.info("检测到中等DPI环境，使用适中窗口策略")
        
    else:  # 标准DPI环境
        base_width = 1200
        base_height = 800
        width_ratio = 0.80
        height_ratio = 0.75
        logger.info("检测到标准DPI环境，使用传统窗口策略")
    
    # 计算推荐尺寸（基础尺寸和屏幕比例的较大值）
    ratio_width = int(screen_width * width_ratio)
    ratio_height = int(screen_height * height_ratio)
    
    optimal_width = max(base_width, ratio_width)
    optimal_height = max(base_height, ratio_height)
    
    # 确保不超过屏幕尺寸（留出边距）
    max_width = int(screen_width * 0.98)
    max_height = int(screen_height * 0.95)
    
    optimal_width = min(optimal_width, max_width)
    optimal_height = min(optimal_height, max_height)
    
    logger.info(f"DPI感知窗口尺寸: {optimal_width}x{optimal_height} (DPI缩放: {dpi_scale:.2f})")
    return optimal_width, optimal_height


def get_optimal_window_size(root: tk.Tk = None, 
                          min_width: int = 1024, 
                          min_height: int = 768,
                          max_width_ratio: float = 0.9,
                          max_height_ratio: float = 0.85) -> Tuple[int, int]:
    """
    计算最佳窗口尺寸（传统方法，保留向后兼容）
    
    Args:
        root: Tkinter根窗口实例
        min_width: 最小窗口宽度
        min_height: 最小窗口高度
        max_width_ratio: 相对屏幕宽度的最大比例
        max_height_ratio: 相对屏幕高度的最大比例（考虑任务栏）
        
    Returns:
        (width, height) 元组
    """
    screen_info = get_screen_info(root)
    screen_width = screen_info['width']
    screen_height = screen_info['height']
    
    # 计算推荐窗口尺寸
    recommended_width = int(screen_width * max_width_ratio)
    recommended_height = int(screen_height * max_height_ratio)
    
    # 应用最小尺寸限制
    optimal_width = max(recommended_width, min_width)
    optimal_height = max(recommended_height, min_height)
    
    # 确保不超过屏幕尺寸
    optimal_width = min(optimal_width, screen_width)
    optimal_height = min(optimal_height, screen_height)
    
    logger.info(f"计算最佳窗口尺寸: {optimal_width}x{optimal_height}")
    return optimal_width, optimal_height


def get_window_geometry_string(root: tk.Tk = None, use_dpi_aware: bool = True) -> str:
    """
    获取适合当前屏幕的窗口几何字符串
    
    Args:
        root: Tkinter根窗口实例
        use_dpi_aware: 是否使用DPI感知计算（推荐）
    
    Returns:
        几何字符串，如 "1600x1000"
    """
    if use_dpi_aware:
        width, height = get_dpi_aware_window_size(root)
    else:
        width, height = get_optimal_window_size(root)
        
    geometry_str = f"{width}x{height}"
    logger.info(f"窗口几何字符串: {geometry_str}")
    return geometry_str


def is_small_screen(root: tk.Tk = None, threshold_width: int = 1366) -> bool:
    """
    判断是否为小屏幕
    
    Args:
        root: Tkinter根窗口实例
        threshold_width: 小屏幕判断阈值宽度
        
    Returns:
        True表示小屏幕
    """
    screen_info = get_screen_info(root)
    is_small = screen_info['width'] <= threshold_width
    logger.debug(f"屏幕类型判断: {'小屏幕' if is_small else '大屏幕'}")
    return is_small


def center_window_on_screen(root: tk.Tk, width: int = None, height: int = None) -> None:
    """
    将窗口居中显示在屏幕上
    
    Args:
        root: Tkinter根窗口实例
        width: 窗口宽度，如果为None则使用当前窗口宽度
        height: 窗口高度，如果为None则使用当前窗口高度
    """
    if width is None or height is None:
        root.update_idletasks()
        current_geometry = root.geometry()
        if 'x' in current_geometry:
            size_part = current_geometry.split('+')[0]
            if 'x' in size_part:
                w, h = size_part.split('x')
                width = width or int(w)
                height = height or int(h)
    
    screen_info = get_screen_info(root)
    screen_width = screen_info['width']
    screen_height = screen_info['height']
    
    # 计算居中位置
    x = (screen_width - width) // 2
    y = (screen_height - height) // 2
    
    # 确保窗口不会超出屏幕边界
    x = max(0, x)
    y = max(0, y)
    
    geometry_str = f"{width}x{height}+{x}+{y}"
    root.geometry(geometry_str)
    logger.info(f"窗口居中: {geometry_str}")


def position_window_top_left(root: tk.Tk, width: int = None, height: int = None, 
                           offset_x: int = 450, offset_y: int = 250) -> None:
    """
    将窗口定位到屏幕左上角
    
    Args:
        root: Tkinter根窗口实例
        width: 窗口宽度，如果为None则使用当前窗口宽度
        height: 窗口高度，如果为None则使用当前窗口高度
        offset_x: 距离屏幕左边缘的偏移量（像素）
        offset_y: 距离屏幕上边缘的偏移量（像素）
    """
    if width is None or height is None:
        root.update_idletasks()
        current_geometry = root.geometry()
        if 'x' in current_geometry:
            size_part = current_geometry.split('+')[0]
            if 'x' in size_part:
                w, h = size_part.split('x')
                width = width or int(w)
                height = height or int(h)
    
    screen_info = get_screen_info(root)
    screen_width = screen_info['width']
    screen_height = screen_info['height']
    
    # 设置左上角位置，带有偏移量
    x = offset_x
    y = offset_y
    
    # 确保窗口不会超出屏幕边界
    x = max(0, min(x, screen_width - width))
    y = max(0, min(y, screen_height - height))
    
    geometry_str = f"{width}x{height}+{x}+{y}"
    root.geometry(geometry_str)
    logger.info(f"窗口定位到左上角: {geometry_str}") 