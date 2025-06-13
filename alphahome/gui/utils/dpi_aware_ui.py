"""
DPI感知UI工厂

提供DPI感知的UI元素创建和管理功能，
确保界面在不同DPI环境下的一致性和可读性。
"""
import tkinter as tk
from tkinter import ttk, font as tkFont
from typing import Dict, Any, Optional, Tuple, Union
from .dpi_manager import get_dpi_manager, DisplayMode
from ...common.logging_utils import get_logger

logger = get_logger("dpi_aware_ui")


class DpiAwareFont:
    """DPI感知字体管理"""
    
    def __init__(self):
        self.dpi_manager = get_dpi_manager()
        self._font_cache: Dict[str, tkFont.Font] = {}
    
    def get_font(self, family: str = "Microsoft YaHei UI", size: int = 10, **kwargs) -> tkFont.Font:
        """获取DPI适配的字体"""
        scaled_size = self.dpi_manager.get_scaled_font_size(size)
        
        # 创建字体键用于缓存
        font_key = f"{family}_{scaled_size}_{hash(tuple(sorted(kwargs.items())))}"
        
        if font_key not in self._font_cache:
            self._font_cache[font_key] = tkFont.Font(
                family=family, 
                size=scaled_size, 
                **kwargs
            )
        
        return self._font_cache[font_key]
    
    def get_default_font(self) -> tkFont.Font:
        """获取默认字体"""
        return self.get_font()
    
    def get_header_font(self) -> tkFont.Font:
        """获取标题字体"""
        return self.get_font(size=12, weight="bold")
    
    def get_small_font(self) -> tkFont.Font:
        """获取小字体"""
        return self.get_font(size=8)
    
    def get_monospace_font(self) -> tkFont.Font:
        """获取等宽字体"""
        return self.get_font(family="Consolas", size=9)
    
    def clear_cache(self):
        """清除字体缓存"""
        self._font_cache.clear()


class DpiAwareStyle:
    """DPI感知样式管理"""
    
    def __init__(self):
        self.dpi_manager = get_dpi_manager()
        self.font_manager = DpiAwareFont()
        self._style = ttk.Style()
        self._styles_configured = False
    
    def configure_styles(self):
        """配置DPI适配的TTK样式"""
        if self._styles_configured:
            return
        
        try:
            # 获取缩放后的尺寸
            default_font = self.font_manager.get_default_font()
            header_font = self.font_manager.get_header_font()
            
            # 计算行高
            base_row_height = int(default_font.metrics("linespace") * 1.4)
            scaled_row_height = self.dpi_manager.get_scaled_row_height(base_row_height)
            
            # 配置Treeview样式
            self._style.configure("DpiAware.Treeview", 
                                font=default_font, 
                                rowheight=scaled_row_height)
            # 表头使用普通字体（不加粗），保持现代简洁的设计风格
            self._style.configure("DpiAware.Treeview.Heading", 
                                font=default_font)
            
            # 配置Button样式
            self._style.configure("DpiAware.TButton", 
                                font=default_font)
            
            # 配置Label样式
            self._style.configure("DpiAware.TLabel", 
                                font=default_font)
            
            # 配置Entry样式
            self._style.configure("DpiAware.TEntry", 
                                font=default_font)
            
            # 配置Combobox样式
            self._style.configure("DpiAware.TCombobox", 
                                font=default_font)
            
            # 配置Frame样式
            padding = self.dpi_manager.get_scaled_spacing(10)
            self._style.configure("DpiAware.TFrame", 
                                padding=padding)
            
            logger.info(f"DPI感知样式配置完成，行高: {scaled_row_height}")
            self._styles_configured = True
            
        except Exception as e:
            logger.warning(f"配置DPI感知样式失败: {e}")
    
    def get_style(self) -> ttk.Style:
        """获取样式对象"""
        self.configure_styles()
        return self._style
    
    def reset_styles(self):
        """重置样式（用于DPI模式切换）"""
        self._styles_configured = False
        self.font_manager.clear_cache()
        self.configure_styles()


class DpiAwareUIFactory:
    """DPI感知UI元素工厂"""
    
    def __init__(self):
        self.dpi_manager = get_dpi_manager()
        self.font_manager = DpiAwareFont()
        self.style_manager = DpiAwareStyle()
    
    def create_treeview(self, parent, columns: tuple, **kwargs) -> ttk.Treeview:
        """创建DPI适配的Treeview"""
        # 确保样式已配置
        self.style_manager.configure_styles()
        
        # 设置默认样式
        if 'style' not in kwargs:
            kwargs['style'] = "DpiAware.Treeview"
        
        # 创建Treeview
        tree = ttk.Treeview(parent, columns=columns, **kwargs)
        
        logger.debug(f"创建DPI适配Treeview，列数: {len(columns)}")
        return tree
    
    def create_button(self, parent, text: str = "", **kwargs) -> ttk.Button:
        """创建DPI适配的Button"""
        self.style_manager.configure_styles()
        
        if 'style' not in kwargs:
            kwargs['style'] = "DpiAware.TButton"
        
        button = ttk.Button(parent, text=text, **kwargs)
        
        # 如果指定了尺寸，进行DPI缩放
        if 'width' in kwargs or 'height' in kwargs:
            width = kwargs.get('width', 100)
            height = kwargs.get('height', 30)
            scaled_width, scaled_height = self.dpi_manager.get_scaled_button_size(width, height)
            # TTK Button的width/height是字符单位，这里需要特殊处理
            
        return button
    
    def create_label(self, parent, text: str = "", **kwargs) -> ttk.Label:
        """创建DPI适配的Label"""
        self.style_manager.configure_styles()
        
        if 'style' not in kwargs:
            kwargs['style'] = "DpiAware.TLabel"
        
        return ttk.Label(parent, text=text, **kwargs)
    
    def create_entry(self, parent, **kwargs) -> ttk.Entry:
        """创建DPI适配的Entry"""
        self.style_manager.configure_styles()
        
        if 'style' not in kwargs:
            kwargs['style'] = "DpiAware.TEntry"
        
        # 缩放宽度
        if 'width' in kwargs:
            # Entry的width是字符单位，根据字体缩放进行调整
            original_width = kwargs['width']
            font_scale = self.dpi_manager.scale_factors.font_scale
            # 字体变大时，相同字符数占用更多空间，所以稍微减少width
            kwargs['width'] = max(5, int(original_width / font_scale * 0.9))
        
        return ttk.Entry(parent, **kwargs)
    
    def create_combobox(self, parent, **kwargs) -> ttk.Combobox:
        """创建DPI适配的Combobox"""
        self.style_manager.configure_styles()
        
        if 'style' not in kwargs:
            kwargs['style'] = "DpiAware.TCombobox"
        
        # 缩放宽度（同Entry）
        if 'width' in kwargs:
            original_width = kwargs['width']
            font_scale = self.dpi_manager.scale_factors.font_scale
            kwargs['width'] = max(5, int(original_width / font_scale * 0.9))
        
        return ttk.Combobox(parent, **kwargs)
    
    def create_frame(self, parent, **kwargs) -> ttk.Frame:
        """创建DPI适配的Frame"""
        self.style_manager.configure_styles()
        
        if 'style' not in kwargs:
            kwargs['style'] = "DpiAware.TFrame"
        
        # 缩放padding
        if 'padding' in kwargs:
            if isinstance(kwargs['padding'], (int, str)):
                base_padding = int(kwargs['padding'])
                kwargs['padding'] = self.dpi_manager.get_scaled_spacing(base_padding)
            elif isinstance(kwargs['padding'], (list, tuple)):
                # 处理多值padding
                scaled_padding = []
                for p in kwargs['padding']:
                    scaled_padding.append(self.dpi_manager.get_scaled_spacing(int(p)))
                kwargs['padding'] = scaled_padding
        
        return ttk.Frame(parent, **kwargs)
    
    def create_labelframe(self, parent, text: str = "", **kwargs) -> ttk.LabelFrame:
        """创建DPI适配的LabelFrame"""
        self.style_manager.configure_styles()
        
        # LabelFrame暂时使用默认样式，但应用padding缩放
        if 'padding' in kwargs:
            if isinstance(kwargs['padding'], (int, str)):
                base_padding = int(kwargs['padding'])
                kwargs['padding'] = self.dpi_manager.get_scaled_spacing(base_padding)
        
        return ttk.LabelFrame(parent, text=text, **kwargs)
    
    def create_scrollbar(self, parent, **kwargs) -> ttk.Scrollbar:
        """创建滚动条"""
        return ttk.Scrollbar(parent, **kwargs)
    
    def create_text(self, parent, **kwargs) -> tk.Text:
        """创建DPI适配的Text widget"""
        # Text widget使用tkinter.Text，需要手动设置字体
        if 'font' not in kwargs:
            kwargs['font'] = self.font_manager.get_monospace_font()
        
        return tk.Text(parent, **kwargs)
    
    def get_scaled_dimensions(self, base_width: int, base_height: int) -> Tuple[int, int]:
        """获取缩放后的尺寸"""
        # 4K环境下使用更大的基础尺寸
        if self.dpi_manager.current_mode == DisplayMode.UHD_4K:
            # 4K环境：提供更大的基础尺寸
            enhanced_width = max(base_width, int(base_width * 1.3))
            enhanced_height = max(base_height, int(base_height * 1.25))
            width = int(enhanced_width * self.dpi_manager.scale_factors.button_scale)
            height = int(enhanced_height * self.dpi_manager.scale_factors.button_scale)
        else:
            width = int(base_width * self.dpi_manager.scale_factors.button_scale)
            height = int(base_height * self.dpi_manager.scale_factors.button_scale)
        
        return (max(width, base_width), max(height, base_height))
    
    def get_scaled_spacing(self, spacing: int) -> int:
        """获取缩放后的间距"""
        return self.dpi_manager.get_scaled_spacing(spacing)
    
    def refresh_for_dpi_change(self):
        """DPI模式切换时刷新UI工厂"""
        logger.info("刷新DPI感知UI工厂")
        self.dpi_manager = get_dpi_manager()  # 重新获取管理器
        self.font_manager = DpiAwareFont()    # 重新创建字体管理器
        self.style_manager.reset_styles()     # 重置样式


# 全局UI工厂实例
_ui_factory: Optional[DpiAwareUIFactory] = None


def get_ui_factory() -> DpiAwareUIFactory:
    """获取全局DPI感知UI工厂实例"""
    global _ui_factory
    if _ui_factory is None:
        _ui_factory = DpiAwareUIFactory()
    return _ui_factory


def initialize_ui_factory() -> DpiAwareUIFactory:
    """初始化DPI感知UI工厂"""
    global _ui_factory
    _ui_factory = DpiAwareUIFactory()
    logger.info("DPI感知UI工厂初始化完成")
    return _ui_factory


def refresh_ui_factory():
    """刷新UI工厂（用于DPI模式切换）"""
    global _ui_factory
    if _ui_factory:
        _ui_factory.refresh_for_dpi_change()
        logger.info("DPI感知UI工厂已刷新")


# 便捷函数
def create_dpi_aware_treeview(parent, columns: tuple, **kwargs) -> ttk.Treeview:
    """便捷函数：创建DPI适配的Treeview"""
    return get_ui_factory().create_treeview(parent, columns, **kwargs)


def create_dpi_aware_button(parent, text: str = "", **kwargs) -> ttk.Button:
    """便捷函数：创建DPI适配的Button"""
    return get_ui_factory().create_button(parent, text, **kwargs)


def create_dpi_aware_label(parent, text: str = "", **kwargs) -> ttk.Label:
    """便捷函数：创建DPI适配的Label"""
    return get_ui_factory().create_label(parent, text, **kwargs)


def create_dpi_aware_entry(parent, **kwargs) -> ttk.Entry:
    """便捷函数：创建DPI适配的Entry"""
    return get_ui_factory().create_entry(parent, **kwargs)


def create_dpi_aware_combobox(parent, **kwargs) -> ttk.Combobox:
    """便捷函数：创建DPI适配的Combobox"""
    return get_ui_factory().create_combobox(parent, **kwargs)


def create_dpi_aware_frame(parent, **kwargs) -> ttk.Frame:
    """便捷函数：创建DPI适配的Frame"""
    return get_ui_factory().create_frame(parent, **kwargs) 