"""
高DPI管理器

负责检测DPI环境、计算缩放因子、管理显示模式配置，
为GUI界面提供高DPI适配支持。
"""
import ctypes
import platform
import tkinter as tk
from typing import Dict, Any, Optional, Tuple
from enum import Enum
import json
import os
from ...common.logging_utils import get_logger

logger = get_logger("dpi_manager")


class DisplayMode(Enum):
    """显示模式枚举"""
    AUTO = "auto"           # 自动检测
    STANDARD = "standard"   # 标准模式(96 DPI)
    HIGH_DPI = "high_dpi"   # 高DPI模式(120-192 DPI)
    UHD_4K = "uhd_4k"      # 4K优化模式(192+ DPI)


class DpiInfo:
    """DPI信息类"""
    def __init__(self):
        self.logical_dpi: float = 96.0          # 逻辑DPI
        self.physical_dpi: float = 96.0         # 物理DPI  
        self.scale_factor: float = 1.0          # Windows缩放比例
        self.logical_resolution: Tuple[int, int] = (1920, 1080)  # 逻辑分辨率
        self.physical_resolution: Tuple[int, int] = (1920, 1080) # 物理分辨率
        self.is_high_dpi: bool = False          # 是否为高DPI环境


class ScaleFactors:
    """缩放因子类"""
    def __init__(self, dpi_info: DpiInfo, mode: DisplayMode = DisplayMode.AUTO):
        self.base_scale = dpi_info.scale_factor
        self.mode = mode
        
        # 计算各种UI元素的缩放因子
        if mode == DisplayMode.AUTO:
            self._calculate_auto_factors(dpi_info)
        elif mode == DisplayMode.STANDARD:
            self._calculate_standard_factors()
        elif mode == DisplayMode.HIGH_DPI:
            self._calculate_high_dpi_factors(dpi_info)
        elif mode == DisplayMode.UHD_4K:
            self._calculate_4k_factors(dpi_info)
    
    def _calculate_auto_factors(self, dpi_info: DpiInfo):
        """自动计算缩放因子"""
        if dpi_info.scale_factor >= 2.0:
            # 4K优化模式
            self.font_scale = 1.2
            self.row_height_scale = 1.3
            self.column_width_scale = 1.1
            self.spacing_scale = 1.2
            self.button_scale = 1.2
        elif dpi_info.scale_factor >= 1.5:
            # 高DPI模式
            self.font_scale = 1.1
            self.row_height_scale = 1.2
            self.column_width_scale = 1.05
            self.spacing_scale = 1.1
            self.button_scale = 1.1
        else:
            # 标准模式
            self._calculate_standard_factors()
    
    def _calculate_standard_factors(self):
        """标准模式缩放因子"""
        self.font_scale = 1.0
        self.row_height_scale = 1.0
        self.column_width_scale = 1.0
        self.spacing_scale = 1.0
        self.button_scale = 1.0
    
    def _calculate_high_dpi_factors(self, dpi_info: DpiInfo):
        """高DPI模式缩放因子"""
        base_factor = min(dpi_info.scale_factor, 1.5)
        self.font_scale = 1.0 + (base_factor - 1.0) * 0.5
        self.row_height_scale = 1.0 + (base_factor - 1.0) * 0.6
        self.column_width_scale = 1.0 + (base_factor - 1.0) * 0.3
        self.spacing_scale = base_factor * 0.8
        self.button_scale = base_factor * 0.8
    
    def _calculate_4k_factors(self, dpi_info: DpiInfo):
        """4K优化模式缩放因子"""
        # 针对4K+200%缩放的优化，保持字体原始大小，优化其他元素
        # 字体保持原始大小，通过优化布局和间距提升体验
        self.font_scale = 1.0   # 保持字体原始大小，避免过大
        self.row_height_scale = 1.3  # 适当增加行间距，保持可读性
        self.column_width_scale = 1.3  # 增加列宽缩放，给内容更多空间
        self.spacing_scale = 1.2  # 适度增加组件间距
        self.button_scale = 1.35  # 保持按钮易点击性


class DpiManager:
    """DPI管理器主类"""
    
    CONFIG_FILE = "dpi_config.json"
    
    def __init__(self):
        self.dpi_info = DpiInfo()
        self.current_mode = DisplayMode.AUTO
        self.scale_factors = ScaleFactors(self.dpi_info)
        self.config_path = os.path.join(os.path.expanduser("~"), ".alphahome", self.CONFIG_FILE)
        
        # 确保配置目录存在
        os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
        
        # 加载配置
        self.load_config()
        
        # 检测DPI环境
        self.detect_dpi_environment()
        
        # 如果是AUTO模式，应用推荐的显示模式
        if self.current_mode == DisplayMode.AUTO:
            recommended_mode = self.recommend_display_mode()
            logger.info(f"AUTO模式检测到推荐显示模式: {recommended_mode.value}")
            self.current_mode = recommended_mode
            # 保存新的模式设置
            self.save_config()
        
        # 重新计算缩放因子（基于检测结果和配置）
        self.update_scale_factors()
    
    def detect_dpi_environment(self) -> None:
        """检测DPI环境"""
        logger.info("开始检测DPI环境")
        
        try:
            # 创建临时窗口获取屏幕信息
            temp_root = tk.Tk()
            temp_root.withdraw()
            
            try:
                # 获取逻辑分辨率和DPI
                logical_width = temp_root.winfo_screenwidth()
                logical_height = temp_root.winfo_screenheight()
                logical_dpi = temp_root.winfo_fpixels('1i')
                
                self.dpi_info.logical_resolution = (logical_width, logical_height)
                self.dpi_info.logical_dpi = logical_dpi
                
                # 尝试获取物理分辨率（Windows）
                physical_width, physical_height = self._get_physical_resolution()
                self.dpi_info.physical_resolution = (physical_width, physical_height)
                
                # 计算缩放比例
                if physical_width > 0 and physical_height > 0:
                    scale_x = physical_width / logical_width
                    scale_y = physical_height / logical_height
                    self.dpi_info.scale_factor = max(scale_x, scale_y)
                else:
                    # 备用方法：基于DPI计算
                    self.dpi_info.scale_factor = logical_dpi / 96.0
                
                # 物理DPI估算
                if physical_width > 0:
                    # 假设典型显示器尺寸进行估算
                    estimated_diagonal_inches = self._estimate_screen_diagonal(physical_width, physical_height)
                    self.dpi_info.physical_dpi = (physical_width**2 + physical_height**2)**0.5 / estimated_diagonal_inches
                else:
                    self.dpi_info.physical_dpi = logical_dpi * self.dpi_info.scale_factor
                
                # 判断是否为高DPI环境
                self.dpi_info.is_high_dpi = self.dpi_info.scale_factor >= 1.25
                
                logger.info(f"DPI检测完成:")
                logger.info(f"  逻辑分辨率: {self.dpi_info.logical_resolution}")
                logger.info(f"  物理分辨率: {self.dpi_info.physical_resolution}")
                logger.info(f"  逻辑DPI: {self.dpi_info.logical_dpi:.1f}")
                logger.info(f"  物理DPI: {self.dpi_info.physical_dpi:.1f}")
                logger.info(f"  缩放比例: {self.dpi_info.scale_factor:.2f}")
                logger.info(f"  高DPI环境: {self.dpi_info.is_high_dpi}")
                
            finally:
                temp_root.destroy()
                
        except Exception as e:
            logger.error(f"DPI检测失败: {e}")
            # 使用默认值
            self.dpi_info.logical_resolution = (1920, 1080)
            self.dpi_info.physical_resolution = (1920, 1080)
            self.dpi_info.logical_dpi = 96.0
            self.dpi_info.physical_dpi = 96.0
            self.dpi_info.scale_factor = 1.0
            self.dpi_info.is_high_dpi = False
    
    def _get_physical_resolution(self) -> Tuple[int, int]:
        """获取物理分辨率（Windows特定）"""
        if platform.system() != "Windows":
            return (0, 0)
        
        try:
            # 使用Windows API获取真实分辨率
            user32 = ctypes.windll.user32
            screensize = user32.GetSystemMetrics(78), user32.GetSystemMetrics(79)  # SM_CXVIRTUALSCREEN, SM_CYVIRTUALSCREEN
            
            # 尝试获取主显示器的物理分辨率
            hdc = user32.GetDC(None)
            if hdc:
                try:
                    gdi32 = ctypes.windll.gdi32
                    physical_width = gdi32.GetDeviceCaps(hdc, 118)  # HORZRES
                    physical_height = gdi32.GetDeviceCaps(hdc, 117)  # VERTRES
                    
                    if physical_width > 0 and physical_height > 0:
                        return (physical_width, physical_height)
                finally:
                    user32.ReleaseDC(None, hdc)
            
            # 备用方法：尝试获取原始输入
            try:
                # 这个方法可能在某些系统上有效
                import winreg
                key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, 
                                   r"SYSTEM\CurrentControlSet\Control\GraphicsDrivers\Configuration")
                # 这里需要更复杂的注册表读取逻辑，暂时使用备用方案
                winreg.CloseKey(key)
            except:
                pass
            
            return screensize
            
        except Exception as e:
            logger.debug(f"获取物理分辨率失败: {e}")
            return (0, 0)
    
    def _estimate_screen_diagonal(self, width: int, height: int) -> float:
        """估算屏幕对角线长度（英寸）"""
        # 基于分辨率的经验估算
        pixel_count = width * height
        
        if pixel_count >= 8294400:  # 4K (3840x2160)
            return 27.0  # 典型4K显示器
        elif pixel_count >= 2073600:  # 1080p (1920x1080)
            return 24.0  # 典型1080p显示器
        elif pixel_count >= 1440000:  # 1440p (2560x1440)
            return 27.0  # 典型1440p显示器
        else:
            return 21.0  # 较小显示器
    
    def set_display_mode(self, mode: DisplayMode) -> None:
        """设置显示模式"""
        if mode != self.current_mode:
            logger.info(f"切换显示模式: {self.current_mode.value} -> {mode.value}")
            self.current_mode = mode
            self.update_scale_factors()
            self.save_config()
    
    def update_scale_factors(self) -> None:
        """更新缩放因子"""
        self.scale_factors = ScaleFactors(self.dpi_info, self.current_mode)
        logger.debug(f"缩放因子更新:")
        logger.debug(f"  字体缩放: {self.scale_factors.font_scale:.2f}")
        logger.debug(f"  行高缩放: {self.scale_factors.row_height_scale:.2f}")
        logger.debug(f"  列宽缩放: {self.scale_factors.column_width_scale:.2f}")
    
    def get_scaled_font_size(self, base_size: int) -> int:
        """获取缩放后的字体大小"""
        return max(8, int(base_size * self.scale_factors.font_scale))
    
    def get_scaled_row_height(self, base_height: int) -> int:
        """获取缩放后的行高"""
        return max(16, int(base_height * self.scale_factors.row_height_scale))
    
    def get_scaled_column_width(self, base_width: int) -> int:
        """获取缩放后的列宽"""
        return max(30, int(base_width * self.scale_factors.column_width_scale))
    
    def get_scaled_spacing(self, base_spacing: int) -> int:
        """获取缩放后的间距"""
        return max(2, int(base_spacing * self.scale_factors.spacing_scale))
    
    def get_scaled_button_size(self, base_width: int, base_height: int) -> Tuple[int, int]:
        """获取缩放后的按钮尺寸"""
        width = max(50, int(base_width * self.scale_factors.button_scale))
        height = max(20, int(base_height * self.scale_factors.button_scale))
        return (width, height)
    
    def save_config(self) -> None:
        """保存配置"""
        try:
            config = {
                "display_mode": self.current_mode.value,
                "last_detected_scale": self.dpi_info.scale_factor,
                "last_detected_resolution": self.dpi_info.logical_resolution
            }
            
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
            
            logger.debug(f"DPI配置已保存: {self.config_path}")
            
        except Exception as e:
            logger.warning(f"保存DPI配置失败: {e}")
    
    def load_config(self) -> None:
        """加载配置"""
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                
                # 加载显示模式
                mode_str = config.get("display_mode", "auto")
                try:
                    self.current_mode = DisplayMode(mode_str)
                except ValueError:
                    logger.warning(f"无效的显示模式: {mode_str}，使用默认模式")
                    self.current_mode = DisplayMode.AUTO
                
                logger.debug(f"DPI配置已加载: 模式={self.current_mode.value}")
            else:
                logger.debug("DPI配置文件不存在，使用默认设置")
                
        except Exception as e:
            logger.warning(f"加载DPI配置失败: {e}")
            self.current_mode = DisplayMode.AUTO
    
    def get_display_info(self) -> Dict[str, Any]:
        """获取显示信息摘要"""
        return {
            "current_mode": self.current_mode.value,
            "logical_resolution": self.dpi_info.logical_resolution,
            "physical_resolution": self.dpi_info.physical_resolution,
            "scale_factor": self.dpi_info.scale_factor,
            "is_high_dpi": self.dpi_info.is_high_dpi,
            "font_scale": self.scale_factors.font_scale,
            "row_height_scale": self.scale_factors.row_height_scale
        }
    
    def recommend_display_mode(self) -> DisplayMode:
        """推荐显示模式"""
        if self.dpi_info.scale_factor >= 2.0:
            return DisplayMode.UHD_4K
        elif self.dpi_info.scale_factor >= 1.5:
            return DisplayMode.HIGH_DPI
        else:
            return DisplayMode.STANDARD


# 全局DPI管理器实例
_dpi_manager: Optional[DpiManager] = None


def get_dpi_manager() -> DpiManager:
    """获取全局DPI管理器实例"""
    global _dpi_manager
    if _dpi_manager is None:
        _dpi_manager = DpiManager()
    return _dpi_manager


def initialize_dpi_manager() -> DpiManager:
    """初始化DPI管理器"""
    global _dpi_manager
    _dpi_manager = DpiManager()
    logger.info("DPI管理器初始化完成")
    return _dpi_manager 