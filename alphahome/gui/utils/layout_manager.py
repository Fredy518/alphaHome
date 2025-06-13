"""
动态布局管理器

提供表格列宽动态调整、响应式布局等功能，
用于实现GUI界面的智能布局适配。
"""
import tkinter as tk
from tkinter import ttk
from typing import Dict, List, Any, Tuple, Optional
from ...common.logging_utils import get_logger
from .dpi_manager import get_dpi_manager

logger = get_logger("layout_manager")


class ColumnConfig:
    """列配置类，定义表格列的属性"""
    
    def __init__(self, 
                 column_id: str,
                 title: str,
                 min_width: int = 50,
                 preferred_width: int = 100,
                 weight: float = 1.0,
                 stretch: bool = True,
                 anchor: str = "w",
                 priority: int = 1):
        """
        Args:
            column_id: 列标识符
            title: 列标题
            min_width: 最小宽度
            preferred_width: 首选宽度
            weight: 权重（用于分配剩余空间）
            stretch: 是否可拉伸
            anchor: 对齐方式
            priority: 优先级（1=最高，数值越大优先级越低）
        """
        self.column_id = column_id
        self.title = title
        self.min_width = min_width
        self.preferred_width = preferred_width
        self.weight = weight
        self.stretch = stretch
        self.anchor = anchor
        self.priority = priority
        self.current_width = preferred_width


class DynamicColumnManager:
    """动态列宽管理器"""
    
    def __init__(self, treeview: ttk.Treeview):
        """
        Args:
            treeview: 要管理的Treeview组件
        """
        self.treeview = treeview
        self.columns: Dict[str, ColumnConfig] = {}
        self.last_calculated_width = 0
        self._configure_event_bound = False
        self.dpi_manager = get_dpi_manager()  # 添加DPI管理器
        
    def add_column(self, config: ColumnConfig) -> None:
        """添加列配置"""
        self.columns[config.column_id] = config
        logger.debug(f"添加列配置: {config.column_id}, 最小宽度: {config.min_width}")
        
    def configure_columns(self, available_width: int = None) -> None:
        """
        配置所有列的宽度
        
        Args:
            available_width: 可用宽度，如果为None则自动计算
        """
        if not self.columns:
            logger.warning("没有配置列信息，跳过列宽配置")
            return
            
        if available_width is None:
            available_width = self._get_available_width()
            
        # 避免重复计算相同宽度
        if available_width == self.last_calculated_width and available_width > 0:
            logger.debug(f"宽度未变化({available_width})，跳过重新配置")
            return
            
        self.last_calculated_width = available_width
        
        logger.info(f"开始配置列宽，可用宽度: {available_width}, 列数: {len(self.columns)}")
        
        # 计算列宽
        column_widths = self._calculate_column_widths(available_width)
        
        if not column_widths:
            logger.warning("列宽计算结果为空")
            return
        
        # 应用列宽设置
        success_count = 0
        for column_id, width in column_widths.items():
            if column_id in self.columns:
                config = self.columns[column_id]
                try:
                    self.treeview.column(
                        column_id,
                        width=width,
                        minwidth=config.min_width,
                        stretch=config.stretch,
                        anchor=config.anchor
                    )
                    config.current_width = width
                    success_count += 1
                    logger.debug(f"设置列 {column_id} 宽度: {width}")
                except tk.TclError as e:
                    logger.error(f"设置列 {column_id} 宽度失败: {e}")
        
        logger.info(f"列宽配置完成，成功设置 {success_count}/{len(column_widths)} 列")
                    
    def _get_available_width(self) -> int:
        """获取Treeview的可用宽度"""
        try:
            self.treeview.update_idletasks()
            width = self.treeview.winfo_width()
            # 减去滚动条宽度（如果存在）
            if width > 50:  # 确保获取到有效宽度
                return max(width - 20, 300)  # 预留滚动条空间
            else:
                # 如果无法获取实际宽度，使用父容器宽度
                parent = self.treeview.master
                if parent:
                    parent_width = parent.winfo_width()
                    return max(parent_width - 40, 300)
                return 800  # 默认宽度
        except tk.TclError:
            return 800  # 默认宽度
            
    def _calculate_column_widths(self, available_width: int) -> Dict[str, int]:
        """
        计算各列的宽度（支持DPI缩放）
        
        Args:
            available_width: 总可用宽度
            
        Returns:
            列ID到宽度的映射
        """
        if not self.columns:
            return {}
            
        # 按优先级排序列
        sorted_columns = sorted(self.columns.values(), key=lambda x: x.priority)
        
        # 应用DPI缩放到最小宽度和首选宽度
        scaled_columns = []
        for col in sorted_columns:
            scaled_col = ColumnConfig(
                column_id=col.column_id,
                title=col.title,
                min_width=self.dpi_manager.get_scaled_column_width(col.min_width),
                preferred_width=self.dpi_manager.get_scaled_column_width(col.preferred_width),
                weight=col.weight,
                stretch=col.stretch,
                anchor=col.anchor,
                priority=col.priority
            )
            scaled_columns.append(scaled_col)
        
        # 首先分配最小宽度
        total_min_width = sum(col.min_width for col in scaled_columns)
        
        if total_min_width >= available_width:
            # 空间不足，只能分配最小宽度
            logger.warning(f"空间不足，仅分配最小宽度。需要: {total_min_width}, 可用: {available_width}")
            return {col.column_id: col.min_width for col in scaled_columns}
        
        # 计算剩余空间
        remaining_width = available_width - total_min_width
        
        # 分配首选宽度
        column_widths = {}
        used_preferred_width = 0
        
        for col in scaled_columns:
            preferred_extra = col.preferred_width - col.min_width
            if used_preferred_width + preferred_extra <= remaining_width:
                column_widths[col.column_id] = col.preferred_width
                used_preferred_width += preferred_extra
            else:
                column_widths[col.column_id] = col.min_width
        
        # 分配剩余空间（按权重）
        remaining_width -= used_preferred_width
        
        if remaining_width > 0:
            # 找出可拉伸的列
            stretchable_columns = [col for col in scaled_columns if col.stretch]
            
            if stretchable_columns:
                total_weight = sum(col.weight for col in stretchable_columns)
                
                for col in stretchable_columns:
                    if total_weight > 0:
                        extra_width = int(remaining_width * col.weight / total_weight)
                        column_widths[col.column_id] += extra_width
        
        logger.debug(f"DPI适配后的列宽: {column_widths}")
        logger.debug(f"DPI缩放因子: {self.dpi_manager.scale_factors.column_width_scale:.2f}")
        return column_widths
        
    def bind_resize_event(self) -> None:
        """绑定窗口大小变化事件"""
        if not self._configure_event_bound:
            self.treeview.bind('<Configure>', self._on_treeview_configure)
            self._configure_event_bound = True
            logger.debug("已绑定窗口大小变化事件")
            
    def _on_treeview_configure(self, event) -> None:
        """响应Treeview大小变化事件"""
        if event.widget == self.treeview:
            # 延迟执行以避免频繁调用
            self.treeview.after_idle(self.configure_columns)
            
    def get_column_info(self) -> List[Dict[str, Any]]:
        """获取所有列的信息（用于调试）"""
        info = []
        for col in self.columns.values():
            info.append({
                'column_id': col.column_id,
                'title': col.title,
                'min_width': col.min_width,
                'preferred_width': col.preferred_width,
                'current_width': col.current_width,
                'weight': col.weight,
                'priority': col.priority
            })
        return info
        
    def refresh_for_dpi_change(self):
        """DPI模式切换时刷新列管理器"""
        logger.info("刷新列管理器以适配DPI变化")
        self.dpi_manager = get_dpi_manager()  # 重新获取DPI管理器
        self.last_calculated_width = 0  # 重置缓存的宽度
        # 重新配置列宽
        self.configure_columns()


def create_data_collection_column_manager(treeview: ttk.Treeview) -> DynamicColumnManager:
    """
    为数据采集表格创建列管理器
    
    Args:
        treeview: 数据采集页面的Treeview
        
    Returns:
        配置好的列管理器
    """
    manager = DynamicColumnManager(treeview)
    dpi_manager = get_dpi_manager()
    
    # 根据DPI环境调整列配置
    if dpi_manager.dpi_info.scale_factor >= 2.0:
        # 4K高DPI环境：优化列宽分配，增加更新时间列宽度并允许拉伸
        columns = [
            ColumnConfig("selected", "选择", min_width=60, preferred_width=70, weight=0, stretch=False, anchor="center", priority=1),
            ColumnConfig("data_source", "数据源", min_width=130, preferred_width=160, weight=0.5, stretch=False, anchor="center", priority=2),
            ColumnConfig("type", "类型", min_width=90, preferred_width=120, weight=0.8, stretch=False, anchor="w", priority=3),
            ColumnConfig("name", "名称", min_width=150, preferred_width=200, weight=1.0, stretch=True, anchor="w", priority=1),
            ColumnConfig("description", "描述", min_width=250, preferred_width=400, weight=2.5, stretch=True, anchor="w", priority=4),
            ColumnConfig("latest_update_time", "更新时间", min_width=240, preferred_width=280, weight=0.8, stretch=True, anchor="center", priority=2),
        ]
    elif dpi_manager.dpi_info.scale_factor >= 1.5:
        # 高DPI环境：适度增加宽度，更新时间列允许拉伸
        columns = [
            ColumnConfig("selected", "选择", min_width=55, preferred_width=65, weight=0, stretch=False, anchor="center", priority=1),
            ColumnConfig("data_source", "数据源", min_width=70, preferred_width=90, weight=0.5, stretch=False, anchor="center", priority=2),
            ColumnConfig("type", "类型", min_width=80, preferred_width=110, weight=0.8, stretch=False, anchor="w", priority=3),
            ColumnConfig("name", "名称", min_width=165, preferred_width=235, weight=1.2, stretch=True, anchor="w", priority=1),
            ColumnConfig("description", "描述", min_width=225, preferred_width=375, weight=2.5, stretch=True, anchor="w", priority=4),
            ColumnConfig("latest_update_time", "更新时间", min_width=150, preferred_width=190, weight=0.8, stretch=True, anchor="center", priority=2),
        ]
    else:
        # 标准DPI环境：优化后的配置，更新时间列允许拉伸
        columns = [
            ColumnConfig("selected", "选择", min_width=50, preferred_width=60, weight=0, stretch=False, anchor="center", priority=1),
            ColumnConfig("data_source", "数据源", min_width=60, preferred_width=80, weight=0.5, stretch=False, anchor="center", priority=2),
            ColumnConfig("type", "类型", min_width=70, preferred_width=100, weight=0.8, stretch=False, anchor="w", priority=3),
            ColumnConfig("name", "名称", min_width=150, preferred_width=220, weight=1.2, stretch=True, anchor="w", priority=1),
            ColumnConfig("description", "描述", min_width=200, preferred_width=350, weight=2.5, stretch=True, anchor="w", priority=4),
            ColumnConfig("latest_update_time", "更新时间", min_width=140, preferred_width=180, weight=0.8, stretch=True, anchor="center", priority=2),
        ]
    
    for col in columns:
        manager.add_column(col)
    
    logger.info(f"创建数据采集表格列管理器，DPI缩放: {dpi_manager.dpi_info.scale_factor:.2f}")
    return manager


def create_task_status_column_manager(treeview: ttk.Treeview) -> DynamicColumnManager:
    """
    为任务状态表格创建列管理器（4K优化）
    
    Args:
        treeview: 任务状态页面的Treeview
        
    Returns:
        配置好的列管理器
    """
    manager = DynamicColumnManager(treeview)
    dpi_manager = get_dpi_manager()
    
    # 根据DPI环境调整列配置
    if dpi_manager.dpi_info.scale_factor >= 2.0:
        # 4K高DPI环境：增加状态和更新时间列宽度，允许拉伸调整
        columns = [
            ColumnConfig("name", "任务名称", min_width=150, preferred_width=250, weight=1.5, stretch=True, anchor="w", priority=1),
            ColumnConfig("status", "最后状态", min_width=160, preferred_width=190, weight=0.8, stretch=True, anchor="center", priority=2),
            ColumnConfig("update_time", "更新时间", min_width=240, preferred_width=280, weight=1.0, stretch=True, anchor="center", priority=3),
            ColumnConfig("details", "详情", min_width=200, preferred_width=400, weight=2.5, stretch=True, anchor="w", priority=4),
        ]
    elif dpi_manager.dpi_info.scale_factor >= 1.5:
        # 高DPI环境：适度增加宽度，状态和更新时间列允许拉伸
        columns = [
            ColumnConfig("name", "任务名称", min_width=135, preferred_width=225, weight=1.5, stretch=True, anchor="w", priority=1),
            ColumnConfig("status", "最后状态", min_width=90, preferred_width=120, weight=0.8, stretch=True, anchor="center", priority=2),
            ColumnConfig("update_time", "更新时间", min_width=150, preferred_width=190, weight=1.0, stretch=True, anchor="center", priority=3),
            ColumnConfig("details", "详情", min_width=175, preferred_width=350, weight=2.5, stretch=True, anchor="w", priority=4),
        ]
    else:
        # 标准DPI环境：状态和更新时间列允许拉伸
        columns = [
            ColumnConfig("name", "任务名称", min_width=120, preferred_width=200, weight=1.5, stretch=True, anchor="w", priority=1),
            ColumnConfig("status", "最后状态", min_width=80, preferred_width=100, weight=0.8, stretch=True, anchor="center", priority=2),
            ColumnConfig("update_time", "更新时间", min_width=130, preferred_width=170, weight=1.0, stretch=True, anchor="center", priority=3),
            ColumnConfig("details", "详情", min_width=150, preferred_width=300, weight=2.5, stretch=True, anchor="w", priority=4),
        ]
    
    for col in columns:
        manager.add_column(col)
    
    logger.info(f"创建任务状态表格列管理器，DPI缩放: {dpi_manager.dpi_info.scale_factor:.2f}")
    return manager 