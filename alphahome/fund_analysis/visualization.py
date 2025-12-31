"""
可视化模块 - 基金分析可视化功能

本模块提供 Visualization 类，用于生成各种分析图表：
- plot_nav: 净值曲线图
- plot_drawdown: 回撤图（水下曲线）
- plot_monthly_heatmap: 月度收益热力图
- plot_weights: 持仓权重变化图

依赖说明：
- 所有功能需要 matplotlib >= 3.3
- 缺少 matplotlib 时抛出 ImportError 并给出安装提示

使用说明：
- 所有绑图方法返回 (fig, ax) 元组
- 支持无 GUI 环境（使用 Agg backend）
- 可选传入 ax 参数在现有图上绑制
"""

import logging
from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# 检查 matplotlib 是否可用
_MATPLOTLIB_AVAILABLE = False
try:
    import matplotlib
    matplotlib.use('Agg')  # 使用非 GUI backend
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from matplotlib.figure import Figure
    from matplotlib.axes import Axes
    _MATPLOTLIB_AVAILABLE = True
except ImportError:
    plt = None
    Figure = None
    Axes = None


def _check_matplotlib():
    """检查 matplotlib 是否可用"""
    if not _MATPLOTLIB_AVAILABLE:
        raise ImportError(
            "可视化功能需要 matplotlib >= 3.3，请运行: pip install matplotlib"
        )


class Visualization:
    """
    可视化类
    
    提供基金分析相关的可视化功能：
    - 净值曲线图
    - 回撤图
    - 月度收益热力图
    - 持仓权重变化图
    
    示例:
        >>> from alphahome.fund_analysis.visualization import Visualization
        >>> 
        >>> viz = Visualization()
        >>> nav = pd.Series([1.0, 1.05, 1.02, 1.08],
        ...                 index=pd.date_range('2024-01-01', periods=4))
        >>> fig, ax = viz.plot_nav(nav)
        >>> fig.savefig('nav.png')
    """
    
    def __init__(self, figsize: Tuple[int, int] = (12, 6), dpi: int = 100):
        """
        初始化可视化类
        
        参数:
            figsize: 默认图表大小，默认 (12, 6)
            dpi: 图表分辨率，默认 100
        """
        _check_matplotlib()
        self.figsize = figsize
        self.dpi = dpi
        
        # 设置中文字体支持
        self._setup_chinese_font()
    
    def _setup_chinese_font(self):
        """设置中文字体支持"""
        try:
            # 尝试使用系统中文字体
            plt.rcParams['font.sans-serif'] = [
                'SimHei', 'Microsoft YaHei', 'WenQuanYi Micro Hei',
                'DejaVu Sans', 'Arial Unicode MS'
            ]
            plt.rcParams['axes.unicode_minus'] = False
        except Exception as e:
            logger.debug(f"设置中文字体失败: {e}")
    
    def plot_nav(
        self,
        nav_series: pd.Series,
        benchmark: Optional[pd.Series] = None,
        ax: Optional['Axes'] = None,
        save_path: Optional[str] = None,
        title: str = "净值曲线",
        nav_label: str = "组合净值",
        benchmark_label: str = "基准净值"
    ) -> Tuple['Figure', 'Axes']:
        """
        绑制净值曲线图
        
        参数:
            nav_series: 组合净值序列
            benchmark: 基准净值序列（可选）
            ax: matplotlib Axes 对象（可选，用于在现有图上绑制）
            save_path: 保存路径（可选）
            title: 图表标题
            nav_label: 组合净值图例标签
            benchmark_label: 基准净值图例标签
        
        返回:
            Tuple[Figure, Axes]: matplotlib 图表对象
        
        示例:
            >>> fig, ax = viz.plot_nav(nav, benchmark=benchmark_nav)
            >>> fig.savefig('nav_comparison.png')
        """
        _check_matplotlib()
        
        if ax is None:
            fig, ax = plt.subplots(figsize=self.figsize, dpi=self.dpi)
        else:
            fig = ax.get_figure()
        
        # 绘制组合净值
        if nav_series is not None and not nav_series.empty:
            # 归一化到起始值为 1
            normalized_nav = nav_series / nav_series.iloc[0]
            ax.plot(normalized_nav.index, normalized_nav.values,
                   label=nav_label, color='#2980b9', linewidth=1.5)
        
        # 绘制基准净值
        if benchmark is not None and not benchmark.empty:
            # 对齐到组合日期范围
            if nav_series is not None and not nav_series.empty:
                start_date = nav_series.index[0]
                benchmark_aligned = benchmark[benchmark.index >= start_date]
                if not benchmark_aligned.empty:
                    normalized_benchmark = benchmark_aligned / benchmark_aligned.iloc[0]
                    ax.plot(normalized_benchmark.index, normalized_benchmark.values,
                           label=benchmark_label, color='#e74c3c', linewidth=1.5,
                           linestyle='--', alpha=0.8)
        
        # 设置图表样式
        ax.set_title(title, fontsize=14, fontweight='bold')
        ax.set_xlabel('日期', fontsize=10)
        ax.set_ylabel('净值', fontsize=10)
        ax.legend(loc='upper left')
        ax.grid(True, alpha=0.3)
        
        # 设置日期格式
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        fig.autofmt_xdate()
        
        # 保存图表
        if save_path:
            fig.savefig(save_path, bbox_inches='tight', dpi=self.dpi)
            logger.info(f"净值曲线图已保存到: {save_path}")
        
        return fig, ax
    
    def plot_drawdown(
        self,
        nav_series: pd.Series,
        ax: Optional['Axes'] = None,
        save_path: Optional[str] = None,
        title: str = "回撤曲线",
        fill_color: str = '#e74c3c',
        fill_alpha: float = 0.3
    ) -> Tuple['Figure', 'Axes']:
        """
        绘制回撤图（水下曲线）
        
        参数:
            nav_series: 净值序列
            ax: matplotlib Axes 对象（可选）
            save_path: 保存路径（可选）
            title: 图表标题
            fill_color: 填充颜色
            fill_alpha: 填充透明度
        
        返回:
            Tuple[Figure, Axes]: matplotlib 图表对象
        """
        _check_matplotlib()
        
        if ax is None:
            fig, ax = plt.subplots(figsize=self.figsize, dpi=self.dpi)
        else:
            fig = ax.get_figure()
        
        if nav_series is None or nav_series.empty:
            ax.set_title(title)
            return fig, ax
        
        # 计算水下曲线
        running_max = nav_series.expanding().max()
        underwater = (nav_series / running_max) - 1
        
        # 绘制水下曲线
        ax.fill_between(underwater.index, underwater.values, 0,
                       color=fill_color, alpha=fill_alpha)
        ax.plot(underwater.index, underwater.values,
               color=fill_color, linewidth=1)
        
        # 标记最大回撤点
        min_idx = underwater.idxmin()
        min_val = underwater.min()
        ax.scatter([min_idx], [min_val], color='#c0392b', s=50, zorder=5)
        ax.annotate(f'最大回撤: {min_val:.2%}',
                   xy=(min_idx, min_val),
                   xytext=(10, -20),
                   textcoords='offset points',
                   fontsize=9,
                   color='#c0392b')
        
        # 设置图表样式
        ax.set_title(title, fontsize=14, fontweight='bold')
        ax.set_xlabel('日期', fontsize=10)
        ax.set_ylabel('回撤', fontsize=10)
        ax.grid(True, alpha=0.3)
        ax.axhline(y=0, color='black', linewidth=0.5)
        
        # 设置 y 轴为百分比格式
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:.0%}'))
        
        # 设置日期格式
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        fig.autofmt_xdate()
        
        # 保存图表
        if save_path:
            fig.savefig(save_path, bbox_inches='tight', dpi=self.dpi)
            logger.info(f"回撤曲线图已保存到: {save_path}")
        
        return fig, ax
    
    def plot_monthly_heatmap(
        self,
        monthly_returns: pd.DataFrame,
        ax: Optional['Axes'] = None,
        save_path: Optional[str] = None,
        title: str = "月度收益热力图",
        cmap: str = 'RdYlGn',
        annot: bool = True
    ) -> Tuple['Figure', 'Axes']:
        """
        绘制月度收益热力图（纯 matplotlib 实现，不依赖 seaborn）
        
        参数:
            monthly_returns: 月度收益矩阵，索引为年份，列为月份 (1-12)
            ax: matplotlib Axes 对象（可选）
            save_path: 保存路径（可选）
            title: 图表标题
            cmap: 颜色映射
            annot: 是否显示数值标注
        
        返回:
            Tuple[Figure, Axes]: matplotlib 图表对象
        """
        _check_matplotlib()
        
        if ax is None:
            fig, ax = plt.subplots(figsize=(14, max(6, len(monthly_returns) * 0.5)),
                                   dpi=self.dpi)
        else:
            fig = ax.get_figure()
        
        if monthly_returns is None or monthly_returns.empty:
            ax.set_title(title)
            return fig, ax
        
        # 确保列为 1-12 月
        month_cols = list(range(1, 13))
        data = monthly_returns.reindex(columns=month_cols)
        
        # 创建热力图
        values = data.values
        
        # 处理 NaN 值
        masked_values = np.ma.masked_invalid(values)
        
        # 确定颜色范围（对称）
        vmax = np.nanmax(np.abs(values)) if not np.all(np.isnan(values)) else 0.1
        vmin = -vmax
        
        # 绘制热力图
        im = ax.imshow(masked_values, cmap=cmap, aspect='auto',
                      vmin=vmin, vmax=vmax)
        
        # 设置坐标轴
        ax.set_xticks(np.arange(12))
        ax.set_xticklabels([f'{m}月' for m in range(1, 13)])
        ax.set_yticks(np.arange(len(data.index)))
        ax.set_yticklabels([str(y) for y in data.index])
        
        # 添加数值标注
        if annot:
            for i in range(len(data.index)):
                for j in range(12):
                    val = values[i, j]
                    if not np.isnan(val):
                        # 根据背景色选择文字颜色
                        text_color = 'white' if abs(val) > vmax * 0.5 else 'black'
                        ax.text(j, i, f'{val:.1%}',
                               ha='center', va='center',
                               color=text_color, fontsize=8)
        
        # 添加颜色条
        cbar = fig.colorbar(im, ax=ax, shrink=0.8)
        cbar.ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:.0%}'))
        
        # 设置标题
        ax.set_title(title, fontsize=14, fontweight='bold', pad=20)
        
        # 保存图表
        if save_path:
            fig.savefig(save_path, bbox_inches='tight', dpi=self.dpi)
            logger.info(f"月度收益热力图已保存到: {save_path}")
        
        return fig, ax
    
    def plot_weights(
        self,
        weights_ts: pd.DataFrame,
        ax: Optional['Axes'] = None,
        save_path: Optional[str] = None,
        title: str = "持仓权重变化",
        top_n: int = 10
    ) -> Tuple['Figure', 'Axes']:
        """
        绘制持仓权重随时间变化的堆叠面积图
        
        参数:
            weights_ts: 权重时序 DataFrame，索引为日期，列为 fund_id
            ax: matplotlib Axes 对象（可选）
            save_path: 保存路径（可选）
            title: 图表标题
            top_n: 显示前 N 大持仓，其余合并为"其他"
        
        返回:
            Tuple[Figure, Axes]: matplotlib 图表对象
        """
        _check_matplotlib()
        
        if ax is None:
            fig, ax = plt.subplots(figsize=self.figsize, dpi=self.dpi)
        else:
            fig = ax.get_figure()
        
        if weights_ts is None or weights_ts.empty:
            ax.set_title(title)
            return fig, ax
        
        # 处理数据
        weights = weights_ts.copy()
        
        # 如果列数超过 top_n，合并小权重为"其他"
        if len(weights.columns) > top_n:
            # 计算平均权重排序
            avg_weights = weights.mean().sort_values(ascending=False)
            top_cols = avg_weights.head(top_n).index.tolist()
            other_cols = [c for c in weights.columns if c not in top_cols]
            
            # 创建新的 DataFrame
            plot_data = weights[top_cols].copy()
            if other_cols:
                plot_data['其他'] = weights[other_cols].sum(axis=1)
        else:
            plot_data = weights
        
        # 确保权重和为 1（处理可能的舍入误差）
        plot_data = plot_data.div(plot_data.sum(axis=1), axis=0).fillna(0)
        
        # 绘制堆叠面积图
        ax.stackplot(plot_data.index, plot_data.T.values,
                    labels=plot_data.columns,
                    alpha=0.8)
        
        # 设置图表样式
        ax.set_title(title, fontsize=14, fontweight='bold')
        ax.set_xlabel('日期', fontsize=10)
        ax.set_ylabel('权重', fontsize=10)
        ax.set_ylim(0, 1)
        
        # 设置 y 轴为百分比格式
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:.0%}'))
        
        # 图例
        ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1),
                 fontsize=8, ncol=1)
        
        # 设置日期格式
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        fig.autofmt_xdate()
        
        # 调整布局以适应图例
        fig.tight_layout()
        
        # 保存图表
        if save_path:
            fig.savefig(save_path, bbox_inches='tight', dpi=self.dpi)
            logger.info(f"持仓权重图已保存到: {save_path}")
        
        return fig, ax
    
    def to_bytes(self, fig: 'Figure', format: str = 'png') -> bytes:
        """
        将图表转换为字节流
        
        参数:
            fig: matplotlib Figure 对象
            format: 图片格式，默认 'png'
        
        返回:
            bytes: 图片字节流
        """
        _check_matplotlib()
        
        buf = BytesIO()
        fig.savefig(buf, format=format, bbox_inches='tight', dpi=self.dpi)
        buf.seek(0)
        return buf.read()
    
    def close(self, fig: 'Figure'):
        """
        关闭图表，释放资源
        
        参数:
            fig: matplotlib Figure 对象
        """
        _check_matplotlib()
        plt.close(fig)

