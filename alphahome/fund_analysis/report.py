"""
报告生成器 - 基金分析报告生成模块

本模块提供 ReportBuilder 类，用于生成各种格式的分析报告：
- summary_dataframe: 汇总统计 DataFrame
- to_dict: JSON 可序列化字典（唯一序列化出口）
- to_excel: Excel 报告（需要 openpyxl）
- to_html: HTML 报告（需要 jinja2）

依赖说明：
- 核心功能（summary_dataframe, to_dict）无额外依赖
- Excel 导出需要 openpyxl
- HTML 报告需要 jinja2
"""

import json
import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd

from ._schema import METRICS_SCHEMA, MetricDefinition

logger = logging.getLogger(__name__)


class ReportBuilder:
    """
    报告生成器
    
    提供多种格式的报告生成能力：
    - summary_dataframe: 汇总统计表格
    - to_dict: JSON 可序列化字典
    - to_excel: Excel 报告
    - to_html: HTML 报告
    
    示例:
        >>> from alphahome.fund_analysis import ReportBuilder
        >>> 
        >>> builder = ReportBuilder()
        >>> metrics = {'cumulative_return': 0.15, 'sharpe_ratio': 1.2}
        >>> df = builder.summary_dataframe(metrics)
        >>> print(df)
    """
    
    def __init__(self):
        """初始化报告生成器"""
        pass
    
    def summary_dataframe(self, metrics: Dict[str, Any]) -> pd.DataFrame:
        """
        生成汇总统计 DataFrame
        
        参数:
            metrics: 指标字典，key 为指标名，value 为指标值
        
        返回:
            pd.DataFrame: 汇总统计表格
                - 列: ['指标名称', '值', '单位']
                - 数值使用小数（非百分比），单位列标注 '%' 或 '倍' 等
        
        示例:
            >>> metrics = {'cumulative_return': 0.15, 'sharpe_ratio': 1.2}
            >>> df = builder.summary_dataframe(metrics)
            >>> print(df)
               指标名称    值  单位
            0  累计收益率  0.15   %
            1   夏普比率   1.2
        """
        if not metrics:
            return pd.DataFrame(columns=['指标名称', '值', '单位'])
        
        rows = []
        for key, value in metrics.items():
            # 获取指标定义
            metric_def = METRICS_SCHEMA.get(key)
            
            if metric_def is not None:
                name = metric_def.name
                unit = metric_def.unit
            else:
                # 未定义的指标使用 key 作为名称
                name = key
                unit = ""
            
            # 处理值
            display_value = self._format_value(value)
            
            rows.append({
                '指标名称': name,
                '值': display_value,
                '单位': unit
            })
        
        return pd.DataFrame(rows)
    
    def to_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        输出结构化字典（可 JSON 序列化）
        
        这是报告生成的单一数据源，确保所有输出都可以被 JSON 序列化。
        
        转换规则:
        - pd.Series -> list of {date/index, value}
        - pd.DataFrame -> list of dict (records) 或嵌套字典
        - date/datetime -> str (YYYY-MM-DD)
        - np.nan/None -> null (JSON)
        - np.integer/np.floating -> Python int/float
        
        参数:
            data: 待转换的数据字典
        
        返回:
            dict: 可 JSON 序列化的字典
        
        示例:
            >>> data = {'metrics': {'return': 0.1}, 'nav': pd.Series([1.0, 1.1])}
            >>> result = builder.to_dict(data)
            >>> json.dumps(result)  # 不会抛出异常
        """
        return self._serialize_recursive(data)
    
    def to_excel(
        self,
        data: Dict[str, Any],
        path: str = 'report.xlsx',
        include_charts: bool = False
    ) -> None:
        """
        导出 Excel 报告
        
        工作表结构:
        - 'Summary': 汇总统计
        - 'Monthly Returns': 月度收益矩阵（如有）
        - 'Trades': 交易记录（如有）
        - 'Holdings': 持仓历史（如有）
        
        参数:
            data: 报告数据字典，应包含以下可选 key:
                - metrics: 指标字典
                - monthly_returns: 月度收益 DataFrame
                - trades: 交易记录 DataFrame
                - holdings: 持仓历史 DataFrame
            path: 输出文件路径，默认 'report.xlsx'
            include_charts: 是否包含图表（暂不支持）
        
        抛出:
            ImportError: 当 openpyxl 未安装时
        
        示例:
            >>> data = {'metrics': {'return': 0.1}}
            >>> builder.to_excel(data, 'my_report.xlsx')
        """
        try:
            import openpyxl
        except ImportError:
            raise ImportError(
                "Excel 导出功能需要 openpyxl，请运行: pip install openpyxl"
            )
        
        with pd.ExcelWriter(path, engine='openpyxl') as writer:
            # Summary 工作表
            if 'metrics' in data and data['metrics']:
                summary_df = self.summary_dataframe(data['metrics'])
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Monthly Returns 工作表
            if 'monthly_returns' in data:
                monthly = data['monthly_returns']
                if isinstance(monthly, pd.DataFrame) and not monthly.empty:
                    monthly.to_excel(writer, sheet_name='Monthly Returns')
                elif isinstance(monthly, dict) and monthly:
                    # 从字典重建 DataFrame
                    monthly_df = pd.DataFrame(monthly)
                    monthly_df.to_excel(writer, sheet_name='Monthly Returns')
            
            # Trades 工作表
            if 'trades' in data:
                trades = data['trades']
                if isinstance(trades, pd.DataFrame) and not trades.empty:
                    trades.to_excel(writer, sheet_name='Trades', index=False)
                elif isinstance(trades, list) and trades:
                    trades_df = pd.DataFrame(trades)
                    trades_df.to_excel(writer, sheet_name='Trades', index=False)
            
            # Holdings 工作表
            if 'holdings' in data:
                holdings = data['holdings']
                if isinstance(holdings, pd.DataFrame) and not holdings.empty:
                    holdings.to_excel(writer, sheet_name='Holdings', index=False)
                elif isinstance(holdings, list) and holdings:
                    holdings_df = pd.DataFrame(holdings)
                    holdings_df.to_excel(writer, sheet_name='Holdings', index=False)
        
        logger.info(f"Excel 报告已导出到: {path}")
    
    def to_html(
        self,
        data: Dict[str, Any],
        charts: Optional[Dict[str, bytes]] = None,
        title: str = "基金绩效分析报告"
    ) -> str:
        """
        生成 HTML 报告
        
        参数:
            data: 报告数据字典
            charts: 图表字典，key 为图表名，value 为 PNG 图片的 bytes
                   图表将以内联 base64 编码的方式嵌入 HTML
            title: 报告标题
        
        返回:
            str: HTML 报告内容
        
        抛出:
            ImportError: 当 jinja2 未安装时
        
        示例:
            >>> data = {'metrics': {'return': 0.1}}
            >>> html = builder.to_html(data, title='我的报告')
            >>> with open('report.html', 'w') as f:
            ...     f.write(html)
        """
        try:
            from jinja2 import Template
        except ImportError:
            raise ImportError(
                "HTML 报告功能需要 jinja2，请运行: pip install jinja2"
            )
        
        import base64
        
        # 准备模板数据
        template_data = {
            'title': title,
            'metrics': [],
            'monthly_returns': None,
            'charts': [],
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
        
        # 处理指标
        if 'metrics' in data and data['metrics']:
            summary_df = self.summary_dataframe(data['metrics'])
            template_data['metrics'] = summary_df.to_dict('records')
        
        # 处理月度收益
        if 'monthly_returns' in data:
            monthly = data['monthly_returns']
            if isinstance(monthly, pd.DataFrame) and not monthly.empty:
                template_data['monthly_returns'] = monthly.to_html(
                    classes='table table-striped',
                    float_format=lambda x: f'{x:.2%}' if pd.notna(x) else ''
                )
            elif isinstance(monthly, dict) and monthly:
                monthly_df = pd.DataFrame(monthly)
                template_data['monthly_returns'] = monthly_df.to_html(
                    classes='table table-striped',
                    float_format=lambda x: f'{x:.2%}' if pd.notna(x) else ''
                )
        
        # 处理图表
        if charts:
            for name, img_bytes in charts.items():
                if img_bytes:
                    b64_img = base64.b64encode(img_bytes).decode('utf-8')
                    template_data['charts'].append({
                        'name': name,
                        'data': f'data:image/png;base64,{b64_img}'
                    })
        
        # HTML 模板
        html_template = self._get_html_template()
        template = Template(html_template)
        
        return template.render(**template_data)
    
    def _format_value(self, value: Any) -> Any:
        """格式化单个值用于显示"""
        if value is None:
            return None
        if isinstance(value, float):
            if np.isnan(value):
                return None
            if np.isinf(value):
                return "∞" if value > 0 else "-∞"
            return value
        if isinstance(value, (np.floating, np.integer)):
            if np.isnan(value):
                return None
            return float(value) if isinstance(value, np.floating) else int(value)
        return value
    
    def _serialize_recursive(self, obj: Any) -> Any:
        """递归序列化对象为 JSON 兼容格式"""
        # None
        if obj is None:
            return None
        
        # pandas NaN
        if isinstance(obj, float) and np.isnan(obj):
            return None
        
        # numpy 类型
        if isinstance(obj, np.floating):
            if np.isnan(obj):
                return None
            if np.isinf(obj):
                return "Infinity" if obj > 0 else "-Infinity"
            return float(obj)
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.bool_):
            return bool(obj)
        if isinstance(obj, np.ndarray):
            return [self._serialize_recursive(x) for x in obj.tolist()]
        
        # 日期时间
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        if isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        if isinstance(obj, pd.Timestamp):
            return obj.strftime('%Y-%m-%d')
        
        # pandas Series
        if isinstance(obj, pd.Series):
            if obj.empty:
                return []
            result = []
            for idx, val in obj.items():
                item = {
                    'index': self._serialize_recursive(idx),
                    'value': self._serialize_recursive(val)
                }
                result.append(item)
            return result
        
        # pandas DataFrame
        if isinstance(obj, pd.DataFrame):
            if obj.empty:
                return {}
            # 转换为嵌套字典格式
            return {
                str(col): {
                    str(idx): self._serialize_recursive(val)
                    for idx, val in obj[col].items()
                }
                for col in obj.columns
            }
        
        # 字典
        if isinstance(obj, dict):
            return {
                str(k): self._serialize_recursive(v)
                for k, v in obj.items()
            }
        
        # 列表/元组
        if isinstance(obj, (list, tuple)):
            return [self._serialize_recursive(x) for x in obj]
        
        # 基本类型
        if isinstance(obj, (str, int, float, bool)):
            if isinstance(obj, float) and (np.isnan(obj) or np.isinf(obj)):
                return None if np.isnan(obj) else ("Infinity" if obj > 0 else "-Infinity")
            return obj
        
        # 其他类型尝试转换为字符串
        try:
            return str(obj)
        except Exception:
            logger.warning(f"无法序列化类型 {type(obj)}，返回 None")
            return None
    
    def _get_html_template(self) -> str:
        """获取 HTML 报告模板"""
        return '''<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{ title }}</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            background-color: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            padding: 30px;
            margin-bottom: 20px;
        }
        h1 {
            color: #2c3e50;
            border-bottom: 2px solid #3498db;
            padding-bottom: 10px;
        }
        h2 {
            color: #34495e;
            margin-top: 30px;
        }
        .table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }
        .table th, .table td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }
        .table th {
            background-color: #3498db;
            color: white;
        }
        .table tr:hover {
            background-color: #f5f5f5;
        }
        .table-striped tbody tr:nth-child(odd) {
            background-color: #f9f9f9;
        }
        .chart-container {
            margin: 20px 0;
            text-align: center;
        }
        .chart-container img {
            max-width: 100%;
            height: auto;
            border-radius: 4px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .footer {
            text-align: center;
            color: #7f8c8d;
            font-size: 0.9em;
            margin-top: 30px;
            padding-top: 20px;
            border-top: 1px solid #eee;
        }
        .metric-value {
            font-weight: bold;
            color: #2980b9;
        }
        .metric-unit {
            color: #7f8c8d;
            font-size: 0.9em;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>{{ title }}</h1>
        
        {% if metrics %}
        <h2>绩效指标汇总</h2>
        <table class="table table-striped">
            <thead>
                <tr>
                    <th>指标名称</th>
                    <th>值</th>
                    <th>单位</th>
                </tr>
            </thead>
            <tbody>
                {% for metric in metrics %}
                <tr>
                    <td>{{ metric['指标名称'] }}</td>
                    <td class="metric-value">
                        {% if metric['值'] is not none %}
                            {% if metric['单位'] == '%' %}
                                {{ "%.2f"|format(metric['值'] * 100) }}
                            {% else %}
                                {{ "%.4f"|format(metric['值']) if metric['值'] is number else metric['值'] }}
                            {% endif %}
                        {% else %}
                            -
                        {% endif %}
                    </td>
                    <td class="metric-unit">{{ metric['单位'] }}</td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
        {% endif %}
        
        {% if monthly_returns %}
        <h2>月度收益</h2>
        {{ monthly_returns | safe }}
        {% endif %}
        
        {% if charts %}
        <h2>图表</h2>
        {% for chart in charts %}
        <div class="chart-container">
            <h3>{{ chart.name }}</h3>
            <img src="{{ chart.data }}" alt="{{ chart.name }}">
        </div>
        {% endfor %}
        {% endif %}
        
        <div class="footer">
            <p>报告生成时间: {{ generated_at }}</p>
            <p>由 AlphaHome Fund Analysis 生成</p>
        </div>
    </div>
</body>
</html>'''

