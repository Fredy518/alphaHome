"""
研究流水线步骤定义

定义了数据加载、因子计算、结果分析和保存等具体步骤
"""
from __future__ import annotations
import pandas as pd
import numpy as np
from typing import Dict, Any, List, TYPE_CHECKING
import logging

# 添加项目根目录到路径
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from research.tools.pipeline import Step, DataLoadStep, SaveResultsStep as BaseSaveResultsStep
from .factors import (
    calculate_moving_averages,
    calculate_volume_features,
    calculate_price_features,
    calculate_technical_indicators
)
from plotly.subplots import make_subplots
import plotly.graph_objects as go

if TYPE_CHECKING:
    from research.tools.context import ResearchContext


logger = logging.getLogger(__name__)


class LoadStockDataStep(DataLoadStep):
    """
    从数据库加载股票日线数据
    """
    
    def run(self, **kwargs) -> Dict[str, Any]:
        """执行数据加载"""
        # 获取参数
        stock_list = kwargs.get('stock_list', [])
        start_date = kwargs.get('start_date')
        end_date = kwargs.get('end_date')
        
        logger.info(f"开始加载股票数据: {len(stock_list)}只股票, {start_date} 至 {end_date}")
        
        try:
            # 使用providers数据提供层获取股票数据
            df = self.context.data_tool.get_stock_data(
                symbols=stock_list,
                start_date=start_date,
                end_date=end_date,
                adjust=True  # 使用复权价格
            )

            if df.empty:
                logger.warning("未查询到任何数据")
                return {'stock_data': pd.DataFrame()}

            # 数据已经在providers层进行了类型转换，这里只需要验证
            logger.info(f"数据列: {list(df.columns)}")
            logger.info(f"数据类型: {df.dtypes.to_dict()}")
            
            logger.info(f"成功加载 {len(df)} 条数据")
            
            # 数据质量检查
            null_counts = df.isnull().sum()
            if null_counts.any():
                logger.warning(f"数据中存在空值:\n{null_counts[null_counts > 0]}")
            
            return {'stock_data': df}
            
        except Exception as e:
            logger.error(f"数据加载失败: {e}")
            raise


class CalculateFactorsStep(Step):
    """
    计算技术因子
    """

    def __init__(self, context, ma_windows: List[int] = None, volume_window: int = 20):
        """
        初始化因子计算步骤

        Args:
            context: 研究上下文
            ma_windows: 移动平均窗口列表
            volume_window: 成交量分析窗口
        """
        super().__init__(context)
        self.ma_windows = ma_windows or [5, 10, 20, 60]
        self.volume_window = volume_window

    def run(self, **kwargs) -> Dict[str, Any]:
        """执行因子计算"""
        df = kwargs['stock_data']

        if df.empty:
            logger.warning("数据为空，跳过因子计算")
            return {'factor_data': pd.DataFrame()}

        # 使用初始化时设置的参数，但允许运行时覆盖
        ma_windows = kwargs.get('ma_windows', self.ma_windows)
        volume_window = kwargs.get('volume_window', self.volume_window)
        
        logger.info(f"开始计算因子，MA窗口: {ma_windows}, 成交量窗口: {volume_window}")
        
        # 按股票分组计算
        result_list = []
        
        for ts_code, group_df in df.groupby('ts_code'):
            logger.debug(f"计算股票 {ts_code} 的因子...")
            
            # 确保数据按日期排序
            group_df = group_df.sort_values('trade_date').copy()
            
            try:
                # 1. 计算移动平均线
                ma_features = calculate_moving_averages(group_df, windows=ma_windows)
                
                # 2. 计算成交量特征
                volume_features = calculate_volume_features(group_df, window=volume_window)
                
                # 3. 计算价格特征
                price_features = calculate_price_features(group_df)
                
                # 4. 计算技术指标
                tech_indicators = calculate_technical_indicators(group_df)
                
                # 合并所有特征
                for feat_df in [ma_features, volume_features, price_features, tech_indicators]:
                    if not feat_df.empty:
                        group_df = group_df.merge(
                            feat_df, 
                            on=['ts_code', 'trade_date'], 
                            how='left'
                        )
                
                result_list.append(group_df)
                
            except Exception as e:
                logger.error(f"计算股票 {ts_code} 因子时出错: {e}")
                continue
        
        # 合并所有结果
        if result_list:
            factor_data = pd.concat(result_list, ignore_index=True)
            logger.info(f"因子计算完成，共 {len(factor_data)} 条记录，{len(factor_data.columns)} 个特征")
        else:
            factor_data = pd.DataFrame()
            logger.warning("所有股票的因子计算都失败了")
        
        return {'factor_data': factor_data}


class AnalyzeResultsStep(Step):
    """
    分析因子结果
    """
    
    def run(self, **kwargs) -> Dict[str, Any]:
        """执行结果分析"""
        factor_data = kwargs.get('factor_data', pd.DataFrame())
        
        if factor_data.empty:
            logger.warning("因子数据为空，跳过分析")
            return {'analysis_results': {}}
        
        logger.info("开始分析因子结果...")
        
        analysis_results = {}
        
        # 1. 基础统计
        logger.info("计算基础统计信息...")
        analysis_results['basic_stats'] = {
            'total_records': len(factor_data),
            'stocks_count': factor_data['ts_code'].nunique(),
            'date_range': {
                'start': str(factor_data['trade_date'].min()),
                'end': str(factor_data['trade_date'].max())
            }
        }
        
        # 2. 因子统计
        logger.info("计算因子统计信息...")
        factor_columns = [col for col in factor_data.columns 
                         if col not in ['ts_code', 'trade_date', 'open', 'high', 
                                       'low', 'close', 'pre_close', 'change', 
                                       'pct_chg', 'vol', 'amount']]
        
        if factor_columns:
            factor_stats = factor_data[factor_columns].describe().to_dict()
            analysis_results['factor_stats'] = factor_stats
            
            # 计算因子相关性
            logger.info("计算因子相关性矩阵...")
            correlation_matrix = factor_data[factor_columns].corr()
            analysis_results['factor_correlations'] = correlation_matrix.to_dict()
        
        # 3. 收益率分析
        if 'pct_chg' in factor_data.columns:
            logger.info("分析收益率分布...")
            returns_analysis = {
                'mean_return': float(factor_data['pct_chg'].mean()),
                'std_return': float(factor_data['pct_chg'].std()),
                'min_return': float(factor_data['pct_chg'].min()),
                'max_return': float(factor_data['pct_chg'].max()),
                'positive_days': int((factor_data['pct_chg'] > 0).sum()),
                'negative_days': int((factor_data['pct_chg'] < 0).sum())
            }
            analysis_results['returns_analysis'] = returns_analysis
        
        # 4. 按股票分组分析
        logger.info("按股票分组分析...")
        stock_analysis = []
        for ts_code, group in factor_data.groupby('ts_code'):
            stock_info = {
                'ts_code': ts_code,
                'records': len(group),
                'avg_close': float(group['close'].mean()),
                'avg_volume': float(group['vol'].mean()),
                'total_return': float(((group['close'].iloc[-1] / group['close'].iloc[0]) - 1) * 100)
                              if len(group) > 0 else 0
            }
            stock_analysis.append(stock_info)
        
        analysis_results['stock_analysis'] = stock_analysis
        
        # 5. 最新数据快照
        latest_date = factor_data['trade_date'].max()
        latest_data = factor_data[factor_data['trade_date'] == latest_date]
        analysis_results['latest_snapshot'] = latest_data.to_dict('records')
        
        logger.info("结果分析完成")
        
        # 保存分析结果供后续使用
        return {
            'analysis_results': analysis_results,
            'factor_data': factor_data  # 传递给下一步
        }


class SaveResultsStep(BaseSaveResultsStep):
    """
    保存研究结果
    """

    def __init__(self, context: "ResearchContext"):
        super().__init__(context)
        self.logger = logging.getLogger(self.__class__.__name__)

    def _save_df_to_csv(self, df: pd.DataFrame, file_name: str) -> Path:
        """辅助函数，用于保存DataFrame到CSV"""
        output_path = self.context.project_path / self.output_dir
        output_path.mkdir(parents=True, exist_ok=True)
        filepath = output_path / file_name
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        self.logger.info(f"数据已保存至: {filepath}")
        return filepath

    def run(self, **kwargs) -> Dict[str, Any]:
        """执行结果保存"""
        factor_data = kwargs.get('factor_data', pd.DataFrame())
        analysis_results = kwargs.get('analysis_results', {})

        self.logger.info("开始保存研究结果...")

        saved_files = []

        # 1. 保存因子数据到CSV
        if self.save_to_csv and not factor_data.empty:
            from datetime import datetime
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

            # 保存完整因子数据
            factor_file = f"factor_data_{timestamp}.csv"
            filepath = self._save_df_to_csv(factor_data, factor_file)
            saved_files.append(str(filepath))

            # 保存分析摘要
            if analysis_results:
                import json
                summary_file = self.context.project_path / self.output_dir / f"analysis_summary_{timestamp}.json"
                summary_file.parent.mkdir(exist_ok=True)

                with open(summary_file, 'w', encoding='utf-8') as f:
                    json.dump(analysis_results, f, indent=2, ensure_ascii=False, default=str)

                saved_files.append(str(summary_file))
                self.logger.info(f"分析摘要已保存至: {summary_file}")

            # 按股票分别保存
            stock_output_dir = Path(self.output_dir) / "stocks"
            for ts_code, group in factor_data.groupby('ts_code'):
                stock_file = f"{ts_code}_{timestamp}.csv"
                # 此处直接调用to_csv，因为路径已包含子目录
                output_path = self.context.project_path / stock_output_dir
                output_path.mkdir(parents=True, exist_ok=True)
                stock_filepath = output_path / stock_file
                group.to_csv(stock_filepath, index=False, encoding='utf-8-sig')
                saved_files.append(str(stock_filepath))

        # 2. 保存到数据库
        if self.save_to_db and not factor_data.empty:
            table_name = kwargs.get('db_table', 'research_factor_results')
            try:
                # 假设db_manager有一个可以保存DataFrame的接口
                if hasattr(self.context.db_manager, 'save_dataframe'):
                    self.context.db_manager.save_dataframe(
                        factor_data, table_name, if_exists='append'
                    )
                    self.logger.info(f"数据已保存到数据库表: {table_name}")
                else:
                    self.logger.warning("db_manager没有'save_dataframe'方法，跳过数据库保存。")
            except Exception as e:
                self.logger.error(f"保存到数据库失败: {e}")

        self.logger.info(f"结果保存完成，共保存 {len(saved_files)} 个文件")

        return {
            'saved_files': saved_files,
            'summary': {
                'total_records': len(factor_data),
                'files_saved': len(saved_files),
                'save_to_csv': self.save_to_csv,
                'save_to_db': self.save_to_db
            }
        }


class FactorICAnalysisStep(Step):
    """
    因子IC分析步骤

    计算并展示因子的IC（信息系数）、ICIR（信息比R）、IC均值、IC标准差和IC时间序列图。
    """

    def __init__(self, context: "ResearchContext", factor_cols: List[str]):
        super().__init__(context)
        if not factor_cols:
            raise ValueError("必须提供至少一个因子列名。")
        self.factor_cols = factor_cols
        self.logger = logging.getLogger(self.__class__.__name__)

    def run(self, **kwargs) -> Dict[str, Any]:
        self.logger.info(f"开始对因子进行IC分析: {', '.join(self.factor_cols)}")
        data = kwargs.get("factor_data")
        if data is None or data.empty:
            self.logger.warning("因子数据为空，跳过IC分析。")
            return {}

        if "forward_return" not in data.columns:
            raise ValueError("数据中缺少 'forward_return' 列，无法进行IC分析。")

        ic_results = {}
        for factor in self.factor_cols:
            if factor not in data.columns:
                self.logger.warning(f"因子 '{factor}' 不在数据中，已跳过。")
                continue

            # 按日期计算截面IC
            ic_series = data.groupby("trade_date").apply(
                lambda x: x[factor].corr(x["forward_return"], method="spearman")
            )
            ic_series = ic_series.dropna()

            if ic_series.empty:
                self.logger.warning(f"因子 '{factor}' 的IC序列为空，可能数据不足。")
                continue

            # 计算IC指标
            ic_mean = ic_series.mean()
            ic_std = ic_series.std()
            icir = ic_mean / ic_std if ic_std != 0 else 0
            positive_ratio = (ic_series > 0).sum() / len(ic_series) * 100

            ic_results[factor] = {
                "IC均值": ic_mean,
                "IC标准差": ic_std,
                "ICIR": icir,
                "IC为正概率(%)": positive_ratio,
                "IC序列": ic_series,
            }

            self.logger.info(
                f"因子 [{factor}] | IC均值: {ic_mean:.4f} | "
                f"ICIR: {icir:.4f} | IC为正概率: {positive_ratio:.2f}%"
            )

        # 可视化IC时间序列
        self._plot_ic_series(ic_results)

        # 将分析结果存储到上下文中，方便后续使用
        self.context.store_analysis_result("ic_analysis", ic_results)

        return {"factor_data": data}

    def _plot_ic_series(self, ic_results: Dict[str, Dict[str, Any]]):
        """绘制IC时间序列图"""
        if not ic_results:
            return

        num_factors = len(ic_results)
        fig = make_subplots(
            rows=num_factors,
            cols=1,
            subplot_titles=[f"因子: {factor}" for factor in ic_results.keys()],
            vertical_spacing=0.1,
        )

        for i, (factor, results) in enumerate(ic_results.items()):
            ic_series = results["IC序列"]
            ic_mean = results["IC均值"]

            # 添加IC序列线
            fig.add_trace(
                go.Scatter(
                    x=ic_series.index,
                    y=ic_series.values,
                    mode="lines",
                    name="IC",
                    legendgroup=f"group{i}",
                ),
                row=i + 1,
                col=1,
            )
            # 添加IC均值线
            fig.add_hline(
                y=ic_mean,
                line_width=2,
                line_dash="dash",
                line_color="red",
                annotation_text=f"IC均值: {ic_mean:.4f}",
                annotation_position="bottom right",
                row=i + 1,  # type: ignore
                col=1,  # type: ignore
            )

        fig.update_layout(
            title_text="因子IC时间序列分析",
            height=250 * num_factors,
            showlegend=False,
            template="plotly_white",
        )
        fig.show()


class FactorQuantileAnalysisStep(Step):
    """
    因子分位数分析步骤

    根据因子值将股票分组，计算并展示各组的累计收益率。
    """

    def __init__(
        self,
        context: "ResearchContext",
        factor_col: str,
        quantiles: int = 5,
        forward_return_col: str = "forward_return",
    ):
        super().__init__(context)
        self.factor_col = factor_col
        self.quantiles = quantiles
        self.forward_return_col = forward_return_col
        self.logger = logging.getLogger(self.__class__.__name__)

    def run(self, **kwargs) -> Dict[str, Any]:
        self.logger.info(
            f"开始对因子 '{self.factor_col}' 进行 {self.quantiles} 分位数分析"
        )
        data = kwargs.get("factor_data")
        if data is None or data.empty:
            self.logger.warning("因子数据为空，跳过分位数分析。")
            return {}

        if self.factor_col not in data.columns:
            raise ValueError(f"数据中缺少因子列 '{self.factor_col}'")
        if self.forward_return_col not in data.columns:
            raise ValueError(f"数据中缺少远期收益率列 '{self.forward_return_col}'")

        # 1. 计算因子分位数
        data["factor_quantile"] = data.groupby("trade_date")[self.factor_col].transform(
            lambda x: pd.qcut(x, self.quantiles, labels=False, duplicates="drop") + 1
        )
        data = data.dropna(subset=["factor_quantile"])
        data["factor_quantile"] = data["factor_quantile"].astype(int)

        # 2. 计算每个分位数组的日均收益率
        quantile_returns = data.groupby(["trade_date", "factor_quantile"])[
            self.forward_return_col
        ].mean()
        quantile_returns = quantile_returns.unstack()

        # 3. 计算累计收益率
        cumulative_returns = (1 + quantile_returns).cumprod()
        cumulative_returns = cumulative_returns.fillna(1.0)

        # 4. 可视化
        self._plot_quantile_returns(cumulative_returns)

        # 5. 存储结果
        self.context.store_analysis_result(
            "quantile_analysis",
            {
                "quantile_returns": quantile_returns,
                "cumulative_returns": cumulative_returns,
            },
        )

        return {"factor_data": data}

    def _plot_quantile_returns(self, cumulative_returns: pd.DataFrame):
        fig = go.Figure()
        for i in range(1, self.quantiles + 1):
            if i in cumulative_returns.columns:
                fig.add_trace(
                    go.Scatter(
                        x=cumulative_returns.index,
                        y=cumulative_returns[i],
                        mode="lines",
                        name=f"分位 {i}",
                    )
                )

        fig.update_layout(
            title=f"因子 '{self.factor_col}' 分位数累计收益率",
            xaxis_title="日期",
            yaxis_title="累计收益",
            legend_title="因子分位",
            template="plotly_white",
        )
        fig.show()
