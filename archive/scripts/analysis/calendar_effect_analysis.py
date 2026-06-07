#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
A股宽基指数日历效应分析脚本

分析沪深300、中证500、创业板指等主要指数在不同月份的平均表现和胜率，
检测是否存在显著的日历效应（January Effect等）。
"""

from __future__ import annotations

import logging
from typing import Dict, Optional
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy import stats

from alphahome.common.logging_utils import get_logger
from alphahome.common.db_manager import DBManager

logger = get_logger(__name__)

# A股主要宽基指数
INDEXES = {
    "000300.SH": "沪深300",
    "000905.SH": "中证500",
    "399006.SZ": "创业板指",
    "000852.SH": "中证1000",
    "000016.SH": "上证50",
    "000001.SH": "上证综指"
}


class CalendarEffectAnalyzer:
    """日历效应分析器"""

    def __init__(self, db_manager: DBManager):
        self.db = db_manager
        self.monthly_returns = {}  # {index_code: DataFrame}

    async def fetch_index_data(self, start_date: str = "20100101", end_date: str = "20241231") -> Dict[str, pd.DataFrame]:
        """获取指数日线数据"""
        index_data = {}

        for ts_code, name in INDEXES.items():
            logger.info(f"获取指数数据: {name} ({ts_code})")

            query = f"""
            SELECT trade_date, close
            FROM tushare.index_factor_pro
            WHERE ts_code = '{ts_code}'
              AND trade_date >= '{start_date}'
              AND trade_date <= '{end_date}'
            ORDER BY trade_date
            """

            try:
                records = await self.db.fetch(query)
                if records:
                    df = pd.DataFrame([dict(r) for r in records])
                    if not {"trade_date", "close"}.issubset(df.columns):
                        logger.warning(f"返回数据缺少必要列，跳过: {name}")
                        continue
                    df["trade_date"] = pd.to_datetime(df["trade_date"])
                    df = df.set_index("trade_date")
                    df["close"] = df["close"].astype(float)
                    index_data[ts_code] = df
                    logger.info(f"获取到 {len(df)} 条数据")
                else:
                    logger.warning(f"未获取到数据: {name}")
            except Exception as e:
                logger.error(f"获取数据失败 {name}: {e}")

        return index_data

    def calculate_monthly_returns(self, index_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """计算月度收益率"""
        monthly_returns = {}

        for ts_code, df in index_data.items():
            if df.empty:
                continue

            # 计算日收益率
            daily_returns = df["close"].pct_change()

            # 重采样到月度收益率
            monthly_return = daily_returns.resample("ME").apply(lambda x: (1 + x).prod() - 1)
            monthly_return = monthly_return.dropna()

            # 转换为DataFrame，包含年月信息
            monthly_df = pd.DataFrame({
                "monthly_return": monthly_return,
                "year": monthly_return.index.year,
                "month": monthly_return.index.month
            })

            monthly_returns[ts_code] = monthly_df
            logger.info(f"{INDEXES[ts_code]}: 计算出 {len(monthly_df)} 个月度收益率")

        return monthly_returns

    def analyze_monthly_performance(self, monthly_returns: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """分析各月份的表现"""
        results = {}

        for ts_code, df in monthly_returns.items():
            index_name = INDEXES[ts_code]

            # 按月份分组分析
            monthly_stats = []
            for month in range(1, 13):
                month_data = df[df["month"] == month]

                if len(month_data) > 0:
                    stats_dict = {
                        "月份": month,
                        "样本数": len(month_data),
                        "平均收益率": month_data["monthly_return"].mean(),
                        "标准差": month_data["monthly_return"].std(),
                        "胜率": (month_data["monthly_return"] > 0).mean(),
                        "最大收益": month_data["monthly_return"].max(),
                        "最大亏损": month_data["monthly_return"].min(),
                        "夏普比率": month_data["monthly_return"].mean() / month_data["monthly_return"].std() if month_data["monthly_return"].std() > 0 else 0
                    }
                    monthly_stats.append(stats_dict)

            results[index_name] = pd.DataFrame(monthly_stats)

        return results

    def statistical_test(self, monthly_returns: Dict[str, pd.DataFrame]) -> Dict[str, Dict]:
        """进行统计检验"""
        test_results = {}

        for ts_code, df in monthly_returns.items():
            index_name = INDEXES[ts_code]

            # ANOVA检验：检验各月份平均收益率是否有显著差异
            monthly_groups = [df[df["month"] == m]["monthly_return"] for m in range(1, 13)]
            monthly_groups = [g for g in monthly_groups if len(g) > 0]

            try:
                f_stat, p_value = stats.f_oneway(*monthly_groups)
                test_results[index_name] = {
                    "F统计量": f_stat,
                    "p值": p_value,
                    "显著性": "显著" if p_value < 0.05 else "不显著",
                    "置信水平": "95%" if p_value < 0.05 else "无"
                }
            except Exception as e:
                logger.warning(f"ANOVA检验失败 {index_name}: {e}")
                test_results[index_name] = {"错误": str(e)}

        return test_results

    def plot_monthly_performance(self, monthly_stats: Dict[str, pd.DataFrame], save_path: Optional[Path] = None):
        """绘制月份表现图表"""
        # 设置中文字体
        plt.rcParams['font.sans-serif'] = ['SimHei', 'DejaVu Sans']
        plt.rcParams['axes.unicode_minus'] = False

        # 创建子图
        n_indexes = len(monthly_stats)
        fig, axes = plt.subplots(n_indexes, 2, figsize=(15, 4*n_indexes))
        if n_indexes == 1:
            axes = [axes]

        for i, (index_name, stats_df) in enumerate(monthly_stats.items()):
            # 平均收益率
            ax1 = axes[i][0]
            months = stats_df["月份"]
            avg_returns = stats_df["平均收益率"] * 100  # 转为百分比

            bars = ax1.bar(months, avg_returns, color='skyblue', alpha=0.7)
            ax1.set_title(f'{index_name} - 各月平均收益率')
            ax1.set_xlabel('月份')
            ax1.set_ylabel('平均收益率 (%)')
            ax1.grid(True, alpha=0.3)

            # 在柱子上标注数值
            for bar, val in zip(bars, avg_returns):
                height = bar.get_height()
                ax1.text(bar.get_x() + bar.get_width()/2., height + (0.001 if height >= 0 else -0.001),
                        f'{val:.2f}', ha='center', va='bottom' if height >= 0 else 'top', fontsize=8)

            # 胜率
            ax2 = axes[i][1]
            win_rates = stats_df["胜率"] * 100  # 转为百分比

            ax2.plot(months, win_rates, marker='o', linewidth=2, markersize=6, color='orange')
            ax2.set_title(f'{index_name} - 各月胜率')
            ax2.set_xlabel('月份')
            ax2.set_ylabel('胜率 (%)')
            ax2.grid(True, alpha=0.3)
            ax2.set_ylim(0, 100)

            # 在点上标注数值
            for x, y in zip(months, win_rates):
                ax2.text(x, y + 1, f'{y:.1f}', ha='center', va='bottom', fontsize=8)

        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            logger.info(f"图表已保存到: {save_path}")
            plt.close(fig)
        else:
            plt.show()

    def generate_report(self, monthly_stats: Dict[str, pd.DataFrame], test_results: Dict[str, Dict]) -> str:
        """生成分析报告"""
        report = []
        report.append("# A股宽基指数日历效应分析报告")
        report.append("")
        report.append("## 分析概述")
        report.append("本报告分析了A股主要宽基指数在不同月份的历史表现，考察是否存在日历效应。")
        report.append("")

        # 汇总统计
        for index_name, stats_df in monthly_stats.items():
            report.append(f"## {index_name}")
            report.append("")

            # 整体统计
            total_samples = stats_df["样本数"].sum()
            avg_return = stats_df["平均收益率"].mean()
            best_month = stats_df.loc[stats_df["平均收益率"].idxmax(), "月份"]
            worst_month = stats_df.loc[stats_df["平均收益率"].idxmin(), "月份"]

            report.append(f"- 样本期数: {total_samples}")
            report.append(f"- 平均月度收益率: {avg_return:.2%}")
            report.append(f"- 最强月份: {best_month}月")
            report.append(f"- 最弱月份: {worst_month}月")
            report.append("")

            # 月度详情
            report.append("### 月度表现详情")
            report.append("")
            report.append("|月份|样本数|平均收益率|胜率|夏普比率|")
            report.append("|---|---|---|---|---|")

            for _, row in stats_df.iterrows():
                report.append(
                    f"|{row['月份']}|{row['样本数']}|{row['平均收益率']:.2%}|{row['胜率']:.2%}|{row['夏普比率']:.2f}|"
                )

            report.append("")

            # 统计检验
            if index_name in test_results:
                test_result = test_results[index_name]
                report.append("### 统计检验结果")
                report.append("")
                if "F统计量" in test_result:
                    report.append(f"- F统计量: {test_result['F统计量']:.4f}")
                    report.append(f"- p值: {test_result['p值']:.4f}")
                    report.append(f"- 显著性: {test_result['显著性']}")
                    report.append(f"- 置信水平: {test_result['置信水平']}")
                else:
                    report.append(f"- 错误: {test_result.get('错误', '未知错误')}")
                report.append("")

        report.append("## 结论与建议")
        report.append("")
        report.append("### 日历效应判断标准")
        report.append("- **存在日历效应**: 各月收益率差异显著（p < 0.05），且某些月份表现显著优于其他月份")
        report.append("- **不存在日历效应**: 各月收益率差异不显著，或表现较为均衡")
        report.append("")
        report.append("### 投资建议")
        report.append("- 如果存在显著的日历效应，可以考虑在表现较好的月份增加仓位")
        report.append("- 日历效应并非确定性规律，应结合其他分析方法")
        report.append("- 历史表现不代表未来，建议定期更新分析")
        report.append("")

        return "\n".join(report)

    async def run_analysis(self, start_date: str = "20100101", end_date: str = "20241231",
                    save_plot: bool = True, output_dir: Optional[str] = None) -> str:
        """运行完整分析"""
        logger.info("开始日历效应分析...")

        # 创建输出目录
        if output_dir:
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
        else:
            output_path = Path("analysis_results")
            output_path.mkdir(exist_ok=True)

        # 1. 获取数据
        logger.info("获取指数数据...")
        index_data = await self.fetch_index_data(start_date, end_date)

        if not index_data:
            raise ValueError("未能获取到任何指数数据")

        # 2. 计算月度收益率
        logger.info("计算月度收益率...")
        self.monthly_returns = self.calculate_monthly_returns(index_data)

        # 3. 分析月度表现
        logger.info("分析月度表现...")
        monthly_stats = self.analyze_monthly_performance(self.monthly_returns)

        # 4. 统计检验
        logger.info("进行统计检验...")
        test_results = self.statistical_test(self.monthly_returns)

        # 5. 生成图表
        logger.info("生成可视化图表...")
        plot_path = output_path / "calendar_effect_analysis.png" if save_plot else None
        self.plot_monthly_performance(monthly_stats, plot_path)

        # 6. 生成报告
        logger.info("生成分析报告...")
        report = self.generate_report(monthly_stats, test_results)

        # 保存报告
        report_path = output_path / "calendar_effect_report.md"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report)

        logger.info(f"分析完成！报告已保存到: {report_path}")
        if save_plot:
            logger.info(f"图表已保存到: {plot_path}")

        return report


def main():
    """主函数"""
    import asyncio
    import os
    import json
    from pathlib import Path
    from alphahome.common.db_manager import DBManager

    async def run():
        # 获取数据库连接信息
        db_url = os.getenv("DATABASE_URL")

        if not db_url:
            # 尝试从配置文件读取
            config_paths = [
                Path.home() / ".alphahome" / "config.json",
                Path("config.json"),
                Path("config.example.json")
            ]

            for config_path in config_paths:
                if config_path.exists():
                    try:
                        with open(config_path, 'r', encoding='utf-8') as f:
                            config = json.load(f)
                            db_url = config.get("database", {}).get("url")
                            if db_url:
                                logger.info(f"从配置文件加载数据库连接: {config_path}")
                                break
                    except Exception as e:
                        logger.warning(f"读取配置文件失败 {config_path}: {e}")
                        continue

        if not db_url:
            raise ValueError(
                "未找到数据库连接信息。请设置环境变量 DATABASE_URL，"
                "或在配置文件中设置 database.url"
            )

        # 初始化数据库连接
        db_manager = DBManager(db_url)
        await db_manager.connect()

        try:
            # 创建分析器并运行
            analyzer = CalendarEffectAnalyzer(db_manager)
            out_dir = "calendar_effect_analysis"
            report = await analyzer.run_analysis(
                start_date="20100101",
                end_date="20241231",
                save_plot=True,
                output_dir=out_dir
            )

            print(f"分析完成！请查看 {out_dir} 目录中的结果文件。")

        finally:
            await db_manager.close()

    # 运行异步函数
    asyncio.run(run())


if __name__ == "__main__":
    main()
