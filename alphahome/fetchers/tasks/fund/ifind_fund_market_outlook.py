#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
iFind 基金市场分析数据任务

获取基金市场分析数据，使用同花顺 iFind 的 basic_data_service 接口。
指标：ths_mkt_outlook_fund（市场分析）

数据来源：同花顺 iFind API
更新频率：按报告期更新
"""

import logging
import pandas as pd
from typing import Dict, Any, Optional, List
import asyncio

from alphahome.fetchers.sources.ifind.ifind_task import iFindTask
from alphahome.fetchers.sources.tushare.tushare_api import TushareAPI
from alphahome.common.task_system.task_decorator import task_register

logger = logging.getLogger(__name__)

@task_register()
class iFindFundMarketOutlookTask(iFindTask):
    """获取基金市场分析数据（basic_data_service: ths_mkt_outlook_fund）"""

    # 1. 核心属性
    name: str = "ifind_fund_market_outlook"
    description = "获取基金市场分析数据（同花顺 iFind）"
    table_name: str = "fund_market_look"  # 与ifind_fund_market_forwardlook共用数据表
    primary_keys = ["fund_code", "year", "period"]  # 复合主键
    date_column = "report_date"  # 报告期对应的日期字段，用于GUI交互
    data_source = "ifind"
    domain = "fund"  # 业务域标识

    # 2. iFindTask 必需属性
    api_endpoint: str = "basic_data_service"
    indicators: str = "ths_mkt_outlook_fund"  # 市场分析指标
    
    # 3. 报告期映射
    PERIOD_MAPPING = {
        "Q1": "100",    # 第一季度
        "Q2": "101",    # 第二季度  
        "Q3": "102",    # 第三季度
        "Q4": "103",    # 第四季度
        "H1": "104",    # 中报
        "A": "105"      # 年报
    }
    
    # 4. 字段映射：将API返回字段映射为有意义的字段名
    column_mapping = {
        "thscode": "fund_code",                    # 基金代码
        "ths_mkt_outlook_fund": "text"             # 内容
    }
    
    # 5. 数据库表结构定义
    schema_def = {
        "fund_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "year": {"type": "VARCHAR(4)", "constraints": "NOT NULL"},
        "period": {"type": "VARCHAR(3)", "constraints": "NOT NULL"},
        "period_name": {"type": "VARCHAR(20)"},
        "report_date": {"type": "DATE", "constraints": "NOT NULL"},
        "indicator": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "text": {"type": "TEXT"},
        # update_time 会自动添加
    }

    # 主键定义
    primary_keys = ["fund_code", "year", "period", "indicator"]

    # 自定义索引
    indexes = [
        {"name": "idx_fund_market_look_indicator", "columns": "indicator"},
        {"name": "idx_fund_market_look_report_date", "columns": "report_date"},
        {"name": "idx_fund_market_look_year_period", "columns": ["year", "period"]},
        {"name": "idx_fund_market_look_update_time", "columns": "update_time"},
    ]
    
    # 6. 代码级默认配置
    default_concurrent_limit = 1  # iFind 一次性获取所有数据，无需并发
    update_type = "incremental"   # 增量更新
    default_start_year = "2022"   # 默认起始年度（全量模式使用）
    FUND_CODE_BATCH_SIZE = 1000   # iFind API 单次请求的最大基金代码数量（免费账户限制1万条数据）
    
    def __init__(self, db_connection, **kwargs):
        """
        初始化 iFindFundMarketOutlookTask。
        
        Args:
            db_connection: 数据库连接
            **kwargs: 传递给父类的参数，包括：
                - year: 年度，如 "2025"
                - period: 报告期，如 "Q1", "H1", "A" 等
                - fund_codes: 可选的基金代码列表
        """
        super().__init__(db_connection, **kwargs)
        
        # 获取年度和报告期参数
        self.year = kwargs.get("year", "2025")
        self.start_year = kwargs.get("start_year", self.default_start_year)  # 起始年度
        self.periods = kwargs.get("periods", ["Q1", "Q2", "Q3", "Q4", "H1", "A"])  # 默认获取所有报告期
        self.fund_codes = kwargs.get("fund_codes", [])
        self.full_mode = kwargs.get("full_mode", False)  # 是否为全量模式
        
        # 验证报告期参数
        for period in self.periods:
            if period not in self.PERIOD_MAPPING:
                raise ValueError(f"不支持的报告期: {period}。支持的报告期: {list(self.PERIOD_MAPPING.keys())}")
        
        # 初始化 TushareAPI 用于获取基金代码
        self.tushare_api = None
        
        self.logger.info(f"任务初始化完成: 年度={self.year}, 起始年度={self.start_year}, 报告期={self.periods}, 全量模式={self.full_mode}")

    def _get_period_name(self, period: str) -> str:
        """获取报告期中文名称"""
        period_names = {
            "Q1": "第一季度",
            "Q2": "第二季度",
            "Q3": "第三季度",
            "Q4": "第四季度",
            "H1": "中报",
            "A": "年报"
        }
        return period_names.get(period, period)

    def _get_report_date(self, year: str, period: str) -> str:
        """
        将报告期转换为对应的日期

        Args:
            year: 年度，如 "2024"
            period: 报告期，如 "Q1", "H1", "A"

        Returns:
            str: 日期字符串，如 "2024-03-31"
        """
        period_dates = {
            "Q1": f"{year}-03-31",    # 第一季度末
            "Q2": f"{year}-06-30",    # 第二季度末
            "Q3": f"{year}-09-30",    # 第三季度末
            "Q4": f"{year}-12-31",    # 第四季度末
            "H1": f"{year}-06-30",    # 中报（半年报）
            "A": f"{year}-12-31"      # 年报
        }
        return period_dates.get(period, f"{year}-12-31")

    async def get_fund_codes_from_tushare(self) -> List[str]:
        """从 Tushare API 获取基金代码列表"""
        try:
            if self.tushare_api is None:
                # 获取Tushare token
                tushare_token = self.config_manager.get_tushare_token()
                self.tushare_api = TushareAPI(tushare_token, logger=self.logger)

            # 获取基金基础信息
            df = await self.tushare_api.query(
                api_name="fund_basic",
                fields=["ts_code", "status"],
                market="O"  # 场外基金
            )
            if df is not None and not df.empty:
                # 过滤有效的基金代码，转换为 iFind 格式
                fund_codes = []
                for ts_code in df['ts_code'].dropna():
                    if ts_code.endswith('.OF'):  # 开放式基金
                        fund_codes.append(ts_code)

                self.logger.info(f"从 Tushare 获取到 {len(fund_codes)} 个基金代码")
                return fund_codes
            else:
                self.logger.warning("从 Tushare 获取基金代码失败")
                return []

        except Exception as e:
            self.logger.error(f"从 Tushare 获取基金代码时出错: {e}")
            return []

    async def get_fund_codes_from_db(self) -> List[str]:
        """从数据库 fund_basic 表获取基金代码列表"""
        try:
            query = """
            SELECT ts_code
            FROM tushare.fund_basic
            WHERE ts_code LIKE '%.OF'
            AND status = 'L'  -- 只获取上市的基金
            ORDER BY ts_code
            """

            # 使用异步数据库查询
            result = await self.db.fetch(query)
            if result:
                fund_codes = [row['ts_code'] for row in result]
                self.logger.info(f"从数据库获取到 {len(fund_codes)} 个基金代码")
                return fund_codes
            else:
                self.logger.warning("数据库中没有找到基金代码")
                return []

        except Exception as e:
            self.logger.warning(f"从数据库获取基金代码失败: {e}")
            return []

    async def get_batch_list(self, **kwargs) -> List[Dict[str, Any]]:
        """
        按报告期生成批次列表。每个批次包含一个报告期的所有基金数据。
        """
        # 获取基金代码列表
        fund_codes = []

        # 1. 优先使用用户传入的基金代码
        if self.fund_codes:
            fund_codes = self.fund_codes
            self.logger.info(f"使用用户传入的 {len(fund_codes)} 个基金代码")
        else:
            # 2. 尝试从数据库获取
            fund_codes = await self.get_fund_codes_from_db()

            # 3. 如果数据库没有，从 Tushare 获取
            if not fund_codes:
                fund_codes = await self.get_fund_codes_from_tushare()

        if not fund_codes:
            self.logger.error("无法获取基金代码列表")
            return []

        # 将基金代码按指定大小分批
        fund_code_chunks = [fund_codes[i:i + self.FUND_CODE_BATCH_SIZE]
                            for i in range(0, len(fund_codes), self.FUND_CODE_BATCH_SIZE)]
        self.logger.info(f"已将 {len(fund_codes)} 个基金代码拆分为 {len(fund_code_chunks)} 个批次，每批最多 {self.FUND_CODE_BATCH_SIZE} 个。")

        # 按年度、报告期和基金代码批次生成任务批次
        batches = []

        # 确定年度范围
        if self.full_mode:
            # 全量模式：从起始年度到当前年度
            years = [str(year) for year in range(int(self.start_year), int(self.year) + 1)]
        else:
            # 增量模式：只处理指定年度
            years = [self.year]

        for year in years:
            for period in self.periods:
                period_code = self.PERIOD_MAPPING[period]
                period_name = self._get_period_name(period)
                report_date = self._get_report_date(year, period)

                for i, chunk in enumerate(fund_code_chunks):
                    batch = {
                        "fund_codes": chunk,
                        "year": year,
                        "period": period,
                        "period_code": period_code,
                        "period_name": period_name,
                        "report_date": report_date,
                        "batch_num": i + 1,
                        "total_batches": len(fund_code_chunks)
                    }
                    batches.append(batch)

        self.logger.info(f"总共生成了 {len(batches)} 个批次任务")
        return batches

    async def prepare_params(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        """
        为 basic_data_service 端点准备参数。
        重写基类方法以支持带参数的指标调用。
        """
        if not batch or "fund_codes" not in batch:
            raise ValueError("批次不能为空或缺少基金代码")

        fund_codes = batch["fund_codes"]
        year = batch["year"]
        period_code = batch["period_code"]

        # 构造 iFind API 所需的参数格式
        params = {
            "endpoint": self.api_endpoint,
            "codes": ",".join(fund_codes),  # 基金代码用逗号分隔
            "indicators": [self.indicators],  # 指标列表
            "indiparams": [[year, period_code]]  # 年度和报告期参数
        }

        self.logger.debug(f"为批次准备参数: codes={len(fund_codes)}个, year={year}, period={period_code}")
        return params

    def process_data(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        处理从API获取的原始数据。

        Args:
            data: 从API获取的原始DataFrame
            **kwargs: 额外参数，包含当前批次的年度和报告期信息

        Returns:
            pd.DataFrame: 处理后的数据
        """
        if data is None or data.empty:
            return data

        # 从 kwargs 获取当前批次的信息
        batch_info = kwargs.get("batch_info", {})
        year = batch_info.get("year", self.year)
        period_code = batch_info.get("period_code", "100")
        period_name = batch_info.get("period_name", "第一季度")
        report_date = batch_info.get("report_date", f"{year}-03-31")

        # 执行字段映射
        if self.column_mapping:
            rename_mapping = {}
            for old_col, new_col in self.column_mapping.items():
                if old_col in data.columns:
                    rename_mapping[old_col] = new_col

            if rename_mapping:
                data = data.rename(columns=rename_mapping)
                self.logger.debug(f"字段映射完成: {rename_mapping}")

        # 添加年度和报告期信息
        data['year'] = year
        data['period'] = period_code
        data['period_name'] = period_name
        data['report_date'] = report_date
        data['indicator'] = "market_outlook"  # 区别于forwardlook的标识

        # 清理空值和无效数据
        for col in data.columns:
            if data[col].dtype == 'object':
                data[col] = data[col].replace(['--', '', 'nan'], None)

        # 过滤掉市场分析为空的记录
        if 'text' in data.columns:
            data = data[data['text'].notna()]
            data = data[data['text'] != '']

        self.logger.info(f"处理完成，{year}年{period_name}有效数据 {len(data)} 条")
        return data

    def get_display_name(self) -> str:
        """返回任务的显示名称"""
        periods_str = "、".join([self._get_period_name(p) for p in self.periods])
        return f"iFind基金市场分析 ({self.year}年{periods_str})"

    def get_date_range_for_periods(self) -> tuple:
        """
        获取当前报告期对应的日期范围，用于GUI交互

        Returns:
            tuple: (start_date, end_date) 格式为 "YYYY-MM-DD"
        """
        if not self.periods:
            return (f"{self.year}-01-01", f"{self.year}-12-31")

        # 获取所有报告期对应的日期
        dates = [self._get_report_date(self.year, period) for period in self.periods]
        dates.sort()

        # 返回最早和最晚的日期
        return (dates[0], dates[-1])

    def supports_incremental_update(self) -> bool:
        """
        明确声明此任务不支持智能增量更新。

        智能增量更新是基于数据库中已有数据的最新日期来自动推断更新范围。
        由于本任务的数据是按季度、半年度、年度等报告期发布的，与具体的交易日期无直接关联，
        因此无法通过最新日期来确定需要补充的数据。

        返回 False 会告知任务执行框架，当用户选择"智能增量"模式时，应直接跳过此任务。
        对于此任务，用户应使用"全量更新"或"手动增量"（指定日期范围）模式。

        Returns:
            bool: 始终返回 False。
        """
        return False

    def get_incremental_skip_reason(self) -> str:
        """
        返回不支持智能增量更新的原因说明

        Returns:
            str: 跳过原因说明
        """
        return "基金市场分析数据按报告期发布（季报、半年报、年报），不适合按日期进行智能增量更新。建议使用全量更新或手动增量模式。"

    def get_incremental_date_filter(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        根据日期范围过滤需要更新的年度和报告期（仅用于手动增量模式）

        注意：此任务不支持智能增量更新，此方法仅用于手动增量模式的日期范围过滤

        Args:
            start_date: 开始日期 "YYYY-MM-DD"
            end_date: 结束日期 "YYYY-MM-DD"

        Returns:
            Dict: 过滤后的参数
        """
        # 记录日志说明这是手动增量模式
        self.logger.info(f"手动增量模式：根据日期范围 {start_date} 到 {end_date} 过滤报告期")

        # 从日期中提取年度范围
        start_year = int(start_date[:4])
        end_year = int(end_date[:4])

        # 确定需要更新的年度范围
        years_to_update = list(range(start_year, end_year + 1))

        # 根据日期范围确定需要更新的报告期
        filtered_periods = []

        # 对每个年度检查哪些报告期在日期范围内
        for year in years_to_update:
            for period in self.periods:
                report_date = self._get_report_date(str(year), period)
                if start_date <= report_date <= end_date:
                    if period not in filtered_periods:
                        filtered_periods.append(period)

        # 如果没有匹配的报告期，使用默认配置
        if not filtered_periods:
            filtered_periods = self.periods
            self.logger.warning(f"日期范围内没有匹配的报告期，使用默认配置: {filtered_periods}")
        else:
            self.logger.info(f"匹配的报告期: {filtered_periods}")

        # 使用最新年度作为主要年度，但支持多年度查询
        target_year = str(max(years_to_update))

        return {
            "year": target_year,
            "start_year": str(start_year),
            "periods": filtered_periods,
            "fund_codes": self.fund_codes,
            "full_mode": len(years_to_update) > 1  # 跨年度时启用全量模式逻辑
        }
