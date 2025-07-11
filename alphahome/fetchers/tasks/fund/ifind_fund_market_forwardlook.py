#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
iFind 基金市场展望数据任务

获取基金市场展望数据，使用同花顺 iFind 的 basic_data_service 接口。
指标：ths_mkt_forwardlook_fund（市场展望）

数据来源：同花顺 iFind API
更新频率：按报告期更新
"""

import logging
import pandas as pd
from typing import Dict, Any, Optional, List, Union
import asyncio
from datetime import datetime

from alphahome.fetchers.sources.ifind.ifind_task import iFindTask
from alphahome.fetchers.sources.tushare.tushare_api import TushareAPI
from alphahome.common.task_system.task_decorator import task_register

logger = logging.getLogger(__name__)

@task_register()
class iFindFundMarketforwardlookTask(iFindTask):
    """获取基金市场展望数据（basic_data_service: ths_mkt_forwardlook_fund）"""

    # 1. 核心属性
    name: str = "ifind_fund_market_forwardlook"
    description = "获取基金市场展望数据（同花顺 iFind）"
    table_name: str = "fund_market_look" # 与ifind_fund_market_outlook共用数据表
    primary_keys = ["fund_code", "year", "period"]  # 复合主键
    date_column = "report_date"  # 报告期对应的日期字段，用于GUI交互
    data_source = "ifind"
    domain = "fund"  # 业务域标识

    # 2. iFindTask 必需属性
    api_endpoint: str = "basic_data_service"
    indicators: str = "ths_mkt_forwardlook_fund"  # 市场展望指标
    
    # 3. 报告期映射（forwardlook只有年报和半年报数据）
    PERIOD_MAPPING = {
        "H1": "104",    # 中报（半年报）
        "A": "105"      # 年报
    }
    
    # 4. 字段映射：将API返回字段映射为有意义的字段名
    column_mapping = {
        "thscode": "fund_code",                    # 基金代码
        "ths_mkt_forwardlook_fund": "text"         # 内容
    }
    

    
    # 6. 代码级默认配置
    default_concurrent_limit = 1  # iFind 一次性获取所有数据，无需并发
    update_type = "incremental"   # 增量更新
    default_start_year = "2010"   # 默认起始年度（全量模式使用）
    FUND_CODE_BATCH_SIZE = 1000   # iFind API 单次请求的最大基金代码数量（免费账户限制1万条数据）

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
    
    def __init__(self, db_connection, **kwargs):
        """
        初始化 iFindFundMarketforwardlookTask。
        
        Args:
            db_connection: 数据库连接
            **kwargs: 传递给父类的参数，包括：
                - year: 年度，如 "2024"
                - periods: 报告期，如 "Q1", "H1", "A" 等
                - fund_codes: 可选的基金代码列表
        """
        super().__init__(db_connection, **kwargs)
        # 基准年份，仅用于全量更新或默认增量模式。在手动增量模式下，此参数将被忽略。
        self.year = kwargs.get("year", str(datetime.now().year))
        self.start_year = kwargs.get("start_year", self.default_start_year)  # 起始年度
        self.periods = kwargs.get("periods", ["H1", "A"])  # 默认获取半年报和年报
        self.fund_codes = kwargs.get("fund_codes", [])
        self.full_mode = kwargs.get("full_mode", False)  # 是否为全量模式
        
        # 手动增量模式下的精确待办列表
        self.updates_to_run = kwargs.get("updates_to_run")
        
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
            "H1": "中报",
            "A": "年报"
        }
        return period_names.get(period, period)

    def _get_period_code(self, period: str) -> str:
        """获取报告期对应的数字代码"""
        return self.PERIOD_MAPPING.get(period, "105") # 默认年报

    def _get_report_date(self, year: str, period: str) -> str:
        """
        将报告期转换为对应的日期

        Args:
            year: 年度，如 "2024"
            period: 报告期，如 "H1", "A"

        Returns:
            str: 日期字符串，如 "2024-06-30"
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

        # --- 批次生成逻辑重构 ---
        # 优先使用手动增量模式提供的精确待办列表
        if self.updates_to_run is not None:
            self.logger.info(f"手动增量模式：将根据 {len(self.updates_to_run)} 个精确的(年度,报告期)对生成任务。")
            for item in self.updates_to_run:
                year = item['year']
                period = item['period']
                period_code = self.PERIOD_MAPPING[period]
                period_name = self._get_period_name(period)
                report_date = self._get_report_date(year, period)

                for i, chunk in enumerate(fund_code_chunks):
                    batches.append({
                        "fund_codes": chunk, "year": year, "period": period,
                        "period_code": period_code, "period_name": period_name,
                        "report_date": report_date, "batch_num": i + 1,
                        "total_batches": len(fund_code_chunks)
                    })
        else:
            # 默认或全量模式
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
                        batches.append({
                            "fund_codes": chunk, "year": year, "period": period,
                            "period_code": period_code, "period_name": period_name,
                            "report_date": report_date, "batch_num": i + 1,
                            "total_batches": len(fund_code_chunks)
                        })

        self.logger.info(f"总共生成了 {len(batches)} 个批次任务")
        return batches

    async def prepare_params(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        """
        为给定的批次准备iFind API请求参数。
        这个方法覆盖了基类的实现，以处理更复杂的参数结构（年度、报告期）。
        """
        codes_str = ";".join(batch['fund_codes'])
        
        # 从 batch 中动态获取 year 和 period_code
        year = batch.get('year', self.year)
        # 修正：使用 period_code 而不是 period
        period_code = batch.get('period_code', self._get_period_code(self.periods[0] if self.periods else 'A'))
        
        # 根据父类 iFindTask 的期望，直接构造参数字典
        params = {
            "endpoint": self.api_endpoint,
            "codes": codes_str,
            "indicators": self.indicators,
            "indiparams": [[year, period_code]]  # iFind特定格式: [[年度, 报告期代码]]
        }
        self.logger.debug(f"为批次准备参数: 年份={year}, 报告期代码={period_code}, 代码数量={len(batch['fund_codes'])}")
        return params

    def get_display_name(self) -> str:
        """返回任务的显示名称"""
        periods_str = "、".join([self._get_period_name(p) for p in self.periods])
        return f"iFind基金市场展望 ({self.year}年{periods_str})"

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

    def supports_incremental_update(self) -> Union[bool, str]:
        """
        声明此任务对增量更新的支持级别。

        - 返回 'manual': 表示仅支持手动增量（需要日期范围）。
        - 返回 True: 表示同时支持手动和智能增量。
        - 返回 False: 表示完全不支持增量更新。

        对于此任务，数据按报告期发布，不适合基于最新日期的“智能”推断，
        因此只支持“手动”模式。
        
        Returns:
            str: 返回 'manual'
        """
        return 'manual'

    def get_incremental_skip_reason(self) -> str:
        """
        返回不支持智能增量更新的原因说明

        Returns:
            str: 跳过原因说明
        """
        return "基金市场展望数据按报告期发布（半年报、年报），不适合按日期进行智能增量更新。建议使用全量更新或手动增量模式。"

    def get_incremental_date_filter(self, start_date: str, end_date: str) -> dict:
        """
        根据给定的日期范围，计算出需要执行的（年份, 报告期）组合。
        这是手动增量模式的核心过滤逻辑。
        """
        self.logger.info(f"手动增量模式：根据日期范围 {start_date} 到 {end_date} 过滤报告期")
        
        try:
            start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
        except ValueError:
            self.logger.error(f"无效的日期格式: {start_date} 或 {end_date}。应为 YYYY-MM-DD。")
            return {"updates_to_run": [], "fund_codes": self.fund_codes}

        start_year = start_date_obj.year
        end_year = end_date_obj.year
        years_to_update = list(range(start_year, end_year + 1))

        updates_to_run = []
        for year in years_to_update:
            for period in self.periods:
                report_date_str = self._get_report_date(str(year), period)
                report_date_obj = datetime.strptime(report_date_str, "%Y-%m-%d").date()
                if start_date_obj <= report_date_obj <= end_date_obj:
                    updates_to_run.append({"year": str(year), "period": period})

        if not updates_to_run:
            self.logger.warning(f"在指定的日期范围 {start_date} 到 {end_date} 内没有找到匹配的报告期，任务将不会执行。")
        else:
            self.logger.info(f"手动增量模式匹配到 {len(updates_to_run)} 个待更新项: {updates_to_run}")

        return {
            "updates_to_run": updates_to_run,
            "fund_codes": self.fund_codes
        }
