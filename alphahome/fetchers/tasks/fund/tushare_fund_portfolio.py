#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
公募基金持仓数据 (fund_portfolio) 更新任务
获取公募基金季度末持仓明细数据。
继承自 TushareTask，按 ann_date (公告日期) 增量更新。
"""

import logging
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd

# 导入基础类和装饰器
from ...sources.tushare.tushare_task import TushareTask
from alphahome.common.task_system.task_decorator import task_register

# 导入批处理工具
from ...sources.tushare.batch_utils import generate_natural_day_batches


@task_register()
class TushareFundPortfolioTask(TushareTask):
    """获取公募基金持仓数据"""

    # 1. 核心属性
    name = "tushare_fund_portfolio"
    description = "获取公募基金持仓明细"
    table_name = "fund_portfolio"
    data_source = "tushare"
    domain = "fund"  # 业务域标识
    # 主键：基金代码 + 公告日期 + 股票代码 + 报告期 能够唯一确定一条持仓记录
    primary_keys = ["ts_code", "ann_date", "symbol", "end_date"]
    date_column = "ann_date"  # 使用公告日期进行增量更新
    default_start_date = "19980101"  # 基金持仓数据大致起始日期

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 1
    default_page_size = 8000

    # 2. TushareTask 特有属性
    api_name = "fund_portfolio"  # Tushare API 名称
    # 根据 https://tushare.pro/document/2?doc_id=121 列出字段
    fields = [
        "ts_code",
        "ann_date",
        "end_date",
        "symbol",
        "mkv",
        "amount",
        "stk_mkv_ratio",
        "stk_float_ratio",
    ]

    # 3. 列名映射 (API字段名与数据库列名一致，无需映射)
    column_mapping = {}

    # 4. 数据类型转换
    transformations = {
        "mkv": lambda x: pd.to_numeric(x, errors="coerce"),  # 持有股票市值(元)
        "amount": lambda x: pd.to_numeric(x, errors="coerce"),  # 持有股票数量（股）
        "stk_mkv_ratio": lambda x: pd.to_numeric(x, errors="coerce"),  # 占股票市值比
        "stk_float_ratio": lambda x: pd.to_numeric(
            x, errors="coerce"
        ),  # 占流通股本比例
        # ann_date 和 end_date 由基类 process_data 处理
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "ann_date": {"type": "DATE", "constraints": "NOT NULL"},
        "end_date": {"type": "DATE", "constraints": "NOT NULL"},  # 报告期也加入非空
        "symbol": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},  # 股票代码
        "mkv": {"type": "NUMERIC(20,3)"},
        "amount": {"type": "NUMERIC(20,2)"},
        "stk_mkv_ratio": {"type": "NUMERIC(10,4)"},
        "stk_float_ratio": {"type": "NUMERIC(10,4)"},
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_fund_portfolio_ts_code", "columns": "ts_code"},
        {"name": "idx_fund_portfolio_ann_date", "columns": "ann_date"},
        {"name": "idx_fund_portfolio_end_date", "columns": "end_date"},
        {"name": "idx_fund_portfolio_symbol", "columns": "symbol"},
        {
            "name": "idx_fund_portfolio_update_time",
            "columns": "update_time",
        },  # 新增 update_time 索引
    ]

    # 7. 分批配置 (按自然日，约3个月一批)
    batch_size_days = 90

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。

        fund_portfolio API 的 50101 错误是由于数据量过大导致的 offset 限制。
        解决方案是按基金代码 (ts_code) 分批，而不是按时间分批。

        策略：
        1. 从 fund_basic 表获取所有基金代码
        2. 将基金代码分批（每批约 100-200 个基金）
        3. 每个批次包含基金代码列表和时间范围参数
        """
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        ts_code = kwargs.get("ts_code")  # 可选参数

        if not start_date:
            latest_db_date = await self.get_latest_date_for_task()
            start_date = (
                (latest_db_date + pd.Timedelta(days=1)).strftime("%Y%m%d")
                if latest_db_date
                else self.default_start_date
            )
            self.logger.info(
                f"未提供 start_date，使用数据库最新公告日期+1天或默认起始日期: {start_date}"
            )
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")
            self.logger.info(f"未提供 end_date，使用当前日期: {end_date}")

        if pd.to_datetime(start_date) > pd.to_datetime(end_date):
            self.logger.info(
                f"起始日期 ({start_date}) 晚于结束日期 ({end_date})，无需执行任务。"
            )
            return []

        self.logger.info(
            f"任务 {self.name}: 生成批处理列表 (按基金代码分批)，范围: {start_date} 到 {end_date}, 代码: {ts_code if ts_code else '所有'}"
        )

        try:
            # 按基金代码分批处理，避免 50101 错误
            batch_list = await self._generate_fund_code_batches(
                start_date=start_date,
                end_date=end_date,
                ts_code=ts_code,
                batch_size=100  # 每批 100 个基金
            )
            return batch_list
        except Exception as e:
            self.logger.error(f"任务 {self.name}: 生成批次时出错: {e}", exc_info=True)
            return []

    async def _generate_fund_code_batches(self, start_date: str, end_date: str, ts_code: str = None, batch_size: int = 100) -> List[Dict]:
        """
        按基金代码分批生成批次参数。

        策略：
        1. 如果提供了特定 ts_code，只查询该基金
        2. 如果没有提供 ts_code，从 fund_basic 表获取所有基金代码，然后分批

        Args:
            start_date: 开始日期
            end_date: 结束日期
            ts_code: 可选的特定基金代码
            batch_size: 每批基金数量

        Returns:
            批次参数列表，每个批次包含 ts_code 列表和时间范围
        """
        if ts_code:
            # 如果指定了特定基金，直接返回单个批次
            return [{
                "ts_code": ts_code,
                "start_date": start_date,
                "end_date": end_date
            }]

        # 从 fund_basic 表获取所有基金代码
        try:
            fund_codes = await self._get_all_fund_codes()
            if not fund_codes:
                self.logger.warning("未找到任何基金代码，返回空批次列表")
                return []

            self.logger.info(f"从 fund_basic 表获取到 {len(fund_codes)} 个基金代码")

            # 将基金代码分批
            batches = []
            for i in range(0, len(fund_codes), batch_size):
                batch_codes = fund_codes[i:i + batch_size]

                # 为每个批次创建参数
                # 注意：fund_portfolio API 的 ts_code 参数支持多个代码用逗号分隔
                batch = {
                    "ts_code": ",".join(batch_codes),  # 用逗号连接多个基金代码
                    "start_date": start_date,
                    "end_date": end_date
                }
                batches.append(batch)

            self.logger.info(f"生成了 {len(batches)} 个基金代码批次，每批最多 {batch_size} 个基金")
            return batches

        except Exception as e:
            self.logger.error(f"获取基金代码列表失败: {e}")
            raise

    async def _get_all_fund_codes(self) -> List[str]:
        """
        从 fund_basic 表获取所有基金代码。

        Returns:
            基金代码列表
        """
        try:
            # 直接查询数据库获取基金代码
            query = """
            SELECT DISTINCT ts_code
            FROM tushare.fund_basic
            WHERE status = 'L'  -- 只获取上市状态的基金
            ORDER BY ts_code
            """

            # 使用正确的数据库查询方法
            rows = await self.db.fetch(query)
            fund_codes = [row['ts_code'] for row in rows]

            return fund_codes

        except Exception as e:
            self.logger.error(f"查询 fund_basic 表失败: {e}")
            # 如果查询失败，返回一个空列表，让调用方处理
            return []

    # prepare_params 可以使用基类默认实现，它会传递 batch_list 中的所有参数
