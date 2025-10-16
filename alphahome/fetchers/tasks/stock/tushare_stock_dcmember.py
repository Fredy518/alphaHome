#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
东方财富板块成分 (dc_member) 任务
获取东方财富板块每日成分数据，可以根据概念板块代码和交易日期，获取历史成分。
该任务使用Tushare的dc_member接口获取数据。

{{ AURA-X: [Create] - 基于 Tushare dc_member API 创建东方财富板块成分任务. Source: tushare.pro/document/2?doc_id=363 }}
"""

import logging
from typing import Any, Dict, List

import pandas as pd

# 确认导入路径正确
from ...sources.tushare.tushare_task import TushareTask
from ....common.task_system.task_decorator import task_register

# logger 由 Task 基类提供
# logger = logging.getLogger(__name__)


@task_register()
class TushareStockDcMemberTask(TushareTask):
    """获取东方财富板块成分数据"""

    # 1. 核心属性
    name = "tushare_stock_dcmember"
    description = "获取东方财富板块成分数据"
    table_name = "stock_dcmember"
    primary_keys = ["trade_date", "ts_code", "con_code"]
    date_column = "trade_date"  # 增量任务
    default_start_date = "20241220"  # 根据东方财富数据起始时间设置
    data_source = "tushare"
    domain = "stock"  # 业务域标识，属于股票相关数据

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 3
    default_page_size = 5000  # 单次最大可获取5000条数据

    # 2. TushareTask 特有属性
    api_name = "dc_member"
    # Tushare dc_member 接口实际返回的字段
    fields = [
        "trade_date",
        "ts_code",
        "con_code",
        "name",
    ]

    # 3. 列名映射
    column_mapping = {}

    # 4. 数据类型转换
    transformations = {
        # trade_date 由覆盖的 process_data 方法处理
    }

    # 5. 数据库表结构
    schema_def = {
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "ts_code": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},
        "con_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "name": {"type": "VARCHAR(100)"},
        # 如果 auto_add_update_time=True (默认)，则会自动添加 update_time TIMESTAMP 列
    }

    # 6. 自定义索引
    indexes = [
        # 复合主键 (trade_date, ts_code, con_code) 已自动创建索引
        {"name": "idx_dc_member_trade_date", "columns": "trade_date"},
        {"name": "idx_dc_member_ts_code", "columns": "ts_code"},
        {"name": "idx_dc_member_con_code", "columns": "con_code"},
        {
            "name": "idx_dc_member_update_time",
            "columns": "update_time",
        },  # 新增 update_time 索引
    ]

    # 8. 分批配置
    batch_trade_days = 1  # 每个批次的交易日数量 (1个交易日)

    async def _get_all_stock_codes(self) -> List[str]:
        """
        从 tushare.stock_basic 表获取所有股票代码。
        
        Returns:
            List[str]: 股票代码列表
        """
        try:
            # 查询 tushare.stock_basic 表获取所有股票代码
            query = "SELECT ts_code FROM tushare.stock_basic ORDER BY ts_code"
            result = await self.db.fetch(query)
            
            if result:
                stock_codes = [row["ts_code"] for row in result]
                self.logger.info(f"从 tushare.stock_basic 表获取到 {len(stock_codes)} 个股票代码")
                return stock_codes
            else:
                self.logger.warning("tushare.stock_basic 表为空，未找到任何股票代码")
                return []
                
        except Exception as e:
            self.logger.error(f"查询 tushare.stock_basic 表失败: {e}", exc_info=True)
            return []

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。
        
        全量更新：从 tushare.stock_basic 表获取所有股票代码，按股票代码分批
        增量更新：按交易日分批
        """
        from ....common.constants import UpdateTypes
        
        # 检查更新类型
        update_type = kwargs.get("update_type", UpdateTypes.FULL)
        
        if update_type == UpdateTypes.FULL:
            # 全量更新：按股票代码分批
            self.logger.info(f"任务 {self.name}: 全量更新模式，按股票代码分批")
            
            stock_codes = await self._get_all_stock_codes()
            if not stock_codes:
                self.logger.warning("未找到任何股票代码，任务将跳过执行")
                return []
            
            # 为每个股票代码生成批处理参数
            batch_list = [{"con_code": code} for code in stock_codes]
            self.logger.info(f"任务 {self.name}: 为 {len(batch_list)} 个股票生成批处理列表")
            return batch_list
            
        else:
            # 增量更新：按交易日分批
            self.logger.info(f"任务 {self.name}: 增量更新模式，按交易日分批")
            
            # 参数提取和验证
            start_date = kwargs.get("start_date")
            end_date = kwargs.get("end_date")

            # 支持基类的全量更新机制：如果没有提供日期范围，使用默认范围
            if not start_date:
                start_date = self.default_start_date
                self.logger.info(f"任务 {self.name}: 未提供 start_date，使用默认起始日期: {start_date}")
            if not end_date:
                from datetime import datetime
                end_date = datetime.now().strftime("%Y%m%d")
                self.logger.info(f"任务 {self.name}: 未提供 end_date，使用当前日期: {end_date}")

            if not start_date or not end_date:
                self.logger.error(f"任务 {self.name}: 缺少必要的日期参数")
                return []

            # 如果开始日期晚于结束日期，说明数据已是最新，无需更新
            if pd.to_datetime(start_date) > pd.to_datetime(end_date):
                self.logger.info(
                    f"起始日期 ({start_date}) 晚于结束日期 ({end_date})，无需执行任务。"
                )
                return []

            self.logger.info(
                f"任务 {self.name}: 生成批处理列表，范围: {start_date} 到 {end_date}"
            )

            try:
                # 使用标准的交易日批次生成工具
                from ...sources.tushare.batch_utils import generate_trade_day_batches

                # 准备附加参数
                additional_params = {"fields": ",".join(self.fields or [])}

                batch_list = await generate_trade_day_batches(
                    start_date=start_date,
                    end_date=end_date,
                    batch_size=self.batch_trade_days,
                    ts_code=None,  # dc_member 不需要股票代码参数
                    exchange="SSE",  # 使用上交所作为默认交易所获取交易日历
                    additional_params=additional_params,
                    logger=self.logger,
                )
                self.logger.info(f"任务 {self.name}: 成功生成 {len(batch_list)} 个批次")
                return batch_list

            except Exception as e:
                self.logger.error(
                    f"任务 {self.name}: 生成批次时出错: {e}", exc_info=True
                )
                return []

    # 7. 数据验证规则 (真正生效的验证机制)
    validations = [
        (lambda df: df['trade_date'].notna(), "交易日期不能为空"),
        (lambda df: df['ts_code'].notna(), "板块代码不能为空"),
        (lambda df: df['con_code'].notna(), "成分代码不能为空"),
        (lambda df: df['name'].notna(), "成分股名称不能为空"),
        (lambda df: ~(df['name'].astype(str).str.strip().eq('') | df['name'].isna()), "成分股名称不能为空字符串"),
        (lambda df: ~(df['ts_code'].astype(str).str.strip().eq('') | df['ts_code'].isna()), "板块代码不能为空字符串"),
        (lambda df: ~(df['con_code'].astype(str).str.strip().eq('') | df['con_code'].isna()), "成分代码不能为空字符串"),
    ]


"""
使用方法:
1. 确保 TaskFactory 和 BaseTask 正确实现或导入。
2. 确保数据库连接 (self.db_engine) 和 Tushare 客户端 (self.tushare_client) 在基类或 TaskFactory 中正确初始化。
3. 确保数据库中存在名为 'stock_dcmember' 的表，且结构与 Tushare 返回的 DataFrame 匹配。
4. (可能需要) 在 TaskFactory 中注册此任务。
5. 使用相应的运行脚本来执行此任务。

注意事项:
- 此接口需要用户积累6000积分才能调取
- 单次最大可获取5000条数据，可以通过日期和代码循环获取
- 全量更新：按股票代码分批，获取每个股票属于哪些板块
- 增量更新：按交易日分批，获取最新的成分变化
- 数据包含板块成分股的详细信息
"""
