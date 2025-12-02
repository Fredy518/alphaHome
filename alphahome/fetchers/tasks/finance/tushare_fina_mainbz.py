from datetime import datetime, timedelta
from typing import Dict, List

import numpy as np
import pandas as pd

from ...sources.tushare import TushareTask
from alphahome.common.task_system.task_decorator import task_register
from ...sources.tushare.batch_utils import generate_quarter_end_batches
from ...tools.calendar import get_trade_days_between


@task_register()
class TushareFinaMainbzTask(TushareTask):
    """上市公司主营业务构成数据任务

    获取上市公司主营业务构成数据，支持按产品、地区、行业三种方式获取。
    该任务使用Tushare的fina_mainbz_vip接口获取数据。
    """

    # 1.核心属性
    domain = "finance"  # 业务域标识
    name = "tushare_fina_mainbz"
    description = "获取上市公司主营业务构成数据"
    table_name = "fina_mainbz"
    primary_keys = ["ts_code", "end_date", "bz_item"]
    date_column = "end_date"
    default_start_date = "20001231"  # 最早的数据起始日期

    # 2.自定义索引
    indexes = [
        {"name": "idx_fina_mainbz_code", "columns": "ts_code"},
        {"name": "idx_fina_mainbz_end_date", "columns": "end_date"},
        {"name": "idx_fina_mainbz_code_item", "columns": "bz_code"},
        {"name": "idx_fina_mainbz_item", "columns": "bz_item"},
        {"name": "idx_fina_mainbz_update_time", "columns": "update_time"},
    ]

    # 3.Tushare特有属性
    api_name = "fina_mainbz_vip"
    fields = [
        "ts_code",
        "end_date",
        "bz_code",
        "bz_item",
        "bz_sales",
        "bz_profit",
        "bz_cost",
        "curr_type",
        "update_flag",
    ]

    # 4.数据类型转换
    transformations = {
        "bz_sales": float,
        "bz_profit": float,
        "bz_cost": float,
    }

    # 5.列名映射
    column_mapping = {}

    # 6.表结构定义
    schema_def = {
        "ts_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "end_date": {"type": "DATE", "constraints": "NOT NULL"},
        "bz_code": {"type": "VARCHAR(50)"},
        "bz_item": {"type": "VARCHAR(200)", "constraints": "NOT NULL"},
        "bz_sales": {"type": "NUMERIC(20,4)"},
        "bz_profit": {"type": "NUMERIC(20,4)"},
        "bz_cost": {"type": "NUMERIC(20,4)"},
        "curr_type": {"type": "VARCHAR(20)"},
        "update_flag": {"type": "VARCHAR(20)"},
    }

    # 7.数据验证规则
    validations = [
        (lambda df: df['ts_code'].notna(), "股票代码不能为空"),
        (lambda df: df['end_date'].notna(), "报告期不能为空"),
        (lambda df: df['bz_item'].notna(), "主营业务项目不能为空"),
        (lambda df: df['bz_code'].notna() | df['bz_item'].notna(), "主营业务代码或项目至少有一个不能为空"),
    ]

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """生成批处理参数列表

        对于fina_mainbz_vip接口，需要按季度获取数据，每次获取一个季度的全部数据。
        需要分别获取按产品(P)、按地区(D)、按行业(I)三种类型的数据。

        Args:
            **kwargs: 查询参数，包括start_date、end_date、ts_code等

        Returns:
            List[Dict]: 批处理参数列表
        """
        start_date = kwargs.get("start_date", self.default_start_date)
        end_date = kwargs.get("end_date", datetime.now().strftime("%Y%m%d"))
        
        self.logger.info(f"生成主营业务构成批次: {start_date} 到 {end_date}")

        try:
            # 生成季度末批次
            quarter_batches = await generate_quarter_end_batches(
                start_date=start_date,
                end_date=end_date,
                logger=self.logger,
                date_field="period"  # fina_mainbz_vip接口使用period参数
            )

            # 为每个季度批次添加类型参数，需要分别获取P、D、I三种类型
            batch_list = []
            for batch in quarter_batches:
                for biz_type in ['P', 'D', 'I']:  # P:产品, D:地区, I:行业
                    batch_copy = batch.copy()
                    batch_copy['type'] = biz_type
                    batch_list.append(batch_copy)

            self.logger.info(f"成功生成 {len(batch_list)} 个主营业务构成批次")
            return batch_list

        except Exception as e:
            self.logger.error(f"生成主营业务构成批次时出错: {e}", exc_info=True)
            raise RuntimeError(f"生成主营业务构成批次失败: {e}") from e


    async def prepare_params(self, batch_params: Dict) -> Dict:
        """准备 Tushare API 请求的参数

        fina_mainbz_vip接口的参数格式：
        - period: 报告期（季度末日期）
        - type: 类型（P、D、I）
        - fields: 指定输出字段

        Args:
            batch_params: 批处理参数

        Returns:
            Dict: API请求参数
        """
        params = batch_params.copy()

        # 将period参数从批次参数中提取出来
        if 'period' in params:
            params['period'] = params['period']

        # 确保包含type参数
        if 'type' not in params:
            params['type'] = 'P'  # 默认为产品类型

        return params