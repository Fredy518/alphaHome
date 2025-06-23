import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta

import pandas as pd
from pandas.tseries.offsets import YearBegin, YearEnd  # 需要导入

# 假设 TushareTask 是 Tushare 相关任务的基类。
# 如果导入路径不同，请相应调整。
from alphahome.fetchers.sources.tushare.tushare_task import TushareTask
from alphahome.common.task_system.task_decorator import task_register


@task_register()
class TushareIndexWeightTask(TushareTask):
    """
    从Tushare获取指数成分股及其权重的任务。
    API: index_weight (月度数据)
    """

    # 1. 核心属性 (类似于 tushare_index_swdaily.py)
    name = "tushare_index_weight"  # 任务标识符
    description = "获取指数成分股及权重(月度)"  # 已是中文，无需修改
    table_name = "index_weight"  # 默认数据库表名
    primary_keys = ["index_code", "con_code", "trade_date"]  # 基于Tushare的输出确定
    date_column = "trade_date"  # 用于增量逻辑的主要日期列
    default_start_date = (
        "20050101"  # 如果数据库中无数据，默认的起始日期，可根据需要调整
    )

    # --- 代码级别的默认配置 (可被 config.json 文件覆盖) ---
    default_concurrent_limit = 2 # 恢复并发数
    default_rate_limit_delay = 10 # 设置一个更短的速率限制延迟
    default_page_size = (
        6000  # TushareAPI 处理分页；index_weight 是月度数据，此限制通常不触发
    )

    # 2. TushareTask 特有的属性
    api_name = "index_weight"  # Tushare API 名称
    fields = ["index_code", "con_code", "trade_date", "weight"]  # 需要获取的字段

    # 3. 列名映射 (如果API字段名与数据库列名不同)
    column_mapping = {
        # "api_field_name": "db_column_name"
        # 对于 index_weight, API 字段与期望的数据库列名一致, 因此可能为空
    }

    # 4. 数据类型转换
    transformations = {
        "weight": lambda x: pd.to_numeric(x, errors="coerce"),
        # trade_date 通常由基类的 _process_date_column 方法处理 (如果存在)
    }

    # 5. 数据库表结构
    schema_def = {
        "index_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "con_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "weight": {"type": "FLOAT"},
        # update_time 通常由基类自动添加
        # 主键和 date_column 的索引通常由基类处理
    }

    # 6. 自定义索引 (如果除了主键和 date_column 外还需要其他索引)
    indexes = [
        {
            "name": "idx_tushare_index_weight_update_time",
            "columns": "update_time",
        }  # 新增 update_time 索引
    ]

    # 构造函数：基类的 __init__ 方法期望处理 task_id, task_name, cfg, db_manager, api, logger 参数。
    # 上面定义的类属性将被基类使用。
    # 如果需要基类 TushareTask 之外的特定初始化逻辑，
    # 可以添加一个 __init__ 方法，并首先调用 super().__init__(...)。
    # 目前，我们假设基类像在 tushare_index_swdaily.py 中那样充分处理了这些。

    # 移除 _adjust_dates_for_monthly_api 方法

    # 移除 get_index_codes 方法，因为不再需要预先获取所有指数代码

    async def get_index_codes(self) -> List[str]:
        """
        使用 'index_basic' API 获取所有唯一的Tushare指数代码。
        """
        self.logger.debug(
            f"任务 {self.name} 正在从 'index_basic' API 获取所有指数代码。"
        )
        try:
            if not self.api:
                self.logger.error("TushareAPI 实例 (self.api) 不可用。")
                return []

            df_codes = await self.api.query(
                api_name="index_basic", fields=["ts_code"]
            )

            if df_codes is not None and not df_codes.empty:
                unique_codes = df_codes["ts_code"].unique().tolist()
                self.logger.info(f"成功获取 {len(unique_codes)} 个唯一指数代码。")
                return unique_codes
            else:
                self.logger.warning(
                    "从 'index_basic' API 返回的指数代码为空或DataFrame为空。"
                )
                return []
        except Exception as e:
            self.logger.error(
                f"获取任务 {self.name} 的指数代码时出错: {e}", exc_info=True
            )
            return []

    async def _determine_date_range(self) -> Optional[Dict[str, str]]:
        """
        重写此方法以实现无 look_back 的智能增量更新。
        它会从数据库中的最后日期开始，但不像通用实现那样回看N天。
        """
        self.logger.info(f"任务 {self.name}: 正在确定无 look_back 的智能增量日期范围...")
        
        last_date = await self.get_latest_date() # type: ignore
        
        if last_date:
            start_date = last_date + timedelta(days=1)
            end_date = datetime.now()
            self.logger.info(f"任务 {self.name}: 找到最后日期 {last_date.strftime('%Y-%m-%d')}，将从 {start_date.strftime('%Y-%m-%d')} 开始更新。")
        else:
            start_date_str = self.default_start_date
            start_date = datetime.strptime(start_date_str, "%Y%m%d")
            end_date = datetime.now()
            self.logger.info(f"任务 {self.name}: 未找到数据，将从默认起始日期 {start_date_str} 开始更新。")

        return {
            "start_date": start_date.strftime("%Y%m%d"),
            "end_date": end_date.strftime("%Y%m%d"),
        }

    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        """
        生成批处理列表。每个批次对应一个 index_code 和一个月份的起止日期。
        """
        start_date_str = kwargs.get("start_date")
        end_date_str = kwargs.get("end_date")

        if not start_date_str or not end_date_str:
            self.logger.error("get_batch_list 需要 start_date 和 end_date。")
            return []

        index_codes = await self.get_index_codes()
        if not index_codes:
            self.logger.warning(f"任务 {self.name}: 未找到指数代码以创建批处理。")
            return []

        try:
            start_dt = pd.to_datetime(start_date_str, format='%Y%m%d')
            end_dt = pd.to_datetime(end_date_str, format='%Y%m%d')
        except ValueError as e:
            self.logger.error(f"无法将日期 {start_date_str}, {end_date_str} 解析为 datetime 对象: {e}")
            return []

        batches = []
        date_format = "%Y%m%d"

        # 生成月份范围
        month_range = pd.date_range(start=start_dt.replace(day=1), end=end_dt, freq='MS')

        for index_code in index_codes:
            for month_start in month_range:
                month_end = month_start + pd.offsets.MonthEnd(1)
                
                # 确保我们不会超出总的结束日期
                if month_end > end_dt:
                    month_end = end_dt
                
                # 确保我们不会在开始日期之前创建批次
                if month_start < start_dt:
                    month_start = start_dt

                batches.append({
                    "index_code": index_code,
                    "start_date": month_start.strftime(date_format),
                    "end_date": month_end.strftime(date_format)
                })

        self.logger.info(f"为 {len(index_codes)} 个指数代码在 {len(month_range)} 个月份内生成了 {len(batches)} 个批次。")
        return batches
    
    # _decompose_batch_on_offset_limit 方法不再需要，可以删除
    # async def _decompose_batch_on_offset_limit(...):
    #     ...
