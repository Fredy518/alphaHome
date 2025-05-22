#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
财报披露计划 (disclosure_date) 更新任务
获取上市公司财报的预计披露日期和实际披露日期。
继承自 TushareTask，按季度获取数据。
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dateutil.relativedelta import relativedelta

from ...sources.tushare.tushare_task import TushareTask
from ...task_decorator import task_register
from ...tools.batch_utils import generate_quarter_end_batches

@task_register()
class TushareFinaDisclosureTask(TushareTask):
    """获取上市公司财报披露计划数据"""

    # 1. 核心属性
    name = "tushare_fina_disclosure"
    description = "获取上市公司财报披露计划数据"
    table_name = "tushare_fina_disclosure"
    primary_keys = ["ts_code", "ann_date", "end_date"] # 财报披露计划数据，按季度获取，有可能更改原披露日期
    date_column = "end_date"  # 财报周期日期作为主要日期列
    default_start_date = "19901231"  # 开始日期
    commit = True  # 数据插入后自动提交事务

    # New attribute for quarterly lookback
    quarter_lookback = 3  # Fetch data for the current quarter and the N previous quarters

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 10
    default_page_size = 3000

    # 2. 自定义索引
    indexes = [
        {"name": "idx_fina_disclosure_code", "columns": "ts_code"},
        {"name": "idx_fina_disclosure_ann_date", "columns": "ann_date"},
        {"name": "idx_fina_disclosure_end_date", "columns": "end_date"},
        {"name": "idx_fina_disclosure_pre_date", "columns": "pre_date"},
        {"name": "idx_fina_disclosure_actual_date", "columns": "actual_date"},
        {"name": "idx_fina_disclosure_update_time", "columns": "update_time"}
    ]

    # 3. Tushare特有属性
    api_name = "disclosure_date"
    fields = [
        "ts_code", "ann_date", "end_date", "pre_date", 
        "actual_date", "modify_date"
    ]

    # 4. 数据类型转换
    transformations = {
        # 日期字段由基类的 process_data 方法自动处理
    }

    # 5. 列名映射
    column_mapping = {}

    # 6. 表结构定义
    schema = {
        "ts_code": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},
        "ann_date": {"type": "DATE", "constraints": ""},
        "end_date": {"type": "DATE", "constraints": "NOT NULL"},
        "pre_date": {"type": "DATE", "constraints": ""},
        "actual_date": {"type": "DATE", "constraints": ""},
        "modify_date": {"type": "DATE", "constraints": ""}
    }

    async def _determine_execution_dates(self, **kwargs) -> Tuple[Optional[str], Optional[str], bool, str]:
        """
        覆盖基类方法，根据不同模式计算执行日期范围。
        优先级: 1. 智能增量 (季度回溯) -> 2. 全量模式 -> 3. 手动指定日期
        
        智能增量: 通过smart_incremental标志或根据日期特征推断，使用季度回溯逻辑计算日期范围
        全量导入: 使用 self.default_start_date 和当前日期
        手动增量: 使用 kwargs 中的 start_date 和 end_date
        """
        try:
            # 获取基本参数
            user_start_date = kwargs.get('start_date')
            user_end_date = kwargs.get('end_date')
            is_force_full = kwargs.get('force_full', False)
            is_smart_incremental = kwargs.get('smart_incremental', False)
            
            # 尝试推断是否是智能增量调用（即使没有明确标记）
            is_likely_smart_incremental = False
            
            # 检查1：如果有明确的标记
            if is_smart_incremental:
                is_likely_smart_incremental = True
                self.logger.info(f"任务 {self.name}: 检测到 smart_incremental=True 标志")
            
            # 检查2：如果end_date接近当前日期，这可能是智能增量自动设置的
            if user_end_date and not is_likely_smart_incremental:
                try:
                    today = datetime.now()
                    end_date_dt = datetime.strptime(user_end_date, '%Y%m%d')
                    days_diff = (today - end_date_dt).days
                    
                    # 如果结束日期是最近7天内，可能是智能增量设置的
                    if -1 <= days_diff <= 7:
                        # 进一步验证start_date特征，如果为日历月末或接近值
                        if user_start_date:
                            start_date_dt = datetime.strptime(user_start_date, '%Y%m%d')
                            # 如果开始日期是月底或月初附近的日期
                            day = start_date_dt.day
                            if day in [1, 2, 28, 29, 30, 31] or abs(day - 15) <= 2:
                                is_likely_smart_incremental = True
                                self.logger.info(f"任务 {self.name}: 通过日期特征分析推断为智能增量调用 (start={user_start_date}, end={user_end_date})")
                except Exception as e:
                    self.logger.debug(f"任务 {self.name}: 日期特征分析失败: {e}")
            
            # 根据判断结果决定执行路径
            if (is_smart_incremental or is_likely_smart_incremental) and not is_force_full:
                # 智能增量模式: 使用季度回溯逻辑
                self.logger.info(f"任务 {self.name}: 执行智能增量季度回溯 (quarter_lookback={self.quarter_lookback}) 日期逻辑。")
                
                # 忽略已存在的 start_date 和 end_date，强制执行季度回溯计算
                current_date = datetime.now()
                
                # 计算当前季度的结束日期
                current_quarter_month_end = ((current_date.month - 1) // 3 + 1) * 3
                final_end_date_dt = datetime(current_date.year, current_quarter_month_end, 1) + relativedelta(months=1) - relativedelta(days=1)

                # Add time control: if before 21:00, set end date to yesterday
                # 如果当前时间在21:00之前，将结束日期设置为前一天
                if current_date.hour < 21:
                    final_end_date_dt = final_end_date_dt - timedelta(days=1)

                # 计算回溯季度的起始日期
                start_lookback_period_dt = final_end_date_dt - relativedelta(months=3 * (self.quarter_lookback -1))
                first_month_of_start_quarter = ((start_lookback_period_dt.month - 1) // 3) * 3 + 1
                final_start_date_dt = datetime(start_lookback_period_dt.year, first_month_of_start_quarter, 1)

                final_start_date_str = final_start_date_dt.strftime('%Y%m%d')
                final_end_date_str = final_end_date_dt.strftime('%Y%m%d')

                self.logger.info(f"任务 {self.name}: 智能增量季度回溯计算的日期范围: start_date={final_start_date_str}, end_date={final_end_date_str}")
                return final_start_date_str, final_end_date_str, False, ""
                
            elif is_force_full:
                # 全量导入模式: 使用默认起始日期到当前日期
                self.logger.info(f"任务 {self.name}: 检测到 force_full=True，执行全量导入日期逻辑。")
                final_start_date_str = self.default_start_date
                final_end_date_str = datetime.now().strftime('%Y%m%d')
                self.logger.info(f"任务 {self.name}: 全量导入日期范围: start_date={final_start_date_str}, end_date={final_end_date_str}")
                return final_start_date_str, final_end_date_str, False, ""
                
            elif user_start_date and user_end_date:
                # 手动增量模式: 用户明确指定了起止日期
                self.logger.info(f"任务 {self.name}: 检测到手动指定的日期范围: start_date={user_start_date}, end_date={user_end_date}")
                return user_start_date, user_end_date, False, ""
                
            else:
                # 兜底情况: 未明确标识模式或未支持的模式
                self.logger.warning(f"任务 {self.name}: 无法确定执行模式 (非智能增量/全量导入/手动增量)，使用当前日期作为默认范围。")
                current_date_str = datetime.now().strftime('%Y%m%d')
                return current_date_str, current_date_str, False, "无法确定执行模式，使用当前日期作为默认范围"
            
        except Exception as e:
            self.logger.error(f"任务 {self.name}: 在 _determine_execution_dates 中计算日期范围失败: {e}", exc_info=True)
            current_date_str = datetime.now().strftime('%Y%m%d')
            # In case of error, fallback to a very narrow range to avoid unintended full fetches if dates are bad
            return current_date_str, current_date_str, False, f"Error in date calculation: {e}"

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """生成按季度的批处理参数列表
        
        使用 batch_utils.generate_quarter_end_batches 生成按季度的批次。
        对于财报披露数据，按季度获取是最合适的方式，因为财报是按季度发布的。
        
        Args:
            **kwargs: 查询参数，包括start_date、end_date、ts_code等
            
        Returns:
            List[Dict]: 批处理参数列表，每个批次包含一个季度的结束日期
        """
        # 获取起止日期参数
        start_date = kwargs.get('start_date')
        end_date = kwargs.get('end_date')
        ts_code = kwargs.get('ts_code')

        if not start_date or not end_date:
            self.logger.error(f"任务 {self.name}: 必须提供 start_date 和 end_date 参数")
            return []

        self.logger.info(f"任务 {self.name}: 使用季度批次工具生成批处理列表，范围: {start_date} 到 {end_date}")

        try:
            # 使用 generate_quarter_end_batches 生成季度批次
            # 这个函数生成的批次参数中使用 'period' 作为日期参数的键名
            quarter_batches = await generate_quarter_end_batches(
                start_date=start_date,
                end_date=end_date,
                ts_code=ts_code,
                logger=self.logger
            )
            
            # 将 'period' 参数映射为 'end_date' 参数（因为 disclosure_date API 使用 end_date 参数）
            batch_list = []
            for batch in quarter_batches:
                # 提取 'period' 值作为 'end_date'
                if 'period' in batch:
                    batch_params = {'end_date': batch['period']}
                    # 复制其他参数
                    for key, value in batch.items():
                        if key != 'period':
                            batch_params[key] = value
                    batch_list.append(batch_params)
            
            self.logger.info(f"任务 {self.name}: 成功生成 {len(batch_list)} 个季度批次")
            return batch_list
        except Exception as e:
            self.logger.error(f"任务 {self.name}: 生成季度批次时出错: {e}", exc_info=True)
            return []

    async def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        异步处理从API获取的原始数据。
        此方法可以被子类覆盖以实现特定的数据转换逻辑。
        """
        # 首先调用父类的通用处理逻辑 (如果它存在且做了有用的事)
        # 假设父类的 process_data 是同步的
        df = super().process_data(df)

        # 如果df为空或者不是DataFrame，则直接返回
        if not isinstance(df, pd.DataFrame) or df.empty:
            return df
        
        # 确保没有空的字符串在日期列（处理成NULL更合适）
        for date_col in ['ann_date', 'pre_date', 'actual_date', 'modify_date']:
            if date_col in df.columns:
                df[date_col] = df[date_col].replace('', None)
        
        # 返回处理后的数据
        return df 