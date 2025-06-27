#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
居民消费价格指数 (cn_cpi) 更新任务
获取中国居民消费价格指数(CPI)数据。
继承自 TushareTask。
使用月度批次生成。
"""

from datetime import datetime
from typing import Any, Dict, List

import pandas as pd

# 确认导入路径正确 (相对于当前文件)
from ...sources.tushare.tushare_task import TushareTask
from ....common.task_system.task_decorator import task_register

# 导入月份批次生成工具函数
from ...sources.tushare.batch_utils import generate_month_batches


@task_register()
class TushareMacroCpiTask(TushareTask):
    """获取中国居民消费价格指数(CPI)数据"""

    # 1. 核心属性
    name = "tushare_macro_cpi"
    description = "获取中国居民消费价格指数(CPI)"
    table_name = "macro_cpi"
    primary_keys = ["month_end_date"]  # 修改: 使用 month_end_date 作为主键
    date_column = "month_end_date"  # 修改: 使用 month_end_date 作为主要日期列
    default_start_date = "19960101"  # API 支持YYYYMM，但为与 month_end_date 保持一致性
    data_source = "tushare"

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5  # 默认并发限制
    default_page_size = 5000  # 单次最大5000行

    # 启用单批次处理模式
    single_batch = True

    # 2. TushareTask 特有属性
    api_name = "cn_cpi"
    # Tushare cn_cpi 接口实际返回的字段 (根据文档 [https://tushare.pro/document/2?doc_id=228])
    fields = [
        "month",
        "nt_val",
        "nt_yoy",
        "nt_mom",
        "nt_accu",
        "town_val",
        "town_yoy",
        "town_mom",
        "town_accu",
        "cnt_val",
        "cnt_yoy",
        "cnt_mom",
        "cnt_accu",
    ]

    # 3. 列名映射 (API字段名与数据库列名不完全一致时，进行映射)
    column_mapping = {}  # 无需映射

    # 4. 数据类型转换 (日期列在基类处理，数值列转换为 float)
    # 所有字段转换为 float，除了 month
    transformations = {field: float for field in fields if field != "month"}

    # 5. 数据库表结构
    schema_def = {
        "month": {
            "type": "VARCHAR(10)",
            "constraints": "NOT NULL",
        },  # YYYYMM 格式, 保留原始月份
        "month_end_date": {"type": "DATE", "constraints": "NOT NULL"},  # 新增: 月末日期
        "nt_val": {"type": "NUMERIC(15,4)"},  # 全国当月值
        "nt_yoy": {"type": "NUMERIC(15,4)"},  # 全国同比（%）
        "nt_mom": {"type": "NUMERIC(15,4)"},  # 全国环比（%）
        "nt_accu": {"type": "NUMERIC(15,4)"},  # 全国累计值
        "town_val": {"type": "NUMERIC(15,4)"},  # 城市当月值
        "town_yoy": {"type": "NUMERIC(15,4)"},  # 城市同比（%）
        "town_mom": {"type": "NUMERIC(15,4)"},  # 城市环比（%）
        "town_accu": {"type": "NUMERIC(15,4)"},  # 城市累计值
        "cnt_val": {"type": "NUMERIC(15,4)"},  # 农村当月值
        "cnt_yoy": {"type": "NUMERIC(15,4)"},  # 农村同比（%）
        "cnt_mom": {"type": "NUMERIC(15,4)"},  # 农村环比（%）
        "cnt_accu": {"type": "NUMERIC(15,4)"},  # 农村累计值
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        # {"name": "idx_macro_cpi_month", "columns": "month"}, # 移除或注释掉
        {
            "name": "idx_cpi_month_orig",
            "columns": "month",
        },  # 可选：为原始 month 列保留索引
        {"name": "idx_macro_cpi_update_time", "columns": "update_time"},
    ]

    # 7. 分批配置
    # 批次大小，每次获取12个月的数据（1年）
    batch_month_size = 12

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表，使用月份批次。
        """
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

        # 转换为YYYYMM格式
        if len(start_date) == 8:  # YYYYMMDD格式
            start_date = start_date[:6]  # 取YYYYMM部分
        if len(end_date) == 8:  # YYYYMMDD格式
            end_date = end_date[:6]  # 取YYYYMM部分

        self.logger.info(
            f"任务 {self.name}: 使用月份批次生成批处理列表，范围: {start_date} 到 {end_date}"
        )

        try:
            # 使用月份批次工具函数，每个批次12个月（1年）
            batch_list = await generate_month_batches(
                start_m=start_date,
                end_m=end_date,
                batch_size=self.batch_month_size,
                logger=self.logger,
            )
            self.logger.info(
                f"任务 {self.name}: 成功生成 {len(batch_list)} 个月度批次。"
            )
            return batch_list
        except Exception as e:
            self.logger.error(
                f"任务 {self.name}: 生成月度批次时出错: {e}", exc_info=True
            )
            # 抛出异常以便上层调用者感知
            raise RuntimeError(f"任务 {self.name}: 生成月度批次失败") from e

    def process_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        处理从API获取的原始数据（重写基类扩展点）
        """
        if not isinstance(df, pd.DataFrame) or df.empty:
            self.logger.info(
                f"任务 {self.name}: process_data 接收到空 DataFrame，跳过处理。"
            )
            return df

        # 首先调用基类的数据处理方法（应用基础转换）
        df = super().process_data(df, **kwargs)

        # CPI特定的数据处理
        original_rows = len(df)
        if "month" in df.columns:
            # 1. 预过滤无效月份字符串 (例如，非数字或长度不为6的)
            initial_valid_mask = df["month"].astype(str).str.match(r"^\d{6}$").fillna(False)
            invalid_month_count = (~initial_valid_mask).sum()
            if invalid_month_count > 0:
                self.logger.warning(
                    f"任务 {self.name}: 发现 {invalid_month_count} 行无效的 'month' 格式，已过滤。"
                )
                df = df[initial_valid_mask].copy()
                if df.empty:
                    self.logger.info(
                        f"任务 {self.name}: 过滤无效月份后DataFrame为空，跳过后续处理。"
                    )
                    return df

            try:
                # 2. 手动生成 month_end_date 列（从 YYYYMM 转换为该月的最后一天）
                self.logger.debug(f"任务 {self.name}: 开始生成 month_end_date 列")
                
                def convert_month_to_end_date(month_str):
                    """将 YYYYMM 格式转换为该月的最后一天"""
                    try:
                        if pd.isna(month_str) or month_str == '':
                            return None
                        
                        month_str = str(month_str).strip()
                        if len(month_str) != 6 or not month_str.isdigit():
                            return None
                            
                        year = int(month_str[:4])
                        month = int(month_str[4:6])
                        
                        # 创建该月第一天，然后找到下月第一天，再减去一天得到月末
                        from datetime import datetime
                        import calendar
                        
                        # 获取该月的最后一天
                        last_day = calendar.monthrange(year, month)[1]
                        month_end = datetime(year, month, last_day)
                        
                        return month_end.date()
                    except Exception as e:
                        self.logger.warning(f"转换月份 '{month_str}' 时出错: {e}")
                        return None
                
                df["month_end_date"] = df["month"].apply(convert_month_to_end_date)
                
                # 检查转换结果
                null_count = df["month_end_date"].isna().sum()
                if null_count > 0:
                    self.logger.warning(
                        f"任务 {self.name}: 有 {null_count} 行的 month_end_date 转换失败，将被过滤掉"
                    )
                    # 过滤掉转换失败的行
                    df = df[df["month_end_date"].notna()].copy()
                
                self.logger.info(
                    f"任务 {self.name}: 成功生成 month_end_date 列，剩余 {len(df)} 行数据"
                )
                
            except Exception as e:
                self.logger.error(
                    f"任务 {self.name}: 生成 'month_end_date' 列时出错: {e}",
                    exc_info=True,
                )
                # 如果生成失败，确保不会有 null 值传递到数据库
                return pd.DataFrame()

        # 验证过滤后是否还有数据
        if len(df) < original_rows:
            self.logger.warning(
                f"任务 {self.name}: 在日期处理后，数据从 {original_rows} 行减少到 {len(df)} 行。"
            )

        if df.empty:
            self.logger.info(
                f"任务 {self.name}: 处理后无有效数据，返回空DataFrame。"
            )

        return df

    # 7. 数据验证规则 (真正生效的验证机制)
    validations = [
        (lambda df: df['month'].notna(), "月份不能为空"),
        (lambda df: df['nt_val'].notna(), "全国当月CPI值不能为空"),
        (lambda df: df['nt_yoy'].notna(), "全国同比增长率不能为空"),
        (lambda df: ~(df['month'].astype(str).str.strip().eq('') | df['month'].isna()), "月份不能为空字符串"),
        (lambda df: df['nt_val'] >= 0, "CPI值应为非负数"),
        (lambda df: df['nt_val'] <= 1000, "CPI值应在合理范围内（≤1000）"),
        (lambda df: df['nt_yoy'].abs() <= 100, "同比增长率应在合理范围内（±100%）"),
    ]
