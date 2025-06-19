from datetime import datetime, timedelta
from typing import Dict, List

import numpy as np  # 添加numpy用于处理无穷大值
import pandas as pd

from ...sources.tushare import TushareTask
from alphahome.common.task_system.task_decorator import task_register
from ...tools.batch_utils import generate_natural_day_batches


@task_register()
class TushareStockReportRcTask(TushareTask):
    """获取券商盈利预测数据任务

    数据来源: Tushare report_rc 接口 (https://tushare.pro/document/2?doc_id=292)
    """

    # 1.核心属性
    name = "tushare_stock_report_rc"
    description = "获取上市公司业绩快报"
    table_name = "stock_report_rc"
    primary_keys = ["ts_code", "report_date", "org_name", "author_name", "quarter"]
    date_column = "report_date"  # 使用报告日期作为主要日期
    default_start_date = "19900101"

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 10
    default_page_size = 3000

    # 2.自定义索引
    indexes = [
        {"name": "idx_reportrc_code", "columns": "ts_code"},
        {"name": "idx_reportrc_report_date", "columns": "report_date"},
        {"name": "idx_reportrc_org", "columns": "org_name"},
        {"name": "idx_reportrc_quarter", "columns": "quarter"},
        {"name": "idx_reportrc_update_time", "columns": "update_time"},
    ]

    # 3.Tushare特有属性
    api_name = "report_rc"  # Tushare API 名称
    fields = [
        "ts_code",
        "name",
        "report_date",
        "report_title",
        "report_type",
        "classify",
        "org_name",
        "author_name",
        "quarter",
        "op_rt",
        "op_pr",
        "tp",
        "np",
        "eps",
        "pe",
        "rd",
        "roe",
        "ev_ebitda",
        "rating",
        "max_price",
        "min_price",
        "imp_dg",
        "create_time",
    ]

    # 4.数据类型转换
    transformations = {
        "op_rt": float,
        "op_pr": float,
        "tp": float,
        "np": float,
        "eps": float,
        "pe": float,
        "rd": float,
        "roe": float,
        "ev_ebitda": float,
        "max_price": float,
        "min_price": float,
        "create_time": lambda x: pd.to_datetime(x) if pd.notna(x) else None,
    }

    # 5.列名映射 (No mapping needed for this API)
    column_mapping = {}

    # 6.表结构定义
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "ann_date": {"type": "DATE"},
        "end_date": {"type": "DATE"},
        "report_date": {"type": "DATE", "constraints": "NOT NULL"},
        "org_name": {"type": "VARCHAR(100)", "constraints": "NOT NULL"},
        "author_name": {"type": "VARCHAR(255)", "constraints": "NOT NULL"},
        "quarter": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},
        "name": {"type": "VARCHAR(50)"},
        "report_title": {"type": "TEXT"},  # Titles can be long
        "report_type": {"type": "VARCHAR(50)"},
        "classify": {"type": "VARCHAR(50)"},
        "op_rt": {"type": "NUMERIC(20,4)"},
        "op_pr": {"type": "NUMERIC(20,4)"},
        "tp": {"type": "NUMERIC(20,4)"},
        "np": {"type": "NUMERIC(20,4)"},
        "eps": {"type": "NUMERIC(20,4)"},
        "pe": {"type": "NUMERIC(20,4)"},
        "rd": {"type": "NUMERIC(20,4)"},
        "roe": {"type": "NUMERIC(20,4)"},
        "ev_ebitda": {"type": "NUMERIC(20,4)"},
        "rating": {"type": "VARCHAR(50)"},
        "max_price": {"type": "NUMERIC(20,4)"},
        "min_price": {"type": "NUMERIC(20,4)"},
        "imp_dg": {"type": "VARCHAR(50)"},
        "create_time": {"type": "TIMESTAMP"},
    }

    # 7.数据验证规则 (Optional)
    validations = []

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """生成批处理参数列表 (使用自然日批次工具, 基于 report_date)

        Args:
            **kwargs: 查询参数，包括start_date、end_date (对应 report_date), ts_code, full_update等

        Returns:
            List[Dict]: 批处理参数列表
        """
        full_update = kwargs.get("full_update", False)
        ts_code = kwargs.get("ts_code")  # Allow filtering by ts_code if provided

        if full_update:
            start_date = self.default_start_date
            end_date = datetime.now().strftime("%Y%m%d")
            self.logger.info(
                f"任务 {self.name}: 全量更新模式，自动设置日期范围: {start_date} 到 {end_date}"
            )
        else:
            start_date = kwargs.get("start_date")
            end_date = kwargs.get("end_date")
            if not start_date or not end_date:
                self.logger.error(
                    f"任务 {self.name}: 非全量更新模式下，必须提供 start_date 和 end_date 参数 (对应 report_date)"
                )
                return []
            self.logger.info(
                f"任务 {self.name}: 使用自然日批次工具生成批处理列表 (基于 report_date)，范围: {start_date} 到 {end_date}"
            )

        batch_size_days = 30  # User requested 30-day batches

        try:
            # Note: The batch generator uses start_date/end_date keys.
            # The TushareTask base class should correctly pass these as start_date/end_date
            # parameters to the Tushare API, which filters on 'report_date'.
            batch_list = await generate_natural_day_batches(
                start_date=start_date,  # type: ignore
                end_date=end_date,  # type: ignore
                batch_size=batch_size_days,
                ts_code=ts_code,  # Pass ts_code to batch generator if provided
                logger=self.logger,
            )
            return batch_list
        except Exception as e:
            self.logger.error(
                f"任务 {self.name}: 生成自然日批次时出错: {e}", exc_info=True
            )
            return []

    async def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        异步处理从API获取的原始数据。
        此方法可以被子类覆盖以实现特定的数据转换逻辑。
        """
        self.logger.info(f"开始处理 report_rc 数据，原始数据形状: {df.shape}")
        
        # 假设父类的 process_data 是同步的
        df = super().process_data(df)

        # 如果df为空或者不是DataFrame，则直接返回
        if not isinstance(df, pd.DataFrame) or df.empty:
            return df

        # 数据质量检查和清理
        numeric_cols = [col for col in df.columns if col in [
            "op_rt", "op_pr", "tp", "np", "eps", "pe", "rd", "roe", 
            "ev_ebitda", "max_price", "min_price"
        ]]
        
        if numeric_cols:
            self.logger.debug(f"检查 {len(numeric_cols)} 个数值列: {numeric_cols}")
            
            # 1. 检查无穷大值
            inf_counts = {}
            for col in numeric_cols:
                if col in df.columns:
                    inf_count = np.isinf(df[col]).sum()
                    if inf_count > 0:
                        inf_counts[col] = inf_count
                        self.logger.warning(f"列 {col} 包含 {inf_count} 个无穷大值")
            
            # 2. 检查超出NUMERIC(20,4)范围的值
            # NUMERIC(20,4): 总共20位数字，4位小数，所以整数部分最多16位
            max_value = 9999999999999999.9999  # 16位整数 + 4位小数
            min_value = -9999999999999999.9999
            extreme_counts = {}
            
            for col in numeric_cols:
                if col in df.columns:
                    extreme_mask = (df[col] > max_value) | (df[col] < min_value)
                    extreme_count = extreme_mask.sum()
                    if extreme_count > 0:
                        extreme_counts[col] = extreme_count
                        self.logger.warning(f"列 {col} 包含 {extreme_count} 个超出NUMERIC(20,4)范围的值")
                        # 记录样本
                        extreme_samples = df.loc[extreme_mask, col].head(3).tolist()
                        self.logger.warning(f"列 {col} 的极值样本: {extreme_samples}")
            
            # 3. 清理数据：将无穷大值和极值替换为NaN
            for col in numeric_cols:
                if col in df.columns:
                    # 替换无穷大值
                    df[col] = df[col].replace([np.inf, -np.inf], np.nan)
                    # 替换超出范围的极值
                    extreme_mask = (df[col] > max_value) | (df[col] < min_value)
                    if extreme_mask.any():
                        df.loc[extreme_mask, col] = np.nan
            
            # 4. 记录清理结果
            if inf_counts:
                self.logger.info(f"已清理无穷大值的列: {inf_counts}")
            if extreme_counts:
                self.logger.info(f"已清理极值的列: {extreme_counts}")

        # 2. 填充特定列的空值
        if "org_name" in df.columns:
            # fillna处理None/NaN, replace处理空字符串
            df["org_name"] = df["org_name"].fillna("无").replace("", "无")

        if "author_name" in df.columns:
            df["author_name"] = df["author_name"].fillna("无").replace("", "无")

                # 3. 过滤掉quarter字段为NULL的记录
        if "quarter" in df.columns:
            quarter_null_mask = df["quarter"].isnull() | (df["quarter"] == "")
            quarter_null_count = quarter_null_mask.sum()
            
            if quarter_null_count > 0:
                self.logger.warning(f"发现 {quarter_null_count} 行的quarter字段为空，将过滤掉这些记录")
                
                # 过滤掉quarter为NULL的记录
                df = df[~quarter_null_mask].copy()  # type: ignore
                df = df.reset_index(drop=True)  # 重置索引
                
                self.logger.info(f"quarter字段过滤完成: 过滤掉 {quarter_null_count} 行，剩余 {len(df)} 行")

        self.logger.info(f"数据处理完成，最终数据形状: {df.shape}")
        return df
