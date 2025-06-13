from datetime import datetime, timedelta
from typing import Dict, List

import pandas as pd

from ...sources.tushare import TushareTask
from alphahome.common.task_system.task_decorator import task_register
from ...tools.batch_utils import generate_natural_day_batches


@task_register()
class TushareFinaIndicatorTask(TushareTask):
    """股票财务指标数据任务

    获取上市公司财务指标数据，包括每股指标、盈利能力、营运能力、成长能力、偿债能力等指标。
    该任务使用Tushare的fina_indicator接口获取数据。
    """

    # 1.核心属性
    name = "tushare_fina_indicator"
    description = "获取上市公司财务指标数据"
    table_name = "tushare_fina_indicator"
    primary_keys = ["ts_code", "end_date", "ann_date"]
    date_column = "end_date"
    default_start_date = "19900101"

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5
    default_page_size = 10000

    # 2.自定义索引
    indexes = [
        {"name": "idx_fina_indicator_code", "columns": "ts_code"},
        {"name": "idx_fina_indicator_end_date", "columns": "end_date"},
        {"name": "idx_fina_indicator_ann_date", "columns": "ann_date"},
        {"name": "idx_fina_indicator_update_time", "columns": "update_time"},
    ]

    # 3.Tushare特有属性
    api_name = "fina_indicator_vip"
    fields = [
        "ts_code",
        "ann_date",
        "end_date",
        "eps",
        "dt_eps",
        "total_revenue_ps",
        "revenue_ps",
        "capital_rese_ps",
        "surplus_rese_ps",
        "undist_profit_ps",
        "extra_item",
        "profit_dedt",
        "gross_margin",
        "current_ratio",
        "quick_ratio",
        "cash_ratio",
        "invturn_days",
        "arturn_days",
        "inv_turn",
        "ar_turn",
        "ca_turn",
        "fa_turn",
        "assets_turn",
        "op_income",
        "valuechange_income",
        "interst_income",
        "daa",
        "ebit",
        "ebitda",
        "fcff",
        "fcfe",
        "current_exint",
        "noncurrent_exint",
        "interestdebt",
        "netdebt",
        "tangible_asset",
        "working_capital",
        "networking_capital",
        "invest_capital",
        "retained_earnings",
        "diluted2_eps",
        "bps",
        "ocfps",
        "retainedps",
        "cfps",
        "ebit_ps",
        "fcff_ps",
        "fcfe_ps",
        "netprofit_margin",
        "grossprofit_margin",
        "cogs_of_sales",
        "expense_of_sales",
        "profit_to_gr",
        "saleexp_to_gr",
        "adminexp_to_gr",
        "finaexp_to_gr",
        "impai_ttm",
        "gc_of_gr",
        "op_of_gr",
        "ebit_of_gr",
        "roe",
        "roe_waa",
        "roe_dt",
        "roa",
        "npta",
        "roic",
        "roe_yearly",
        "roa2_yearly",
        "roe_avg",
        "opincome_of_ebt",
        "investincome_of_ebt",
        "n_op_profit_of_ebt",
        "tax_to_ebt",
        "dtprofit_to_profit",
        "salescash_to_or",
        "ocf_to_or",
        "ocf_to_opincome",
        "capitalized_to_da",
        "debt_to_assets",
        "assets_to_eqt",
        "dp_assets_to_eqt",
        "ca_to_assets",
        "nca_to_assets",
        "tbassets_to_totalassets",
        "int_to_talcap",
        "eqt_to_talcapital",
        "currentdebt_to_debt",
        "longdeb_to_debt",
        "ocf_to_shortdebt",
        "debt_to_eqt",
        "eqt_to_debt",
        "eqt_to_interestdebt",
        "tangibleasset_to_debt",
        "tangasset_to_intdebt",
        "tangibleasset_to_netdebt",
        "ocf_to_debt",
        "ocf_to_interestdebt",
        "ocf_to_netdebt",
        "ebit_to_interest",
        "longdebt_to_workingcapital",
        "ebitda_to_debt",
        "turn_days",
        "roa_yearly",
        "roa_dp",
        "fixed_assets",
        "profit_prefin_exp",
        "non_op_profit",
        "op_to_ebt",
        "nop_to_ebt",
        "ocf_to_profit",
        "cash_to_liqdebt",
        "cash_to_liqdebt_withinterest",
        "op_to_liqdebt",
        "op_to_debt",
        "roic_yearly",
        "total_fa_trun",
        "profit_to_op",
        "q_opincome",
        "q_investincome",
        "q_dtprofit",
        "q_eps",
        "q_netprofit_margin",
        "q_gsprofit_margin",
        "q_exp_to_sales",
        "q_profit_to_gr",
        "q_saleexp_to_gr",
        "q_adminexp_to_gr",
        "q_finaexp_to_gr",
        "q_impair_to_gr_ttm",
        "q_gc_to_gr",
        "q_op_to_gr",
        "q_roe",
        "q_dt_roe",
        "q_npta",
        "q_oprate",
        "q_op_qoq",
        "q_profit_yoy",
        "q_profit_qoq",
        "q_netprofit_yoy",
        "q_netprofit_qoq",
        "q_sales_yoy",
        "q_sales_qoq",
        "q_gr_yoy",
        "q_gr_qoq",
        "q_roe_yoy",
        "q_dt_roe_yoy",
        "q_npta_yoy",
        "q_oprate_yoy",
        "q_op_yoy",
        "debt_to_assets_cl",
        "ca_to_assets_cl",
        "nca_to_assets_cl",
        "debt_to_eqt_cl",
        "eqt_to_debt_cl",
        "eqt_to_interestdebt_cl",
        "tangibleasset_to_debt_cl",
        "tangasset_to_intdebt_cl",
        "tangibleasset_to_netdebt_cl",
        "ocf_to_debt_cl",
        "ocf_to_interestdebt_cl",
        "ocf_to_netdebt_cl",
        "ebit_to_interest_cl",
        "longdebt_to_workingcapital_cl",
        "ebitda_to_debt_cl",
        "bps_yoy",
        "assets_yoy",
        "eqt_yoy",
        "tr_yoy",
        "or_yoy",
        "op_yoy",
        "ebt_yoy",
        "netprofit_yoy",
        "dt_netprofit_yoy",
        "ocf_yoy",
        "roe_yoy",
        "update_flag",
        "rd_exp",
    ]

    # 4.数据类型转换 (Most are numeric)
    transformations = {
        # Define transformations for non-float types or specific conversions
        "update_flag": lambda x: str(x) if pd.notna(x) else None,
        # All other fields from 'fields' list are expected to be float or can be coerced
        **{
            field: float
            for field in fields
            if field not in ["ts_code", "ann_date", "end_date", "update_flag"]
        },
    }

    # 5.列名映射 (No mapping needed for this API)
    column_mapping = {}

    # 6.表结构定义 (Define schema based on fields and expected types)
    schema_def = {
        "ts_code": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},
        "ann_date": {"type": "DATE", "constraints": "NOT NULL"},
        "end_date": {"type": "DATE", "constraints": "NOT NULL"},
        **{
            field: {"type": "NUMERIC(20,4)"}
            for field in fields
            if field not in ["ts_code", "ann_date", "end_date", "update_flag"]
        },
        "update_flag": {
            "type": "VARCHAR(1)"
        },  # Assuming update_flag is a single character
    }

    # 7.数据验证规则 (Optional, add specific checks if needed)
    validations = []

    def process_data(self, data):
        """处理从Tushare获取的数据

        重写父类方法，额外处理ann_date为NULL或空字符串的情况，并确保主键唯一性
        """
        # 首先调用父类的process_data方法进行基本处理
        data = super().process_data(data)

        # 处理ann_date为NULL或空字符串的情况
        if "ann_date" in data.columns and "end_date" in data.columns:
            # Identify rows where ann_date is NaN or an empty string
            is_na = data["ann_date"].isna()
            is_empty_str = data["ann_date"] == ""

            condition_to_fill = is_na | is_empty_str

            if condition_to_fill.any():
                num_to_fill = condition_to_fill.sum()
                self.logger.warning(
                    f"发现 {num_to_fill} 条记录的ann_date为isna()或为空字符串，将使用相应的end_date进行填充。"
                )

                # Fill ann_date using end_date for the identified rows
                data.loc[condition_to_fill, "ann_date"] = data.loc[
                    condition_to_fill, "end_date"
                ]

                # After attempting to fill, check if any of those specific rows still have problematic ann_date
                # This could happen if end_date was also NaN or empty for those rows
                # Check only the rows we attempted to fill for remaining issues
                # Ensure to access .loc[condition_to_fill, 'ann_date'] again to get the *updated* values
                updated_ann_dates_for_filled_rows = data.loc[
                    condition_to_fill, "ann_date"
                ]
                problem_still_exists_in_filled_rows = (
                    updated_ann_dates_for_filled_rows.isna()
                    | (updated_ann_dates_for_filled_rows == "")
                )

                if problem_still_exists_in_filled_rows.any():
                    num_still_problematic = problem_still_exists_in_filled_rows.sum()
                    self.logger.error(
                        f"尝试从end_date填充后，原先 {num_to_fill} 条记录中仍有 {num_still_problematic} 条的ann_date为isna()或为空字符串。请检查这些记录的end_date值。"
                    )

                    # 由于 ann_date 是 NOT NULL 约束，我们必须确保没有空值
                    # 移除无法修复的记录
                    problem_rows_indices = condition_to_fill[
                        problem_still_exists_in_filled_rows
                    ].index
                    self.logger.warning(
                        f"由于 ann_date 是 NOT NULL 约束，正在移除 {len(problem_rows_indices)} 条无法修复的记录。"
                    )
                    data = data.drop(problem_rows_indices)
                else:
                    self.logger.info(
                        f"对 {num_to_fill} 条记录的ann_date为空或空字符串的情况，已成功尝试使用end_date填充。"
                    )

        # 检查主键列是否存在任何空值
        for key in self.primary_keys:
            if key in data.columns:
                null_mask = data[key].isna()
                if null_mask.any():
                    null_count = null_mask.sum()
                    self.logger.warning(
                        f"发现 {null_count} 条记录的主键列 '{key}' 包含空值，这些记录将被移除。"
                    )
                    data = data[~null_mask]

        # 检查主键重复情况
        if len(self.primary_keys) > 0 and len(data) > 0:
            # 检查数据中是否有基于主键的重复记录
            duplicated = data.duplicated(subset=self.primary_keys, keep="first")
            if duplicated.any():
                num_duplicates = duplicated.sum()
                self.logger.warning(
                    f"发现 {num_duplicates} 条基于主键 {self.primary_keys} 的重复记录，将保留第一条出现的记录。"
                )
                data = data.drop_duplicates(subset=self.primary_keys, keep="first")

        return data

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """生成批处理参数列表 (使用自然日批次工具)

        Args:
            **kwargs: 查询参数，包括start_date、end_date、ts_code等

        Returns:
            List[Dict]: 批处理参数列表
        """
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        ts_code = kwargs.get("ts_code")  # Allow filtering by ts_code if provided

        if not start_date or not end_date:
            self.logger.error(f"任务 {self.name}: 必须提供 start_date 和 end_date 参数")
            return []

        # Using 365 days batch size similar to income/cashflow tasks
        batch_size_days = 365
        self.logger.info(
            f"任务 {self.name}: 使用自然日批次工具生成批处理列表，范围: {start_date} 到 {end_date}，批次大小: {batch_size_days} 天"
        )

        try:
            batch_list = await generate_natural_day_batches(
                start_date=start_date,
                end_date=end_date,
                batch_size=batch_size_days,
                ts_code=ts_code,  # Pass ts_code to batch generator
                logger=self.logger,
            )
            return batch_list
        except Exception as e:
            self.logger.error(
                f"任务 {self.name}: 生成自然日批次时出错: {e}", exc_info=True
            )
            return []

    async def pre_execute(self, stop_event=None, **kwargs):
        """执行前准备工作，确保表结构正确

        重写父类方法，确保表的主键包含 ann_date
        """
        # 调用父类的 pre_execute
        await super().pre_execute(stop_event=stop_event, **kwargs)

        # 检查表是否存在
        try:
            # 使用 db_manager 的 table_exists 方法检查表是否存在
            table_exists = await self.db.table_exists(self)
            if not table_exists:
                self.logger.info(f"表 '{self.table_name}' 不存在，将在执行任务时创建。")
                return  # 表不存在，后续操作不需要执行

            # 表存在，检查并处理现有数据
            if table_exists:
                # 1. 检查 ann_date 为 NULL 的记录 - 修改SQL查询，避免在DATE类型上使用空字符串比较
                null_check_query = (
                    f"SELECT COUNT(*) FROM {self.get_full_table_name()} WHERE ann_date IS NULL"
                )
                null_count_result = await self.db.fetch_one(null_check_query)
                null_count = null_count_result[0] if null_count_result else 0

                if null_count > 0:
                    self.logger.warning(
                        f"表 '{self.table_name}' 中存在 {null_count} 条 ann_date 为 NULL 的记录，尝试使用 end_date 填充"
                    )
                    # 使用 end_date 填充 NULL 的 ann_date
                    update_null_query = f"UPDATE {self.get_full_table_name()} SET ann_date = end_date WHERE ann_date IS NULL"
                    await self.db.execute(update_null_query)
                    self.logger.info(
                        f"已将 {null_count} 条记录的 ann_date 用 end_date 填充"
                    )

                # 2. 检查并处理潜在的主键冲突
                # 主要出现在将 ann_date 加入主键后，可能导致之前相同的 ts_code + end_date 组合现在变成了重复主键
                if "ann_date" in self.primary_keys:
                    # 检查重复记录
                    duplicate_check_query = f"""
                    SELECT ts_code, end_date, COUNT(*) as count
                    FROM {self.get_full_table_name()}
                    GROUP BY ts_code, end_date
                    HAVING COUNT(*) > 1
                    """
                    duplicate_result = await self.db.fetch(duplicate_check_query)

                    if duplicate_result and len(duplicate_result) > 0:
                        duplicate_count = sum(row[2] for row in duplicate_result) - len(
                            duplicate_result
                        )
                        self.logger.warning(
                            f"表 '{self.table_name}' 中存在 {len(duplicate_result)} 组重复的 ts_code+end_date 组合，总计 {duplicate_count} 条冲突记录"
                        )

                        # 解决方案：保留每组中最新的记录，删除其余记录
                        # 这里我们使用 update_time 作为决定保留哪条记录的依据
                        for row in duplicate_result:
                            ts_code = row[0]
                            end_date = row[1]

                            # 删除除最新记录外的所有重复记录
                            delete_duplicates_query = f"""
                            DELETE FROM {self.get_full_table_name()}
                            WHERE ts_code = '{ts_code}' AND end_date = '{end_date}'
                            AND ctid NOT IN (
                                SELECT ctid
                                FROM {self.get_full_table_name()}
                                WHERE ts_code = '{ts_code}' AND end_date = '{end_date}'
                                ORDER BY update_time DESC NULLS LAST
                                LIMIT 1
                            )
                            """
                            result = await self.db.execute(delete_duplicates_query)
                            self.logger.info(
                                f"已从表 '{self.table_name}' 中删除 ts_code='{ts_code}', end_date='{end_date}' 的冗余记录"
                            )

                # 3. 检查当前表的主键
                try:
                    pk_query = f"""
                    SELECT a.attname
                    FROM   pg_index i
                    JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                    WHERE  i.indrelid = '{self.get_full_table_name()}'::regclass
                    AND    i.indisprimary
                    """
                    primary_keys = await self.db.fetch(pk_query)
                    current_pks = [row[0] for row in primary_keys]

                    # 4. 检查是否需要更新主键
                    if set(current_pks) != set(self.primary_keys):
                        self.logger.info(
                            f"检测到表 '{self.table_name}' 的主键需要更新: 当前={current_pks}, 目标={self.primary_keys}"
                        )

                        # 更新主键约束前，确保没有冲突的主键值
                        # 再次检查是否存在潜在冲突的记录
                        pk_conflict_check_query = f"""
                        SELECT COUNT(*) 
                        FROM (
                            SELECT {', '.join(self.primary_keys)}, COUNT(*) 
                            FROM {self.get_full_table_name()} 
                            GROUP BY {', '.join(self.primary_keys)} 
                            HAVING COUNT(*) > 1
                        ) t
                        """

                        conflict_result = await self.db.fetch_one(
                            pk_conflict_check_query
                        )
                        if conflict_result and conflict_result[0] > 0:
                            self.logger.warning(
                                f"在更新主键约束前检测到 {conflict_result[0]} 组冲突的主键值，将保留每组中最新的记录。"
                            )

                            # 创建临时表用于删除重复记录
                            temp_table = f"temp_{self.table_name}_{int(datetime.now().timestamp())}"

                            # 1. 创建临时表，只保留每个主键组合的最新记录
                            create_temp_table_query = f"""
                            CREATE TEMP TABLE {temp_table} AS
                            SELECT DISTINCT ON ({', '.join(self.primary_keys)}) *
                            FROM {self.get_full_table_name()}
                            ORDER BY {', '.join(self.primary_keys)}, update_time DESC NULLS LAST
                            """
                            await self.db.execute(create_temp_table_query)

                            # 2. 截断原表
                            truncate_query = f"TRUNCATE TABLE {self.get_full_table_name()}"
                            await self.db.execute(truncate_query)

                            # 3. 从临时表恢复数据到原表
                            restore_query = f"INSERT INTO {self.get_full_table_name()} SELECT * FROM {temp_table}"
                            await self.db.execute(restore_query)

                            # 4. 删除临时表
                            drop_temp_query = f"DROP TABLE {temp_table}"
                            await self.db.execute(drop_temp_query)

                            self.logger.info(
                                f"已成功处理冲突的主键值，现在可以安全地更新主键约束。"
                            )

                        # 更新主键约束
                        # 删除现有主键约束
                        drop_pk_query = f"""
                        ALTER TABLE {self.get_full_table_name()} DROP CONSTRAINT IF EXISTS {self.table_name}_pkey;
                        """
                        await self.db.execute(drop_pk_query)

                        # 添加新的主键约束
                        add_pk_query = f"""
                        ALTER TABLE {self.get_full_table_name()} ADD PRIMARY KEY ({', '.join(self.primary_keys)});
                        """
                        await self.db.execute(add_pk_query)

                        self.logger.info(
                            f"表 '{self.table_name}' 的主键已更新为 {self.primary_keys}"
                        )
                    else:
                        self.logger.debug(
                            f"表 '{self.table_name}' 的主键已经匹配: {current_pks}"
                        )

                except Exception as e:
                    self.logger.error(
                        f"检查或更新表主键时发生错误: {str(e)}", exc_info=True
                    )

        except Exception as e:
            self.logger.error(
                f"检查和更新表 '{self.table_name}' 的主键时发生错误: {e}", exc_info=True
            )
            # 不抛出异常，继续执行任务
