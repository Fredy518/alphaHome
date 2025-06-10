import logging
from typing import Any, Dict, List, Optional

import pandas as pd
from pandas.tseries.offsets import YearBegin, YearEnd  # 需要导入

# 假设 TushareTask 是 Tushare 相关任务的基类。
# 如果导入路径不同，请相应调整。
from alphahome.fetchers.sources.tushare.tushare_task import TushareTask
from alphahome.fetchers.task_decorator import task_register


@task_register()
class TushareIndexWeightTask(TushareTask):
    """
    从Tushare获取指数成分股及其权重的任务。
    API: index_weight (月度数据)
    """

    # 1. 核心属性 (类似于 tushare_index_swdaily.py)
    name = "tushare_index_weight"  # 任务标识符
    description = "获取指数成分股及权重(月度)"  # 已是中文，无需修改
    table_name = "tushare_index_weight"  # 默认数据库表名
    primary_keys = ["index_code", "con_code", "trade_date"]  # 基于Tushare的输出确定
    date_column = "trade_date"  # 用于增量逻辑的主要日期列
    default_start_date = (
        "20050101"  # 如果数据库中无数据，默认的起始日期，可根据需要调整
    )

    # --- 代码级别的默认配置 (可被 config.json 文件覆盖) ---
    default_concurrent_limit = 10
    default_page_size = (
        8000  # TushareAPI 处理分页；index_weight 是月度数据，此限制通常不触发
    )
    date_split_interval_years = 2  # 新增：按2年拆分日期范围生成批次

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
    schema = {
        "index_code": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},
        "con_code": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},  # 股票代码
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

    def _adjust_dates_for_monthly_api(
        self, start_date_str: Optional[str], end_date_str: Optional[str]
    ) -> tuple[Optional[str], Optional[str]]:
        """
        将输入的日期范围调整为月初和月末，适用于月度API (如index_weight)。

        例如:
        - 输入: start='20220115', end='20220520'
        - 输出: start='20220101', end='20220531'

        参数:
            start_date_str: 起始日期 (YYYYMMDD 格式)
            end_date_str: 结束日期 (YYYYMMDD 格式)

        返回:
            tuple: (调整后的起始日期, 调整后的结束日期)，如果解析失败则返回 (None, None)
        """
        try:
            if not start_date_str or not end_date_str:
                self.logger.warning(
                    f"调整日期: 无效的输入日期范围 ({start_date_str} - {end_date_str})。"
                )
                return None, None

            # 将字符串日期转换为 Timestamp 对象
            start_dt = pd.to_datetime(start_date_str, format="%Y%m%d")
            end_dt = pd.to_datetime(end_date_str, format="%Y%m%d")

            # 将起始日期调整为月初 (当月第一天)
            adj_start_dt = start_dt.replace(day=1)

            # 将结束日期调整为月末
            # 先移动到下个月的第一天，然后减去一天
            next_month = end_dt.replace(day=1) + pd.DateOffset(months=1)
            adj_end_dt = next_month - pd.DateOffset(days=1)

            if adj_start_dt > adj_end_dt:
                self.logger.warning(
                    f"原始日期 {start_date_str}-{end_date_str} 调整后开始日期 {adj_start_dt.strftime('%Y%m%d')} 晚于结束日期 {adj_end_dt.strftime('%Y%m%d')}。仅使用开始日期的月份。"
                )
                adj_end_dt = (
                    start_dt.replace(day=1) + pd.DateOffset(months=1)
                ) - pd.DateOffset(days=1)

            return adj_start_dt.strftime("%Y%m%d"), adj_end_dt.strftime("%Y%m%d")
        except ValueError as e:
            self.logger.error(f"调整日期时出错 ({start_date_str}, {end_date_str}): {e}")
            return None, None

    def _calculate_incremental_dates(
        self, latest_db_date_str: Optional[str]
    ) -> tuple[Optional[str], str]:
        """
        计算增量更新的原始开始和结束日期。
        返回 (有效开始日期字符串, 有效结束日期字符串)
        """
        # 根据当前时间判断使用哪一天作为结束日期
        current_time = pd.Timestamp.now()
        if current_time.hour >= 18:
            # 晚于18:00，使用当天日期作为end_date
            today_str = current_time.strftime("%Y%m%d")
            self.logger.info(
                f"当前时间晚于18:00，使用当前日期作为结束日期: {today_str}"
            )
        else:
            # 早于18:00，使用昨天日期作为end_date
            yesterday = current_time - pd.Timedelta(days=1)
            today_str = yesterday.strftime("%Y%m%d")
            self.logger.info(
                f"当前时间早于18:00，使用昨天日期作为结束日期: {today_str}"
            )

        # 使用类属性获取 default_start_date
        current_default_start = self.task_specific_config.get(
            "default_start_date", self.default_start_date
        )

        if latest_db_date_str:
            try:
                start_dt = pd.to_datetime(
                    latest_db_date_str, format="%Y%m%d"
                ) + pd.Timedelta(days=1)
                effective_start_date_str = start_dt.strftime("%Y%m%d")
            except ValueError:
                self.logger.warning(
                    f"无效的 latest_db_date_format: {latest_db_date_str}。回退到默认起始日期 {current_default_start}。"
                )
                effective_start_date_str = current_default_start
        else:
            self.logger.info(
                f"数据库中未找到任务 {self.task_id} 的最新日期。使用默认起始日期: {current_default_start}"
            )
            effective_start_date_str = current_default_start

        return effective_start_date_str, today_str

    async def get_index_codes(self) -> List[str]:
        """
        使用 'index_basic' API 获取所有唯一的Tushare指数代码。
        """
        self.logger.debug(
            f"任务 {self.name} 正在从 'index_basic' API 获取所有指数代码。"
        )  # 使用 self.name
        try:
            if not self.api:
                self.logger.error("TushareAPI 实例 (self.api) 不可用。")
                return []

            df_codes = await self.api.query(
                api_name="index_basic", fields=["ts_code"]
            )  # ts_code 是 index_basic 中的字段

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

    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        """
        生成批处理列表，每个批处理对应一个 index_code 和一个 N年 的日期范围。
        N 由 self.date_split_interval_years 定义。
        """
        # 从 kwargs 获取总的日期范围，这由 TushareTask.execute 根据模式确定
        # (全量模式用 default_start_date 到今天；增量模式用 DB最新日期+1 到今天)
        # smart_incremental_update 也会调用 execute 并传入调整后的 start/end_date
        overall_start_date_str = kwargs.get("start_date")
        overall_end_date_str = kwargs.get("end_date")

        self.logger.info(
            f"任务 {self.name}: 开始为总日期范围 {overall_start_date_str}-{overall_end_date_str} 生成批处理列表 (每批 {self.date_split_interval_years} 年)。"
        )

        if not overall_start_date_str or not overall_end_date_str:
            self.logger.error(
                f"任务 {self.name}: get_batch_list 需要总的 start_date 和 end_date。接收到 start='{overall_start_date_str}', end='{overall_end_date_str}'"
            )
            return []

        index_codes = await self.get_index_codes()
        if not index_codes:
            self.logger.warning(f"任务 {self.name}: 未找到指数代码以创建批处理。")
            return []

        batches = []
        date_format = "%Y%m%d"
        interval_years = self.date_split_interval_years

        try:
            overall_start_dt = pd.to_datetime(
                overall_start_date_str, format=date_format
            )
            overall_end_dt = pd.to_datetime(overall_end_date_str, format=date_format)
        except ValueError as e:
            self.logger.error(
                f"任务 {self.name}: 解析总日期范围 ({overall_start_date_str} - {overall_end_date_str}) 失败: {e}"
            )
            return []

        for index_code in index_codes:
            current_period_start_dt = overall_start_dt
            while current_period_start_dt <= overall_end_dt:
                # 计算当前周期的结束日期
                # current_period_end_dt = current_period_start_dt + pd.DateOffset(years=interval_years) - pd.DateOffset(days=1)
                # 更精确的方式，确保年份正确增加，并处理年底
                period_end_year = current_period_start_dt.year + interval_years - 1
                current_period_end_dt = pd.Timestamp(
                    year=period_end_year, month=12, day=31
                )

                # 如果计算出的周期结束日期超过了总的结束日期，则使用总的结束日期
                if current_period_end_dt > overall_end_dt:
                    current_period_end_dt = overall_end_dt

                batches.append(
                    {
                        "index_code": index_code,
                        "start_date": current_period_start_dt.strftime(date_format),
                        "end_date": current_period_end_dt.strftime(date_format),
                    }
                )

                # 设置下一个周期的开始日期
                # next_period_start_dt = current_period_end_dt + pd.DateOffset(days=1) # 这是旧的，如果用YearEnd
                next_period_start_dt = pd.Timestamp(
                    year=current_period_end_dt.year + 1, month=1, day=1
                )

                # 如果下一个周期的开始已经晚于总结束日期，则停止为当前 index_code 生成批次
                if (
                    next_period_start_dt > overall_end_dt
                    and current_period_end_dt >= overall_end_dt
                ):
                    break
                current_period_start_dt = next_period_start_dt

        self.logger.info(
            f"任务 {self.name}: 基于 {len(index_codes)} 个指数代码和 {interval_years}年/批 的拆分规则，总共创建了 {len(batches)} 个批处理。"
        )
        return batches

    async def smart_incremental_update(self, **kwargs: Any) -> Dict[str, Any]:
        """
        重写 smart_incremental_update 方法，为月度的 'index_weight' API 调整日期。
        从数据库中记录的最新日期的后一天开始获取数据，直到当前日期，
        并将此范围调整为完整的月份。

        特殊处理：
        - 如果当前日期小于月末，且调整后的开始日期和结束日期在同一个月内，则跳过更新
          这是为了避免在月中重复请求未完成月份的数据
        """
        self.logger.info(f"任务 {self.name} 正在启动智能增量更新 (月度调整)。")

        latest_db_date_str = await self.get_latest_date()
        effective_start_date_str, effective_end_date_str = (
            self._calculate_incremental_dates(latest_db_date_str)
        )

        if not effective_start_date_str:
            self.logger.warning(
                f"任务 {self.name}: 未能计算出有效的开始日期用于智能增量。"
            )
            return {
                "task_id": self.name,
                "status": "failed",
                "message": "未能计算有效开始日期",
            }

        start_dt_obj_check = pd.to_datetime(effective_start_date_str, format="%Y%m%d")
        end_dt_obj_check = pd.to_datetime(effective_end_date_str, format="%Y%m%d")

        if start_dt_obj_check > end_dt_obj_check:
            self.logger.info(
                f"任务 {self.name}: 有效开始日期 {effective_start_date_str} "
                f"晚于有效结束日期 {effective_end_date_str}。无需获取新数据。"
            )
            return {
                "task_id": self.name,
                "status": "no_new_data",
                "api": self.api_name,
                "message": f"无新日期范围 ({effective_start_date_str} - {effective_end_date_str})。数据库中最新: {latest_db_date_str}",
                "rows": 0,
                "failed_batches": 0,  # 确保返回 rows 和 failed_batches
            }

        # 重新启用: 调整日期为月初和月末
        adj_start_date_str, adj_end_date_str = self._adjust_dates_for_monthly_api(
            effective_start_date_str, effective_end_date_str
        )

        if not adj_start_date_str or not adj_end_date_str:
            self.logger.warning(f"任务 {self.name}: 调整月度日期失败，将使用原始日期。")
            adj_start_date_str, adj_end_date_str = (
                effective_start_date_str,
                effective_end_date_str,
            )

        # 新增: 检查今天是否小于月末，且调整后的日期是否在同一个月内
        today = pd.Timestamp.now()
        month_end = (today.replace(day=1) + pd.DateOffset(months=1)) - pd.DateOffset(
            days=1
        )

        if today.day < month_end.day:  # 今天小于月末
            adj_start_dt = pd.to_datetime(adj_start_date_str, format="%Y%m%d")
            adj_end_dt = pd.to_datetime(adj_end_date_str, format="%Y%m%d")

            # 检查调整后的开始日期和结束日期是否在同一个月
            same_month = (adj_start_dt.year == adj_end_dt.year) and (
                adj_start_dt.month == adj_end_dt.month
            )

            # 检查结束日期是否是当前月
            is_current_month = (adj_end_dt.year == today.year) and (
                adj_end_dt.month == today.month
            )

            if same_month and is_current_month:
                self.logger.info(
                    f"任务 {self.name}: 当前日期 {today.strftime('%Y%m%d')} 小于月末 {month_end.strftime('%Y%m%d')}，"
                    f"且调整后的日期范围 ({adj_start_date_str} - {adj_end_date_str}) 在当前同一月份内。"
                    f"将跳过更新，等待月末数据完整后再更新。"
                )
                return {
                    "task_id": self.name,
                    "status": "skipped",
                    "api": self.api_name,
                    "message": f"当前为月中 ({today.strftime('%Y%m%d')})，跳过同月份范围 ({adj_start_date_str} - {adj_end_date_str}) 的更新。",
                    "rows": 0,
                    "failed_batches": 0,
                }

        self.logger.info(
            f"任务 {self.name}: 智能增量更新将使用调整后的日期范围: "
            f"{adj_start_date_str} 到 {adj_end_date_str}"
        )

        # 使用调整后的日期调用基类的 execute 方法
        return await super().execute(
            start_date=adj_start_date_str, end_date=adj_end_date_str, **kwargs
        )

    # 基类 TushareTask (或 BaseTask) 中继承的 `execute` 方法期望执行以下操作：
    # 1. 调用 `await self.get_batch_list(**kwargs)` (传递其自身的kwargs，如 start_date, end_date, mode)。
    # 2. 对于列表中的每个 `batch_params` (例如 `{'index_code': ...}`):
    #    a. 通过合并 batch_params 和来自 execute 参数的日期来构造最终的 API 参数：
    #       `final_api_params = {**batch_params, 'start_date': start_date_from_execute, 'end_date': end_date_from_execute}`。
    #    b. 调用 `await self.query_api(params=final_api_params)` (该方法使用 self.api_name, self.fields, self.page_size)。
    #    c. 调用 `await self.process_and_save_data(data_df, batch_params=batch_params)` (该方法处理转换、验证、保存)。
    # 此任务依赖于父类 execute 方法中的此类行为。
    # `query_api` 和 `process_and_save_data` 是基类方法的假设名称。
