#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
基金规模数据 (fund_share) 更新任务
获取基金规模数据，包含上海和深圳ETF基金。
继承自 TushareTask，按日期增量更新。

特别说明：
- Tushare fund_share 在部分时段存在 SZ 日期错位 1 天的问题。
- 本任务会在执行前自动探测问题是否存在。
- 如问题未修复，入库前自动修正 SZ trade_date，并按原请求日期范围回裁剪。
"""

from bisect import bisect_left, bisect_right
from typing import Any, Dict, List, Optional

import pandas as pd

# 导入基础类和装饰器
from ...sources.tushare.tushare_task import TushareTask
from alphahome.common.task_system.task_decorator import task_register
from ...tools.calendar import get_last_trade_day, get_next_trade_day, get_trade_cal

# 导入批处理工具
from ...sources.tushare.batch_utils import generate_trade_day_batches


@task_register()
class TushareFundShareTask(TushareTask):
    """获取基金规模数据 (含ETF)"""

    # 1. 核心属性
    domain = "fund"  # 业务域标识
    name = "tushare_fund_share"
    description = "获取基金规模数据 (含ETF)"
    table_name = "fund_share"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    default_start_date = "20000101"  # 根据实际情况调整

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5
    default_page_size = 2000

    # 2. TushareTask 特有属性
    api_name = "fund_share"
    fields = ["ts_code", "trade_date", "fd_share"]

    # 3. 列名映射 (无需映射)
    column_mapping = {}

    # 4. 数据类型转换
    transformations = {
        "fd_share": lambda x: pd.to_numeric(x, errors="coerce")
        # trade_date 由基类 process_data 中的 _process_date_column 处理
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "fd_share": {"type": "NUMERIC(20,2)"},  # 单位：万份
        # update_time 会自动添加
        # 主键 ("ts_code", "trade_date") 索引由基类根据 primary_keys 自动处理
    }

    # 6. 数据验证规则
    validations = [
        (lambda df: df['ts_code'].notna(), "基金代码不能为空"),
        (lambda df: df['trade_date'].notna(), "交易日期不能为空"),
        (lambda df: df['fd_share'] >= 0, "基金份额不能为负数"),
    ]

    # 7. 自定义索引 (主键已包含，无需额外添加)
    indexes = [
        {
            "name": "idx_tushare_fund_share_update_time",
            "columns": "update_time",
        }  # 新增 update_time 索引
    ]

    # 7. 分批配置 (根据接口特性和数据量调整)
    batch_trade_days_single_code = 360  # 单基金查询时，每个批次的交易日数量 (约1.5年)
    batch_trade_days_all_codes = 5  # 全市场查询时，每个批次的交易日数量 (1周)

    # 8. SZ 日期错位探测/修正配置
    bug_probe_lookback_days = 15
    bug_probe_mode_ratio_threshold = 0.75
    bug_probe_page_limit = 2000

    def __init__(self, db_connection, api_token=None, api=None, **kwargs):
        super().__init__(db_connection, api_token=api_token, api=api, **kwargs)
        self._fd_share_bug_probe_result: Optional[Dict[str, Any]] = None

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表 (使用交易日批次工具)。
        支持按日期范围和可选的 ts_code 进行批处理。
        """
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        ts_code = kwargs.get("ts_code")  # 可选的基金代码

        # 检查必要的日期参数
        if not start_date:
            # 如果未提供 start_date，尝试从数据库获取最新日期 + 1天作为开始日期
            latest_db_date = await self.get_latest_date()
            if latest_db_date:
                start_date = (latest_db_date + pd.Timedelta(days=1)).strftime("%Y%m%d")
            else:
                start_date = self.default_start_date
            self.logger.info(f"未提供 start_date，使用: {start_date}")

        if not end_date:
            end_date = pd.Timestamp.now().strftime("%Y%m%d")  # 默认到今天
            self.logger.info(f"未提供 end_date，使用: {end_date}")

        # 如果开始日期晚于结束日期，说明数据已是最新，无需更新
        if pd.to_datetime(start_date) > pd.to_datetime(end_date):
            self.logger.info(
                f"起始日期 ({start_date}) 晚于结束日期 ({end_date})，无需执行任务。"
            )
            return []

        self.logger.info(
            f"任务 {self.name}: 生成批处理列表，范围: {start_date} 到 {end_date}, 代码: {ts_code if ts_code else '所有'}"
        )

        try:
            batch_list = await generate_trade_day_batches(
                start_date=start_date,
                end_date=end_date,
                # 根据是否提供 ts_code 选择不同的批次大小
                batch_size=(
                    self.batch_trade_days_single_code
                    if ts_code
                    else self.batch_trade_days_all_codes
                ),
                # 将 ts_code 传递给批处理函数，以便在参数中包含它
                ts_code=ts_code,
                logger=self.logger,
                # 注意：fund_share API 可能需要 market 参数，但 generate_trade_day_batches 目前不直接支持
                # 如果需要按 market 分批，需要自定义 get_batch_list 逻辑或扩展工具函数
            )
            # 批处理函数返回的字典已包含 start_date 和 end_date
            # 如果提供了 ts_code，它也会包含在字典中，可以直接用于 API 调用
            return batch_list
        except Exception as e:
            self.logger.error(
                f"任务 {self.name}: 生成交易日批次时出错: {e}", exc_info=True
            )
            return []

    async def _pre_execute(self, stop_event=None, **kwargs):
        """
        执行前先探测 fund_share SZ 日期错位是否仍然存在。
        """
        await super()._pre_execute(stop_event=stop_event, **kwargs)
        await self._ensure_fd_share_bug_probe()

    def _normalize_probe_df(self, df: Optional[pd.DataFrame]) -> pd.DataFrame:
        """
        统一探测阶段 DataFrame 的格式。
        """
        if df is None or df.empty:
            return pd.DataFrame(columns=["ts_code", "trade_date", "fd_share", "exchange"])

        out = df.copy()
        required_cols = {"ts_code", "trade_date", "fd_share"}
        if not required_cols.issubset(out.columns):
            missing = sorted(required_cols - set(out.columns))
            self.logger.warning(f"探测数据缺少字段: {missing}")
            return pd.DataFrame(columns=["ts_code", "trade_date", "fd_share", "exchange"])

        out["ts_code"] = out["ts_code"].astype(str)
        out["trade_date"] = pd.to_datetime(out["trade_date"], errors="coerce")
        out["fd_share"] = pd.to_numeric(out["fd_share"], errors="coerce").round(6)
        out["exchange"] = out["ts_code"].str[-2:]
        out = out.dropna(subset=["ts_code", "trade_date", "fd_share"])
        return out[["ts_code", "trade_date", "fd_share", "exchange"]]

    def _build_exchange_mode_summary(self, df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        """
        计算各交易所“每个代码最新日期”的众数日期，用于判断是否存在系统性错位。
        """
        if df.empty:
            return {}

        per_code_max = (
            df.groupby(["exchange", "ts_code"], as_index=False)["trade_date"]
            .max()
        )
        summary: Dict[str, Dict[str, Any]] = {}

        for exchange, group in per_code_max.groupby("exchange"):
            counts = group["trade_date"].value_counts(dropna=True)
            if counts.empty:
                continue

            mode_date = pd.Timestamp(counts.index[0]).normalize()
            mode_ratio = float(counts.iloc[0] / len(group))

            summary[exchange] = {
                "mode_date": mode_date,
                "mode_ratio": mode_ratio,
                "code_count": int(len(group)),
            }

        return summary

    async def _infer_sz_shift_days_from_summary(self, summary: Dict[str, Dict[str, Any]]) -> int:
        """
        根据 SH 与 SZ 的众数最新日期在“交易日序列”中的相对关系判断修正方向：
        - SZ 众数日期的前一个交易日 == SH 众数日期 -> SZ 需 -1 个交易日修正
        - SZ 众数日期的后一个交易日 == SH 众数日期 -> SZ 需 +1 个交易日修正
        """
        sz = summary.get("SZ")
        sh = summary.get("SH")
        if not sz or not sh:
            return 0

        if (
            sz["mode_ratio"] < self.bug_probe_mode_ratio_threshold
            or sh["mode_ratio"] < self.bug_probe_mode_ratio_threshold
        ):
            return 0

        sz_mode = pd.Timestamp(sz["mode_date"]).normalize()
        sh_mode = pd.Timestamp(sh["mode_date"]).normalize()

        if sz_mode == sh_mode:
            return 0

        sz_mode_str = sz_mode.strftime("%Y%m%d")
        sz_prev_trade = await get_last_trade_day(sz_mode_str, n=1, exchange="SZSE")
        sz_next_trade = await get_next_trade_day(sz_mode_str, n=1, exchange="SZSE")

        if sz_prev_trade and pd.to_datetime(sz_prev_trade).normalize() == sh_mode:
            return -1
        if sz_next_trade and pd.to_datetime(sz_next_trade).normalize() == sh_mode:
            return 1

        # 回退：若交易日历不足，退化为自然日差比较（仅用于兜底）
        delta_days = int((sz_mode - sh_mode).days)
        if delta_days == 1:
            return -1
        if delta_days == -1:
            return 1
        return 0

    def _serialize_mode_summary_for_log(self, summary: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        将时间戳对象转换为可读日志结构。
        """
        out: Dict[str, Any] = {}
        for exchange, item in summary.items():
            out[exchange] = {
                "mode_date": item["mode_date"].strftime("%Y-%m-%d"),
                "mode_ratio": round(float(item["mode_ratio"]), 4),
                "code_count": int(item["code_count"]),
            }
        return out

    async def _probe_fd_share_bug(self) -> Dict[str, Any]:
        """
        执行一次实时探测，验证 SZ 日期错位是否仍存在，并验证 start/end 机制。
        """
        probe_end = pd.Timestamp.now().normalize()
        probe_start = probe_end - pd.Timedelta(days=self.bug_probe_lookback_days)

        start_date = probe_start.strftime("%Y%m%d")
        end_date = probe_end.strftime("%Y%m%d")

        common_params = {
            "api_name": self.api_name,
            "fields": self.fields,
            "limit": self.bug_probe_page_limit,
        }

        start_only_raw = await self.api.query(
            start_date=start_date,
            **common_params,
        )
        with_end_raw = await self.api.query(
            start_date=start_date,
            end_date=end_date,
            **common_params,
        )

        start_only = self._normalize_probe_df(start_only_raw)
        with_end = self._normalize_probe_df(with_end_raw)

        if start_only.empty and with_end.empty:
            return {
                "probe_ok": False,
                "error": "start_only 与 with_end 均未获取到数据",
            }

        start_only_summary = self._build_exchange_mode_summary(start_only)
        with_end_summary = self._build_exchange_mode_summary(with_end)

        shift_start_only = await self._infer_sz_shift_days_from_summary(start_only_summary)
        shift_with_end = await self._infer_sz_shift_days_from_summary(with_end_summary)

        final_shift_days = shift_start_only if shift_start_only != 0 else shift_with_end
        bug_detected = final_shift_days != 0

        # 若 start_only 与 with_end 得出同样结论，说明仅比较是否给 end_date 并不能识别问题
        start_end_comparison_not_revealing = (
            shift_start_only != 0 and shift_start_only == shift_with_end
        )

        return {
            "probe_ok": True,
            "start_date": start_date,
            "end_date": end_date,
            "rows_start_only": int(len(start_only)),
            "rows_with_end": int(len(with_end)),
            "shift_start_only": int(shift_start_only),
            "shift_with_end": int(shift_with_end),
            "shift_days": int(final_shift_days),
            "bug_detected": bool(bug_detected),
            "start_end_comparison_not_revealing": bool(start_end_comparison_not_revealing),
            "start_only_summary": self._serialize_mode_summary_for_log(start_only_summary),
            "with_end_summary": self._serialize_mode_summary_for_log(with_end_summary),
        }

    async def _ensure_fd_share_bug_probe(self):
        """
        保证当前任务实例完成一次 bug 探测；若探测失败则中止任务，避免错误入库。
        """
        if self._fd_share_bug_probe_result is not None:
            return

        probe = await self._probe_fd_share_bug()
        self._fd_share_bug_probe_result = probe

        if not probe.get("probe_ok", False):
            raise RuntimeError(f"fund_share bug 探测失败: {probe.get('error', 'unknown error')}")

        if probe.get("bug_detected", False):
            self.logger.warning(
                "检测到 fund_share SZ 日期错位仍存在。"
                f"shift_days={probe.get('shift_days')}, "
                f"start_only={probe.get('start_only_summary')}, "
                f"with_end={probe.get('with_end_summary')}, "
                f"start_end_comparison_not_revealing={probe.get('start_end_comparison_not_revealing')}"
            )
        else:
            self.logger.info(
                "fund_share SZ 日期错位探测结果：未发现错位。"
                f"start_only={probe.get('start_only_summary')}, "
                f"with_end={probe.get('with_end_summary')}"
            )

    @staticmethod
    def _shift_ymd_by_natural_days(date_str: str, days: int) -> str:
        return (pd.to_datetime(str(date_str)) + pd.Timedelta(days=days)).strftime("%Y%m%d")

    async def _expand_query_range_for_correction(self, params: Dict[str, Any], shift_days: int) -> Dict[str, Any]:
        """
        根据错位方向扩展查询边界，避免纠偏后丢失边界数据：
        - shift_days < 0（SZ原始日期偏未来 1 个交易日）: end_date 扩展到“下一个交易日”
        - shift_days > 0（SZ原始日期偏过去 1 个交易日）: start_date 扩展到“上一个交易日”
        """
        if shift_days == 0:
            return params

        expanded = params.copy()
        if shift_days < 0 and expanded.get("end_date"):
            next_trade_day = await get_next_trade_day(
                expanded["end_date"], n=1, exchange="SZSE"
            )
            expanded["end_date"] = (
                next_trade_day
                if next_trade_day
                else self._shift_ymd_by_natural_days(expanded["end_date"], 3)
            )
        elif shift_days > 0 and expanded.get("start_date"):
            last_trade_day = await get_last_trade_day(
                expanded["start_date"], n=1, exchange="SZSE"
            )
            expanded["start_date"] = (
                last_trade_day
                if last_trade_day
                else self._shift_ymd_by_natural_days(expanded["start_date"], -3)
            )

        return expanded

    async def _build_sz_trade_day_shift_map(
        self,
        source_dates: pd.Series,
        shift_days: int,
    ) -> Dict[pd.Timestamp, Optional[pd.Timestamp]]:
        """
        构建 SZ 日期到目标日期的映射（按交易日而非自然日）。
        """
        if source_dates.empty or shift_days == 0:
            return {}

        normalized_source = (
            pd.to_datetime(source_dates, errors="coerce")
            .dropna()
            .dt.normalize()
        )
        if normalized_source.empty:
            return {}

        unique_dates = sorted(normalized_source.unique())
        min_date = pd.Timestamp(unique_dates[0]).normalize()
        max_date = pd.Timestamp(unique_dates[-1]).normalize()

        query_start = (min_date - pd.Timedelta(days=40)).strftime("%Y%m%d")
        query_end = (max_date + pd.Timedelta(days=40)).strftime("%Y%m%d")

        cal_df = await get_trade_cal(
            start_date=query_start,
            end_date=query_end,
            exchange="SZSE",
        )
        if cal_df.empty:
            raise RuntimeError("SZSE 交易日历为空，无法执行交易日纠偏")

        cal_work = cal_df.copy()
        cal_work["cal_date"] = pd.to_datetime(
            cal_work.get("cal_date"), errors="coerce"
        ).dt.normalize()
        cal_work["is_open"] = pd.to_numeric(
            cal_work.get("is_open"), errors="coerce"
        )
        open_days = sorted(
            cal_work.loc[cal_work["is_open"] == 1, "cal_date"]
            .dropna()
            .unique()
        )
        if not open_days:
            raise RuntimeError("SZSE 交易日历中无开市日，无法执行交易日纠偏")

        open_day_values = [pd.Timestamp(d).value for d in open_days]
        mapping: Dict[pd.Timestamp, Optional[pd.Timestamp]] = {}

        for raw_date in unique_dates:
            raw_ts = pd.Timestamp(raw_date).normalize()
            raw_val = raw_ts.value

            if shift_days < 0:
                idx = bisect_left(open_day_values, raw_val) - 1
            else:
                idx = bisect_right(open_day_values, raw_val)

            mapped = pd.Timestamp(open_days[idx]).normalize() if 0 <= idx < len(open_days) else None
            mapping[raw_ts] = mapped

        return mapping

    async def _fix_sz_trade_date(self, data: pd.DataFrame, shift_days: int) -> pd.DataFrame:
        """
        对 SZ 代码执行“按交易日”的日期纠偏。
        """
        if data.empty or shift_days == 0:
            return data
        if "ts_code" not in data.columns or "trade_date" not in data.columns:
            return data

        out = data.copy()
        out["ts_code"] = out["ts_code"].astype(str)
        out["trade_date"] = pd.to_datetime(out["trade_date"], errors="coerce")
        sz_mask = out["ts_code"].str.endswith(".SZ", na=False) & out["trade_date"].notna()

        if sz_mask.any():
            sz_dates = out.loc[sz_mask, "trade_date"].dt.normalize()
            date_map = await self._build_sz_trade_day_shift_map(sz_dates, shift_days)
            mapped_dates = sz_dates.map(date_map)

            valid_mask = mapped_dates.notna()
            if valid_mask.any():
                out.loc[mapped_dates.index[valid_mask], "trade_date"] = pd.to_datetime(
                    mapped_dates[valid_mask].values
                )

            unresolved_count = int((~valid_mask).sum())
            if unresolved_count > 0:
                self.logger.warning(
                    f"SZ 交易日纠偏存在 {unresolved_count} 行未能映射，保留原 trade_date。"
                )

        return out

    def _clip_to_original_range(self, data: pd.DataFrame, original_params: Dict[str, Any]) -> pd.DataFrame:
        """
        使用原始请求区间裁剪数据（在边界扩展和纠偏之后执行）。
        """
        if data.empty or "trade_date" not in data.columns:
            return data

        out = data.copy()
        out["trade_date"] = pd.to_datetime(out["trade_date"], errors="coerce")
        before_rows = len(out)

        start_date = original_params.get("start_date")
        end_date = original_params.get("end_date")

        if start_date:
            out = out[out["trade_date"] >= pd.to_datetime(str(start_date))]
        if end_date:
            out = out[out["trade_date"] <= pd.to_datetime(str(end_date))]

        dropped = before_rows - len(out)
        if dropped > 0:
            self.logger.debug(f"纠偏后按原始范围裁剪了 {dropped} 行数据")

        return out

    async def fetch_batch(self, params: Dict[str, Any], stop_event=None) -> Optional[pd.DataFrame]:
        """
        重写批次获取：
        1) 先探测 bug 状态
        2) 如未修复则扩展查询边界
        3) 获取数据后修正 SZ 日期
        4) 再按原始日期范围回裁剪
        """
        await self._ensure_fd_share_bug_probe()

        probe = self._fd_share_bug_probe_result or {}
        shift_days = int(probe.get("shift_days", 0))
        original_params = params.copy()
        query_params = await self._expand_query_range_for_correction(params, shift_days)

        if query_params != original_params:
            self.logger.debug(
                f"fund_share 查询区间扩展: original={original_params}, expanded={query_params}"
            )

        data = await super().fetch_batch(query_params, stop_event=stop_event)
        if data is None or data.empty:
            return data

        if shift_days != 0:
            data = await self._fix_sz_trade_date(data, shift_days)
            data = self._clip_to_original_range(data, original_params)

            valid_pk_cols = [pk for pk in self.primary_keys if pk in data.columns]
            if valid_pk_cols:
                before = len(data)
                data = data.drop_duplicates(subset=valid_pk_cols, keep="last").copy()
                removed = before - len(data)
                if removed > 0:
                    self.logger.debug(
                        f"SZ 日期纠偏后主键去重移除了 {removed} 行 (keys={valid_pk_cols})"
                    )

        return data

