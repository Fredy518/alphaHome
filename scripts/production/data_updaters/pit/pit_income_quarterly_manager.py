#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PIT利润表管理器
==============

负责pit_income_quarterly表的历史全量回填和增量更新

功能特点:
1. 从tushare.fina_income同步数据到PIT表
2. 支持历史全量回填和增量更新
3. 自动处理数据转换和清洗
4. 提供数据验证和状态检查

Author: AI Assistant
Date: 2025-08-11
"""

import sys
import os
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import pandas as pd
import concurrent.futures
from functools import lru_cache

# 添加项目路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from .base.pit_table_manager import PITTableManager
    from .base.pit_config import PITConfig
except ImportError:
    from base.pit_table_manager import PITTableManager
    from base.pit_config import PITConfig

from typing import Dict, Any

class PITIncomeQuarterlyManager(PITTableManager):
    """PIT利润表管理器"""

    def __init__(self):
        super().__init__('pit_income_quarterly')

        # 利润表特定配置
        self.tushare_table = self.table_config['tushare_table']
        self.key_fields = self.table_config['key_fields']
        self.data_fields = self.table_config['data_fields']

        # 性能优化：缓存机制
        self._historical_cache = {}  # 历史数据缓存
        self._bs_cache = {}  # 资产负债表缓存
        self._batch_report_cache = {}  # 批次内report数据缓存

        # 性能优化：预编译SQL模板
        self._compiled_queries = {
            'historical_lookup': self._prepare_historical_query_template(),
            'bs_snapshot': self._prepare_bs_snapshot_query_template()
        }

    def full_backfill(self,
                     start_date: str = None,
                     end_date: str = None,
                     batch_size: int = None) -> Dict[str, Any]:
        """
        历史全量回填

        Args:
            start_date: 开始日期 (ann_date)
            end_date: 结束日期 (ann_date)
            batch_size: 批次大小

        Returns:
            执行结果统计
        """
        self.logger.info("开始PIT利润表历史全量回填")

        # 设置默认参数
        if start_date is None or end_date is None:
            start_date, end_date = PITConfig.get_backfill_date_range(start_date, end_date)

        if batch_size is None:
            batch_size = self.batch_size

        self.logger.info(f"回填日期范围: {start_date} ~ {end_date}")
        self.logger.info(f"每批股票数: {batch_size}")

        try:
            # 0. 确保目标表存在
            self._ensure_table_exists()
            # 1. 获取tushare数据
            tushare_data = self._fetch_tushare_data(start_date, end_date)

            if tushare_data.empty:
                self.logger.warning("未找到需要回填的数据")
                return {'backfilled_records': 0, 'message': '无数据需要回填'}

            self.logger.info(f"从tushare获取到 {len(tushare_data)} 条数据")

            # 2. 数据预处理
            processed_data = self._preprocess_data(tushare_data)

            # 2.9 确保唯一键包含 data_source
            self._ensure_income_unique_keys()
            # 2.95 确保索引
            self.ensure_indexes()
            # 3. 批量插入PIT表
            result = self._batch_upsert_to_pit(processed_data, batch_size)

            # 确保唯一键包含 data_source（迁移一次，幂等）



            self.logger.info(f"历史回填完成: 新增 {result['inserted']}, 更新 {result['updated']}")

            return {
                'backfilled_records': result['inserted'] + result['updated'],
                'inserted_records': result['inserted'],
                'updated_records': result['updated'],
                'error_records': result['errors'],
                'message': f'成功回填 {result["inserted"] + result["updated"]} 条记录'
            }

        except Exception as e:
            self.logger.error(f"历史回填失败: {e}")
            return {
                'backfilled_records': 0,
                'error': str(e),
                'message': '历史回填失败'
            }

    def incremental_update(self,
                          days: int = None,
                          batch_size: int = None) -> Dict[str, Any]:
        """
        增量更新

        Args:
            days: 增量更新天数
            batch_size: 批次大小

        Returns:
            执行结果统计
        """
        self.logger.info("开始PIT利润表增量更新")

        # 设置默认参数
        if days is None:
            days = PITConfig.DEFAULT_DATE_RANGES['incremental_days']

        if batch_size is None:
            batch_size = self.batch_size

        # 计算增量更新日期范围
        start_date, end_date = PITConfig.get_incremental_date_range(days)

        self.logger.info(f"增量更新日期范围: {start_date} ~ {end_date}")
        self.logger.info(f"每批股票数: {batch_size}")

        try:
            # 0. 确保目标表存在
            self._ensure_table_exists()
            # 1. 获取增量数据
            incremental_data = self._fetch_tushare_data(start_date, end_date)

            if incremental_data.empty:
                self.logger.info("未找到需要更新的数据")
                return {'updated_records': 0, 'message': '无数据需要更新'}

            self.logger.info(f"从tushare获取到 {len(incremental_data)} 条增量数据")

            # 2. 数据预处理
            processed_data = self._preprocess_data(incremental_data)

            # 2.5 确保唯一键包含 data_source（幂等迁移）
            self._ensure_income_unique_keys()
            # 2.95 确保索引
            self.ensure_indexes()
            # 3. 批量更新PIT表
            result = self._batch_upsert_to_pit(processed_data, batch_size)

            self.logger.info(f"增量更新完成: 新增 {result['inserted']}, 更新 {result['updated']}")

            return {
                'updated_records': result['inserted'] + result['updated'],
                'inserted_records': result['inserted'],
                'updated_records': result['updated'],
                'error_records': result['errors'],
                'message': f'成功更新 {result["inserted"] + result["updated"]} 条记录'
            }

        except Exception as e:
            self.logger.error(f"增量更新失败: {e}")
            return {
                'updated_records': 0,
                'error': str(e),
                'message': '增量更新失败'
            }
    def single_backfill(self,
                         ts_code: str,
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None,
                         batch_size: Optional[int] = None,
                         do_validate: bool = True) -> Dict[str, Any]:
        """个股历史回填（可选验证）。
        - 仅对指定 ts_code 进行历史区间内的数据获取、预处理与入库
        - 不影响既有全量/增量逻辑
        """
        if not ts_code:
            return {'backfilled_records': 0, 'error': '缺少 ts_code', 'message': '必须提供 --ts-code 才能执行单股回填'}

        self.logger.info(f"开始个股历史回填: ts_code={ts_code}")

        # 默认参数
        if start_date is None or end_date is None:
            start_date, end_date = PITConfig.get_backfill_date_range(start_date, end_date)
        if batch_size is None:
            batch_size = self.batch_size

        self.logger.info(f"回填日期范围: {start_date} ~ {end_date}")

        try:
            # 0. 确保目标表存在
            self._ensure_table_exists()
            # 1. 获取数据（带 ts_code 过滤）
            raw = self._fetch_tushare_data(start_date, end_date, ts_code=ts_code)
            if raw is None or raw.empty:
                self.logger.warning("未找到需要回填的数据（单股）")
                return {'backfilled_records': 0, 'message': '无数据需要回填', 'ts_code': ts_code}

            self.logger.info(f"从tushare获取到 {len(raw)} 条数据（单股）")
            # 2. 数据预处理
            processed = self._preprocess_data(raw)

            # 2.9 确保唯一键＆索引
            self._ensure_income_unique_keys()
            self.ensure_indexes()

            # 3. 批量写入
            result = self._batch_upsert_to_pit(processed, batch_size)
            out = {
                'ts_code': ts_code,
                'backfilled_records': result['inserted'] + result['updated'],
                'inserted_records': result['inserted'],
                'updated_records': result['updated'],
                'error_records': result['errors'],
                'message': f"单股回填完成，共 {result['inserted'] + result['updated']} 条（ins={result['inserted']}, upd={result['updated']}）"
            }

            # 4. 可选：针对该股做轻量验证（仅报告摘要，辅助核对逻辑正确性）
            if do_validate:
                try:
                    out['validation'] = self._validate_single_stock(ts_code, start_date, end_date)
                except Exception as ve:
                    self.logger.warning(f"单股回填验证失败（忽略不中断）: {ve}")
            return out
        except Exception as e:
            self.logger.error(f"个股历史回填失败: {e}")
            return {
                'ts_code': ts_code,
                'backfilled_records': 0,
                'error': str(e),
                'message': '个股历史回填失败'
            }

    def _validate_single_stock(self, ts_code: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """对指定股票在给定日期范围内进行轻量验证，辅助核对处理逻辑正确性。
        验证内容：
        - 按来源统计行数（report/express/forecast）
        - 核心字段是否全部为空的记录数量（应尽量为0；forecast 线索行可能允许）
        - ann_date/ts_code/end_date 关键字段完整性
        注：DB 层唯一键已约束重复键，本处不再重复检测。
        """
        pit_cols = self._get_table_columns(PITConfig.PIT_SCHEMA, self.table_name)
        core_fields = [c for c in self.data_fields if c in pit_cols]
        select_cols = ['ts_code', 'end_date', 'ann_date', 'data_source'] + ([
            'conversion_status'] if 'conversion_status' in pit_cols else []) + core_fields
        sql = (
            f"SELECT {', '.join(select_cols)} FROM {PITConfig.PIT_SCHEMA}.{self.table_name} "
            f"WHERE ts_code=%s AND ann_date >= %s AND ann_date <= %s "
            f"ORDER BY ann_date, data_source, end_date"
        )
        df = self.context.query_dataframe(sql, (ts_code, start_date, end_date))
        if df is None or df.empty:
            return {'ts_code': ts_code, 'range': [start_date, end_date], 'rows': 0}
        work = df.copy()
        work['ann_date'] = pd.to_datetime(work['ann_date']).dt.date
        # 统计
        by_src = work['data_source'].value_counts(dropna=False).to_dict() if 'data_source' in work.columns else {}
        # 核心字段全空统计
        if core_fields:
            all_null = work[core_fields].isna().all(axis=1)
            null_count = int(all_null.sum())
        else:
            null_count = 0
        issues = 0
        if null_count > 0:
            issues += 1
        key_null = int(work[['ts_code','end_date','ann_date']].isna().any(axis=1).sum())
        if key_null > 0:
            issues += 1
        result = {
            'ts_code': ts_code,
            'range': [start_date, end_date],
            'rows': int(len(work)),
            'by_source': by_src,
            'all_core_null_rows': null_count,
            'key_field_null_rows': key_null,
            'status': 'passed' if issues == 0 else 'warning'
        }
        return result


    def _fetch_tushare_data(self, start_date: str, end_date: str, ts_code: Optional[str] = None) -> pd.DataFrame:
        """从tushare获取利润表/快报/预告三源数据并统一为标准列集合
        注意：不再依赖report_type映射，而是分别从三张原始表取数并标注data_source
        额外：为支持"累计转单季"季度化，向前扩展取数起点，以确保具备上一季度(Qn-1)累计值。
        例如：历史回填从 2000-01-01 开始，则实际取数从 1999-01-01 开始。
        """
        # 向前扩展起始日期（至少向前一年），以确保获取到上一季度累计值
        try:
            sd = pd.to_datetime(start_date).date() if start_date else None
            if sd is not None:
                from dateutil.relativedelta import relativedelta
                extended_sd = (sd - relativedelta(years=1))
                start_date_ext = extended_sd.strftime('%Y-%m-%d')
                if start_date_ext != start_date:
                    self.logger.info(f"季度化前置扩展取数起点: {start_date} -> {start_date_ext}")
            else:
                start_date_ext = start_date
        except Exception:
            start_date_ext = start_date

        # 如果是历史全量回填，才进行扩展；增量更新按原窗口
        # 通过调用方传入的 start_date 是否等于默认 backfill_start 进行启发式判断无法在此方法内可靠实现
        # 因此统一扩展，但最终在预处理过滤不在原窗口的记录（is_extended）

        # 1) 正式财报（Report）
        df_report = self._fetch_income_report(start_date_ext, end_date, ts_code=ts_code)
        # 2) 业绩快报（Express）
        df_express = self._fetch_income_express(start_date_ext, end_date, ts_code=ts_code)
        # 3) 业绩预告（Forecast）
        df_forecast = self._fetch_income_forecast(start_date_ext, end_date, ts_code=ts_code)

        # 标记扩展行（仅用于内部季度化计算，最终会在预处理阶段过滤掉不在原始时间窗口内的数据）
        try:
            boundary = pd.to_datetime(start_date).date() if start_date else None
        except Exception:
            boundary = None
        for df in [df_report, df_express, df_forecast]:
            if df is not None and not df.empty:
                if 'is_extended' not in df.columns:
                    if boundary is None:
                        df['is_extended'] = False
                    else:
                        df['is_extended'] = pd.to_datetime(df['ann_date']).dt.date < boundary

        frames = [df for df in [df_report, df_express, df_forecast] if df is not None and not df.empty]
        if not frames:
            return pd.DataFrame()
        out_df = pd.concat(frames, ignore_index=True)
        return out_df

    def _fetch_income_report(self, start_date: str, end_date: str, ts_code: Optional[str] = None) -> pd.DataFrame:
        """从 tushare.fina_income 获取正式财报（Report），并标准化列名与data_source"""
        # 动态扩展：若源表包含 total_profit / n_income 列，则一并取数，便于后续持久化与质量评估
        src_cols = self._get_table_columns(PITConfig.TUSHARE_SCHEMA, 'fina_income')
        extra_take = []
        if 'total_profit' in src_cols:
            extra_take.append('total_profit')
        if 'n_income' in src_cols:
            extra_take.append('n_income')
        fields = self.key_fields + self.data_fields + extra_take
        field_list = ', '.join(fields)
        base_sql = f"""
        SELECT {field_list}
        FROM {PITConfig.TUSHARE_SCHEMA}.fina_income
        WHERE ann_date >= %s AND ann_date <= %s
          AND ts_code IS NOT NULL AND end_date IS NOT NULL
        """
        params = [start_date, end_date]
        if ts_code:
            base_sql += " AND ts_code = %s"
            params.append(ts_code)
        base_sql += " ORDER BY ts_code, end_date, ann_date"
        df = self.context.query_dataframe(base_sql, tuple(params))
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.copy()
        # 标注来源与转换状态：report 原始值
        if 'conversion_status' not in df.columns:
            df['conversion_status'] = None
        df['conversion_status'] = df['conversion_status'].fillna('RPT_ORIG')
        df['data_source'] = 'report'
        return df

    def _fetch_income_express(self, start_date: str, end_date: str, ts_code: Optional[str] = None) -> pd.DataFrame:
        """从 tushare.fina_express 获取业绩快报（Express），自适应列名并标准化输出。
        策略：列名自动探测 + 显式投影；优先选择最接近 fina_income 定义的列名。
        """
        cols = self._get_table_columns(PITConfig.TUSHARE_SCHEMA, 'fina_express')
        # 列名候选
        rev_cands = ['revenue','total_revenue','oper_rev','total_oper_rev']
        op_cands  = ['operate_profit','op_profit','oper_profit']
        par_cands = ['n_income_attr_p','netprofit_parent','net_profit_attr_parcom','netprofit_atsopc']
        tot_cands = ['n_income','net_profit','netprofit']
        rev_col = self._choose_column(cols, rev_cands)
        op_col  = self._choose_column(cols, op_cands)
        par_col = self._choose_column(cols, par_cands)
        tot_col = self._choose_column(cols, tot_cands)
        # 额外候选：total_profit / n_income 原字段（若存在则保留，便于后续保底与持久化）
        tp_cands  = ['total_profit','tprofit','profit_total']
        nin_cands = ['n_income','net_profit','netprofit']
        tp_col  = self._choose_column(cols, tp_cands)
        nin_col = self._choose_column(cols, nin_cands)
        # 构建 SELECT 列表（不存在的列用 NULL 占位）
        select_parts = [
            'ts_code', 'end_date', 'ann_date',
            (f"{rev_col} as revenue" if rev_col else "NULL::numeric as revenue"),
            (f"{op_col} as operate_profit" if op_col else "NULL::numeric as operate_profit"),
            (f"{par_col} as n_income_attr_p" if par_col else "NULL::numeric as n_income_attr_p"),
            (f"{tot_col} as n_income" if tot_col else "NULL::numeric as n_income"),
            (f"{tp_col} as total_profit" if tp_col else "NULL::numeric as total_profit"),
        ]
        base_sql = f"""
        SELECT {', '.join(select_parts)}
        FROM {PITConfig.TUSHARE_SCHEMA}.fina_express
        WHERE ann_date >= %s AND ann_date <= %s
          AND ts_code IS NOT NULL AND end_date IS NOT NULL
        """
        params = [start_date, end_date]
        if ts_code:
            base_sql += " AND ts_code = %s"
            params.append(ts_code)
        base_sql += " ORDER BY ts_code, end_date, ann_date"
        df = self.context.query_dataframe(base_sql, tuple(params))
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.copy()
        # 缺列用空值补齐（确保有标准列）
        for col in self.data_fields:
            if col not in df.columns:
                df[col] = None
        # 标注来源与转换状态：express 原始值（若后续增强会被覆盖为 EST_* 或 FALLBACK_*）
        if 'conversion_status' not in df.columns:
            df['conversion_status'] = None
        df['conversion_status'] = df['conversion_status'].fillna('EXP_ORIG')
        df['data_source'] = 'express'
        base_cols = self.key_fields + self.data_fields + ['data_source']
        # 将可能用于保底与评估的扩展列一并保留，后续UPSERT按目标表存在性写入
        for extra in ['total_profit','n_income']:
            if extra in df.columns:
                base_cols.append(extra)
        return df[base_cols]

    def _fetch_income_forecast(self, start_date: str, end_date: str, ts_code: Optional[str] = None) -> pd.DataFrame:
        """从 tushare.fina_forecast 获取业绩预告（Forecast）
        优化：自适应列名，优先使用净利润区间的中值；同时写入 net_profit_mid 作为预告中值保留。
        注意：为便于对比，短期继续将中值写入 n_income_attr_p；长期建议仅存 net_profit_mid，n_income_attr_p 留空。
        """
        cols = self._get_table_columns(PITConfig.TUSHARE_SCHEMA, 'fina_forecast')
        min_cands = ['net_profit_min','netprofit_min','n_income_min','profit_min']
        max_cands = ['net_profit_max','netprofit_max','n_income_max','profit_max']
        one_cands = ['net_profit','netprofit','n_income']
        # 变化率与基准净利列
        pct_min_cands = ['p_change_min','profit_yoy_min','change_min']
        pct_max_cands = ['p_change_max','profit_yoy_max','change_max']
        base_cands = ['last_parent_net','last_netprofit_parent','last_net_profit']

        min_col = self._choose_column(cols, min_cands)
        max_col = self._choose_column(cols, max_cands)
        one_col = self._choose_column(cols, one_cands)
        pct_min_col = self._choose_column(cols, pct_min_cands)
        pct_max_col = self._choose_column(cols, pct_max_cands)
        base_col = self._choose_column(cols, base_cands)

        select_parts = ['ts_code','end_date','ann_date']
        if min_col:
            select_parts += [f"{min_col} AS net_profit_min"]
        if max_col:
            select_parts += [f"{max_col} AS net_profit_max"]
        if one_col and not (min_col or max_col):
            select_parts += [f"{one_col} AS net_profit"]
        if pct_min_col:
            select_parts += [f"{pct_min_col} AS p_change_min"]
        if pct_max_col:
            select_parts += [f"{pct_max_col} AS p_change_max"]
        if base_col:
            select_parts += [f"{base_col} AS last_parent_net"]

        base_sql = f"""
        SELECT {', '.join(select_parts)}
        FROM {PITConfig.TUSHARE_SCHEMA}.fina_forecast
        WHERE ann_date >= %s AND ann_date <= %s
          AND ts_code IS NOT NULL AND end_date IS NOT NULL
        """
        params = [start_date, end_date]
        if ts_code:
            base_sql += " AND ts_code = %s"
            params.append(ts_code)
        base_sql += " ORDER BY ts_code, end_date, ann_date"
        df = self.context.query_dataframe(base_sql, tuple(params))
        if df is None or df.empty:
            return pd.DataFrame()
        work = df.copy()


        # 构建同比基准（若 last_parent_net 缺失，则用上年同期 report 的归母净利润累计作为基准）
        try:
            work['prev_year_end'] = (pd.to_datetime(work['end_date']) - pd.DateOffset(years=1)).dt.date
            base_start = work['prev_year_end'].min()
            base_end = work['prev_year_end'].max()
            # 动态选择基准列：优先 n_income_attr_p，其次 n_income
            inc_cols = self._get_table_columns(PITConfig.TUSHARE_SCHEMA, 'fina_income')
            base_col = 'n_income_attr_p' if 'n_income_attr_p' in inc_cols else ('n_income' if 'n_income' in inc_cols else None)
            if base_col is not None:
                base_query = f"""
                SELECT ts_code, end_date, ann_date, {base_col} AS yoy_base
                FROM {PITConfig.TUSHARE_SCHEMA}.fina_income
                WHERE end_date >= %s AND end_date <= %s
                  AND ts_code IS NOT NULL AND end_date IS NOT NULL AND ann_date IS NOT NULL
                ORDER BY ts_code, end_date, ann_date
                """
                base_df = self.context.query_dataframe(base_query, (str(base_start), str(base_end)))
                if base_df is not None and not base_df.empty:
                    base_df = base_df.copy()
                    base_df['end_date'] = pd.to_datetime(base_df['end_date']).dt.date
                    base_df.sort_values(['ts_code','end_date','ann_date'], inplace=True)
                    base_df = base_df.drop_duplicates(['ts_code','end_date'], keep='last')
                    yoy_base = base_df[['ts_code','end_date','yoy_base']].rename(columns={'end_date':'prev_year_end'})
                    work = work.merge(yoy_base, on=['ts_code','prev_year_end'], how='left')
        except Exception as e:
            self.logger.debug(f"FC-YOY-BASE: failed to build yoy base: {e}")

        # 统一构造 mid（元），记录来源阶段
        range_mid = None; single_mid = None; pct_mid = None
        # 1) 绝对值区间
        if 'net_profit_min' in work.columns or 'net_profit_max' in work.columns:
            min_v = pd.to_numeric(work.get('net_profit_min'), errors='coerce') if 'net_profit_min' in work.columns else None
            max_v = pd.to_numeric(work.get('net_profit_max'), errors='coerce') if 'net_profit_max' in work.columns else None
            if min_v is not None or max_v is not None:
                if min_v is not None and max_v is not None:
                    range_mid = (min_v + max_v) / 2.0
                else:
                    range_mid = min_v if min_v is not None else max_v
                if range_mid is not None:
                    range_mid = range_mid.astype(float) * 10000.0
        # 2) 单一净利
        if 'net_profit' in work.columns:
            single_mid = pd.to_numeric(work['net_profit'], errors='coerce')
            if single_mid is not None:
                single_mid = single_mid.astype(float) * 10000.0
        # 3) 同比区间 + 基准净利推导
        if ('p_change_min' in work.columns or 'p_change_max' in work.columns):
            # 统一基准到"元"口径：last_parent_net（万元）优先，缺失则用上一年同期报告累计 yoy_base（元）
            base_lp = pd.to_numeric(work['last_parent_net'], errors='coerce') if 'last_parent_net' in work.columns else None
            base_lp_yuan = base_lp.astype(float) * 10000.0 if base_lp is not None else None
            yoy_series = pd.to_numeric(work.get('yoy_base'), errors='coerce') if 'yoy_base' in work.columns else None
            base_yuan = None
            if base_lp_yuan is not None and yoy_series is not None:
                base_yuan = base_lp_yuan.where(base_lp_yuan.notna(), yoy_series)
            elif base_lp_yuan is not None:
                base_yuan = base_lp_yuan
            else:
                base_yuan = yoy_series
            if base_yuan is not None:
                est_min = None; est_max = None
                pct_min = pd.to_numeric(work['p_change_min'], errors='coerce') if 'p_change_min' in work.columns else None
                pct_max = pd.to_numeric(work['p_change_max'], errors='coerce') if 'p_change_max' in work.columns else None
                if pct_min is not None:
                    est_min = base_yuan.astype(float) * (1.0 + pct_min.astype(float) / 100.0)
                if pct_max is not None:
                    est_max = base_yuan.astype(float) * (1.0 + pct_max.astype(float) / 100.0)
                if est_min is not None and est_max is not None:
                    pct_mid = est_min.where(est_min.notna(), est_max)
                    pct_mid = pct_mid.where(pct_mid.notna(), est_max.where(est_max.notna(), est_min))
                    pct_mid = (pct_mid + (est_max.where(est_max.notna(), est_min))) / 2.0 if est_max is not None else est_min
                elif est_min is not None or est_max is not None:
                    pct_mid = est_min if est_min is not None else est_max

        # 合并候选（逐行优先级：range -> single -> pct），避免某一候选列全为空时覆盖其它有效候选
        mid = None
        cands = []
        if range_mid is not None:
            cands.append(range_mid)
        if single_mid is not None:
            cands.append(single_mid)
        if pct_mid is not None:
            cands.append(pct_mid)
        if cands:
            mid = cands[0].copy()
            for s in cands[1:]:
                if s is not None:
                    mid = mid.where(mid.notna(), s)

        # 构造输出（包含所有行），并携带 fc_hint 便于后续不过度过滤
        out = work[['ts_code','end_date','ann_date']].copy()
        # 线索标记：是否存在任何一个 forecast 数值线索
        has_min = work['net_profit_min'].notna() if 'net_profit_min' in work.columns else pd.Series(False, index=work.index)
        has_max = work['net_profit_max'].notna() if 'net_profit_max' in work.columns else pd.Series(False, index=work.index)
        has_one = work['net_profit'].notna() if 'net_profit' in work.columns else pd.Series(False, index=work.index)
        has_pct = (work['p_change_min'].notna() if 'p_change_min' in work.columns else pd.Series(False, index=work.index)) | \
                  (work['p_change_max'].notna() if 'p_change_max' in work.columns else pd.Series(False, index=work.index))
        out['fc_hint'] = (has_min | has_max | has_one | has_pct).fillna(False)

        # 写入估计中值（可为空）
        out['net_profit_mid'] = mid if mid is not None else None
        # 将中值写入归母净利（便于统一管线），可为空
        for col in self.data_fields:
            if col == 'n_income_attr_p':
                out[col] = out['net_profit_mid']
            else:
                out[col] = None
        # 携带原始 forecast 线索列，供后续季度化阶段做 PCT 兜底估计
        for extra in ['p_change_min','p_change_max','last_parent_net','yoy_base']:
            if extra in work.columns:
                out[extra] = work[extra]
        # 转换状态：有中值则标记 FC_MID_10K，否则 FC_NO_MID
        out['conversion_status'] = out['net_profit_mid'].apply(lambda v: 'FC_MID_10K' if pd.notna(v) else 'FC_NO_MID')
        out['data_source'] = 'forecast'


        return out


    def _preprocess_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """数据预处理"""

        if data.empty:
            return data

        self.logger.info(f"开始数据预处理: {len(data)} 条记录")

        processed_data = data.copy()

        # 1. data_source 已在各自_fetch_*阶段固定为 report/express/forecast
        # 若缺失则默认为 report（保守回退），但会记录告警
        if 'data_source' not in processed_data.columns:
            self.logger.warning("输入数据缺少data_source列，回退为'report'")
            processed_data['data_source'] = 'report'

        # 2. 数据类型转换
        # 转换日期字段
        if 'end_date' in processed_data.columns:
            processed_data['end_date'] = pd.to_datetime(processed_data['end_date']).dt.date
        if 'ann_date' in processed_data.columns:
            processed_data['ann_date'] = pd.to_datetime(processed_data['ann_date']).dt.date

        # 2.5. 计算year和quarter字段（从end_date提取）
        if 'end_date' in processed_data.columns:
            # 转换为datetime以便提取年份和月份
            end_date_dt = pd.to_datetime(processed_data['end_date'])

            # 提取年份
            processed_data['year'] = end_date_dt.dt.year

            # 根据月份计算季度
            processed_data['quarter'] = end_date_dt.dt.month.map({
                1: 1, 2: 1, 3: 1,      # Q1: 1-3月
                4: 2, 5: 2, 6: 2,      # Q2: 4-6月
                7: 3, 8: 3, 9: 3,      # Q3: 7-9月
                10: 4, 11: 4, 12: 4    # Q4: 10-12月
            })

            self.logger.info(f"计算year和quarter字段: {len(processed_data)} 条记录")

        # 4.1 Express 归母净利增强（在完成基本类型转换后再执行，确保日期/数值可用）
        processed_data = self._enrich_express_parent_profit(processed_data)

        # 4.2 数值字段处理（在增强后统一转数值，防御型处理）
        for field in list(self.data_fields) + ['total_profit','n_income','net_profit_mid','year','quarter','conversion_status']:
            if field in processed_data.columns:
                # 转换为数值类型（非数值字段如 conversion_status 跳过）
                if field in ['conversion_status']:
                    continue
                processed_data[field] = pd.to_numeric(processed_data[field], errors='coerce')

        # 5. 去重处理 (保留最新的ann_date)
        processed_data = processed_data.sort_values(['ts_code', 'end_date', 'ann_date'])
        # 注意：必须包含 data_source 以避免不同来源（report/express/forecast）被误判为重复而丢失 conversion_status
        processed_data = processed_data.drop_duplicates(
            subset=['ts_code', 'end_date', 'ann_date', 'data_source'],
            keep='last'
        )

        # 5.5 "累计转单季"季度化（分层：report -> express -> forecast）
        try:
            processed_data = self._quarterize_to_single(processed_data)
            # 过滤掉为计算依赖而扩展的行（不在原始时间窗口内）
            if 'is_extended' in processed_data.columns:
                before_len = len(processed_data)
                processed_data = processed_data[~processed_data['is_extended'].fillna(False)].copy()
                processed_data.drop(columns=['is_extended'], inplace=True, errors='ignore')
                after_len = len(processed_data)
                if before_len != after_len:
                    self.logger.info(f"季度化后已移除扩展依赖行: {before_len - after_len} 条")
            # 方案1：跳过不可季度化的记录（不入库）
            # 标准：在季度化与必要的回填后，若所有核心单季字段均为空，则判定为不可季度化
            # 例外：forecast 行若存在任何"预告线索"（fc_hint=True），即便当前无法产出 mid，也先保留，避免信息丢失
            if 'fc_hint' not in processed_data.columns:
                processed_data['fc_hint'] = False
            core_fields = [f for f in self.data_fields if f in processed_data.columns]
            if core_fields:
                not_all_null = processed_data[core_fields].notna().any(axis=1)
            else:
                not_all_null = pd.Series([True]*len(processed_data), index=processed_data.index)
            keep_forecast_hint = processed_data.get('data_source').eq('forecast') & processed_data['fc_hint'].fillna(False)
            keep_mask = not_all_null | keep_forecast_hint
            # 明确剔除 ANNUAL_ONLY 标记（上市前仅年报）
            if 'conversion_status' in processed_data.columns:
                annual_only_mask = processed_data['conversion_status'].eq('ANNUAL_ONLY')
                keep_mask = keep_mask & ~annual_only_mask.fillna(False)
            before_len2 = len(processed_data)
            kept_hint = int((keep_forecast_hint & ~not_all_null).sum())
            processed_data = processed_data.loc[keep_mask].copy()
            after_len2 = len(processed_data)
            removed = before_len2 - after_len2
            if removed > 0:
                self.logger.info(f"方案1启用：已跳过不可季度化记录 {removed} 条（不入库）；其中保留 forecast 线索行 {kept_hint} 条")
            else:
                if kept_hint > 0:
                    self.logger.info(f"方案1：保留 forecast 线索行 {kept_hint} 条（fc_hint=True）")
        except Exception as e:
            self.logger.error(f"季度化处理失败: {e}")

        # 5.6 为 forecast 记录填充利润代理字段
        processed_data = self._fill_forecast_profit_proxies(processed_data)

        # 6. 数据质量检查
        before_count = len(processed_data)
        processed_data = processed_data.dropna(subset=['ts_code', 'end_date', 'ann_date'])
        after_count = len(processed_data)
        if before_count != after_count:
            self.logger.warning(f"移除了 {before_count - after_count} 条关键字段为空的记录")

        # 6.1 为确保依赖顺序，最终排序按 ann_date 升序（同日内先 report 再 express 再 forecast）
        try:
            cat_type = pd.CategoricalDtype(categories=['report','express','forecast'], ordered=True)
            if 'data_source' in processed_data.columns:
                processed_data['data_source'] = processed_data['data_source'].astype(cat_type)
            processed_data = processed_data.sort_values(['ann_date','data_source','ts_code','end_date'])
        except Exception:
            processed_data = processed_data.sort_values(['ann_date','ts_code','end_date'])

        self.logger.info(f"数据预处理完成: {len(processed_data)} 条记录")
        return processed_data



    def _ensure_income_unique_keys(self) -> None:
        """将 pit_income_quarterly 唯一键升级为 (ts_code, end_date, ann_date, data_source)。幂等执行。
        注意：仅调整索引/约束，不改表结构列。
        """
        try:
            sqls = [
                # 删除旧唯一约束（如果存在）
                f"""
                DO $$ BEGIN
                    IF EXISTS (
                        SELECT 1 FROM pg_constraint c
                        JOIN pg_class t ON c.conrelid = t.oid
                        JOIN pg_namespace n ON n.oid = t.relnamespace
                        WHERE n.nspname = '{PITConfig.PIT_SCHEMA}'
                          AND t.relname = '{self.table_name}'
                          AND c.conname = '{self.table_name}_ts_code_end_date_ann_date_key'
                    ) THEN
                        ALTER TABLE {PITConfig.PIT_SCHEMA}.{self.table_name}
                        DROP CONSTRAINT {self.table_name}_ts_code_end_date_ann_date_key;
                    END IF;
                END $$;
                """,
                # 创建新唯一约束
                f"""
                DO $$ BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint c
                        JOIN pg_class t ON c.conrelid = t.oid
                        JOIN pg_namespace n ON n.oid = t.relnamespace
                        WHERE n.nspname = '{PITConfig.PIT_SCHEMA}'
                          AND t.relname = '{self.table_name}'
                          AND c.conname = '{self.table_name}_uniq_with_source'
                    ) THEN
                        ALTER TABLE {PITConfig.PIT_SCHEMA}.{self.table_name}
                        ADD CONSTRAINT {self.table_name}_uniq_with_source UNIQUE (ts_code, end_date, ann_date, data_source);
                    END IF;
                END $$;
                """,
            ]
            for s in sqls:
                self.context.db_manager.execute_sync(s)
        except Exception as e:
            self.logger.warning(f"唯一键迁移失败或无需迁移: {e}")


        # 性能优化：预编译SQL模板方法
    def _prepare_historical_query_template(self) -> str:
        """预编译历史数据查询模板"""
        return """
        SELECT ts_code, end_date, ann_date, revenue, oper_cost, operate_profit,
               total_profit, n_income, n_income_attr_p
        FROM {pit_schema}.pit_income_quarterly
        WHERE data_source='report' AND ts_code = ANY(%s)
          AND ann_date >= %s AND ann_date <= %s
        ORDER BY ts_code, end_date, ann_date
        """

    def _prepare_bs_snapshot_query_template(self) -> str:
        """预编译资产负债表快照查询模板"""
        return """
        WITH req(ts_code, target_end_date, target_ann_date) AS (
            VALUES {values_placeholder}
        ), joined AS (
            SELECT r.ts_code,
                   r.target_end_date,
                   b.end_date AS bs_end_date,
                   b.ann_date AS bs_ann_date,
                   b.total_hldr_eqy_exc_min_int,
                   b.minority_int
            FROM {tushare_schema}.fina_balancesheet b
            JOIN req r ON r.ts_code=b.ts_code
            WHERE b.end_date < r.target_end_date::date
              AND b.ann_date <= r.target_ann_date::date
        ), ranked AS (
            SELECT ts_code, target_end_date,
                   total_hldr_eqy_exc_min_int, minority_int,
                   ROW_NUMBER() OVER (PARTITION BY ts_code, target_end_date ORDER BY bs_end_date DESC, bs_ann_date DESC) AS rn
            FROM joined
        )
        SELECT ts_code, target_end_date AS end_date, total_hldr_eqy_exc_min_int, minority_int
        FROM ranked WHERE rn=1
        """

    def _batch_query_historical_data(self, ts_codes: List[str], start_date: str, end_date: str) -> pd.DataFrame:
        """批量查询历史数据，构建缓存映射"""
        if not ts_codes:
            return pd.DataFrame()

        cache_key = (tuple(sorted(ts_codes)), start_date, end_date)
        if cache_key in self._historical_cache:
            return self._historical_cache[cache_key]

        sql = self._compiled_queries['historical_lookup'].format(pit_schema=PITConfig.PIT_SCHEMA)
        df = self.context.query_dataframe(sql, (ts_codes, start_date, end_date))

        if df is not None and not df.empty:
            # 构建便于查找的映射
            df_copy = df.copy()
            df_copy['end_date'] = pd.to_datetime(df_copy['end_date']).dt.date
            df_copy['ann_date'] = pd.to_datetime(df_copy['ann_date']).dt.date
            self._historical_cache[cache_key] = df_copy
            return df_copy
        else:
            self._historical_cache[cache_key] = pd.DataFrame()
            return pd.DataFrame()

    def _batch_query_bs_snapshot(self, keys_df: pd.DataFrame) -> pd.DataFrame:
        """批量查询资产负债表快照"""
        if keys_df is None or keys_df.empty:
            return pd.DataFrame()

        # 生成批量VALUES子句
        values_rows = ",\n            ".join([
            f"('{r.ts_code}', DATE '{pd.to_datetime(r.end_date).date()}', DATE '{pd.to_datetime(r.ann_date).date()}')"
            for r in keys_df.itertuples(index=False)
        ])

        sql = self._compiled_queries['bs_snapshot'].format(
            tushare_schema=PITConfig.TUSHARE_SCHEMA,
            values_placeholder=values_rows
        )

        try:
            snap = self.context.query_dataframe(sql)
            return snap if snap is not None else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def _calculate_optimal_batch_size(self, data_size: int, ts_code_count: int) -> int:
        """基于数据量和股票数量动态计算最优批次大小"""
        if data_size > 100000 or ts_code_count > 1000:
            return 20  # 大数据量时减少批次大小
        elif data_size > 50000 or ts_code_count > 500:
            return 50
        else:
            return 100  # 小数据量时可以增大批次

    # 6. 列名自动探测工具
    def _get_table_columns(self, schema: str, table: str) -> set:
        sql = f"SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s"
        try:
            df = self.context.query_dataframe(sql, (schema, table))
            return set(df['column_name'].tolist()) if df is not None else set()
        except Exception:
            return set()

    def _choose_column(self, cols: set, candidates: list) -> str:
        for c in candidates:
            if c in cols:
                return c
        return ''

    def ensure_indexes(self) -> None:
        """幂等创建收入PIT表的加速索引。"""
        try:
            import os
            sql_path = os.path.join(os.path.dirname(__file__), 'database', 'create_pit_income_indexes.sql')
            if os.path.exists(sql_path):
                with open(sql_path, 'r', encoding='utf-8') as f:
                    ddl = f.read()
                self.context.db_manager.execute_sync(ddl)
                self.logger.info("已确保 PIT 利润表相关索引存在（幂等）")
        except Exception as e:
            self.logger.error(f"创建收入表索引失败: {e}")

    def _enrich_express_parent_profit(self, df: pd.DataFrame) -> pd.DataFrame:
        """当 express 缺少 n_income_attr_p 时，使用分级回退策略估算归母净利润。
        策略优先级：REPORT_RATIO > BS_RATIO；无法估算则保持空并打标。
        【优化】使用批量查询和缓存机制替代循环查询。
        """
        if df is None or df.empty or 'data_source' not in df.columns:
            return df
        work = df.copy()
        for col in ['ts_code','end_date','ann_date']:
            if col not in work.columns:
                return df
        # 仅处理 express 且 n_income_attr_p 为空，且存在可能的总净利润列
        mask_express = work['data_source'] == 'express'
        if 'n_income_attr_p' not in work.columns:
            work['n_income_attr_p'] = None
        need_rows = mask_express & work['n_income_attr_p'].isna() & work['n_income'].notna()
        if not need_rows.any():
            # 增加保底：如果仍为空且有 n_income（总净利），直接回填
            fallback_rows = mask_express & work['n_income_attr_p'].isna() & work.get('n_income', pd.Series([None]*len(work))).notna()
            if fallback_rows.any():
                work.loc[fallback_rows, 'n_income_attr_p'] = pd.to_numeric(work.loc[fallback_rows, 'n_income'], errors='coerce')
                if 'conversion_status' in work.columns:
                    work.loc[fallback_rows, 'conversion_status'] = 'FALLBACK_NI'  # 缩短以适配 varchar(20)
            return work

        fill_count = 0

        # 1) REPORT_RATIO（批次内）
        try:
            report_df = work[work.get('data_source') == 'report'][['ts_code','end_date','ann_date','n_income_attr_p','n_income']].copy()
            if not report_df.empty and 'n_income' in report_df.columns:
                report_df.sort_values(['ts_code','end_date','ann_date'], inplace=True)
                report_df = report_df.drop_duplicates(['ts_code','end_date'], keep='last')
                report_df['report_ratio'] = pd.to_numeric(report_df['n_income_attr_p'], errors='coerce') / pd.to_numeric(report_df['n_income'], errors='coerce')
                report_df.loc[(report_df['report_ratio'] <= 0) | (report_df['report_ratio'] > 1), 'report_ratio'] = None
                merge_cols = ['ts_code','end_date']
                tmp = work.loc[need_rows, merge_cols + ['n_income']].merge(
                    report_df[merge_cols + ['report_ratio']], on=merge_cols, how='left'
                )
                n_total = pd.to_numeric(tmp['n_income'], errors='coerce')
                r_ratio = pd.to_numeric(tmp['report_ratio'], errors='coerce')
                est = n_total.astype(float) * r_ratio.astype(float)
                est_valid = est.notna()
                if est_valid.any():
                    upd_idx = work.loc[need_rows].index[est_valid]
                    work.loc[upd_idx, 'n_income_attr_p'] = est[est_valid]
                    if 'conversion_status' in work.columns:
                        work.loc[upd_idx, 'conversion_status'] = 'EST_RPT_RATIO'
                    fill_count += len(upd_idx)
        except Exception as e:
            self.logger.warning(f"批次内report比例估算失败: {e}")

        # 1b) REPORT_RATIO（历史 PIT 回看，PIT 原则 + lookback）【优化：批量查询】
        remaining = mask_express & work['n_income_attr_p'].isna() & work['n_income'].notna()
        if remaining.any():
            try:
                # 准备窗口与 ts 列表
                ts_list = sorted(work.loc[remaining, 'ts_code'].dropna().unique().tolist())
                if ts_list:
                    min_ann = work.loc[remaining, 'ann_date'].min()
                    max_ann = work.loc[remaining, 'ann_date'].max()
                    lookback_months = PITConfig.PIT_TABLES['pit_income_quarterly'].get('INCOME_EXPRESS_FILL_LOOKBACK_MONTHS', 9)
                    from dateutil.relativedelta import relativedelta
                    lower_bound = (pd.to_datetime(min_ann) - relativedelta(months=lookback_months)).date()

                    # 【优化】使用批量查询缓存替代循环查询
                    ref = self._batch_query_historical_data(ts_list, str(lower_bound), str(max_ann))

                    if ref is not None and not ref.empty:
                        # 批量处理匹配逻辑
                        need_df = work.loc[remaining, ['ts_code','end_date','ann_date','n_income']].copy()
                        need_df['ann_dt'] = pd.to_datetime(need_df['ann_date'])

                        # 向量化匹配：构建 (ts_code,end_date) -> 最近历史报告的映射
                        merged = need_df.merge(ref, on=['ts_code','end_date'], how='left', suffixes=('','_r'))

                        # 应用PIT原则：历史报告的ann_date <= 当前记录的ann_date
                        merged = merged[merged['ann_date_r'] <= merged['ann_date']]

                        # 按 (ts_code,end_date,ann_date) 取最近的ann_date_r
                        merged.sort_values(['ts_code','end_date','ann_date_r'], inplace=True)
                        merged = merged.drop_duplicates(['ts_code','end_date','ann_date'], keep='last')

                        # 计算比例并估算
                        ratio = pd.to_numeric(merged['n_income_attr_p'], errors='coerce') / pd.to_numeric(merged['n_income_r'], errors='coerce')
                        ratio[(ratio <= 0) | (ratio > 1)] = None
                        est = pd.to_numeric(merged['n_income'], errors='coerce').astype(float) * ratio.astype(float)

                        # 批量更新结果
                        valid_mask = est.notna()
                        if valid_mask.any():
                            # 【关键修复】建立正确的索引对应关系，避免错位问题
                            valid_merged = merged[valid_mask].copy()
                            valid_est = est[valid_mask]

                            # 为merged添加work的原始索引信息
                            work_remaining_data = work.loc[remaining, ['ts_code','end_date','ann_date','n_income']].copy()
                            work_remaining_data['work_idx'] = work_remaining_data.index

                            # 合并以建立正确对应关系
                            update_data = work_remaining_data.merge(
                                valid_merged[['ts_code','end_date','ann_date','n_income_attr_p']],
                                left_on=['ts_code','end_date','ann_date','n_income'],
                                right_on=['ts_code','end_date','ann_date','n_income_attr_p'],
                                how='inner'  # 只保留匹配的行
                            )

                            batch_update_count = 0
                            for _, row in update_data.iterrows():
                                work_idx = row['work_idx']
                                if pd.isna(work.at[work_idx, 'n_income_attr_p']) and pd.notna(row.get('n_income_attr_p')):
                                    # 【修复】数据类型转换
                                    work.at[work_idx, 'n_income_attr_p'] = pd.to_numeric(row['n_income_attr_p'], errors='coerce')
                                    if 'conversion_status' in work.columns:
                                        work.at[work_idx, 'conversion_status'] = 'EST_RPT_RATIO_DB'
                                    batch_update_count += 1

                            fill_count += batch_update_count
                            if batch_update_count:
                                self.logger.info(f"express归母净利回填（历史PIT批量查询）完成：{batch_update_count} 条")

            except Exception as e:
                self.logger.warning(f"历史PIT回看批量回填失败：{e}")

        # 2) BS_RATIO 【优化：批量查询】
        remaining = mask_express & work['n_income_attr_p'].isna() & work['n_income'].notna()
        if remaining.any():
            try:
                # 【优化】使用批量查询替代单条查询
                snap = self._batch_query_bs_snapshot(work.loc[remaining, ['ts_code','end_date','ann_date']])

                if snap is not None and not snap.empty:
                    merged = work.loc[remaining, ['ts_code','end_date','ann_date','n_income']].merge(
                        snap, on=['ts_code','end_date'], how='left'
                    )
                    parent = pd.to_numeric(merged.get('total_hldr_eqy_exc_min_int'), errors='coerce')
                    minority = pd.to_numeric(merged.get('minority_int'), errors='coerce')
                    denom = parent + minority
                    ratio = parent / denom
                    ratio[(denom <= 0) | (ratio <= 0) | (ratio > 1)] = None
                    n_total2 = pd.to_numeric(merged['n_income'], errors='coerce')
                    est = n_total2.astype(float) * ratio.astype(float)
                    est_valid = est.notna()

                    if est_valid.any():
                        # 【关键修复】批量更新，避免索引错位问题
                        valid_merged = merged[est_valid].copy()
                        valid_est = est[est_valid]

                        # 为merged添加work的原始索引信息
                        work_remaining_data = work.loc[remaining, ['ts_code','end_date','ann_date']].copy()
                        work_remaining_data['work_idx'] = work_remaining_data.index

                        # 合并以建立正确对应关系
                        update_data = work_remaining_data.merge(
                            valid_merged[['ts_code','end_date','ann_date']],
                            left_on=['ts_code','end_date','ann_date'],
                            right_on=['ts_code','end_date','ann_date'],
                            how='inner'  # 只保留匹配的行
                        )

                        bs_fill_count = 0
                        for _, row in update_data.iterrows():
                            work_idx = row['work_idx']
                            # 找到对应的est值
                            matched_row = valid_merged[
                                (valid_merged['ts_code'] == row['ts_code']) &
                                (valid_merged['end_date'] == row['end_date']) &
                                (valid_merged['ann_date'] == row['ann_date'])
                            ]
                            if not matched_row.empty:
                                est_val = valid_est.loc[matched_row.index[0]]
                                if pd.isna(work.at[work_idx, 'n_income_attr_p']) and pd.notna(est_val):
                                    # 【修复】数据类型转换
                                    work.at[work_idx, 'n_income_attr_p'] = pd.to_numeric(est_val, errors='coerce')
                                    if 'conversion_status' in work.columns:
                                        work.at[work_idx, 'conversion_status'] = 'EST_BS_RATIO'
                                    bs_fill_count += 1

                        fill_count += bs_fill_count
                        if bs_fill_count:
                            self.logger.info(f"express归母净利回填（资产负债表比例批量查询）完成：{bs_fill_count} 条")

            except Exception as e:
                self.logger.warning(f"资产负债表比例批量回填失败：{e}")

        if fill_count:
            self.logger.info(f"express归母净利增强总计完成：{fill_count} 条记录")

        return work

    def _quarterize_to_single(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        将年度累计值转换为单季度值（季度化）。
        规则：单季(Qn) = 累计(Qn) - 累计(Qn-1)，其中 Q1 直接等于 累计(Q1)。
        分层顺序：先 report（自足），再 express（依赖 report 上期累计），最后 forecast（依赖 report 上期累计）。
        额外：上市前仅年报（仅Q4）标注为 ANNUAL_ONLY，且不进行季度差分。
        【优化】使用向量化操作和预计算映射替代循环查询，大幅提升性能。
        """
        if df is None or df.empty:
            return df
        work = df.copy()
        # 统一日期类型（防止 end_date/ann_date 类型不一致导致等值比较失败）
        if 'end_date' in work.columns:
            work['end_date'] = pd.to_datetime(work['end_date']).dt.date
        if 'ann_date' in work.columns:
            work['ann_date'] = pd.to_datetime(work['ann_date']).dt.date
        # 基础累计值快照（不被后续覆盖），并统一日期类型
        base = df.copy()
        if 'end_date' in base.columns:
            base['end_date'] = pd.to_datetime(base['end_date']).dt.date
        if 'ann_date' in base.columns:
            base['ann_date'] = pd.to_datetime(base['ann_date']).dt.date
        # 需要季度化的字段集合（仅对存在于当前数据框中的累计性指标进行）
        candidate_fields = ['revenue', 'oper_cost', 'operate_profit', 'total_profit', 'n_income', 'n_income_attr_p']
        q_fields = [c for c in candidate_fields if c in work.columns]
        if not q_fields:
            return work
        # 确保辅助字段存在
        if 'year' not in work.columns or 'quarter' not in work.columns:
            end_dt = pd.to_datetime(work['end_date'])
            work['year'] = end_dt.dt.year
            work['quarter'] = end_dt.dt.month.map({1:1,2:1,3:1,4:2,5:2,6:2,7:3,8:3,9:3,10:4,11:4,12:4})
        # 计算上一季度季末日期（用于严格匹配 Qn-1）
        work['prev_end_date'] = work['end_date'].apply(lambda d: self._prev_quarter_end(d, 1))
        # 基础累计值快照（不被后续覆盖）
        base = df.copy()

        # 初始化统计信息
        quarterly_stats = {
            'report': {
                'total_records': 0,
                'q2q4_records': 0,
                'missing_prev_cumulative': 0,
                'affected_single_calculation': 0,
                'field_affected_breakdown': {field: 0 for field in q_fields},
                'annual_only_records': 0
            },
            'express': {
                'total_records': 0,
                'q2q4_records': 0,
                'missing_prev_cumulative': 0,
                'affected_single_calculation': 0
            },
            'forecast': {
                'total_records': 0,
                'q2q4_records': 0,
                'missing_prev_cumulative': 0,
                'affected_single_calculation': 0
            }
        }

        # ---------- 1) 处理 report 【优化：向量化处理】----------
        mask_r = work.get('data_source').eq('report') if 'data_source' in work.columns else pd.Series([False]*len(work), index=work.index)
        if mask_r.any():
            quarterly_stats['report']['total_records'] = mask_r.sum()
            self.logger.info(f"开始处理 report 季度化: {mask_r.sum()} 条记录")

            # 检测"仅Q4"的上市前年报情况：同(ts_code,year)只有Q4
            r = work.loc[mask_r].copy()
            grp = r.groupby(['ts_code','year'])
            annual_only_idx = []
            for (ts, _), g in grp:
                qs = set(g['quarter'].dropna().astype(int).unique().tolist())
                if qs == {4}:
                    annual_only_idx.extend(g.index.tolist())
            if annual_only_idx:
                quarterly_stats['report']['annual_only_records'] = len(annual_only_idx)
                work.loc[annual_only_idx, 'conversion_status'] = 'ANNUAL_ONLY'
                for c in q_fields:
                    work.loc[annual_only_idx, c] = None

            # 创建 report 数据映射【优化：预先构建】
            r_data = work.loc[mask_r & ~work.index.isin(annual_only_idx)].copy()
            if not r_data.empty:
                r_lookup = {}
                for idx, row in r_data.iterrows():
                    key = (row['ts_code'], row['end_date'])
                    r_lookup[key] = {c: row.get(c) for c in q_fields}

                # 处理 Q1（直接等于累计）
                mask_r_q1 = mask_r & work['quarter'].eq(1) & ~work.index.isin(annual_only_idx)
                if mask_r_q1.any():
                    for c in q_fields:
                        if c in base.columns:
                            work.loc[mask_r_q1, c] = pd.to_numeric(base.loc[mask_r_q1, c], errors='coerce')
                    work.loc[mask_r_q1, 'conversion_status'] = 'QTR_DIFF_RPT'

                # 处理 Q2-Q4（需要差分计算）【优化：向量化处理】
                mask_r_q2q4 = mask_r & work['quarter'].ne(1) & ~work.index.isin(annual_only_idx)
                if mask_r_q2q4.any():
                    quarterly_stats['report']['q2q4_records'] = mask_r_q2q4.sum()
                    r_q2q4_data = work.loc[mask_r_q2q4].copy()

                    # 为每个 report 记录找到对应的上一季度 report 记录
                    prev_values = []
                    missing_prev_count = 0
                    for idx, row in r_q2q4_data.iterrows():
                        ts = row['ts_code']
                        prev_end = row['prev_end_date']

                        # 查找对应的上一季度 report 数据
                        prev_data = r_lookup.get((ts, prev_end))
                        if prev_data:
                            prev_values.append(prev_data)
                        else:
                            missing_prev_count += 1
                            prev_values.append({c: None for c in q_fields})

                    quarterly_stats['report']['missing_prev_cumulative'] = missing_prev_count

                    # 批量更新差分结果
                    affected_calculation = 0
                    for c in q_fields:
                        if c in base.columns:
                            cur_vals = pd.to_numeric(base.loc[mask_r_q2q4, c], errors='coerce')
                            prev_vals = pd.Series([pv.get(c) for pv in prev_values], index=mask_r_q2q4[mask_r_q2q4].index)

                            # 计算差分
                            diff_vals = cur_vals - pd.to_numeric(prev_vals, errors='coerce')
                            valid_diff = diff_vals.notna()

                            if valid_diff.any():
                                work.loc[mask_r_q2q4 & valid_diff, c] = diff_vals[valid_diff]
                                work.loc[mask_r_q2q4 & valid_diff, 'conversion_status'] = 'QTR_DIFF_RPT'
                            else:
                                work.loc[mask_r_q2q4, c] = None
                                affected_calculation += mask_r_q2q4.sum()
                                quarterly_stats['report']['field_affected_breakdown'][c] = mask_r_q2q4.sum()

                    if affected_calculation > 0:
                        quarterly_stats['report']['affected_single_calculation'] = affected_calculation

            self.logger.info(f"report 季度化处理完成")
        # 【优化】不再需要这些映射，因为已经在向量化处理中构建了 r_lookup
        # 【优化】预先构建 report 数据映射，避免循环查询（在所有处理前构建一次）
        r_lookup = {}
        if mask_r.any():
            r_data = work.loc[mask_r, ['ts_code','end_date','ann_date'] + q_fields].copy()
            r_data['ann_dt'] = pd.to_datetime(r_data['ann_date'])

            # 按 ts_code 和 end_date 分组，找到每个分组中最近的记录
            r_data = r_data.sort_values(['ts_code','end_date','ann_dt'])
            r_data = r_data.drop_duplicates(['ts_code','end_date'], keep='last')

            # 创建映射字典
            for idx, row in r_data.iterrows():
                key = (row['ts_code'], row['end_date'])
                r_lookup[key] = {c: row.get(c) for c in q_fields}

        # 【关键修复】为季度化准备report累计值映射（从base获取原始累计值）
        r_cumulative_lookup = {}
        if mask_r.any():
            # 从base中获取report的原始累计值（未季度化的）
            base_report = base[base['data_source'] == 'report'].copy()
            if not base_report.empty:
                # 按ts_code和end_date分组，取最新的ann_date记录
                base_report = base_report.sort_values(['ts_code','end_date','ann_date'])
                base_report = base_report.drop_duplicates(['ts_code','end_date'], keep='last')

                # 创建累计值映射字典
                for idx, row in base_report.iterrows():
                    key = (row['ts_code'], row['end_date'])
                    r_cumulative_lookup[key] = {c: row.get(c) for c in q_fields}

                self.logger.info(f"构建了 {len(r_cumulative_lookup)} 个report累计值映射")

        # 【数据质量分析】使用独立脚本进行分析，避免主脚本膨胀
        # 数据质量分析已移至独立的 pit_data_quality_analyzer.py 脚本
        # 这里只保留简化的基础统计
        if mask_r.any():
            self.logger.info(f"数据质量基础统计：Report记录总数 {mask_r.sum()} 条")

        # ---------- 2) 处理 express（依赖 report 上一季累计；缺失回填同季 report 单季）【优化：向量化处理】----------
        mask_e = work.get('data_source').eq('express') if 'data_source' in work.columns else pd.Series([False]*len(work), index=work.index)
        if mask_e.any():
            quarterly_stats['express']['total_records'] = mask_e.sum()
            self.logger.info(f"开始处理 express 季度化: {mask_e.sum()} 条记录")

            # 处理 Q1（直接等于累计）
            mask_e_q1 = mask_e & work['quarter'].eq(1)
            if mask_e_q1.any():
                for c in q_fields:
                    if c in base.columns:
                        work.loc[mask_e_q1, c] = pd.to_numeric(base.loc[mask_e_q1, c], errors='coerce')
                work.loc[mask_e_q1, 'conversion_status'] = 'QTR_DIFF_EXP'

            # 处理 Q2-Q4（需要差分计算）【优化：向量化处理】
            mask_e_q2q4 = mask_e & work['quarter'].ne(1)
            if mask_e_q2q4.any() and mask_r.any():
                quarterly_stats['express']['q2q4_records'] = mask_e_q2q4.sum()
                e_data = work.loc[mask_e_q2q4].copy()

                # 【关键修复】为每个 express 记录找到对应的 report 累计值，而不是单季值
                prev_cum_values = []

                # 收集所有需要的(ts_code, end_date)组合
                needed_report_keys = []
                for idx, row in e_data.iterrows():
                    ts = row['ts_code']
                    prev_end = row['prev_end_date']
                    needed_report_keys.append((ts, prev_end))

                # 【关键修复】使用预构建的累计值映射
                missing_cumulative_warnings = []
                for ts, prev_end in needed_report_keys:
                    # 首先尝试从累计值映射中获取
                    prev_cum_data = r_cumulative_lookup.get((ts, prev_end))
                    if prev_cum_data:
                        prev_cum_values.append(prev_cum_data)
                    else:
                        # 如果累计值映射中没有，尝试从base中查找
                        base_lookup = base.loc[
                            (base['ts_code'] == ts) & (base['end_date'] == prev_end) & (base['data_source'] == 'report')
                        ]
                        if not base_lookup.empty:
                            prev_cum_values.append(base_lookup.iloc[0][q_fields].to_dict())
                        else:
                            # 记录缺失的累计数据，但继续处理
                            missing_cumulative_warnings.append((ts, prev_end))
                            prev_cum_values.append({c: None for c in q_fields})

                # 更新统计信息
                quarterly_stats['express']['missing_prev_cumulative'] = len(missing_cumulative_warnings)

                # 批量记录累计数据缺失警告
                if missing_cumulative_warnings:
                    self.logger.warning(f"Express累计数据缺失: {len(missing_cumulative_warnings)} 条记录受影响")
                    for ts, prev_end in missing_cumulative_warnings[:3]:  # 只显示前3个示例
                        self.logger.warning(f"  示例: {ts} {prev_end}")
                    if len(missing_cumulative_warnings) > 3:
                        self.logger.warning(f"  ... 还有 {len(missing_cumulative_warnings) - 3} 条类似记录")

                # 批量更新差分结果
                affected_calculation = 0
                for c in q_fields:
                    if c in base.columns:
                        # 当前累计值（从base获取）
                        cur_cum_vals = pd.to_numeric(base.loc[mask_e_q2q4, c], errors='coerce')
                        # 前一季度累计值
                        prev_cum_vals_series = pd.Series([pv.get(c) for pv in prev_cum_values], index=mask_e_q2q4[mask_e_q2q4].index)

                        # 正确的季度化计算：当前累计值 - 前一季度累计值 = 当季单季值
                        single_vals = cur_cum_vals - pd.to_numeric(prev_cum_vals_series, errors='coerce')
                        valid_single = single_vals.notna()

                        if valid_single.any():
                            work.loc[mask_e_q2q4 & valid_single, c] = single_vals[valid_single]
                            work.loc[mask_e_q2q4 & valid_single, 'conversion_status'] = 'QTR_DIFF_EXP'
                        else:
                            work.loc[mask_e_q2q4, c] = None
                            affected_calculation += mask_e_q2q4.sum()

                quarterly_stats['express']['affected_single_calculation'] = affected_calculation

            self.logger.info(f"express 季度化处理完成")
        # ---------- 3) 处理 forecast（同 express 策略）【优化：向量化处理】----------
        mask_f = work.get('data_source').eq('forecast') if 'data_source' in work.columns else pd.Series([False]*len(work), index=work.index)
        if mask_f.any():
            quarterly_stats['forecast']['total_records'] = mask_f.sum()
            self.logger.info(f"开始处理 forecast 季度化: {mask_f.sum()} 条记录")

            # 处理 Q1（直接等于累计）
            mask_f_q1 = mask_f & work['quarter'].eq(1)
            if mask_f_q1.any():
                for c in q_fields:
                    if c in base.columns:
                        work.loc[mask_f_q1, c] = pd.to_numeric(base.loc[mask_f_q1, c], errors='coerce')
                work.loc[mask_f_q1, 'conversion_status'] = 'QTR_DIFF_FC'

            # 处理 Q2-Q4（需要差分计算）【修复：使用累计值进行季度化】
            mask_f_q2q4 = mask_f & work['quarter'].ne(1)
            if mask_f_q2q4.any() and mask_r.any():
                quarterly_stats['forecast']['q2q4_records'] = mask_f_q2q4.sum()
                f_data = work.loc[mask_f_q2q4].copy()

                # 【关键修复】为每个 forecast 记录找到对应的 report 累计值，而不是单季值
                prev_cum_values = []

                # 收集所有需要的(ts_code, end_date)组合
                needed_report_keys = []
                for idx, row in f_data.iterrows():
                    ts = row['ts_code']
                    prev_end = row['prev_end_date']
                    needed_report_keys.append((ts, prev_end))

                # 【关键修复】使用预构建的累计值映射
                missing_cumulative_warnings = []
                for ts, prev_end in needed_report_keys:
                    # 首先尝试从累计值映射中获取
                    prev_cum_data = r_cumulative_lookup.get((ts, prev_end))
                    if prev_cum_data:
                        prev_cum_values.append(prev_cum_data)
                    else:
                        # 如果累计值映射中没有，尝试从base中查找
                        base_lookup = base.loc[
                            (base['ts_code'] == ts) & (base['end_date'] == prev_end) & (base['data_source'] == 'report')
                        ]
                        if not base_lookup.empty:
                            prev_cum_values.append(base_lookup.iloc[0][q_fields].to_dict())
                        else:
                            # 记录缺失的累计数据，但继续处理
                            missing_cumulative_warnings.append((ts, prev_end))
                            prev_cum_values.append({c: None for c in q_fields})

                # 更新统计信息
                quarterly_stats['forecast']['missing_prev_cumulative'] = len(missing_cumulative_warnings)

                # 批量记录累计数据缺失警告
                if missing_cumulative_warnings:
                    self.logger.warning(f"Forecast累计数据缺失: {len(missing_cumulative_warnings)} 条记录受影响")
                    for ts, prev_end in missing_cumulative_warnings[:3]:  # 只显示前3个示例
                        self.logger.warning(f"  示例: {ts} {prev_end}")
                    if len(missing_cumulative_warnings) > 3:
                        self.logger.warning(f"  ... 还有 {len(missing_cumulative_warnings) - 3} 条类似记录")

                # 批量更新差分结果
                affected_calculation = 0
                for c in q_fields:
                    if c in base.columns:
                        # 当前累计值（从base获取）
                        cur_cum_vals = pd.to_numeric(base.loc[mask_f_q2q4, c], errors='coerce')
                        # 前一季度累计值
                        prev_cum_vals_series = pd.Series([pv.get(c) for pv in prev_cum_values], index=mask_f_q2q4[mask_f_q2q4].index)

                        # 正确的季度化计算：当前累计值 - 前一季度累计值 = 当季单季值
                        single_vals = cur_cum_vals - pd.to_numeric(prev_cum_vals_series, errors='coerce')
                        valid_single = single_vals.notna()

                        if valid_single.any():
                            work.loc[mask_f_q2q4 & valid_single, c] = single_vals[valid_single]
                            work.loc[mask_f_q2q4 & valid_single, 'conversion_status'] = 'QTR_DIFF_FC'
                        else:
                            # 对 forecast，当无法获得上一季报告累计时，保留中值（仅限净利润归母）
                            if c == 'n_income_attr_p':
                                work.loc[mask_f_q2q4, c] = cur_cum_vals
                                work.loc[mask_f_q2q4, 'conversion_status'] = 'FC_MID_KEEP'
                            else:
                                work.loc[mask_f_q2q4, c] = None
                                affected_calculation += mask_f_q2q4.sum()

                quarterly_stats['forecast']['affected_single_calculation'] = affected_calculation

            self.logger.info(f"forecast 季度化处理完成")

        # 清理临时列
        work.drop(columns=['prev_end_date'], inplace=True, errors='ignore')

        # 打印季度化统计信息汇总
        self._print_quarterly_stats_summary(quarterly_stats)

        return work

    def _print_quarterly_stats_summary(self, quarterly_stats: Dict[str, Dict[str, Any]]) -> None:
        """
        打印季度化统计信息汇总

        Args:
            quarterly_stats: 季度化统计信息字典
        """
        self.logger.info("=" * 80)
        self.logger.info("📊 季度化统计信息汇总")
        self.logger.info("=" * 80)

        total_affected = 0
        total_missing_cumulative = 0

        for source in ['report', 'express', 'forecast']:
            stats = quarterly_stats[source]

            if stats['total_records'] == 0:
                continue

            self.logger.info(f"\n🔸 {source.upper()} 数据季度化统计:")

            # 基本信息
            self.logger.info(f"  📋 总记录数: {stats['total_records']:,} 条")
            self.logger.info(f"  🎯 Q2-Q4记录数: {stats['q2q4_records']:,} 条")

            if source == 'report':
                self.logger.info(f"  📅 仅年报记录数: {stats['annual_only_records']:,} 条")

            # 累计值缺失统计
            if stats['missing_prev_cumulative'] > 0:
                self.logger.info(f"  ⚠️  上一季度累计值缺失: {stats['missing_prev_cumulative']:,} 条记录")
                total_missing_cumulative += stats['missing_prev_cumulative']

            # 单季计算影响统计
            if stats['affected_single_calculation'] > 0:
                self.logger.info(f"  ❌ 单季计算受影响: {stats['affected_single_calculation']:,} 个字段值")
                total_affected += stats['affected_single_calculation']

                if source == 'report' and stats['field_affected_breakdown']:
                    self.logger.info("  📈 各字段影响明细:")
                    for field, count in stats['field_affected_breakdown'].items():
                        if count > 0:
                            self.logger.info(f"    • {field}: {count:,} 条记录")

        # 总体影响统计
        if total_missing_cumulative > 0 or total_affected > 0:
            self.logger.info(f"\n🎯 总体影响统计:")
            self.logger.info(f"  🔍 累计值缺失总影响: {total_missing_cumulative:,} 条记录")
            self.logger.info(f"  💥 单季计算总影响: {total_affected:,} 个字段值")

            # 计算影响比例
            total_q2q4_records = sum(quarterly_stats[s]['q2q4_records'] for s in ['express', 'forecast'])
            if total_q2q4_records > 0:
                missing_ratio = total_missing_cumulative / total_q2q4_records * 100
                self.logger.info(".2f")
            # 特别关注 report 单季计算影响
            report_q2q4 = quarterly_stats['report']['q2q4_records']
            if report_q2q4 > 0:
                report_affected = quarterly_stats['report']['affected_single_calculation']
                if report_affected > 0:
                    report_ratio = report_affected / (report_q2q4 * len(quarterly_stats['report']['field_affected_breakdown'])) * 100
                    self.logger.info(".2f")
                else:
                    self.logger.info("  ✅ Report数据无单季计算影响")
            else:
                self.logger.info("  ✅ 无明显数据影响")
        self.logger.info("=" * 80)


    def _fill_forecast_profit_proxies(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        为 forecast 数据填充利润代理字段。
        使用 n_income_attr_p (归母净利) 作为 n_income, total_profit 的代理值。
        这是一种在缺少更详细预告数据时的合理近似。
        """
        if df is None or df.empty or 'data_source' not in df.columns:
            return df

        work = df.copy()

        proxy_fields = ['n_income', 'total_profit']
        # 确保代理字段存在
        for f in proxy_fields:
            if f not in work.columns:
                work[f] = None

        mask_forecast = work['data_source'] == 'forecast'
        if not mask_forecast.any():
            return work

        # 仅对存在归母净利预告的行进行处理
        mask_source_profit = work['n_income_attr_p'].notna()

        # 目标是 forecast 数据，且有归母净利，且任一代理字段为空
        mask_target = mask_forecast & mask_source_profit & work[proxy_fields].isna().any(axis=1)

        if mask_target.any():
            self.logger.info(f"为 {mask_target.sum()} 条 forecast 记录填充利润代理字段")

            # 方案1：直接在同一个循环中处理所有代理字段
            # n_income（净利润）和 total_profit（利润总额）都使用归母净利作为代理值
            # 这是合理的近似，因为在业绩预告中通常只披露归母净利
            for field in proxy_fields:
                # 仅填充空值
                work.loc[mask_target, field] = work.loc[mask_target, field].fillna(work.loc[mask_target, 'n_income_attr_p'])

            # 更新状态标记
            if 'conversion_status' in work.columns:
                work.loc[mask_target, 'conversion_status'] = 'FC_PROFIT_PROXY'

        return work


    def _fill_missing_from_report(self, df: pd.DataFrame, prefer_batch_report: bool = False, batch_report_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """对 express/forecast 缺失关键字段进行填充：
        - 批次内优先（如 prefer_batch_report=True 且 df 中包含 report 记录）
        - 其次历史 PIT 回看（PIT 原则：report.ann_date ≤ target.ann_date）
        关键字段：revenue, oper_cost, operate_profit, total_profit
        【优化】使用批量查询和缓存机制替代循环查询。
        """

        if df is None or df.empty:
            return df
        if 'data_source' not in df.columns:
            return df
        work = df.copy()
        # 目标行（express/forecast 且任一关键字段缺失）
        # 扩展key_fields，加入total_profit字段（可通过历史PIT数据填充）
        key_fields = ['revenue','oper_cost','operate_profit','total_profit']
        for k in key_fields:
            if k not in work.columns:
                work[k] = None
        mask_target = work['data_source'].isin(['express','forecast']) & (
            work['revenue'].isna() | work['oper_cost'].isna() | work['operate_profit'].isna() | work['total_profit'].isna()
        )
        if not mask_target.any():
            return work

        total_fill_count = 0

        # 1) 批次内优先使用 report（同 ts_code,end_date，取最近 ann_date）
        if prefer_batch_report:
            # 优先使用显式传入的 batch_report_df；否则尝试从 df 中提取 report 行
            r_source = None
            if batch_report_df is not None and not batch_report_df.empty:
                r_source = batch_report_df.copy()
            elif (work.get('data_source') == 'report').any():
                r_source = work[work['data_source']=='report'].copy()
            if r_source is not None and not r_source.empty:
                available_keys = [k for k in key_fields if k in r_source.columns]
                if available_keys:
                    r = r_source[['ts_code','end_date','ann_date'] + available_keys].copy()
                    # 【关键修复】按ann_date排序，确保选择最新ann_date的记录
                    r.sort_values(['ts_code','end_date','ann_date'], inplace=True)
                    r = r.drop_duplicates(['ts_code','end_date'], keep='last')
                    # 使用前一季度的数据进行填充，避免同报告期数据导致的PIT筛选为空
                    target_for_merge = work.loc[mask_target, ['ts_code','end_date','ann_date']].copy()
                    target_for_merge['end_date_prev'] = target_for_merge['end_date'].apply(
                        lambda d: self._prev_quarter_end(d, 1)
                    )
                    merged = target_for_merge.merge(
                        r, left_on=['ts_code','end_date_prev'], right_on=['ts_code','end_date'],
                        how='left', suffixes=('','_r')
                    )
                    # 仅使用 PIT 合法记录
                    merged = merged[merged['ann_date_r'] <= merged['ann_date']]

                    # 【关键修复】在多条匹配记录中，选择ann_date_r最新的记录
                    # 按 ts_code, end_date(当前), ann_date(当前), ann_date_r 排序，保留最新的记录
                    merged = merged.sort_values(['ts_code','end_date','ann_date','ann_date_r'])
                    merged = merged.drop_duplicates(['ts_code','end_date','ann_date'], keep='last')

                    # 批量更新结果
                    if not merged.empty:
                        # 【关键修复】使用向量化的方式更新，避免索引错位问题
                        # 方法：直接在merged结果上进行更新，确保数据完整性

                        batch_fill_count = 0

                        # 为merged添加原始work的索引信息
                        # merged的行顺序与mask_target不一定一致，需要重新建立对应关系
                        work_target_data = work.loc[mask_target, ['ts_code','end_date','ann_date']].copy()
                        work_target_data['work_idx'] = work_target_data.index

                        # 合并work索引信息与匹配结果
                        final_merge = work_target_data.merge(
                            merged[['ts_code','end_date','ann_date'] + available_keys],
                            left_on=['ts_code','end_date'],
                            right_on=['ts_code','end_date'],
                            how='left',
                            suffixes=('', '_matched')
                        )

                        # 向量化更新：直接使用loc进行批量赋值
                        valid_matches = final_merge['ann_date_matched'].notna()
                        if valid_matches.any():
                            matched_rows = final_merge[valid_matches]

                            for _, row in matched_rows.iterrows():
                                work_idx = row['work_idx']
                                for k in available_keys:
                                    if pd.isna(work.at[work_idx, k]) and pd.notna(row.get(k)):
                                        # 【修复】数据类型转换
                                        value = row[k]
                                        try:
                                            # 对于数值字段，进行类型转换
                                            if k in self.data_fields and k != 'conversion_status':
                                                work.at[work_idx, k] = pd.to_numeric(value, errors='coerce')
                                            else:
                                                # 对于其他字段，直接赋值
                                                work.at[work_idx, k] = value
                                            batch_fill_count += 1
                                        except Exception as e:
                                            self.logger.warning(f"数据类型转换失败 idx={work_idx}, field={k}, value={value}: {e}")
                                            continue

                        total_fill_count += batch_fill_count
                        if batch_fill_count:
                            self.logger.info(f"批次内report回填完成：填充 {batch_fill_count} 个字段值")

        # 2) 历史 PIT 回看（从 PIT 收益表读取 report）【修复：扩展查询范围】
        remain = work['data_source'].isin(['express','forecast']) & (
            work['revenue'].isna() | work['oper_cost'].isna() | work['operate_profit'].isna()
        )
        if remain.any():
            target = work.loc[remain, ['ts_code','end_date','ann_date']].drop_duplicates()
            if not target.empty:
                # 构造回填优先级：当前季度→前一季度→前二季度（end_date 维度）
                target = target.copy()
                target['end_prev1'] = target['end_date'].apply(lambda d: self._prev_quarter_end(d, 1))
                target['end_prev2'] = target['end_date'].apply(lambda d: self._prev_quarter_end(d, 2))
                ts_list = sorted(target['ts_code'].unique().tolist())

                # 【关键修复】扩展查询范围：考虑所有可能需要的report数据
                # 1. 基于当前需要填充的数据的ann_date范围
                min_ann = target['ann_date'].min(); max_ann = target['ann_date'].max()
                # 2. 基于所有end_date的最小值（包括前几季度）
                all_end_dates = pd.concat([
                    target['end_date'],
                    target['end_prev1'].dropna(),
                    target['end_prev2'].dropna()
                ])
                min_end_date = all_end_dates.min()

                # 使用更宽泛的查询范围
                lookback_months = PITConfig.PIT_TABLES['pit_income_quarterly'].get('INCOME_EXPRESS_FILL_LOOKBACK_MONTHS', 9)
                from dateutil.relativedelta import relativedelta

                # 基于ann_date的查询下限
                lower_bound_ann = (pd.to_datetime(min_ann) - relativedelta(months=lookback_months)).date()
                # 基于end_date的查询下限（更早）
                lower_bound_end = (pd.to_datetime(min_end_date) - relativedelta(months=6)).date()
                # 取两者中更早的日期作为查询下限
                lower_bound = min(lower_bound_ann, lower_bound_end)

                self.logger.info(f"历史PIT回看查询范围: {lower_bound} ~ {max_ann}")

                # 【优化】使用批量查询缓存
                ref = self._batch_query_historical_data(ts_list, str(lower_bound), str(max_ann))

                if ref is not None and not ref.empty:
                    # 批量处理优先级匹配逻辑
                    def _apply_fill_batch(level_end_col: str):
                        """批量应用填充逻辑 - 确保匹配最新ann_date的记录"""
                        merged = target.merge(ref, left_on=['ts_code', level_end_col], right_on=['ts_code','end_date'], how='left', suffixes=('','_r'))

                        # 应用PIT原则：历史报告的ann_date <= 当前记录的ann_date
                        merged = merged[merged['ann_date_r'] <= merged['ann_date']]

                        if merged.empty:
                            return 0

                        # 【关键修复】在多条匹配记录中，选择ann_date_r最新的记录
                        # 按 ts_code, ann_date(当前), level_end_col 分组，保留ann_date_r最大的记录
                        merged = merged.sort_values(['ts_code','ann_date', level_end_col, 'ann_date_r'])
                        merged = merged.drop_duplicates(['ts_code','ann_date', level_end_col], keep='last')

                        # 构建索引映射
                        idx_map = {(row.ts_code, row.ann_date): j for j, row in merged.iterrows()}

                        filled = 0
                        # 向量化更新
                        for idx, row in work.loc[remain].iterrows():
                            key = (row['ts_code'], row['ann_date'])
                            j = idx_map.get(key)
                            if j is None:
                                continue
                            for k in key_fields:
                                if pd.isna(work.at[idx, k]) and k in merged.columns and pd.notna(merged.at[j, k]):
                                    # 【修复】数据类型转换，确保兼容性
                                    value = merged.at[j, k]
                                    try:
                                        # 统一对所有字段进行智能类型转换
                                        if k == 'conversion_status':
                                            # 转换状态字段保持原样
                                            work.at[idx, k] = value
                                        else:
                                            # 数值字段统一转换为numeric
                                            work.at[idx, k] = pd.to_numeric(value, errors='coerce')
                                        filled += 1
                                    except Exception as e:
                                        self.logger.warning(f"数据类型转换失败 idx={idx}, field={k}, value={value}: {e}")
                                        continue
                        return filled

                    # 先当前季度 end_date 匹配，再前一季度 end_prev1，再前二季度 end_prev2
                    fill_current = _apply_fill_batch('end_date')
                    total_fill_count += fill_current

                    # 检查是否还有需要填充的记录
                    still_remain = work['data_source'].isin(['express','forecast']) & (
                        work['revenue'].isna() | work['oper_cost'].isna() | work['operate_profit'].isna()
                    )
                    if still_remain.any():
                        fill_prev1 = _apply_fill_batch('end_prev1')
                        total_fill_count += fill_prev1

                        still_remain = work['data_source'].isin(['express','forecast']) & (
                            work['revenue'].isna() | work['oper_cost'].isna() | work['operate_profit'].isna()
                        )
                        if still_remain.any():
                            fill_prev2 = _apply_fill_batch('end_prev2')
                            total_fill_count += fill_prev2

                    if total_fill_count:
                        self.logger.info(f"历史PIT回填（批量查询+季度优先级）完成：共填充 {total_fill_count} 个字段值")

        if total_fill_count:
            self.logger.info(f"_fill_missing_from_report 总计填充：{total_fill_count} 个字段值")

        return work
    @staticmethod
    def _prev_quarter_end(dt: pd.Timestamp | Any, k: int = 1) -> Any:
        """返回 dt 往前第 k 个季度的季末日期（保持 3/31, 6/30, 9/30, 12/31 格式）。
        输入可以是 datetime.date 或可被 to_datetime 解析的对象。
        """
        if pd.isna(dt):
            return None
        d = pd.to_datetime(dt)
        # 直接用 month 映射季度末
        q_map = {1: (3,31), 2: (6,30), 3: (9,30), 4: (12,31)}
        # 当前季度号
        q = ((d.month - 1)//3) + 1
        # 目标季度号（往前 k 个季度）
        tq = ((q - k - 1) % 4) + 1
        # 年份调整
        year_shift = (q - k - 1) // 4
        y = int(d.year + year_shift)
        m, day = q_map[tq]
        try:
            return pd.Timestamp(year=y, month=m, day=day).date()
        except Exception:
            return None


    def _load_bs_snapshot(self, keys_df: pd.DataFrame) -> pd.DataFrame:
        """按PIT原则为每个目标 (ts_code, end_date, ann_date) 回看前一期资产负债快照。
        返回列绑定到目标 end_date（作为匹配键），但选择 b.end_date < 目标end_date 且 b.ann_date <= 目标ann_date 中最接近的一条。
        """
        if keys_df is None or keys_df.empty:
            return pd.DataFrame()
        keys = keys_df[['ts_code','end_date','ann_date']].drop_duplicates()
        values_rows = ",\n            ".join([
            f"('{r.ts_code}', DATE '{pd.to_datetime(r.end_date).date()}', DATE '{pd.to_datetime(r.ann_date).date()}')"
            for r in keys.itertuples(index=False)
        ])
        sql = f"""
        WITH req(ts_code, target_end_date, target_ann_date) AS (
            VALUES {values_rows}
        ), joined AS (
            SELECT r.ts_code,
                   r.target_end_date,
                   b.end_date AS bs_end_date,
                   b.ann_date AS bs_ann_date,
                   b.total_hldr_eqy_exc_min_int,
                   b.minority_int
            FROM {PITConfig.TUSHARE_SCHEMA}.fina_balancesheet b
            JOIN req r ON r.ts_code=b.ts_code
            WHERE b.end_date < r.target_end_date::date
              AND b.ann_date <= r.target_ann_date::date
        ), ranked AS (
            SELECT ts_code, target_end_date,
                   total_hldr_eqy_exc_min_int, minority_int,
                   ROW_NUMBER() OVER (PARTITION BY ts_code, target_end_date ORDER BY bs_end_date DESC, bs_ann_date DESC) AS rn
            FROM joined
        )
        SELECT ts_code, target_end_date AS end_date, total_hldr_eqy_exc_min_int, minority_int
        FROM ranked WHERE rn=1
        """
        try:
            snap = self.context.query_dataframe(sql)
            return snap if snap is not None else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def _sanitize_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        参数清洗:
        - 将 NaN/NaT/pd.NA 转换为 None，避免数据库 numeric/date 列写入异常
        - 对 conversion_status 做安全截断（最长 20 字符），防御未来新增状态过长
        """
        cleaned = {}
        for k, v in params.items():
            try:
                # pandas 的缺失值统一转 None
                if v is None:
                    cleaned[k] = None
                else:
                    # 对于 numpy/pandas NA/NaT
                    if isinstance(v, float) and pd.isna(v):
                        cleaned[k] = None
                    elif hasattr(pd, 'isna') and pd.isna(v):
                        cleaned[k] = None
                    else:
                        cleaned[k] = v
            except Exception:
                cleaned[k] = v
        # conversion_status 安全截断
        cs = cleaned.get('conversion_status')
        if cs is not None:
            s = str(cs)
            if len(s) > 20:
                self.logger.warning("conversion_status 超长(%d)，已截断: %s", len(s), s)
                s = s[:20]
            cleaned['conversion_status'] = s
        return cleaned


    def _batch_upsert_to_pit(self, data: pd.DataFrame, batch_size: int) -> Dict[str, int]:
        """批量UPSERT数据到PIT表 【优化：并行处理+动态批次】"""

        if data.empty:
            return {'inserted': 0, 'updated': 0, 'errors': 0}

        self.logger.info(f"开始批量UPSERT: {len(data)} 条记录")

        inserted_count = 0
        updated_count = 0
        error_count = 0

        # 构建UPSERT SQL（动态扩展 year/quarter/net_profit_mid/total_profit/n_income 等额外列）
        pit_cols = self._get_table_columns(PITConfig.PIT_SCHEMA, self.table_name)
        # conversion_status 为可选持久化字段：若目标表缺列则记录告警但不中断
        if 'conversion_status' in data.columns and 'conversion_status' not in pit_cols:
            self.logger.warning("目标表缺少 conversion_status 列，转换标记将不会被持久化。建议为 pgs_factors.%s 添加该列以便追踪数据转换状态。", self.table_name)
        extra_candidates = ['year','quarter','net_profit_mid','total_profit','n_income','conversion_status']
        extras = [c for c in extra_candidates if c in pit_cols and c in data.columns]
        all_fields = self.key_fields + self.data_fields + extras + ['data_source']
        field_list = ', '.join(all_fields)
        placeholder_list = ', '.join([f'%({field})s' for field in all_fields])

        # 更新字段列表 (排除主键)
        update_fields = [f for f in all_fields if f not in ['ts_code', 'end_date', 'ann_date']]
        update_list = ', '.join([f'{field} = EXCLUDED.{field}' for field in update_fields])

        upsert_sql = f"""
        INSERT INTO {PITConfig.PIT_SCHEMA}.{self.table_name} ({field_list})
        VALUES ({placeholder_list})
        ON CONFLICT (ts_code, end_date, ann_date, data_source) DO UPDATE SET
        {update_list},
        updated_at = CURRENT_TIMESTAMP
        """

        # 【优化】分批处理（顺序：report -> express -> forecast），使用动态批次大小
        try:
            # 动态计算最优批次大小
            data_size = len(data)
            ts_code_count = data['ts_code'].nunique() if 'ts_code' in data.columns else 0
            optimal_batch_size = self._calculate_optimal_batch_size(data_size, ts_code_count)

            STOCKS_PER_BATCH = max(int(batch_size or optimal_batch_size), 1)
            MAX_RECORDS_PER_BATCH = int(PITConfig.PIT_TABLES.get(self.table_name, {}).get('MAX_RECORDS_PER_BATCH', 10000))

            # 确保稳定排序（与预处理阶段一致）
            data_loc = data.copy()
            if 'data_source' in data_loc.columns:
                cat_type = pd.CategoricalDtype(categories=['report','express','forecast'], ordered=True)
                try:
                    data_loc['data_source'] = data_loc['data_source'].astype(cat_type)
                except Exception:
                    pass
            try:
                data_loc = data_loc.sort_values(['ann_date','data_source','ts_code','end_date'] if 'ann_date' in data_loc.columns else (['data_source','ts_code','end_date'] if 'data_source' in data_loc.columns else ['ts_code','end_date']))
            except Exception:
                pass

            # 规划批次：按 ts_code 分桶，每桶至多 STOCKS_PER_BATCH 只股票
            planned_batches = []
            ts_codes = list(dict.fromkeys(data_loc['ts_code'].tolist()))  # 保序去重
            idx = 0
            while idx < len(ts_codes):
                take = STOCKS_PER_BATCH
                # 动态缩减，直到记录数不超过上限或最少1只股票
                while take > 0:
                    sel = ts_codes[idx: idx + take]
                    batch_df = data_loc[data_loc['ts_code'].isin(sel)]
                    if len(batch_df) <= MAX_RECORDS_PER_BATCH or take == 1:
                        if not batch_df.empty:
                            planned_batches.append(batch_df)
                        idx += take
                        break
                    take = max(take // 2, 1)

            self.logger.info(f"采用动态批次策略：每批 {STOCKS_PER_BATCH} 只股票，单批最大 {MAX_RECORDS_PER_BATCH} 条，共 {len(planned_batches)} 批")

        except Exception as plan_err:
            self.logger.warning(f"动态分批规划失败，回退为固定窗口分批：{plan_err}")
            # 回退逻辑：保底，避免中断
            planned_batches = [data.iloc[i:i + 10000] for i in range(0, len(data), 10000)]

        # 【优化】并行处理批次
        def process_single_batch(batch_idx_and_data):
            """处理单个批次的函数（用于并行执行）"""
            b_idx, batch_data = batch_idx_and_data
            batch_inserted = 0
            batch_updated = 0
            batch_errors = 0

            try:
                self.logger.info(f"并行处理批次 {b_idx}: {len(batch_data)} 条记录")

                # 创建批次内数据源掩码
                if 'data_source' in batch_data.columns:
                    mask_r = batch_data['data_source'].eq('report')
                    mask_e = batch_data['data_source'].eq('express')
                    mask_f = batch_data['data_source'].eq('forecast')
                else:
                    mask_r = pd.Series([False]*len(batch_data), index=batch_data.index)
                    mask_e = pd.Series([False]*len(batch_data), index=batch_data.index)
                    mask_f = pd.Series([False]*len(batch_data), index=batch_data.index)

                # 1) report 先写入
                r_batch = batch_data.loc[mask_r]
                if not r_batch.empty:
                    self.logger.debug(f"批次 {b_idx}: 先写入 report {len(r_batch)} 条")
                    r_res = self._upsert_batch(upsert_sql, r_batch, all_fields)
                    batch_inserted += r_res['inserted']
                    batch_updated += r_res['updated']

                # 2) express 预填充后写入
                e_batch = batch_data.loc[mask_e]
                if not e_batch.empty:
                    e_prepared = self._enrich_express_parent_profit(pd.concat([r_batch, e_batch], ignore_index=True))
                    e_prepared = e_prepared[e_prepared.get('data_source').eq('express')]
                    e_prepared = self._fill_missing_from_report(e_prepared, prefer_batch_report=True, batch_report_df=r_batch)
                    self.logger.debug(f"批次 {b_idx}: 再写入 express {len(e_prepared)} 条（已执行增强+填充）")
                    e_res = self._upsert_batch(upsert_sql, e_prepared, all_fields)
                    batch_inserted += e_res['inserted']
                    batch_updated += e_res['updated']

                # 3) forecast 最后写入
                f_batch = batch_data.loc[mask_f]
                if not f_batch.empty:
                    f_prepared = self._fill_missing_from_report(f_batch, prefer_batch_report=True, batch_report_df=r_batch)
                    self.logger.debug(f"批次 {b_idx}: 写入 forecast {len(f_prepared)} 条（已执行填充）")
                    f_res = self._upsert_batch(upsert_sql, f_prepared, all_fields)
                    batch_inserted += f_res['inserted']
                    batch_updated += f_res['updated']

            except Exception as e:
                self.logger.error(f"批次 {b_idx} 处理失败: {e}")
                batch_errors += len(batch_data)

            return {
                'batch_idx': b_idx,
                'inserted': batch_inserted,
                'updated': batch_updated,
                'errors': batch_errors
            }

        # 并行执行所有批次
        max_workers = min(4, len(planned_batches))  # 最多4个并行工作线程
        if max_workers > 1:
            self.logger.info(f"启用并行处理：{max_workers} 个工作线程")

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 准备批次数据（包含索引）
                batch_tasks = [(i+1, batch_data) for i, batch_data in enumerate(planned_batches)]
                batch_results = list(executor.map(process_single_batch, batch_tasks))

            # 汇总结果
            for result in batch_results:
                inserted_count += result['inserted']
                updated_count += result['updated']
                error_count += result['errors']
        else:
            # 串行处理（批次较少时）
            for b_idx, batch_data in enumerate(planned_batches, start=1):
                result = process_single_batch((b_idx, batch_data))
                inserted_count += result['inserted']
                updated_count += result['updated']
                error_count += result['errors']

        self.logger.info(f"批量UPSERT完成: 新增 {inserted_count}, 更新 {updated_count}, 错误 {error_count}")
        return {
            'inserted': inserted_count,
            'updated': updated_count,
            'errors': error_count
        }

    def _upsert_batch(self, upsert_sql: str, batch_data: pd.DataFrame, all_fields: List[str]) -> Dict[str, int]:
        """处理单个批次的UPSERT"""

        inserted_count = 0
        updated_count = 0

        for _, row in batch_data.iterrows():
            try:
                # 准备参数（统一清洗，避免 NaN/超长状态等问题）
                params = {field: row[field] for field in all_fields}
                params = self._sanitize_params(params)

                # 检查是否为更新操作
                existing_check = self.context.query_dataframe(
                    f"SELECT 1 FROM {PITConfig.PIT_SCHEMA}.{self.table_name} "
                    f"WHERE ts_code = %s AND end_date = %s AND ann_date = %s AND data_source = %s",
                    (row['ts_code'], row['end_date'], row['ann_date'], row['data_source'])
                )

                is_update = existing_check is not None and not existing_check.empty

                # 执行UPSERT
                self.context.db_manager.execute_sync(upsert_sql, params)

                if is_update:
                    updated_count += 1
                else:
                    inserted_count += 1

            except Exception as e:
                self.logger.error(f"UPSERT记录失败 {row['ts_code']}-{row['end_date']}-{row['ann_date']}: {e}")
                continue

        return {'inserted': inserted_count, 'updated': updated_count}

def main():
    """主函数 - 命令行接口"""

    parser = argparse.ArgumentParser(description='PIT利润表管理器')
    parser.add_argument('--mode', choices=['full-backfill', 'incremental', 'single-backfill'],
                       help='执行模式')
    parser.add_argument('--start-date', help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='结束日期 (YYYY-MM-DD)')
    parser.add_argument('--days', type=int, help='增量更新天数')
    parser.add_argument('--batch-size', type=int, help='每批股票数（按 ts_code 分批）')
    parser.add_argument('--status', action='store_true', help='显示表状态')
    parser.add_argument('--validate', action='store_true', help='验证数据完整性')
    parser.add_argument('--ts-code', help='指定单股 ts_code（如 600000.SH），用于 single-backfill 模式')

    args = parser.parse_args()

    # 初始化统一日志（方案C）：控制台 +（可选）文件
    try:
        from alphahome.common.logging_utils import setup_logging
        # 文件名按表名区分，避免混淆
        log_fn = f"pit_income_quarterly_{datetime.now().strftime('%Y%m%d')}.log"
        setup_logging(log_level="INFO", log_to_file=True, log_dir="logs", log_filename=log_fn)
    except Exception:
        # 忽略日志初始化异常，继续执行
        pass

    print("📊 PIT利润表管理器")
    print("=" * 60)

    try:
        with PITIncomeQuarterlyManager() as manager:

            # 显示表状态
            if args.status:
                print("📈 表状态:")
                status = manager.get_table_status()
                for key, value in status.items():
                    print(f"  {key}: {value}")
                return 0

            # 仅当未指定 mode 时，单独执行全表验证
            if args.validate and not args.mode:
                print("🔍 数据完整性验证:")
                validation = manager.validate_data_integrity()
                print(f"  总体状态: {validation['overall_status']}")
                print(f"  发现问题: {validation['issues_found']} 个")
                for check in validation['checks']:
                    status_icon = "✅" if check['status'] == 'passed' else "❌"
                    print(f"  {status_icon} {check['check_name']}: {check['message']}")
                return 0

            # 执行主要功能
            if args.mode and args.mode == 'full-backfill':
                result = manager.full_backfill(
                    start_date=args.start_date,
                    end_date=args.end_date,
                    batch_size=args.batch_size
                )
            elif args.mode and args.mode == 'incremental':
                result = manager.incremental_update(
                    days=args.days,
                    batch_size=args.batch_size
                )
            elif args.mode and args.mode == 'single-backfill':
                result = manager.single_backfill(
                    ts_code=args.ts_code,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    batch_size=args.batch_size,
                    do_validate=args.validate
                )

            if args.mode:
                print(f"\n✅ 执行结果:")
                for key, value in result.items():
                    print(f"  {key}: {value}")

                # 若指定了 validate 且非单股模式，则在执行后进行全表数据验证
                if args.validate and args.mode != 'single-backfill':
                    print("\n🔍 执行后数据完整性验证:")
                    validation = manager.validate_data_integrity()
                    print(f"  总体状态: {validation['overall_status']}")
                    print(f"  发现问题: {validation['issues_found']} 个")
                    for check in validation['checks']:
                        status_icon = "✅" if check['status'] == 'passed' else "❌"
                        print(f"  {status_icon} {check['check_name']}: {check['message']}")

                return 0 if 'error' not in result else 1

            return 0

    except Exception as e:
        print(f"\n❌ 执行失败: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
