#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PIT资产负债表管理器
==================

负责pit_balance_quarterly表的历史全量回填和增量更新

功能特点:
1. 从tushare.fina_balancesheet同步数据到PIT表
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

# 添加项目路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from .base.pit_table_manager import PITTableManager
    from .base.pit_config import PITConfig
except ImportError:
    from base.pit_table_manager import PITTableManager
    from base.pit_config import PITConfig

class PITBalanceQuarterlyManager(PITTableManager):
    """PIT资产负债表管理器"""

    def __init__(self):
        super().__init__('pit_balance_quarterly')

        # 资产负债表特定配置
        self.tushare_table = self.table_config['tushare_table']
        self.key_fields = self.table_config['key_fields']
        self.data_fields = self.table_config['data_fields']

    def full_backfill(self,
                     start_date: str = None,
                     end_date: str = None,
                     batch_size: int = None,
                     run_fix_after: bool = False) -> Dict[str, Any]:
        """
        历史全量回填

        Args:
            start_date: 开始日期 (ann_date)
            end_date: 结束日期 (ann_date)
            batch_size: 批次大小

        Returns:
            执行结果统计
        """
        self.logger.info("开始PIT资产负债表历史全量回填")

        # 设置默认参数
        if start_date is None or end_date is None:
            start_date, end_date = PITConfig.get_backfill_date_range(start_date, end_date)

        if batch_size is None:
            batch_size = self.batch_size

        self.logger.info(f"回填日期范围: {start_date} ~ {end_date}")
        self.logger.info(f"批次大小: {batch_size}")

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

            # 2.9 确保唯一键包含 data_source（幂等迁移）
            self._ensure_balance_unique_keys()

            # 2.95 确保索引
            self.ensure_indexes()

            # 3. 批量插入PIT表
            result = self._batch_upsert_to_pit(processed_data, batch_size)

            self.logger.info(f"历史回填完成: 新增 {result['inserted']}, 更新 {result['updated']}")

            out = {
                'backfilled_records': result['inserted'] + result['updated'],
                'inserted_records': result['inserted'],
                'updated_records': result['updated'],
                'error_records': result['errors'],
                'message': f'成功回填 {result["inserted"] + result["updated"]} 条记录'
            }
            if run_fix_after:
                fixed = self.fix_missing_express_fields(start_date=start_date, end_date=end_date)
                out['fix_missing_express_fields'] = fixed
            return out

        except Exception as e:
            self.logger.error(f"历史回填失败: {e}")
            return {
                'backfilled_records': 0,
                'error': str(e),
                'message': '历史回填失败'
            }

    def incremental_update(self,
                          days: int = None,
                          batch_size: int = None,
                          run_fix_after: bool = False) -> Dict[str, Any]:
        """
        增量更新

        Args:
            days: 增量更新天数
            batch_size: 批次大小

        Returns:
            执行结果统计
        """
        self.logger.info("开始PIT资产负债表增量更新")

        # 设置默认参数
        if days is None:
            days = PITConfig.DEFAULT_DATE_RANGES['incremental_days']

        if batch_size is None:
            batch_size = self.batch_size

        # 计算增量更新日期范围
        start_date, end_date = PITConfig.get_incremental_date_range(days)

        self.logger.info(f"增量更新日期范围: {start_date} ~ {end_date}")
        self.logger.info(f"批次大小: {batch_size}")

        try:
            # 1. 获取增量数据
            incremental_data = self._fetch_tushare_data(start_date, end_date)

            if incremental_data.empty:
                self.logger.info("未找到需要更新的数据")
                return {'updated_records': 0, 'message': '无数据需要更新'}

            self.logger.info(f"从tushare获取到 {len(incremental_data)} 条增量数据")

            # 2. 数据预处理
            processed_data = self._preprocess_data(incremental_data)

            # 2.9 确保唯一键包含 data_source（幂等迁移）
            self._ensure_balance_unique_keys()

            # 2.95 确保索引
            self.ensure_indexes()

            # 3. 批量更新PIT表
            result = self._batch_upsert_to_pit(processed_data, batch_size)

            self.logger.info(f"增量更新完成: 新增 {result['inserted']}, 更新 {result['updated']}")

            out = {
                'updated_records': result['inserted'] + result['updated'],
                'inserted_records': result['inserted'],
                'updated_records': result['updated'],
                'error_records': result['errors'],
                'message': f'成功更新 {result["inserted"] + result["updated"]} 条记录'
            }
            if run_fix_after:
                fixed = self.fix_missing_express_fields(days=days)
                out['fix_missing_express_fields'] = fixed
            return out

        except Exception as e:
            self.logger.error(f"增量更新失败: {e}")
            return {
                'updated_records': 0,
                'error': str(e),
                'message': '增量更新失败'
            }

    def _fetch_tushare_data(self, start_date: str, end_date: str, ts_code: Optional[str] = None) -> pd.DataFrame:
        """从tushare获取资产负债表数据
        - 可选 ts_code 过滤以支持单股历史回填
        """

        # 构建查询字段 - 使用tushare表的字段名（快报与报表字段名不完全一致）
        bs_fields = self.key_fields + ['total_assets', 'total_liab', 'total_hldr_eqy_exc_min_int']
        ex_fields = self.key_fields + ['total_assets', 'total_hldr_eqy_exc_min_int']  # fina_express 无 total_liab
        bs_field_list = ', '.join(bs_fields)
        ex_field_list = ', '.join(ex_fields)

        # 统一从两个数据表读取：fina_balancesheet(报告) + fina_express(快报)
        bs_query = f"""
        SELECT {bs_field_list}
        FROM {PITConfig.TUSHARE_SCHEMA}.fina_balancesheet
        WHERE ann_date >= %s AND ann_date <= %s
          AND ts_code IS NOT NULL AND end_date IS NOT NULL
        """
        ex_query = f"""
        SELECT {ex_field_list}
        FROM {PITConfig.TUSHARE_SCHEMA}.fina_express
        WHERE ann_date >= %s AND ann_date <= %s
          AND ts_code IS NOT NULL AND end_date IS NOT NULL
        """

        # 单股过滤
        if ts_code:
            bs_query += " AND ts_code = %s"
            ex_query += " AND ts_code = %s"

        bs_query += " ORDER BY ts_code, end_date, ann_date"
        ex_query += " ORDER BY ts_code, end_date, ann_date"

        self.logger.info("查询 tushare.fina_balancesheet 与 tushare.fina_express ...")
        params = (start_date, end_date) + ((ts_code,) if ts_code else tuple())
        bs_df = self.context.query_dataframe(bs_query, params)
        ex_df = self.context.query_dataframe(ex_query, params)

        frames = []
        if bs_df is not None and not bs_df.empty:
            bs_df = bs_df.copy()
            bs_df['data_source'] = 'report'  # 表级别标注
            frames.append(bs_df)
        if ex_df is not None and not ex_df.empty:
            ex_df = ex_df.copy()
            # 对齐列：快报无 total_liab，补 None 以便后续统一处理
            if 'total_liab' not in ex_df.columns:
                ex_df['total_liab'] = None
            ex_df['data_source'] = 'express'  # 表级别标注
            frames.append(ex_df)

        if not frames:
            return pd.DataFrame()

        df = pd.concat(frames, ignore_index=True)
        return df

    def _preprocess_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """数据预处理"""

        if data.empty:
            return data

        self.logger.info(f"开始数据预处理: {len(data)} 条记录")

        processed_data = data.copy()

        # 1. 字段映射 (tushare字段名 -> PIT表字段名)
        field_mapping = {
            'total_assets': 'tot_assets',
            'total_liab': 'tot_liab',
            'total_hldr_eqy_exc_min_int': 'tot_equity'
        }

        # 应用字段映射
        for tushare_field, pit_field in field_mapping.items():
            if tushare_field in processed_data.columns:
                processed_data[pit_field] = processed_data[tushare_field]
                # 删除原字段
                processed_data = processed_data.drop(tushare_field, axis=1)

        # 若缺失 tot_liab，且具备 tot_assets 与 tot_equity，可用恒等式补齐：负债 = 资产 - 权益
        if 'tot_liab' not in processed_data.columns:
            if 'tot_assets' in processed_data.columns and 'tot_equity' in processed_data.columns:
                a = pd.to_numeric(processed_data['tot_assets'], errors='coerce')
                e = pd.to_numeric(processed_data['tot_equity'], errors='coerce')
                processed_data['tot_liab'] = (a - e).where(a.notna() & e.notna())

        # 1.5. data_source 已在取数阶段按表级别写入；若缺失则默认 report
        if 'data_source' not in processed_data.columns:
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

        # 计算year和quarter字段（从end_date提取）
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

        # 3. 数值字段处理
        for field in self.data_fields:
            if field in processed_data.columns:
                # 转换为数值类型，无效值设为None
                processed_data[field] = pd.to_numeric(processed_data[field], errors='coerce')

        # 3.5 针对 express 缺失字段进行基于 report 的 PIT 填充
        processed_data = self._fill_express_missing_fields(processed_data)

        # 4. 资产负债表特定的数据验证
        # 检查资产负债平衡关系
        if all(field in processed_data.columns for field in ['tot_assets', 'tot_liab', 'tot_equity']):
            # 计算资产负债差异
            processed_data['balance_check'] = (
                processed_data['tot_assets'] - processed_data['tot_liab'] - processed_data['tot_equity']
            ).abs()

            # 标记不平衡的记录
            imbalanced = processed_data['balance_check'] > processed_data['tot_assets'] * 0.01  # 1%容差
            if imbalanced.any():
                imbalanced_count = imbalanced.sum()
                self.logger.warning(f"发现 {imbalanced_count} 条资产负债不平衡的记录")

            # 移除balance_check列
            processed_data = processed_data.drop('balance_check', axis=1)

        # 5. 去重处理 (保留最新的ann_date)
        processed_data = processed_data.sort_values(['ts_code', 'end_date', 'ann_date'])
        processed_data = processed_data.drop_duplicates(
            subset=['ts_code', 'end_date', 'ann_date'],
            keep='last'
        )

        # 5. 数据源标识已在前面根据report_type设置

        # 6. 数据质量检查
        # 移除关键字段为空的记录
        before_count = len(processed_data)
        processed_data = processed_data.dropna(subset=['ts_code', 'end_date', 'ann_date'])
        after_count = len(processed_data)

        if before_count != after_count:
            self.logger.warning(f"移除了 {before_count - after_count} 条关键字段为空的记录")

        # 移除所有数据字段都为空的记录
        data_fields_mask = processed_data[self.data_fields].notna().any(axis=1)
        processed_data = processed_data[data_fields_mask]

        if len(processed_data) != after_count:
            self.logger.warning(f"移除了 {after_count - len(processed_data)} 条数据字段全为空的记录")

        self.logger.info(f"数据预处理完成: {len(processed_data)} 条记录")

        return processed_data

    def _batch_upsert_to_pit(self, data: pd.DataFrame, batch_size: int) -> Dict[str, int]:
        """批量UPSERT数据到PIT表"""

        if data.empty:
            return {'inserted': 0, 'updated': 0, 'errors': 0}

        self.logger.info(f"开始批量UPSERT: {len(data)} 条记录")

        inserted_count = 0
        updated_count = 0
        error_count = 0

        # 构建UPSERT SQL（动态扩展可选列：total_cur_assets/total_cur_liab/inventories/year/quarter）
        pit_cols = self._get_table_columns(PITConfig.PIT_SCHEMA, self.table_name)
        extra_candidates = ['total_cur_assets', 'total_cur_liab', 'inventories', 'year', 'quarter']
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

        # 分批处理（确保依赖顺序：report -> express -> other）
        for i in range(0, len(data), batch_size):
            batch_data = data.iloc[i:i + batch_size]
            self.logger.info(f"处理批次 {i//batch_size + 1}: {len(batch_data)} 条记录")

            try:
                # 1) 先写入 report
                if 'data_source' in batch_data.columns:
                    report_mask = batch_data['data_source'].eq('report')
                    express_mask = batch_data['data_source'].eq('express')
                else:
                    report_mask = pd.Series([False]*len(batch_data), index=batch_data.index)
                    express_mask = pd.Series([False]*len(batch_data), index=batch_data.index)

                report_batch = batch_data.loc[report_mask]
                if not report_batch.empty:
                    self.logger.info(f"批次 {i//batch_size + 1}: 先写入 report {len(report_batch)} 条")
                    r_res = self._upsert_batch(upsert_sql, report_batch, all_fields)
                    inserted_count += r_res['inserted']
                    updated_count += r_res['updated']

                # 2) 对 express 执行填充（可利用已写入的 report），再写入
                express_batch = batch_data.loc[express_mask]
                if not express_batch.empty:
                    # 组合本批次 report + express 以启用“批次内report优先填充”；若预处理已填充，不会重复覆盖
                    expr_input = pd.concat([report_batch, express_batch], ignore_index=True)
                    expr_filled = self._fill_express_missing_fields(expr_input)
                    express_prepared = expr_filled[expr_filled.get('data_source').eq('express')]
                    self.logger.info(f"批次 {i//batch_size + 1}: 再写入 express {len(express_prepared)} 条（已执行PIT填充）")
                    e_res = self._upsert_batch(upsert_sql, express_prepared, all_fields)
                    inserted_count += e_res['inserted']
                    updated_count += e_res['updated']

                # 3) 其余数据源（若有）
                other_mask = ~(report_mask | express_mask)
                other_batch = batch_data.loc[other_mask]
                if not other_batch.empty:
                    self.logger.info(f"批次 {i//batch_size + 1}: 写入其他数据源 {len(other_batch)} 条")
                    o_res = self._upsert_batch(upsert_sql, other_batch, all_fields)
                    inserted_count += o_res['inserted']
                    updated_count += o_res['updated']

            except Exception as e:
                self.logger.error(f"批次 {i//batch_size + 1} 处理失败: {e}")
                error_count += len(batch_data)

        return {
            'inserted': inserted_count,
            'updated': updated_count,
            'errors': error_count
        }

    def single_backfill(self,
                        ts_code: str,
                        start_date: Optional[str] = None,
                        end_date: Optional[str] = None,
                        batch_size: Optional[int] = None,
                        do_validate: bool = True) -> Dict[str, Any]:
        """单个股票历史回填（参考 income_quarterly 写法）。

        Args:
            ts_code: 股票代码（如 600000.SH）
            start_date: 开始日期 (ann_date)
            end_date: 结束日期 (ann_date)
            batch_size: 批次大小
            do_validate: 是否在回填后执行轻量校验
        """
        if not ts_code:
            return {'backfilled_records': 0, 'error': '缺少 ts_code', 'message': '必须提供 --ts-code 才能执行单股回填'}

        self.logger.info(f"开始PIT资产负债表单股历史回填: ts_code={ts_code}")

        # 设置默认参数
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

            # 2.9 确保唯一键包含 data_source；并确保索引
            self._ensure_balance_unique_keys()
            self.ensure_indexes()

            # 3. 批量UPSERT
            result = self._batch_upsert_to_pit(processed, batch_size)

            out: Dict[str, Any] = {
                'ts_code': ts_code,
                'backfilled_records': result['inserted'] + result['updated'],
                'inserted_records': result['inserted'],
                'updated_records': result['updated'],
                'error_records': result['errors'],
                'message': f"单股回填完成，共 {result['inserted'] + result['updated']} 条（ins={result['inserted']}, upd={result['updated']}）"
            }

            # 4. 可选：针对该股做轻量验证（如校验关键字段是否存在、行数等）
            if do_validate:
                try:
                    out['validation'] = self._validate_single_stock(ts_code, start_date, end_date)
                except Exception as ve:
                    self.logger.warning(f"单股回填验证失败（忽略不中断）: {ve}")

            return out
        except Exception as e:
            self.logger.error(f"单股历史回填失败: {e}")
            return {
                'ts_code': ts_code,
                'backfilled_records': 0,
                'error': str(e),
                'message': '单股历史回填失败'
            }

    def _validate_single_stock(self, ts_code: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """对指定股票在给定日期范围内进行轻量验证（资产负债表）。
        验证内容：
        - 按来源统计行数（report/express）
        - 核心字段是否全部为空的记录数量（应尽量为0）
        - ann_date/ts_code/end_date 关键字段完整性
        """
        pit_cols = self._get_table_columns(PITConfig.PIT_SCHEMA, self.table_name)
        core_fields = [c for c in self.data_fields if c in pit_cols]
        select_cols = ['ts_code', 'end_date', 'ann_date', 'data_source'] + core_fields
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
        by_src = work['data_source'].value_counts(dropna=False).to_dict() if 'data_source' in work.columns else {}
        if core_fields:
            all_null = work[core_fields].isna().all(axis=1)
            null_count = int(all_null.sum())
        else:
            null_count = 0
        key_null = int(work[['ts_code','end_date','ann_date']].isna().any(axis=1).sum())
        issues = 1 if (null_count > 0 or key_null > 0) else 0
        return {
            'ts_code': ts_code,
            'range': [start_date, end_date],
            'rows': int(len(work)),
            'by_source': by_src,
            'all_core_null_rows': null_count,
            'key_field_null_rows': key_null,
            'status': 'passed' if issues == 0 else 'warning'
        }

    def fix_missing_express_fields(self, days: int = None, start_date: str = None, end_date: str = None, batch_size: int = None) -> Dict[str, Any]:
        """兜底修复任务：扫描已入库的 express 记录中缺失字段，按 PIT 原则用历史 report 回填。
        - 查询范围：
          * 若传 days，则按增量窗口；否则按 start_date/end_date；都未传则默认最近 90 天
        - 仅修复 data_source='express' 且任一目标字段为空的记录
        - 回填字段：tot_liab, total_cur_assets, total_cur_liab, inventories
        """
        try:
            if batch_size is None:
                batch_size = self.batch_size
            lookback_days = 90
            if days is not None:
                start_date, end_date = PITConfig.get_incremental_date_range(days)
            else:
                if start_date is None or end_date is None:
                    from datetime import date, timedelta
                    end_date = date.today()
                    start_date = end_date - timedelta(days=lookback_days)
            self.logger.info(f"启动兜底修复：范围 {start_date} ~ {end_date}")
            # 保护性基线：统计报告(report)记录关键字段的非空计数（用于事后校验）
            baseline_sql = f"""
            SELECT COUNT(*) AS report_total,
                   SUM(CASE WHEN (tot_liab IS NULL OR total_cur_assets IS NULL OR total_cur_liab IS NULL OR inventories IS NULL) THEN 1 ELSE 0 END) AS report_nulls
            FROM {PITConfig.PIT_SCHEMA}.{self.table_name}
            WHERE data_source='report' AND ann_date >= %s AND ann_date <= %s
            """
            baseline = self.context.query_dataframe(baseline_sql, (start_date, end_date))
            base_report_total = int(baseline.iloc[0]['report_total']) if baseline is not None and not baseline.empty else None
            base_report_nulls = int(baseline.iloc[0]['report_nulls']) if baseline is not None and not baseline.empty else None

            # 找出缺失记录键集合（仅 express）
            sql_missing = f"""
            SELECT ts_code, end_date, ann_date
            FROM {PITConfig.PIT_SCHEMA}.{self.table_name}
            WHERE data_source='express' AND ann_date >= %s AND ann_date <= %s
              AND (tot_liab IS NULL OR total_cur_assets IS NULL OR total_cur_liab IS NULL OR inventories IS NULL)
            ORDER BY ts_code, ann_date
            """
            miss_df = self.context.query_dataframe(sql_missing, (start_date, end_date))
            if miss_df is None or miss_df.empty:
                return {'scanned': 0, 'updated': 0}
            # 批处理
            updated_total = 0
            keys = miss_df[['ts_code','end_date','ann_date']].drop_duplicates()
            for i in range(0, len(keys), batch_size):
                sub = keys.iloc[i:i+batch_size]
                ts_list = sorted(sub['ts_code'].unique().tolist())
                min_ann = sub['ann_date'].min(); max_ann = sub['ann_date'].max()
                # 查询 report 候选
                pit_cols = self._get_table_columns(PITConfig.PIT_SCHEMA, self.table_name)
                fill_cols = [c for c in ['tot_liab','total_cur_assets','total_cur_liab','inventories'] if c in pit_cols]
                select_cols = ', '.join(['ts_code','ann_date'] + fill_cols)
                ref_sql = (
                    f"SELECT {select_cols} FROM {PITConfig.PIT_SCHEMA}.{self.table_name} "
                    f"WHERE data_source='report' AND ts_code=ANY(%s) AND ann_date <= %s ORDER BY ts_code, ann_date"
                )
                ref = self.context.query_dataframe(ref_sql, (ts_list, max_ann))
                if ref is None or ref.empty:
                    continue
                # merge_asof 逐股回填（逐个 ts_code 保证排序要求）
                r = ref.copy(); r['ann_dt'] = pd.to_datetime(r['ann_date']); r.sort_values(['ts_code','ann_dt'], inplace=True)
                e = sub.copy(); e['ann_dt'] = pd.to_datetime(e['ann_date']); e.sort_values(['ts_code','ann_dt'], inplace=True)
                updated_batch = 0
                for code, e_sub in e.groupby('ts_code'):
                    r_sub = r[r['ts_code'] == code]
                    if r_sub.empty:
                        continue
                    e_keep = e_sub[['ts_code','end_date','ann_date','ann_dt']].sort_values('ann_dt')
                    r_keep = r_sub[['ann_dt'] + fill_cols].sort_values('ann_dt')
                    merged = pd.merge_asof(e_keep, r_keep, on='ann_dt', direction='backward', allow_exact_matches=True)
                    for _, row in merged.iterrows():
                        sets = []
                        params = []
                        for c in fill_cols:
                            val = row.get(c)
                            if pd.notna(val):
                                sets.append(f"{c} = %s")
                                params.append(val)
                        if not sets:
                            continue
                        # 数据保护：只更新 express 且目标字段当前为 NULL 的记录，防止误覆盖 report
                        safe_predicates = []
                        for c in fill_cols:
                            safe_predicates.append(f"({c} IS NULL)")
                        safe_where_extra = " AND (" + " OR ".join(safe_predicates) + ")" if safe_predicates else ""
                        upd_sql = (
                            f"UPDATE {PITConfig.PIT_SCHEMA}.{self.table_name} SET " + ', '.join(sets) +
                            f" WHERE ts_code=%s AND end_date=%s AND ann_date=%s AND data_source='express'" + safe_where_extra
                        )
                        params.extend([row['ts_code'], row['end_date'], row['ann_date']])
                        self.context.db_manager.execute_sync(upd_sql, tuple(params))
                        updated_batch += 1
                updated_total += updated_batch
            # 事后校验：report 记录非空统计不应下降（如下降则报警）
            post = self.context.query_dataframe(baseline_sql, (start_date, end_date))
            if post is not None and not post.empty:
                post_nulls = int(post.iloc[0]['report_nulls'])
                if baseline is not None and not baseline.empty:
                    base_report_nulls = int(baseline.iloc[0]['report_nulls'])
                    if post_nulls > base_report_nulls:
                        self.logger.error(
                            f"警告：修复后 report 记录空值计数上升（{base_report_nulls} -> {post_nulls}），请检查 WHERE 条件与数据源过滤！")
            self.logger.info(f"兜底修复完成：扫描 {len(keys)}，修复 {updated_total}")
            return {'scanned': int(len(keys)), 'updated': int(updated_total)}
        except Exception as e:
            self.logger.error(f"兜底修复失败: {e}")
            return {'scanned': 0, 'updated': 0, 'error': str(e)}
    def recover_report_fields_from_tushare(self, start_date: str = '2010-01-01', end_date: str = None, batch_size: int = None) -> Dict[str, Any]:
        """紧急恢复：从 tushare.fina_balancesheet 恢复 report 数据源关键字段（仅填充 NULL，不覆盖已有值）。
        - 仅更新 data_source='report' 且字段为 NULL 的记录
        - 精确键匹配 (ts_code,end_date,ann_date) 并遵循 PIT（同日等时）
        - 恢复前备份受影响期间的 report 行
        """
        try:
            from datetime import datetime
            if end_date is None:
                end_date = datetime.now().strftime('%Y-%m-%d')
            if batch_size is None:
                batch_size = self.batch_size

            self.logger.warning(f"[恢复] 开始恢复 report 字段：范围 {start_date} ~ {end_date}，batch_size={batch_size}")

            # 1) 统计与备份
            stat_sql = f"""
            SELECT COUNT(*) AS affected
            FROM {PITConfig.PIT_SCHEMA}.{self.table_name}
            WHERE data_source='report' AND ann_date >= %s AND ann_date <= %s
              AND (tot_liab IS NULL OR total_cur_assets IS NULL OR total_cur_liab IS NULL OR inventories IS NULL)
            """
            stat_df = self.context.query_dataframe(stat_sql, (start_date, end_date))
            affected = int(stat_df.iloc[0]['affected']) if stat_df is not None and not stat_df.empty else 0
            self.logger.warning(f"[恢复] 受影响 report 记录（存在 NULL）数量：{affected}")

            # 备份受影响区间的 report 记录
            ts_suffix = datetime.now().strftime('%Y%m%d_%H%M%S')
            bkp_table = f"{PITConfig.PIT_SCHEMA}.{self.table_name}_bkp_report_{ts_suffix}"
            bkp_sql = f"""
            CREATE TABLE {bkp_table} AS
            SELECT * FROM {PITConfig.PIT_SCHEMA}.{self.table_name}
            WHERE data_source='report' AND ann_date >= %s AND ann_date <= %s
            """
            try:
                self.context.db_manager.execute_sync(bkp_sql, (start_date, end_date))
                self.logger.info(f"[恢复] 已备份 report 记录到 {bkp_table}")
            except Exception as e:
                self.logger.error(f"[恢复] 备份失败（不中断）：{e}")

            if affected == 0:
                return {'affected_report_nulls': 0, 'updated': 0, 'backup_table': bkp_table}

            # 2) 查找需要恢复的 report 键集合
            keys_sql = f"""
            SELECT ts_code, end_date, ann_date
            FROM {PITConfig.PIT_SCHEMA}.{self.table_name}
            WHERE data_source='report' AND ann_date >= %s AND ann_date <= %s
              AND (tot_liab IS NULL OR total_cur_assets IS NULL OR total_cur_liab IS NULL OR inventories IS NULL)
            ORDER BY ts_code, ann_date
            """
            keys = self.context.query_dataframe(keys_sql, (start_date, end_date))
            if keys is None or keys.empty:
                return {'affected_report_nulls': affected, 'updated': 0, 'backup_table': bkp_table}

            # 3) 分批从 tushare.fina_balancesheet 读取并恢复（精确键匹配）
            upd_total = 0
            for i in range(0, len(keys), batch_size):
                batch = keys.iloc[i:i+batch_size]
                # 从 tushare 源按精确键读取
                # 注意：字段名与目标列对齐
                sel = (
                    "ts_code, end_date, ann_date, total_liab, total_cur_assets, total_cur_liab, inventories"
                )
                src_sql = (
                    f"SELECT {sel} FROM {PITConfig.TUSHARE_SCHEMA}.fina_balancesheet "
                    f"WHERE (ts_code, end_date, ann_date) IN (" + ",".join(["(%s,%s,%s)"]*len(batch)) + ")"
                )
                params = []
                for _, r in batch.iterrows():
                    params.extend([r['ts_code'], r['end_date'], r['ann_date']])
                src = self.context.query_dataframe(src_sql, tuple(params)) if params else None
                if src is None or src.empty:
                    continue
                # 行业屏蔽：银行业不恢复 total_cur_assets/total_cur_liab/inventories
                bank_codes = self._get_bank_ts_codes()
                src['is_bank'] = src['ts_code'].isin(bank_codes) if bank_codes else False
                # 逐行安全更新：仅当目标列为 NULL 时用源值填充
                for row in src.itertuples(index=False):
                    # 银行业仅恢复 tot_liab；其它三列跳过
                    if getattr(row, 'is_bank', False):
                        upd_sql = (
                            f"UPDATE {PITConfig.PIT_SCHEMA}.{self.table_name} SET "
                            f"tot_liab = COALESCE(tot_liab, %s) "
                            f"WHERE ts_code=%s AND end_date=%s AND ann_date=%s AND data_source='report' "
                            f"AND (tot_liab IS NULL AND %s IS NOT NULL)"
                        )
                        vals = [row.total_liab, row.ts_code, row.end_date, row.ann_date, row.total_liab]
                    else:
                        # 非银行：四列按常规 COALESCE 恢复
                        upd_sql = (
                            f"UPDATE {PITConfig.PIT_SCHEMA}.{self.table_name} SET "
                            f"tot_liab = COALESCE(tot_liab, %s), "
                            f"total_cur_assets = COALESCE(total_cur_assets, %s), "
                            f"total_cur_liab = COALESCE(total_cur_liab, %s), "
                            f"inventories = COALESCE(inventories, %s) "
                            f"WHERE ts_code=%s AND end_date=%s AND ann_date=%s AND data_source='report' "
                            f"AND ((tot_liab IS NULL AND %s IS NOT NULL) OR (total_cur_assets IS NULL AND %s IS NOT NULL) "
                            f"OR (total_cur_liab IS NULL AND %s IS NOT NULL) OR (inventories IS NULL AND %s IS NOT NULL))"
                        )
                        vals = [row.total_liab, row.total_cur_assets, row.total_cur_liab, row.inventories,
                                row.ts_code, row.end_date, row.ann_date,
                                row.total_liab, row.total_cur_assets, row.total_cur_liab, row.inventories]
                    try:
                        rowcount = self.context.db_manager.execute_sync(upd_sql, tuple(vals), return_rowcount=True)
                    except TypeError:
                        self.context.db_manager.execute_sync(upd_sql, tuple(vals))
                        rowcount = None
                    if rowcount:
                        upd_total += rowcount


            # 4) 恢复后验证
            post_df = self.context.query_dataframe(stat_sql, (start_date, end_date))
            post_affected = int(post_df.iloc[0]['affected']) if post_df is not None and not post_df.empty else None
            self.logger.warning(f"[恢复] 恢复后 report NULL 记录数量：{post_affected}")

            # 验证 express/forecast 未受影响（仅报告一下影响行数为0的预期）
            chk_sql = f"""
            SELECT data_source, COUNT(*) AS cnt
            FROM {PITConfig.PIT_SCHEMA}.{self.table_name}
            WHERE ann_date >= %s AND ann_date <= %s AND data_source IN ('express','forecast')
            GROUP BY data_source
            """
            chk = self.context.query_dataframe(chk_sql, (start_date, end_date))
            self.logger.info(f"[恢复] express/forecast 行计数（仅核对范围）：{0 if chk is None else chk.to_dict(orient='records')}")

            return {
                'affected_report_nulls_before': affected,
                'affected_report_nulls_after': post_affected,
                'updated': upd_total,
                'backup_table': bkp_table
            }
        except Exception as e:
            self.logger.error(f"[恢复] 恢复失败: {e}")
            return {'error': str(e)}


    def _fill_express_missing_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """当 express 数据缺失关键字段时，按 PIT 原则使用最近的 report 数据进行填充。
        机制：
        1) 先尝试使用当前批次内的 report 记录（零查询开销）
        2) 如仍不足，再扩展查询数据库历史 report 数据（默认回看 9 个月 ≈ 3 个季度）
        3) 严格 PIT：每条 express 仅匹配 ann_date 之前或当日的 report
        """
        if df is None or df.empty:
            return df
        work = df.copy()
        # 目标列
        fill_cols = ['tot_liab', 'total_cur_assets', 'total_cur_liab', 'inventories']
        for c in fill_cols:
            if c not in work.columns:
                work[c] = None
        # 仅选择 express 且任一字段缺失的行
        express_mask = work.get('data_source').eq('express') if 'data_source' in work.columns else pd.Series([False]*len(work))
        need_fill_mask = express_mask & work[fill_cols].isna().any(axis=1)
        if not need_fill_mask.any():
            return work
        targets = work.loc[need_fill_mask, ['ts_code', 'ann_date']].dropna()
        if targets.empty:
            return work
        # 辅助：逐组后向匹配并回填
        def _backward_fill_from_ref(ref_df: pd.DataFrame, label: str) -> int:
            if ref_df is None or ref_df.empty:
                return 0
            r = ref_df.copy()
            r['ann_dt'] = pd.to_datetime(r['ann_date'])
            r.sort_values(['ts_code', 'ann_dt'], inplace=True)
            t = work.loc[need_fill_mask, ['ts_code', 'ann_date']].copy()
            t['orig_idx'] = t.index
            t['ann_dt'] = pd.to_datetime(t['ann_date'])
            t.sort_values(['ts_code', 'ann_dt'], inplace=True)
            local_filled = 0
            for code, e_sub in t.groupby('ts_code'):
                r_sub = r[r['ts_code'] == code]
                if r_sub.empty:
                    continue
                e_keep = e_sub[['orig_idx', 'ann_dt']].sort_values('ann_dt')
                r_keep = r_sub[['ann_dt'] + [c for c in fill_cols if c in r_sub.columns]].sort_values('ann_dt')
                merged = pd.merge_asof(e_keep, r_keep, on='ann_dt', direction='backward', allow_exact_matches=True)
                for _, mrow in merged.iterrows():
                    row_idx = mrow['orig_idx']
                    for c in fill_cols:
                        if c in merged.columns:
                            val = mrow.get(c)
                            if pd.isna(work.at[row_idx, c]) and pd.notna(val):
                                work.at[row_idx, c] = val
                                local_filled += 1
            if local_filled > 0:
                self.logger.info(f"PIT填充（{label}）完成：共填充 {local_filled} 个字段值（express缺失修复）")
            return local_filled
        total_filled = 0
        # 预处理：行业不适用字段屏蔽（银行）
        work = self._exclude_industry_inapplicable_fields(work)

        # Phase 1: 使用批次内的 report 记录
        batch_report = work[work.get('data_source').eq('report')] if 'data_source' in work.columns else pd.DataFrame()
        if not batch_report.empty:
            # 仅保留需要的列
            cols = ['ts_code', 'ann_date'] + [c for c in fill_cols if c in batch_report.columns]
            total_filled += _backward_fill_from_ref(batch_report[cols], label='批次内report')
        # Phase 2: 若仍有缺失，扩展查询数据库历史 report（默认 9 个月回看）
        remaining_mask = express_mask & work[fill_cols].isna().any(axis=1)
        if remaining_mask.any():
            from dateutil.relativedelta import relativedelta
            ts_list = sorted(work.loc[remaining_mask, 'ts_code'].dropna().unique().tolist())
            if ts_list:
                min_ann = work.loc[remaining_mask, 'ann_date'].min()
                max_ann = work.loc[remaining_mask, 'ann_date'].max()
                lookback_months = getattr(PITConfig, 'BALANCE_EXPRESS_FILL_LOOKBACK_MONTHS', 9)
                lower_bound = (pd.to_datetime(min_ann) - relativedelta(months=lookback_months)).date()
                # 动态选择存在的列，避免列不存在导致查询失败
                pit_cols = self._get_table_columns(PITConfig.PIT_SCHEMA, 'pit_balance_quarterly')
                db_fill_cols = [c for c in fill_cols if c in pit_cols]
                select_cols = ', '.join(['ts_code', 'ann_date'] + db_fill_cols)
                sql = (
                    f"SELECT {select_cols} "
                    f"FROM {PITConfig.PIT_SCHEMA}.pit_balance_quarterly "
                    f"WHERE data_source = 'report' AND ts_code = ANY(%s) "
                    f"AND ann_date >= %s AND ann_date <= %s "
                    f"ORDER BY ts_code, ann_date"
                )
                self.logger.info(
                    f"扩展查询历史report用于PIT填充：ts={len(ts_list)}，窗口={lower_bound}~{max_ann}，lookback={lookback_months}个月"
                )
                ref = self.context.query_dataframe(sql, (ts_list, lower_bound, max_ann))
                total_filled += _backward_fill_from_ref(ref, label='历史report')
            return work

    def _get_bank_ts_codes(self) -> set:
        """获取申万一级行业=银行 的股票代码集合（优先 PIT 行业表，失败时回退 Tushare stock_basic）。"""
        # 优先 PIT 行业表
        try:
            q = (
                "WITH latest AS ("
                "  SELECT ts_code, industry_level1 AS sw_l1, obs_date,"
                "         ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY obs_date DESC) rn "
                "  FROM pgs_factors.pit_industry_classification"
                ") SELECT ts_code FROM latest WHERE rn=1 AND sw_l1='银行'"
            )
            df = self.context.query_dataframe(q)
            if df is not None and not df.empty:
                return set(df['ts_code'].tolist())
        except Exception:
            pass
        # 回退 Tushare stock_basic（字段名 industry 如有差异，请按库内实际调整）
        try:
            df2 = self.context.query_dataframe("SELECT ts_code FROM tushare.stock_basic WHERE industry='银行'")
            if df2 is not None and not df2.empty:
                return set(df2['ts_code'].tolist())
        except Exception:
            pass
        return set()


    def _get_table_columns(self, schema: str, table: str) -> set:
        sql = "SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s"
        try:
            df = self.context.query_dataframe(sql, (schema, table))
            return set(df['column_name'].tolist()) if df is not None else set()
        except Exception:
            return set()
    def _exclude_industry_inapplicable_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """根据行业字段适用性配置，将不适用字段置为不参与缺失判断（直接视为已填）。
        当前实现：对银行业屏蔽 total_cur_assets/total_cur_liab/inventories。
        """
        try:
            conf = PITConfig.PIT_TABLES.get(self.table_name, {}).get('INDUSTRY_FIELD_ALLOWLIST', {})
            bank_excl = set(conf.get('银行', {}).get('exclude_fields', []))
            if not bank_excl:
                return df
            bank_codes = self._get_bank_ts_codes()
            if not bank_codes:
                return df
            work = df.copy()
            mask = work['ts_code'].isin(bank_codes) if 'ts_code' in work.columns else pd.Series([False]*len(work))
            for c in bank_excl:
                if c in work.columns:
                    # 行业内不适用字段不参与缺失判断，等价于不需要填充；此处将 NaN 保留，仅在缺失掩码判断环节跳过
                    # 若下游严格要求非空，可选择用 0.0 或 None 保持，但逻辑上应视为 N/A
                    pass
            # 标记列供上游逻辑判断（可选）：_industry_na_flag
            work['_industry_na_flag'] = False
            work.loc[mask, '_industry_na_flag'] = True
            return work
        except Exception:
            return df


    def ensure_indexes(self) -> None:
        """幂等创建优化查询的索引（历史回看/PIT填充等）。"""
        try:
            import os
            sql_path = os.path.join(os.path.dirname(__file__), 'database', 'create_pit_balance_indexes.sql')
            sql_path = os.path.normpath(sql_path)
            if not os.path.exists(sql_path):
                self.logger.warning(f"未找到索引SQL: {sql_path}")
                return
            with open(sql_path, 'r', encoding='utf-8') as f:
                ddl = f.read()
            self.context.db_manager.execute_sync(ddl)
            self.logger.info("已确保 PIT 资产负债表相关索引存在（幂等）")
        except Exception as e:
            self.logger.error(f"创建索引失败: {e}")

    def _upsert_batch(self, upsert_sql: str, batch_data: pd.DataFrame, all_fields: List[str]) -> Dict[str, int]:
        """处理单个批次的UPSERT"""

        inserted_count = 0
        updated_count = 0

        for _, row in batch_data.iterrows():
            try:
                # 准备参数
                params = {field: row[field] for field in all_fields}

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
    def _ensure_balance_unique_keys(self) -> None:
        """将 pit_balance_quarterly 唯一键升级为 (ts_code, end_date, ann_date, data_source)。幂等执行。"""
        try:
            # 删除旧唯一键（若存在）
            self.context.db_manager.execute_sync(
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
                """
            )
            # 新建包含 data_source 的唯一键（若不存在）
            self.context.db_manager.execute_sync(
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
                """
            )
            self.logger.info("唯一键已升级至包含 data_source（balance）")
        except Exception as e:
            self.logger.error(f"升级唯一键失败（balance）: {e}")


def main():
    """主函数 - 命令行接口"""

    parser = argparse.ArgumentParser(description='PIT资产负债表管理器')
    parser.add_argument('--mode', choices=['full-backfill', 'incremental', 'single-backfill'],
                       required=True, help='执行模式')
    parser.add_argument('--start-date', help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='结束日期 (YYYY-MM-DD)')
    parser.add_argument('--days', type=int, help='增量更新天数')
    parser.add_argument('--batch-size', type=int, help='批次大小')
    parser.add_argument('--ts-code', help='单股回填：股票代码 (e.g. 600000.SH)')
    parser.add_argument('--status', action='store_true', help='显示表状态')
    parser.add_argument('--validate', action='store_true', help='验证数据完整性/或单股回填后轻量验证')

    args = parser.parse_args()

    print("🏦 PIT资产负债表管理器")
    print("=" * 60)

    try:
        with PITBalanceQuarterlyManager() as manager:

            # 显示表状态
            if args.status:
                print("📈 表状态:")
                status = manager.get_table_status()
                for key, value in status.items():
                    print(f"  {key}: {value}")
                return 0

            # 验证数据完整性
            if args.validate:
                print("🔍 数据完整性验证:")
                validation = manager.validate_data_integrity()
                print(f"  总体状态: {validation['overall_status']}")
                print(f"  发现问题: {validation['issues_found']} 个")
                for check in validation['checks']:
                    status_icon = "✅" if check['status'] == 'passed' else "❌"
                    print(f"  {status_icon} {check['check_name']}: {check['message']}")
                return 0

            # 执行主要功能
            if args.mode == 'full-backfill':
                result = manager.full_backfill(
                    start_date=args.start_date,
                    end_date=args.end_date,
                    batch_size=args.batch_size
                )
            elif args.mode == 'incremental':
                result = manager.incremental_update(
                    days=args.days,
                    batch_size=args.batch_size
                )
            elif args.mode == 'single-backfill':
                result = manager.single_backfill(
                    ts_code=args.ts_code,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    batch_size=args.batch_size,
                    do_validate=args.validate
                )

            print(f"\n✅ 执行结果:")
            for key, value in result.items():
                print(f"  {key}: {value}")

            return 0 if 'error' not in result else 1

    except Exception as e:
        print(f"\n❌ 执行失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
