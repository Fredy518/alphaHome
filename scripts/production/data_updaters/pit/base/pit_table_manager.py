#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PIT数据表管理基类
================

为所有PIT数据表管理器提供统一的基础功能

Author: AI Assistant
Date: 2025-08-11
"""

import sys
import os
import logging
from abc import ABC, abstractmethod
from datetime import datetime, date
from typing import Dict, List, Optional, Any, Tuple
import pandas as pd

# 添加项目路径 - 从base目录向上6级到达项目根目录
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))))
sys.path.insert(0, project_root)

from research.tools.context import ResearchContext

try:
    from .pit_config import PITConfig
except ImportError:
    from pit_config import PITConfig

class PITTableManager(ABC):
    """PIT数据表管理基类"""

    def __init__(self, table_name: str):
        """
        初始化PIT表管理器

        Args:
            table_name: PIT表名
        """
        if not PITConfig.validate_table_name(table_name):
            raise ValueError(f"无效的PIT表名: {table_name}")

        self.table_name = table_name
        self.table_config = PITConfig.get_table_config(table_name)
        self.batch_size = PITConfig.get_batch_size(table_name)

        self.context = None
        self.logger = None

        # 执行统计
        self.stats = {
            'start_time': None,
            'end_time': None,
            'processed_records': 0,
            'success_records': 0,
            'error_records': 0,
            'skipped_records': 0
        }

    def __enter__(self):
        """上下文管理器入口"""
        # 初始化数据库连接
        self.context = ResearchContext()
        self.context.__enter__()

        # 设置日志
        self._setup_logging()

        # 记录开始时间
        self.stats['start_time'] = datetime.now()

        self.logger.info(f"初始化 {self.table_name} 管理器")
        self.logger.info(f"表配置: {self.table_config['description']}")

        # 确保 updated_at 触发器已部署（幂等执行）
        self._ensure_updated_at_triggers()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        # 记录结束时间
        self.stats['end_time'] = datetime.now()

        # 记录执行统计
        self._log_execution_stats()

        # 清理资源
        if self.context:
            self.context.__exit__(exc_type, exc_val, exc_tb)

    def _setup_logging(self):
        """设置日志（统一走全局日志工具，避免重复处理器与重复打印）"""
        # 延迟引入，避免循环依赖
        try:
            from alphahome.common.logging_utils import get_logger
            # 使用统一 logger；名称沿用原命名，以便过滤/检索
            self.logger = get_logger(f"PIT.{self.table_name}")
        except Exception:
            # 兜底：如果统一日志不可用，退回到简单配置，但禁止冒泡避免重复
            self.logger = logging.getLogger(f"PIT.{self.table_name}")
            self.logger.setLevel(logging.INFO)
            if not self.logger.handlers:
                console = logging.StreamHandler()
                console.setFormatter(logging.Formatter(PITConfig.LOG_FORMAT))
                self.logger.addHandler(console)
            self.logger.propagate = False

    def _log_execution_stats(self):
        """记录执行统计"""
        if self.stats['start_time'] and self.stats['end_time']:
            duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()

            self.logger.info("=" * 60)
            self.logger.info(f"{self.table_name} 执行统计")
            self.logger.info("=" * 60)
            self.logger.info(f"执行时间: {duration:.2f} 秒")
            self.logger.info(f"处理记录: {self.stats['processed_records']:,}")
            self.logger.info(f"成功记录: {self.stats['success_records']:,}")
            self.logger.info(f"错误记录: {self.stats['error_records']:,}")
            self.logger.info(f"跳过记录: {self.stats['skipped_records']:,}")

            if duration > 0:
                rate = self.stats['success_records'] / duration
                self.logger.info(f"处理速度: {rate:.2f} 记录/秒")

    def _ensure_updated_at_triggers(self) -> None:
        """幂等部署 PIT 四张表的 updated_at 触发器（不复用 pgs_factor 中的代码）。
        - 位置: scripts/production/data_updaters/pit/database/create_pit_updated_at_triggers.sql
        - 在任意 Manager 进入上下文时执行一次，保证环境一致性
        """
        try:
            base_dir = os.path.dirname(os.path.dirname(__file__))  # scripts/production/data_updaters/pit
            sql_path = os.path.join(base_dir, 'database', 'create_pit_updated_at_triggers.sql')
            sql_path = os.path.normpath(sql_path)
            if not os.path.exists(sql_path):
                self.logger.warning(f"未找到触发器SQL: {sql_path}")
                return
            with open(sql_path, 'r', encoding='utf-8') as f:
                ddl = f.read()
            self.context.db_manager.execute_sync(ddl)
            self.logger.info("已确保 PIT updated_at 触发器存在（幂等）")
        except Exception as e:
            self.logger.error(f"部署 PIT updated_at 触发器失败: {e}")

    def _table_exists(self, schema: str, table: str) -> bool:
        """检查表是否存在（information_schema.tables）。"""
        try:
            sql = """
            SELECT EXISTS (
              SELECT 1 FROM information_schema.tables
              WHERE table_schema=%s AND table_name=%s
            ) AS ok
            """
            df = self.context.query_dataframe(sql, (schema, table))
            return bool(df is not None and not df.empty and df.iloc[0]['ok'])
        except Exception as e:
            self.logger.error(f"检查表存在性失败: {schema}.{table}, 错误: {e}")
            return False

    def _ensure_table_exists(self) -> None:
        """确保当前 Manager 管理的 PIT 表存在；若不存在则自动创建。
        创建策略：
        1) 优先尝试执行 database/create_{table_name}_table.sql（如存在）
        2) 否则按 pit_config 的 key_fields / data_fields 动态生成最小可用表结构
        同时创建主键/唯一键与基础索引；字段包含 data_source, created_at, updated_at
        """
        schema = PITConfig.PIT_SCHEMA
        table = self.table_name
        if self._table_exists(schema, table):
            return
        self.logger.warning(f"检测到目标表不存在，准备创建：{schema}.{table}")
        try:
            import os
            base_dir = os.path.dirname(os.path.dirname(__file__))  # scripts/production/data_updaters/pit
            ddl_path = os.path.join(base_dir, 'database', f'create_{table}_table.sql')
            executed = False
            if os.path.exists(ddl_path):
                with open(ddl_path, 'r', encoding='utf-8') as f:
                    ddl = f.read()
                self.logger.info(f"使用预置DDL创建表：{ddl_path}")
                self.context.db_manager.execute_sync(ddl)
                executed = True
            if not executed:
                ddl = self._generate_create_table_sql(schema, table)
                self.logger.info(f"使用动态DDL创建表：\n{ddl}")
                self.context.db_manager.execute_sync(ddl)
            self.logger.info(f"创建表成功：{schema}.{table}")
        except Exception as e:
            self.logger.error(f"创建表失败：{schema}.{table}，错误：{e}")
            raise

    def _generate_create_table_sql(self, schema: str, table: str) -> str:
        """基于 pit_config 生成最小可用的建表DDL，包含：
        - key_fields + data_fields + data_source + created_at/updated_at
        - 主键/唯一键：
          * 行业表使用 (ts_code, obs_date, data_source)
          * 其余表使用 (ts_code, end_date, ann_date, data_source)
        - 基础索引： (ts_code), (ts_code,end_date), (ann_date 或 obs_date)
        所有数值列使用 numeric(20,4)，字符串列 varchar，日期列 date。
        """
        cfg = self.table_config
        key_fields = list(cfg.get('key_fields', []))
        data_fields = list(cfg.get('data_fields', []))
        extended_fields: dict[str, str] = dict(cfg.get('extended_fields', {}))
        cols = []
        # 列类型推断
        def col_def(name: str) -> str:
            if name in ('end_date', 'ann_date', 'obs_date'):
                return f"{name} date"
            if name in ('ts_code', 'industry_level1', 'industry_level2', 'industry_level3'):
                return f"{name} varchar(32)"
            return f"{name} numeric(20,4)"
        for k in key_fields:
            cols.append(col_def(k))
        for d in data_fields:
            if d not in key_fields:
                cols.append(col_def(d))
        # 扩展字段（来自配置）
        for cname, ctype in extended_fields.items():
            # 避免重复定义
            if cname not in key_fields and cname not in data_fields:
                cols.append(f"{cname} {ctype}")
        # 标准列
        cols.append("data_source varchar(16) NOT NULL DEFAULT 'report'")
        cols.append("created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP")
        cols.append("updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP")
        # 主键/唯一键
        if table == 'pit_industry_classification':
            pk_fields = ['ts_code', 'obs_date', 'data_source']
        else:
            pk_fields = ['ts_code', 'end_date', 'ann_date', 'data_source']
        col_list = ',\n    '.join(cols)
        pk_list = ', '.join(pk_fields)
        # 索引列
        date_field = 'obs_date' if table == 'pit_industry_classification' else 'ann_date'
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            {col_list},
            CONSTRAINT {table}_pk PRIMARY KEY ({pk_list})
        );
        CREATE INDEX IF NOT EXISTS {table}_idx_ts ON {schema}.{table}(ts_code);
        CREATE INDEX IF NOT EXISTS {table}_idx_ts_end ON {schema}.{table}(ts_code, end_date);
        CREATE INDEX IF NOT EXISTS {table}_idx_date ON {schema}.{table}({date_field});
        """
        return ddl

    def validate_table_structure(self) -> dict:
        """验证当前表结构是否覆盖了配置中的 key_fields、data_fields 与 extended_fields。
        返回缺失字段字典：{'missing': [...], 'unexpected': [...]}，并记录日志。
        """
        try:
            schema = PITConfig.PIT_SCHEMA
            table = self.table_name
            pit_cols = set(self._get_table_columns(schema, table))
            cfg = self.table_config
            expected = set(cfg.get('key_fields', [])) | set(cfg.get('data_fields', [])) | set(cfg.get('extended_fields', {}).keys()) | {'data_source', 'created_at', 'updated_at'}
            missing = sorted(list(expected - pit_cols))
            unexpected = sorted(list(pit_cols - expected))
            if missing:
                self.logger.warning("表结构缺少字段：%s.%s 缺少 %s", schema, table, ','.join(missing))
            else:
                self.logger.info("表结构字段完整：%s.%s", schema, table)
            return {'missing': missing, 'unexpected': unexpected}
        except Exception as e:
            self.logger.error(f"验证表结构失败：{e}")
            return {'missing': [], 'unexpected': []}

    @abstractmethod
    def full_backfill(self, **kwargs) -> Dict[str, Any]:
        """
        历史全量回填

        Returns:
            执行结果统计
        """
        pass

    @abstractmethod
    def incremental_update(self, **kwargs) -> Dict[str, Any]:
        """
        增量更新

        Returns:
            执行结果统计
        """
        pass

    def get_table_status(self) -> Dict[str, Any]:
        """获取表状态"""
        self.logger.info(f"获取 {self.table_name} 状态...")

        try:
            # 根据表类型选择日期字段
            date_field = 'obs_date' if self.table_name == 'pit_industry_classification' else 'ann_date'

            # 基本统计查询
            stats_query = f"""
            SELECT
                COUNT(*) as total_records,
                COUNT(DISTINCT ts_code) as unique_stocks,
                MIN({date_field}) as earliest_date,
                MAX({date_field}) as latest_date
            FROM {PITConfig.PIT_SCHEMA}.{self.table_name}
            """

            result = self.context.query_dataframe(stats_query)

            if result is not None and not result.empty:
                row = result.iloc[0]
                status = {
                    'table_name': self.table_name,
                    'total_records': int(row['total_records']),
                    'unique_stocks': int(row['unique_stocks']),
                    'earliest_date': row['earliest_date'],
                    'latest_date': row['latest_date'],
                    'status': 'healthy' if row['total_records'] > 0 else 'empty'
                }
            else:
                status = {
                    'table_name': self.table_name,
                    'status': 'not_found',
                    'error': '表不存在或无数据'
                }

            return status

        except Exception as e:
            self.logger.error(f"获取表状态失败: {e}")
            return {
                'table_name': self.table_name,
                'status': 'error',
                'error': str(e)
            }

    def validate_data_integrity(self) -> Dict[str, Any]:
        """验证数据完整性"""
        self.logger.info(f"验证 {self.table_name} 数据完整性...")

        validation_results = {
            'table_name': self.table_name,
            'checks': [],
            'overall_status': 'passed',
            'issues_found': 0
        }

        try:
            # 检查1: 空值检查
            null_check = self._check_null_values()
            validation_results['checks'].append(null_check)

            # 检查2: 重复数据检查
            duplicate_check = self._check_duplicates()
            validation_results['checks'].append(duplicate_check)

            # 检查3: 日期范围检查
            date_range_check = self._check_date_ranges()
            validation_results['checks'].append(date_range_check)

            # 统计问题数量
            issues = sum(1 for check in validation_results['checks'] if check['status'] == 'failed')
            validation_results['issues_found'] = issues

            if issues > 0:
                validation_results['overall_status'] = 'failed'

            return validation_results

        except Exception as e:
            self.logger.error(f"数据完整性验证失败: {e}")
            validation_results['overall_status'] = 'error'
            validation_results['error'] = str(e)
            return validation_results

    def _check_null_values(self) -> Dict[str, Any]:
        """检查关键字段的空值"""
        key_fields = self.table_config['key_fields']

        null_counts = {}
        for field in key_fields:
            query = f"""
            SELECT COUNT(*) as null_count
            FROM {PITConfig.PIT_SCHEMA}.{self.table_name}
            WHERE {field} IS NULL
            """

            result = self.context.query_dataframe(query)
            if result is not None and not result.empty:
                null_counts[field] = int(result.iloc[0]['null_count'])

        total_nulls = sum(null_counts.values())

        return {
            'check_name': 'null_values',
            'status': 'passed' if total_nulls == 0 else 'failed',
            'details': null_counts,
            'message': f"关键字段空值总数: {total_nulls}"
        }

    def _check_duplicates(self) -> Dict[str, Any]:
        """检查重复数据"""
        key_fields = ', '.join(self.table_config['key_fields'])

        query = f"""
        SELECT COUNT(*) as duplicate_count
        FROM (
            SELECT {key_fields}, COUNT(*) as cnt
            FROM {PITConfig.PIT_SCHEMA}.{self.table_name}
            GROUP BY {key_fields}
            HAVING COUNT(*) > 1
        ) t
        """

        result = self.context.query_dataframe(query)
        duplicate_count = 0

        if result is not None and not result.empty:
            duplicate_count = int(result.iloc[0]['duplicate_count'])

        return {
            'check_name': 'duplicates',
            'status': 'passed' if duplicate_count == 0 else 'failed',
            'details': {'duplicate_groups': duplicate_count},
            'message': f"重复数据组数: {duplicate_count}"
        }

    def _check_date_ranges(self) -> Dict[str, Any]:
        """检查日期范围合理性"""
        # 根据表类型选择日期字段
        date_field = 'obs_date' if self.table_name == 'pit_industry_classification' else 'ann_date'

        query = f"""
        SELECT
            MIN({date_field}) as min_date,
            MAX({date_field}) as max_date,
            COUNT(*) as total_records
        FROM {PITConfig.PIT_SCHEMA}.{self.table_name}
        WHERE {date_field} IS NOT NULL
        """

        result = self.context.query_dataframe(query)

        if result is not None and not result.empty:
            row = result.iloc[0]
            min_date = row['min_date']
            max_date = row['max_date']

            # 检查日期是否合理（不能是未来日期，不能太早）
            today = date.today()
            reasonable_start = date(2000, 1, 1)

            issues = []
            if max_date > today:
                issues.append(f"存在未来日期: {max_date}")
            if min_date < reasonable_start:
                issues.append(f"存在过早日期: {min_date}")

            return {
                'check_name': 'date_ranges',
                'status': 'passed' if not issues else 'warning',
                'details': {
                    'min_date': str(min_date),
                    'max_date': str(max_date),
                    'issues': issues
                },
                'message': f"日期范围: {min_date} ~ {max_date}"
            }

        return {
            'check_name': 'date_ranges',
            'status': 'failed',
            'details': {},
            'message': '无法获取日期范围信息'
        }

    def cleanup_old_data(self, days_to_keep: int = 365) -> Dict[str, Any]:
        """清理过期数据"""
        self.logger.info(f"清理 {self.table_name} 中 {days_to_keep} 天前的数据...")

        # 计算截止日期
        cutoff_date = datetime.now().date() - pd.Timedelta(days=days_to_keep)

        # 根据表类型选择日期字段
        date_field = 'obs_date' if self.table_name == 'pit_industry_classification' else 'ann_date'

        try:
            # 先查询要删除的记录数
            count_query = f"""
            SELECT COUNT(*) as delete_count
            FROM {PITConfig.PIT_SCHEMA}.{self.table_name}
            WHERE {date_field} < %s
            """

            result = self.context.query_dataframe(count_query, (cutoff_date,))
            delete_count = 0

            if result is not None and not result.empty:
                delete_count = int(result.iloc[0]['delete_count'])

            if delete_count == 0:
                return {
                    'deleted_records': 0,
                    'message': '没有需要清理的过期数据'
                }

            # 执行删除
            delete_query = f"""
            DELETE FROM {PITConfig.PIT_SCHEMA}.{self.table_name}
            WHERE {date_field} < %s
            """

            self.context.db_manager.execute_sync(delete_query, (cutoff_date,))

            self.logger.info(f"成功清理 {delete_count} 条过期数据")

            return {
                'deleted_records': delete_count,
                'cutoff_date': str(cutoff_date),
                'message': f'成功清理 {delete_count} 条过期数据'
            }

        except Exception as e:
            self.logger.error(f"清理过期数据失败: {e}")
            return {
                'deleted_records': 0,
                'error': str(e),
                'message': '清理过期数据失败'
            }
