#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PIT行业分类管理器
================

负责pit_industry_classification表的历史全量回填和增量更新

功能特点:
1. 基于月度快照机制管理行业分类数据
2. 支持申万和中信双重分类体系
3. 自动检测行业变更并生成新快照
4. 提供历史全量回填和增量更新

Author: AI Assistant
Date: 2025-08-11
"""

import sys
import os
import argparse
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
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

class PITIndustryClassificationManager(PITTableManager):
    """PIT行业分类管理器"""
    
    def __init__(self):
        super().__init__('pit_industry_classification')
        
        # 行业分类特定配置
        self.tushare_tables = self.table_config['tushare_tables']
        self.key_fields = self.table_config['key_fields']
        self.data_fields = self.table_config['data_fields']
        self.snapshot_mode = self.table_config.get('snapshot_mode', True)
    
    def full_backfill(self, 
                     start_date: str = None, 
                     end_date: str = None,
                     batch_size: int = None) -> Dict[str, Any]:
        """
        历史全量回填 - 生成历史月度快照
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            batch_size: 批次大小（月份数）
            
        Returns:
            执行结果统计
        """
        self.logger.info("开始PIT行业分类历史全量回填")
        
        # 设置默认参数
        if start_date is None or end_date is None:
            start_date, end_date = PITConfig.get_backfill_date_range(start_date, end_date)
        
        if batch_size is None:
            batch_size = 12  # 默认12个月为一批
        
        self.logger.info(f"回填日期范围: {start_date} ~ {end_date}")
        self.logger.info(f"批次大小: {batch_size} 个月")
        
        try:
            # 0. 确保目标表存在
            self._ensure_table_exists()
            # 1. 获取需要回填的月份
            missing_months = self._find_missing_months(start_date, end_date)

            if not missing_months:
                self.logger.info("没有需要回填的历史数据")
                return {'backfilled_records': 0, 'message': '数据已完整'}
            
            self.logger.info(f"需要回填 {len(missing_months)} 个月的数据")
            
            # 2. 分批处理月份
            total_records = 0
            backfilled_months = 0
            
            for i in range(0, len(missing_months), batch_size):
                batch_months = missing_months[i:i + batch_size]
                
                self.logger.info(f"处理批次 {i//batch_size + 1}: {len(batch_months)} 个月")
                
                try:
                    batch_records = self._process_month_batch(batch_months)
                    total_records += batch_records
                    backfilled_months += len(batch_months)
                    
                    self.logger.info(f"批次完成: {batch_records} 条记录")
                    
                except Exception as e:
                    self.logger.error(f"批次处理失败: {e}")
                    continue
            
            return {
                'backfilled_records': total_records,
                'backfilled_months': backfilled_months,
                'message': f'成功回填 {backfilled_months} 个月，{total_records} 条记录'
            }
            
        except Exception as e:
            self.logger.error(f"历史回填失败: {e}")
            return {
                'backfilled_records': 0,
                'error': str(e),
                'message': '历史回填失败'
            }
    
    def incremental_update(self, 
                          months: int = None,
                          batch_size: int = None) -> Dict[str, Any]:
        """
        增量更新 - 检测行业变更并更新快照
        
        Args:
            months: 检查最近几个月的变更
            batch_size: 批次大小
            
        Returns:
            执行结果统计
        """
        self.logger.info("开始PIT行业分类增量更新")
        
        # 设置默认参数
        if months is None:
            months = 3  # 默认检查最近3个月
        
        # 计算检查日期范围
        end_date = datetime.now().date()
        start_date = end_date - relativedelta(months=months)
        
        self.logger.info(f"检查变更日期范围: {start_date} ~ {end_date}")
        
        try:
            # 0. 确保目标表存在
            self._ensure_table_exists()
            # 1. 检测行业变更
            changes = self._detect_industry_changes(start_date.strftime('%Y-%m-%d'))

            if not changes['has_changes']:
                self.logger.info("未检测到行业变更")
                return {'updated_records': 0, 'message': '无行业变更'}
            
            self.logger.info(f"检测到行业变更: SW {changes['sw_changes']}, CI {changes['ci_changes']}")
            
            # 2. 获取受影响的月份
            affected_months = self._get_affected_months(start_date.strftime('%Y-%m-%d'))
            
            # 3. 重新生成受影响月份的快照
            total_records = 0
            
            for month_date in affected_months:
                self.logger.info(f"重新生成快照: {month_date}")
                
                try:
                    # 删除现有快照
                    self._delete_existing_snapshot(month_date)
                    
                    # 生成新快照
                    month_records = self._generate_monthly_snapshot(month_date)
                    total_records += month_records
                    
                    self.logger.info(f"快照 {month_date} 重新生成完成: {month_records} 条记录")
                    
                except Exception as e:
                    self.logger.error(f"重新生成快照 {month_date} 失败: {e}")
                    continue
            
            return {
                'updated_records': total_records,
                'affected_months': len(affected_months),
                'message': f'基于行业变更更新了 {len(affected_months)} 个月度快照'
            }
            
        except Exception as e:
            self.logger.error(f"增量更新失败: {e}")
            return {
                'updated_records': 0,
                'error': str(e),
                'message': '增量更新失败'
            }
    
    def _find_missing_months(self, start_date: str, end_date: str) -> List[date]:
        """查找缺失的月份"""
        
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date().replace(day=1)
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date().replace(day=1)
        
        # 获取现有的月份
        existing_months_query = """
        SELECT DISTINCT DATE_TRUNC('month', obs_date)::date as month_date
        FROM pgs_factors.pit_industry_classification
        WHERE obs_date >= %s AND obs_date <= %s
        ORDER BY month_date
        """
        
        existing_result = self.context.query_dataframe(
            existing_months_query, 
            (start_dt, self._get_month_end_date(end_dt))
        )
        
        existing_months = set()
        if existing_result is not None and not existing_result.empty:
            existing_months = set(existing_result['month_date'].tolist())
        
        # 生成应该存在的所有月份
        should_exist_months = []
        current_month = start_dt
        
        while current_month <= end_dt:
            should_exist_months.append(current_month)
            current_month = current_month + relativedelta(months=1)
        
        # 找出缺失的月份
        missing_months = [month for month in should_exist_months if month not in existing_months]
        
        return missing_months
    
    def _process_month_batch(self, months: List[date]) -> int:
        """处理月份批次"""
        
        total_records = 0
        
        for month_date in months:
            month_end = self._get_month_end_date(month_date)
            
            self.logger.info(f"生成快照: {month_end}")
            
            # 生成申万快照
            sw_records = self._generate_industry_snapshot('sw', month_end)
            total_records += len(sw_records)
            
            # 生成中信快照
            ci_records = self._generate_industry_snapshot('ci', month_end)
            total_records += len(ci_records)
            
            # 批量插入
            all_records = sw_records + ci_records
            if all_records:
                self._insert_industry_snapshot_batch(all_records)
                self.logger.info(f"快照 {month_end}: SW {len(sw_records)}, CI {len(ci_records)}")
        
        return total_records
    
    def _generate_monthly_snapshot(self, month_date: date) -> int:
        """生成指定月份的行业分类快照"""
        
        # 计算月末日期
        month_end = self._get_month_end_date(month_date)
        
        total_records = 0
        
        # 生成申万快照
        sw_records = self._generate_industry_snapshot('sw', month_end)
        total_records += len(sw_records)
        
        # 生成中信快照
        ci_records = self._generate_industry_snapshot('ci', month_end)
        total_records += len(ci_records)
        
        # 批量插入
        all_records = sw_records + ci_records
        if all_records:
            self._insert_industry_snapshot_batch(all_records)
        
        return total_records
    
    def _generate_industry_snapshot(self, data_source: str, snapshot_date: date) -> List[Dict]:
        """生成指定数据源的行业分类快照"""
        
        # 确定tushare表名
        tushare_table = 'index_swmember' if data_source == 'sw' else 'index_cimember'
        
        # 查询在快照日期有效的行业分类
        query = f"""
        SELECT 
            ts_code,
            l1_code, l1_name,
            l2_code, l2_name, 
            l3_code, l3_name,
            in_date, out_date
        FROM tushare.{tushare_table}
        WHERE (
            (in_date <= %s AND (out_date IS NULL OR out_date > %s))
            OR 
            (in_date <= %s AND out_date IS NULL)
        )
        AND l1_name IS NOT NULL
        ORDER BY ts_code, in_date DESC
        """
        
        industry_data = self.context.query_dataframe(
            query, 
            (snapshot_date, snapshot_date, snapshot_date)
        )
        
        if industry_data is None or industry_data.empty:
            self.logger.warning(f"未找到 {data_source} 在 {snapshot_date} 的行业数据")
            return []
        
        # 每只股票取最新的行业分类
        latest_data = industry_data.groupby('ts_code').first().reset_index()
        
        # 转换为PIT格式
        pit_records = []
        
        for _, row in latest_data.iterrows():
            # 确定特殊处理标识
            requires_special_gpa = self._is_financial_industry(row['l1_name'], row['l2_name'])
            gpa_method = 'null' if requires_special_gpa else 'standard'
            special_reason = self._get_special_handling_reason(row['l1_name'], row['l2_name']) if requires_special_gpa else None
            
            pit_record = {
                'ts_code': row['ts_code'],
                'obs_date': snapshot_date,
                'data_source': data_source,
                'industry_level1': row['l1_name'],
                'industry_level2': row['l2_name'],
                'industry_level3': row['l3_name'],
                'industry_code1': row['l1_code'],
                'industry_code2': row['l2_code'],
                'industry_code3': row['l3_code'],
                'requires_special_gpa_handling': requires_special_gpa,
                'gpa_calculation_method': gpa_method,
                'special_handling_reason': special_reason,
                'data_quality': 'normal',
                'snapshot_version': f"backfill_{snapshot_date.strftime('%Y-%m')}"
            }
            pit_records.append(pit_record)
        
        return pit_records

    def ensure_table_exists(self) -> None:
        """确保行业分类表存在（支持本地DDL）"""
        import os
        try:
            sql_path = os.path.join(os.path.dirname(__file__), 'database', 'create_pit_industry_classification_table.sql')
            sql_path = os.path.normpath(sql_path)
            if not os.path.exists(sql_path):
                self.logger.warning(f"未找到行业分类建表SQL: {sql_path}")
                return
            with open(sql_path, 'r', encoding='utf-8') as f:
                ddl = f.read()
            self.context.db_manager.execute_sync(ddl)
            self.logger.info("行业分类表创建/验证完成")
        except Exception as e:
            self.logger.error(f"创建行业分类表失败: {e}")

    def _insert_industry_snapshot_batch(self, records: List[Dict]):
        """批量插入行业分类快照"""

        if not records:
            return

        # 构建UPSERT SQL
        insert_sql = """
        INSERT INTO pgs_factors.pit_industry_classification (
            ts_code, obs_date, data_source,
            industry_level1, industry_level2, industry_level3,
            industry_code1, industry_code2, industry_code3,
            requires_special_gpa_handling, gpa_calculation_method, special_handling_reason,
            data_quality, snapshot_version
        ) VALUES (
            %(ts_code)s, %(obs_date)s, %(data_source)s,
            %(industry_level1)s, %(industry_level2)s, %(industry_level3)s,
            %(industry_code1)s, %(industry_code2)s, %(industry_code3)s,
            %(requires_special_gpa_handling)s, %(gpa_calculation_method)s, %(special_handling_reason)s,
            %(data_quality)s, %(snapshot_version)s
        )
        ON CONFLICT (ts_code, obs_date, data_source) DO UPDATE SET
            industry_level1 = EXCLUDED.industry_level1,
            industry_level2 = EXCLUDED.industry_level2,
            industry_level3 = EXCLUDED.industry_level3,
            industry_code1 = EXCLUDED.industry_code1,
            industry_code2 = EXCLUDED.industry_code2,
            industry_code3 = EXCLUDED.industry_code3,
            requires_special_gpa_handling = EXCLUDED.requires_special_gpa_handling,
            gpa_calculation_method = EXCLUDED.gpa_calculation_method,
            special_handling_reason = EXCLUDED.special_handling_reason,
            data_quality = EXCLUDED.data_quality,
            snapshot_version = EXCLUDED.snapshot_version,
            updated_at = CURRENT_TIMESTAMP
        """

        # 分批插入
        batch_size = 1000
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]

            for record in batch:
                self.context.db_manager.execute_sync(insert_sql, record)

    def _detect_industry_changes(self, since_date: str) -> Dict:
        """检测行业变更"""

        since_dt = datetime.strptime(since_date, '%Y-%m-%d').date()

        # 检查申万数据变更
        sw_changes = self.context.query_dataframe("""
            SELECT COUNT(*) as change_count
            FROM tushare.index_swmember
            WHERE in_date > %s OR out_date > %s
        """, (since_dt, since_dt))

        # 检查中信数据变更
        ci_changes = self.context.query_dataframe("""
            SELECT COUNT(*) as change_count
            FROM tushare.index_cimember
            WHERE in_date > %s OR out_date > %s
        """, (since_dt, since_dt))

        sw_count = sw_changes.iloc[0]['change_count'] if sw_changes is not None and not sw_changes.empty else 0
        ci_count = ci_changes.iloc[0]['change_count'] if ci_changes is not None and not ci_changes.empty else 0

        return {
            'has_changes': sw_count > 0 or ci_count > 0,
            'sw_changes': sw_count,
            'ci_changes': ci_count
        }

    def _get_affected_months(self, since_date: str) -> List[date]:
        """获取受行业变更影响的月份"""

        since_dt = datetime.strptime(since_date, '%Y-%m-%d').date()
        current_date = datetime.now().date()

        # 从变更开始日期到当前日期的所有月份
        affected_months = []
        current_month = since_dt.replace(day=1)
        end_month = current_date.replace(day=1)

        while current_month <= end_month:
            affected_months.append(current_month)
            current_month = current_month + relativedelta(months=1)

        return affected_months

    def _delete_existing_snapshot(self, month_date: date):
        """删除现有快照"""

        month_end = self._get_month_end_date(month_date)

        delete_sql = """
        DELETE FROM pgs_factors.pit_industry_classification
        WHERE obs_date = %s
        """

        self.context.db_manager.execute_sync(delete_sql, (month_end,))
        self.logger.info(f"删除现有快照: {month_end}")

    def _get_month_end_date(self, month_start: date) -> date:
        """获取月末日期"""
        if month_start.month == 12:
            next_month = month_start.replace(year=month_start.year + 1, month=1)
        else:
            next_month = month_start.replace(month=month_start.month + 1)

        return next_month - timedelta(days=1)

    def _is_financial_industry(self, l1_name: str, l2_name: str) -> bool:
        """判断是否为金融行业"""
        financial_keywords = [
            '银行', '证券', '保险', '信托', '期货', '基金',
            '金融', '投资', '资产管理', '财务公司'
        ]

        industry_text = f"{l1_name} {l2_name}".lower()

        for keyword in financial_keywords:
            if keyword in industry_text:
                return True

        return False

    def _get_special_handling_reason(self, l1_name: str, l2_name: str) -> str:
        """获取特殊处理原因"""
        if '银行' in f"{l1_name} {l2_name}":
            return "银行业营业成本为0导致GPA=100%，需要特殊处理"
        elif '证券' in f"{l1_name} {l2_name}":
            return "证券业成本结构特殊，GPA指标不适用"
        elif '保险' in f"{l1_name} {l2_name}":
            return "保险业成本结构特殊，GPA指标不适用"
        else:
            return "金融业成本结构特殊，GPA指标可能不适用"

def main():
    """主函数 - 命令行接口"""

    parser = argparse.ArgumentParser(description='PIT行业分类管理器')
    parser.add_argument('--mode', choices=['full-backfill', 'incremental'],
                       required=True, help='执行模式')
    parser.add_argument('--start-date', help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='结束日期 (YYYY-MM-DD)')
    parser.add_argument('--months', type=int, help='增量更新检查月数')
    parser.add_argument('--batch-size', type=int, help='批次大小')
    parser.add_argument('--status', action='store_true', help='显示表状态')
    parser.add_argument('--validate', action='store_true', help='验证数据完整性')

    args = parser.parse_args()

    print("🏭 PIT行业分类管理器")
    print("=" * 60)

    try:
        with PITIndustryClassificationManager() as manager:

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
                    months=args.months,
                    batch_size=args.batch_size
                )

            print(f"\n✅ 执行结果:")
            for key, value in result.items():
                print(f"  {key}: {value}")

            return 0 if 'error' not in result else 1

    except Exception as e:
        print(f"\n❌ 执行失败: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
