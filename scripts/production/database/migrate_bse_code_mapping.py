#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
北交所代码切换映射迁移脚本

功能说明：
--------
从 tushare.stock_code_mapping 表获取北交所代码切换映射关系，对除了 stock_basic、
stock_code_mapping 外的所有 tushare schema 中 stock_ 和 fina_ 前缀的数据表进行代码迁移。

迁移逻辑：
--------
1. 查找 ts_code 或 con_code 在 stock_code_mapping 的 ts_code_old 中的记录
2. 复制符合条件的数据
3. 将代码字段从 ts_code_old 映射为 ts_code_new（新代码替换旧代码）
   - 如果表有 ts_code 列，更新 ts_code
   - 如果表有 con_code 列，更新 con_code
   - 如果表同时有这两个列，同时更新两个列
4. 其他字段数据保持不变
5. 使用 upsert 模式保存到原数据表

使用方法：
--------
# 查看将要迁移的表和记录数（不实际执行迁移）
python scripts/production/database/migrate_bse_code_mapping.py --dry-run

# 执行迁移
python scripts/production/database/migrate_bse_code_mapping.py

# 指定特定的表进行迁移
python scripts/production/database/migrate_bse_code_mapping.py --tables stock_daily stock_dividend

# 显示详细信息
python scripts/production/database/migrate_bse_code_mapping.py --verbose
"""

import argparse
import asyncio
import logging
import sys
from datetime import datetime
from typing import List, Dict, Optional, Set, Tuple

import pandas as pd

# 添加项目根目录到 Python 路径
sys.path.insert(0, '.')

from alphahome.common.db_manager import create_async_manager
from alphahome.common.logging_utils import get_logger
from alphahome.common.config_manager import get_database_url

logger = get_logger(__name__)


class BSECodeMappingMigrator:
    """北交所代码映射迁移器"""

    def __init__(self, dry_run: bool = False, verbose: bool = False):
        self.dry_run = dry_run
        self.verbose = verbose
        self.db_manager = None
        
        # 统计信息
        self.stats = {
            'tables_found': 0,
            'tables_processed': 0,
            'tables_skipped': 0,
            'tables_failed': 0,
            'total_records_found': 0,
            'total_records_migrated': 0,
            'start_time': None,
            'end_time': None
        }
        
        # 排除的表
        self.excluded_tables = {'stock_basic', 'stock_code_mapping'}
        
        # 前缀模式
        self.table_prefixes = ['stock_', 'fina_']

    async def initialize(self):
        """初始化数据库连接"""
        try:
            logger.info("正在初始化数据库连接...")
            
            # 获取数据库连接字符串
            db_url = get_database_url()
            if not db_url:
                raise ValueError("无法获取数据库连接字符串，请检查配置文件")
            
            self.db_manager = create_async_manager(db_url)
            await self.db_manager.connect()
            
            logger.info("✅ 数据库连接初始化成功")
            return True
        except Exception as e:
            logger.error(f"❌ 初始化失败: {e}")
            return False

    async def get_code_mapping(self) -> pd.DataFrame:
        """获取代码映射关系"""
        try:
            logger.info("正在从 stock_code_mapping 表获取代码映射...")
            
            query = """
                SELECT ts_code_old, ts_code_new
                FROM tushare.stock_code_mapping
                WHERE ts_code_old IS NOT NULL AND ts_code_new IS NOT NULL
            """
            
            records = await self.db_manager.fetch(query)
            
            if not records:
                logger.warning("未找到任何代码映射记录")
                return pd.DataFrame()
            
            df = pd.DataFrame([dict(r) for r in records])
            logger.info(f"✅ 找到 {len(df)} 条代码映射记录")
            
            if self.verbose:
                logger.info(f"映射示例:\n{df.head(10)}")
            
            return df
        except Exception as e:
            logger.error(f"❌ 获取代码映射失败: {e}")
            raise

    async def get_target_tables(self, specified_tables: Optional[List[str]] = None) -> List[str]:
        """获取需要迁移的目标表列表"""
        try:
            logger.info("正在获取目标表列表...")
            
            # 获取 tushare schema 下的所有表
            all_tables = await self.db_manager.get_all_physical_tables('tushare')
            
            if not all_tables:
                logger.warning("tushare schema 下没有找到任何表")
                return []
            
            # 过滤出需要处理的表
            target_tables = []
            for table in all_tables:
                # 检查表名是否符合前缀要求
                has_prefix = any(table.startswith(prefix) for prefix in self.table_prefixes)
                
                # 排除特定表
                if table in self.excluded_tables:
                    continue
                
                # 如果指定了特定表，只处理指定的表
                if specified_tables and table not in specified_tables:
                    continue
                
                if has_prefix:
                    target_tables.append(table)
            
            logger.info(f"✅ 找到 {len(target_tables)} 个目标表")
            
            if self.verbose:
                logger.info(f"目标表列表: {target_tables}")
            
            return target_tables
        except Exception as e:
            logger.error(f"❌ 获取目标表列表失败: {e}")
            raise

    async def check_table_has_code_columns(self, table_name: str) -> Tuple[bool, bool]:
        """
        检查表是否有 ts_code 或 con_code 列
        
        Returns:
            (has_ts_code, has_con_code): 返回两个布尔值，表示是否存在对应列
        """
        try:
            query = """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'tushare'
                AND table_name = $1
                AND column_name IN ('ts_code', 'con_code')
            """
            
            result = await self.db_manager.fetch(query, table_name)
            column_names = {r['column_name'] for r in result}
            
            has_ts_code = 'ts_code' in column_names
            has_con_code = 'con_code' in column_names
            
            return has_ts_code, has_con_code
        except Exception as e:
            logger.warning(f"检查表 {table_name} 的代码列时出错: {e}")
            return False, False

    async def migrate_table(
        self, 
        table_name: str, 
        code_mapping: pd.DataFrame
    ) -> Tuple[int, int]:
        """
        迁移单个表的数据
        
        Args:
            table_name: 表名
            code_mapping: 代码映射DataFrame，包含 ts_code_old 和 ts_code_new 列
            
        Returns:
            (found_count, migrated_count): 找到的记录数和成功迁移的记录数
        """
        try:
            # 检查表是否有 ts_code 或 con_code 列
            has_ts_code, has_con_code = await self.check_table_has_code_columns(table_name)
            
            if not has_ts_code and not has_con_code:
                logger.debug(f"表 {table_name} 没有 ts_code 或 con_code 列，跳过")
                return 0, 0
            
            logger.info(f"开始处理表: {table_name} (ts_code: {has_ts_code}, con_code: {has_con_code})")
            
            # 获取旧代码列表
            old_codes = code_mapping['ts_code_old'].tolist()
            
            # 创建映射字典用于快速查找
            mapping_dict = dict(zip(code_mapping['ts_code_old'], code_mapping['ts_code_new']))
            
            # 分别查询通过 ts_code 和 con_code 匹配的记录
            all_records_dict = {}  # 使用字典去重，key是记录的某个唯一标识
            
            # 1. 查询通过 ts_code 匹配的记录
            if has_ts_code:
                placeholders = ', '.join([f"${i + 1}" for i in range(len(old_codes))])
                query_ts = f"""
                    SELECT *
                    FROM tushare."{table_name}"
                    WHERE ts_code IN ({placeholders})
                """
                records_ts = await self.db_manager.fetch(query_ts, *old_codes)
                
                if records_ts:
                    logger.info(f"  通过 ts_code 匹配到 {len(records_ts)} 条记录")
                    for r in records_ts:
                        # 使用一条记录的哈希值作为唯一标识
                        key = hash(str(dict(r)))
                        all_records_dict[key] = ('ts_code', dict(r))
            
            # 2. 查询通过 con_code 匹配的记录
            if has_con_code:
                placeholders = ', '.join([f"${i + 1}" for i in range(len(old_codes))])
                query_con = f"""
                    SELECT *
                    FROM tushare."{table_name}"
                    WHERE con_code IN ({placeholders})
                """
                records_con = await self.db_manager.fetch(query_con, *old_codes)
                
                if records_con:
                    logger.info(f"  通过 con_code 匹配到 {len(records_con)} 条记录")
                    for r in records_con:
                        key = hash(str(dict(r)))
                        if key not in all_records_dict:
                            all_records_dict[key] = ('con_code', dict(r))
                        else:
                            # 如果记录已经存在，更新匹配字段
                            existing_match_field, _ = all_records_dict[key]
                            all_records_dict[key] = ('both', dict(r))
            
            if not all_records_dict:
                logger.info(f"表 {table_name} 中没有找到需要迁移的记录")
                return 0, 0
            
            found_count = len(all_records_dict)
            logger.info(f"表 {table_name} 找到 {found_count} 条需要迁移的记录（去重后）")
            
            if self.dry_run:
                logger.info(f"[DRY RUN] 将会迁移 {found_count} 条记录")
                return found_count, 0
            
            # 转换为 DataFrame 并进行映射
            mapped_records = []
            unmapped_count = 0
            
            for key, (match_field, record) in all_records_dict.items():
                # 根据匹配字段决定需要映射哪些字段
                mapped_record = record.copy()
                mapping_failed = False
                
                if match_field == 'ts_code':
                    # 只映射 ts_code
                    old_code = record['ts_code']
                    new_code = mapping_dict.get(old_code)
                    if new_code is None:
                        unmapped_count += 1
                        mapping_failed = True
                    else:
                        mapped_record['ts_code'] = new_code
                        
                elif match_field == 'con_code':
                    # 只映射 con_code
                    old_code = record['con_code']
                    new_code = mapping_dict.get(old_code)
                    if new_code is None:
                        unmapped_count += 1
                        mapping_failed = True
                    else:
                        mapped_record['con_code'] = new_code
                        
                elif match_field == 'both':
                    # 两个字段都需要映射
                    ts_mapped = False
                    con_mapped = False
                    
                    # 映射 ts_code
                    if 'ts_code' in record:
                        old_ts_code = record['ts_code']
                        new_ts_code = mapping_dict.get(old_ts_code)
                        if new_ts_code is not None:
                            mapped_record['ts_code'] = new_ts_code
                            ts_mapped = True
                    
                    # 映射 con_code
                    if 'con_code' in record:
                        old_con_code = record['con_code']
                        new_con_code = mapping_dict.get(old_con_code)
                        if new_con_code is not None:
                            mapped_record['con_code'] = new_con_code
                            con_mapped = True
                    
                    # 至少有一个字段映射成功才算成功
                    if not ts_mapped and not con_mapped:
                        unmapped_count += 1
                        mapping_failed = True
                
                if not mapping_failed:
                    mapped_records.append(mapped_record)
            
            if unmapped_count > 0:
                logger.warning(f"表 {table_name} 有 {unmapped_count} 条记录无法映射，跳过这些记录")
            
            if not mapped_records:
                logger.warning(f"表 {table_name} 映射后没有有效记录，跳过")
                return found_count, 0
            
            # 转换为 DataFrame
            df = pd.DataFrame(mapped_records)
            
            # 使用 upsert 模式保存数据
            # 需要确定主键列
            primary_keys = await self._get_table_primary_keys(table_name)
            
            if not primary_keys:
                logger.warning(f"表 {table_name} 无法确定主键，跳过")
                return found_count, 0
            
            # 检查是否需要分批处理（针对大表）
            batch_size = 50000  # 批次大小
            total_migrated = 0
            
            if len(df) > batch_size:
                logger.info(f"表 {table_name} 数据量较大（{len(df)} 条），将分批处理（每批 {batch_size} 条）")
                
                for i in range(0, len(df), batch_size):
                    batch_df = df.iloc[i:i + batch_size]
                    batch_num = i // batch_size + 1
                    total_batches = (len(df) + batch_size - 1) // batch_size
                    
                    logger.info(f"处理第 {batch_num}/{total_batches} 批（{len(batch_df)} 条记录）")
                    
                    migrated_count = await self.db_manager.upsert(
                        df=batch_df,
                        target=f'tushare."{table_name}"',
                        conflict_columns=primary_keys,
                        timestamp_column='update_time' if 'update_time' in batch_df.columns else None
                    )
                    total_migrated += migrated_count
            else:
                # 数据量不大，直接处理
                total_migrated = await self.db_manager.upsert(
                    df=df,
                    target=f'tushare."{table_name}"',
                    conflict_columns=primary_keys,
                    timestamp_column='update_time' if 'update_time' in df.columns else None
                )
            
            logger.info(f"✅ 表 {table_name} 成功迁移 {total_migrated} 条记录")
            
            return found_count, total_migrated
            
        except Exception as e:
            logger.error(f"❌ 迁移表 {table_name} 失败: {e}", exc_info=True)
            raise

    async def _get_table_primary_keys(self, table_name: str) -> List[str]:
        """获取表的主键列"""
        try:
            # 使用字符串拼接构建查询（table_name 是内部生成的，已验证安全）
            query = f"""
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = 'tushare."{table_name}"'::regclass
                AND i.indisprimary
                ORDER BY a.attnum
            """
            
            records = await self.db_manager.fetch(query)
            
            if records:
                return [record['attname'] for record in records]
            
            # 如果没有主键，尝试查找唯一索引
            query = f"""
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = 'tushare."{table_name}"'::regclass
                AND i.indisunique
                AND i.indkey::text ~ '^[0-9]+$'
                ORDER BY a.attnum
                LIMIT 5
            """
            
            records = await self.db_manager.fetch(query)
            
            if records:
                return [record['attname'] for record in records]
            
            # 默认情况：如果表有 ts_code 或 con_code 列，使用作为唯一键
            # 如果有 trade_date 或 ann_date 列，也加入主键
            query = f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'tushare'
                AND table_name = '{table_name}'
            """
            columns = await self.db_manager.fetch(query)
            
            column_names = [r['column_name'] for r in columns]
            
            # 构建默认主键
            default_keys = []
            if 'ts_code' in column_names:
                default_keys.append('ts_code')
            if 'con_code' in column_names:
                default_keys.append('con_code')
            if 'trade_date' in column_names:
                default_keys.append('trade_date')
            if 'ann_date' in column_names:
                default_keys.append('ann_date')
            if 'end_date' in column_names:
                default_keys.append('end_date')
            
            return default_keys if default_keys else []
            
        except Exception as e:
            logger.warning(f"获取表 {table_name} 的主键时出错: {e}")
            return []

    async def migrate(self, specified_tables: Optional[List[str]] = None):
        """执行迁移"""
        try:
            self.stats['start_time'] = datetime.now()
            
            logger.info("=" * 80)
            logger.info("开始北交所代码切换映射迁移")
            logger.info("=" * 80)
            
            if self.dry_run:
                logger.info("⚠️  DRY RUN 模式：仅模拟，不会实际修改数据")
            
            # 1. 获取代码映射
            code_mapping = await self.get_code_mapping()
            if code_mapping.empty:
                logger.warning("没有找到代码映射，停止迁移")
                return
            
            # 2. 获取目标表列表
            target_tables = await self.get_target_tables(specified_tables)
            if not target_tables:
                logger.warning("没有找到需要迁移的表")
                return
            
            self.stats['tables_found'] = len(target_tables)
            
            # 3. 逐个处理表
            for table_name in target_tables:
                try:
                    found_count, migrated_count = await self.migrate_table(
                        table_name, 
                        code_mapping
                    )
                    
                    self.stats['total_records_found'] += found_count
                    self.stats['total_records_migrated'] += migrated_count
                    self.stats['tables_processed'] += 1
                    
                except Exception as e:
                    logger.error(f"处理表 {table_name} 时出错: {e}")
                    self.stats['tables_failed'] += 1
                    continue
            
            # 4. 输出统计信息
            self.stats['end_time'] = datetime.now()
            self._print_summary()
            
        except Exception as e:
            logger.error(f"迁移过程出错: {e}", exc_info=True)
            raise
        finally:
            await self.cleanup()

    def _print_summary(self):
        """打印统计摘要"""
        logger.info("=" * 80)
        logger.info("迁移完成统计")
        logger.info("=" * 80)
        
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        logger.info(f"发现表数: {self.stats['tables_found']}")
        logger.info(f"成功处理: {self.stats['tables_processed']}")
        logger.info(f"处理失败: {self.stats['tables_failed']}")
        logger.info(f"找到记录数: {self.stats['total_records_found']}")
        logger.info(f"迁移记录数: {self.stats['total_records_migrated']}")
        logger.info(f"耗时: {duration:.2f} 秒")
        
        if self.dry_run:
            logger.info("=" * 80)
            logger.info("⚠️  这是 DRY RUN 模式，没有实际修改数据")
            logger.info("=" * 80)

    async def cleanup(self):
        """清理资源"""
        if self.db_manager:
            await self.db_manager.close()
            logger.info("数据库连接已关闭")


async def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='北交所代码切换映射迁移脚本',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='模拟模式，不实际修改数据'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='显示详细信息'
    )
    
    parser.add_argument(
        '--tables',
        nargs='+',
        help='指定要迁移的表（表名列表）'
    )
    
    args = parser.parse_args()
    
    # 创建迁移器
    migrator = BSECodeMappingMigrator(
        dry_run=args.dry_run,
        verbose=args.verbose
    )
    
    # 初始化
    if not await migrator.initialize():
        logger.error("初始化失败，退出")
        return 1
    
    try:
        # 执行迁移
        await migrator.migrate(specified_tables=args.tables)
        return 0
    except Exception as e:
        logger.error(f"迁移失败: {e}", exc_info=True)
        return 1
    finally:
        await migrator.cleanup()


if __name__ == '__main__':
    exit_code = asyncio.run(main())
    sys.exit(exit_code)

