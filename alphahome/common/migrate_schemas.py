#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据库Schema迁移脚本

将tushare和system schema中的所有表迁移到public schema
使用项目现有的数据库管理架构
"""

import argparse
import sys
from typing import Dict, List, Tuple

from .config_manager import get_database_url
from .db_manager import create_sync_manager
from .logging_utils import get_logger

logger = get_logger(__name__)


class SchemaMigrator:
    """Schema迁移管理器"""
    
    def __init__(self, db_url: str):
        """初始化迁移器
        
        Args:
            db_url (str): 数据库连接URL
        """
        self.db_url = db_url
        self.db_manager = None
        
    def connect(self):
        """建立数据库连接"""
        try:
            self.db_manager = create_sync_manager(self.db_url)
            logger.info("数据库连接创建成功")
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            raise
    
    def get_schema_tables(self) -> Dict[str, List[str]]:
        """获取各schema中的表列表
        
        Returns:
            Dict[str, List[str]]: 各schema及其表的映射
        """
        query = """
        SELECT schemaname, tablename 
        FROM pg_tables 
        WHERE schemaname IN ('tushare', 'system', 'public') 
        ORDER BY schemaname, tablename
        """
        
        try:
            rows = self.db_manager.fetch_sync(query)
            schema_tables = {'tushare': [], 'system': [], 'public': []}
            
            for row in rows:
                schema_name = row['schemaname']
                table_name = row['tablename']
                if schema_name in schema_tables:
                    schema_tables[schema_name].append(table_name)
            
            return schema_tables
        except Exception as e:
            logger.error(f"获取schema表列表失败: {e}")
            raise
    
    def check_table_conflicts(self, source_tables: List[str]) -> List[str]:
        """检查表名冲突
        
        Args:
            source_tables (List[str]): 源表列表
            
        Returns:
            List[str]: 冲突的表名列表
        """
        if not source_tables:
            return []
            
        placeholders = ','.join(['%s'] * len(source_tables))
        query = f"""
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'public' 
        AND tablename IN ({placeholders})
        """
        
        try:
            rows = self.db_manager.fetch_sync(query, tuple(source_tables))
            return [row['tablename'] for row in rows]
        except Exception as e:
            logger.error(f"检查表名冲突失败: {e}")
            raise
    
    def migrate_tables_to_public(self, schema_tables: Dict[str, List[str]]) -> Tuple[bool, str]:
        """迁移表到public schema
        
        Args:
            schema_tables (Dict[str, List[str]]): 需要迁移的schema和表
            
        Returns:
            Tuple[bool, str]: (是否成功, 错误信息)
        """
        # 计算总表数
        total_tables = sum(len(tables) for schema, tables in schema_tables.items() 
                          if schema in ['tushare', 'system'] and tables)
        
        if total_tables == 0:
            return True, "没有需要迁移的表"
        
        logger.info(f"开始迁移 {total_tables} 个表到 public schema")
        
        # 开始事务
        try:
            connection = self.db_manager._get_sync_connection()
            with connection.cursor() as cursor:
                cursor.execute("BEGIN;")
                
                migrated_count = 0
                
                # 迁移system schema的表
                if schema_tables.get('system'):
                    for table_name in schema_tables['system']:
                        sql = f"ALTER TABLE system.{table_name} SET SCHEMA public;"
                        logger.info(f"执行: {sql}")
                        cursor.execute(sql)
                        migrated_count += 1
                        logger.info(f"已迁移 {migrated_count}/{total_tables}: system.{table_name}")
                
                # 迁移tushare schema的表
                if schema_tables.get('tushare'):
                    for table_name in schema_tables['tushare']:
                        sql = f"ALTER TABLE tushare.{table_name} SET SCHEMA public;"
                        logger.info(f"执行: {sql}")
                        cursor.execute(sql)
                        migrated_count += 1
                        logger.info(f"已迁移 {migrated_count}/{total_tables}: tushare.{table_name}")
                
                # 提交事务
                cursor.execute("COMMIT;")
                connection.commit()
                
                logger.info(f"所有 {migrated_count} 个表已成功迁移到 public schema")
                return True, ""
                
        except Exception as e:
            # 回滚事务
            try:
                cursor.execute("ROLLBACK;")
                connection.rollback()
                logger.warning("事务已回滚")
            except:
                pass
            
            error_msg = f"迁移失败: {e}"
            logger.error(error_msg)
            return False, error_msg
    
    def verify_migration(self, original_counts: Dict[str, int]) -> bool:
        """验证迁移结果
        
        Args:
            original_counts (Dict[str, int]): 原始表数量统计
            
        Returns:
            bool: 验证是否通过
        """
        try:
            current_tables = self.get_schema_tables()
            
            # 检查public schema中的表数量
            expected_public_count = (original_counts.get('tushare', 0) + 
                                   original_counts.get('system', 0) + 
                                   original_counts.get('public', 0))
            actual_public_count = len(current_tables['public'])
            
            # 检查原schema是否已清空
            tushare_empty = len(current_tables['tushare']) == 0
            system_empty = len(current_tables['system']) == 0
            
            logger.info(f"验证结果:")
            logger.info(f"  public schema 表数量: {actual_public_count} (期望: {expected_public_count})")
            logger.info(f"  tushare schema 是否为空: {tushare_empty}")
            logger.info(f"  system schema 是否为空: {system_empty}")
            
            success = (actual_public_count == expected_public_count and 
                      tushare_empty and system_empty)
            
            if success:
                logger.info("✓ 迁移验证通过")
            else:
                logger.error("✗ 迁移验证失败")
            
            return success
            
        except Exception as e:
            logger.error(f"验证迁移结果失败: {e}")
            return False
    
    def run_migration(self, force: bool = False) -> bool:
        """执行完整的迁移流程
        
        Args:
            force (bool): 是否跳过确认提示
            
        Returns:
            bool: 迁移是否成功
        """
        try:
            # 1. 连接数据库
            self.connect()
            
            # 2. 获取当前状态
            logger.info("检查当前数据库状态...")
            schema_tables = self.get_schema_tables()
            
            # 统计表数量
            table_counts = {schema: len(tables) for schema, tables in schema_tables.items()}
            logger.info(f"当前状态:")
            for schema, count in table_counts.items():
                logger.info(f"  {schema} schema: {count} 个表")
            
            # 检查是否有需要迁移的表
            tables_to_migrate = {}
            if schema_tables['system']:
                tables_to_migrate['system'] = schema_tables['system']
            if schema_tables['tushare']:
                tables_to_migrate['tushare'] = schema_tables['tushare']
            
            if not tables_to_migrate:
                logger.info("没有需要迁移的表，所有表可能已经在 public schema 中")
                return True
            
            # 显示迁移计划
            total_migrate = sum(len(tables) for tables in tables_to_migrate.values())
            logger.info(f"\n迁移计划:")
            logger.info(f"  将迁移 {total_migrate} 个表到 public schema")
            for schema, tables in tables_to_migrate.items():
                logger.info(f"  {schema} schema: {len(tables)} 个表")
            
            # 3. 检查表名冲突
            all_source_tables = []
            for tables in tables_to_migrate.values():
                all_source_tables.extend(tables)
            
            conflicts = self.check_table_conflicts(all_source_tables)
            if conflicts:
                logger.error(f"发现表名冲突: {conflicts}")
                logger.error("请先解决冲突或删除 public schema 中的冲突表")
                return False
            
            # 4. 用户确认
            if not force:
                print(f"\n即将迁移 {total_migrate} 个表到 public schema")
                print("这个操作将:")
                print("1. 将所有 tushare 和 system schema 中的表移动到 public schema")
                print("2. 保持表结构、数据和索引不变")
                print("3. 操作不可自动撤销")
                
                response = input("\n确认执行迁移? (y/N): ").strip().lower()
                if response not in ['y', 'yes']:
                    logger.info("用户取消迁移操作")
                    return False
            
            # 5. 执行迁移
            success, error_msg = self.migrate_tables_to_public(tables_to_migrate)
            if not success:
                logger.error(f"迁移失败: {error_msg}")
                return False
            
            # 6. 验证结果
            if not self.verify_migration(table_counts):
                logger.error("迁移验证失败")
                return False
            
            logger.info("✓ Schema 迁移完成!")
            return True
            
        except Exception as e:
            logger.error(f"迁移过程中发生错误: {e}")
            return False


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="迁移数据库表到 public schema")
    parser.add_argument("--force", action="store_true", help="跳过确认提示，直接执行")
    parser.add_argument("--db-url", type=str, help="数据库连接URL (覆盖配置文件)")
    
    args = parser.parse_args()
    
    # 获取数据库URL
    db_url = args.db_url
    if not db_url:
        try:
            db_url = get_database_url()
        except Exception as e:
            logger.error(f"从配置文件获取数据库URL失败: {e}")
            sys.exit(1)
    
    if not db_url:
        logger.error("未提供数据库URL，请检查配置文件或使用 --db-url 参数")
        sys.exit(1)
    
    # 执行迁移
    migrator = SchemaMigrator(db_url)
    success = migrator.run_migration(force=args.force)
    
    if success:
        logger.info("Schema迁移成功完成")
        sys.exit(0)
    else:
        logger.error("Schema迁移失败")
        sys.exit(1)


if __name__ == "__main__":
    main() 