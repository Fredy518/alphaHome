#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
财务指标表主键迁移脚本

此脚本处理 fina_indicator 表的历史数据问题：
1. 清理 ann_date 为 NULL 的记录
2. 删除重复记录
3. 更新主键约束以包含 ann_date

这些操作之前在 TushareFinaIndicatorTask.pre_execute 中执行，
现在移到专门的迁移脚本中，符合关注点分离原则。
"""

import asyncio
import logging
from datetime import datetime
from typing import Optional

import asyncpg


class FinaIndicatorMigration:
    """财务指标表迁移工具"""
    
    def __init__(self, db_connection):
        self.db = db_connection
        self.logger = logging.getLogger(__name__)
        self.table_name = "fina_indicator"
        self.target_primary_keys = ["ts_code", "end_date", "ann_date"]
    
    async def migrate(self):
        """执行完整的迁移流程"""
        self.logger.info(f"开始迁移表 {self.table_name}")
        
        try:
            # 检查表是否存在
            if not await self._table_exists():
                self.logger.info(f"表 {self.table_name} 不存在，跳过迁移")
                return
            
            # 步骤1：清理空的 ann_date
            await self._fix_null_ann_date()
            
            # 步骤2：删除重复记录
            await self._remove_duplicates()
            
            # 步骤3：更新主键约束
            await self._update_primary_key()
            
            self.logger.info(f"表 {self.table_name} 迁移完成")
            
        except Exception as e:
            self.logger.error(f"迁移失败: {e}", exc_info=True)
            raise
    
    async def _table_exists(self) -> bool:
        """检查表是否存在"""
        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = $1
        )
        """
        result = await self.db.fetchval(query, self.table_name)
        return result
    
    async def _fix_null_ann_date(self):
        """修复 ann_date 为 NULL 的记录"""
        self.logger.info("步骤1: 修复空的 ann_date")
        
        # 检查空值数量
        count_query = f"SELECT COUNT(*) FROM {self.table_name} WHERE ann_date IS NULL"
        null_count = await self.db.fetchval(count_query)
        
        if null_count > 0:
            self.logger.info(f"发现 {null_count} 条 ann_date 为空的记录")
            
            # 使用 end_date 填充空的 ann_date
            update_query = f"""
            UPDATE {self.table_name} 
            SET ann_date = end_date 
            WHERE ann_date IS NULL
            """
            await self.db.execute(update_query)
            self.logger.info(f"已修复 {null_count} 条记录")
        else:
            self.logger.info("没有发现空的 ann_date 记录")
    
    async def _remove_duplicates(self):
        """删除重复记录"""
        self.logger.info("步骤2: 删除重复记录")
        
        # 检查重复记录
        duplicate_query = f"""
        SELECT ts_code, end_date, ann_date, COUNT(*) as count
        FROM {self.table_name}
        GROUP BY ts_code, end_date, ann_date
        HAVING COUNT(*) > 1
        """
        duplicates = await self.db.fetch(duplicate_query)
        
        if duplicates:
            self.logger.info(f"发现 {len(duplicates)} 组重复记录")
            
            # 创建临时表保留最新记录
            temp_table = f"temp_{self.table_name}_{int(datetime.now().timestamp())}"
            
            create_temp_query = f"""
            CREATE TEMP TABLE {temp_table} AS
            SELECT DISTINCT ON (ts_code, end_date, ann_date) *
            FROM {self.table_name}
            ORDER BY ts_code, end_date, ann_date, update_time DESC NULLS LAST
            """
            await self.db.execute(create_temp_query)
            
            # 清空原表并恢复数据
            await self.db.execute(f"TRUNCATE TABLE {self.table_name}")
            await self.db.execute(f"INSERT INTO {self.table_name} SELECT * FROM {temp_table}")
            await self.db.execute(f"DROP TABLE {temp_table}")
            
            self.logger.info("重复记录清理完成")
        else:
            self.logger.info("没有发现重复记录")
    
    async def _update_primary_key(self):
        """更新主键约束"""
        self.logger.info("步骤3: 更新主键约束")
        
        # 检查当前主键
        current_pk_query = """
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = $1::regclass AND i.indisprimary
        """
        current_pks = await self.db.fetch(current_pk_query, self.table_name)
        current_pk_names = [row['attname'] for row in current_pks]
        
        if set(current_pk_names) != set(self.target_primary_keys):
            self.logger.info(f"更新主键: {current_pk_names} -> {self.target_primary_keys}")
            
            # 删除现有主键约束
            drop_pk_query = f"ALTER TABLE {self.table_name} DROP CONSTRAINT IF EXISTS {self.table_name}_pkey"
            await self.db.execute(drop_pk_query)
            
            # 添加新主键约束
            add_pk_query = f"ALTER TABLE {self.table_name} ADD PRIMARY KEY ({', '.join(self.target_primary_keys)})"
            await self.db.execute(add_pk_query)
            
            self.logger.info("主键约束更新完成")
        else:
            self.logger.info("主键约束已经正确，无需更新")


async def run_migration(connection_string: str):
    """运行迁移脚本"""
    conn = await asyncpg.connect(connection_string)
    try:
        migration = FinaIndicatorMigration(conn)
        await migration.migrate()
    finally:
        await conn.close()


if __name__ == "__main__":
    import os
    
    # 从环境变量获取数据库连接字符串
    db_url = os.getenv("DATABASE_URL", "postgresql://user:password@localhost/dbname")
    
    # 运行迁移
    asyncio.run(run_migration(db_url))
