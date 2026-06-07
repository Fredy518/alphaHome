#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
批量迁移脚本：为现有表创建 rawdata 视图映射

功能：
- 扫描所有数据源 schema（tushare, akshare, ifind, pytdx等）
- 按优先级为现有表创建 rawdata 视图
- tushare 优先，同名表只映射 tushare 的版本
- 生成详细的迁移报告

使用方式：
    python scripts/migrate_existing_tables_to_rawdata.py

输出：
    - 迁移完成统计
    - 创建的视图清单
    - 跳过的表清单（若有）
"""

import asyncio
import sys
from pathlib import Path

# 将项目根目录添加到 sys.path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from alphahome.common.db_manager import create_async_manager
from alphahome.common.logging_utils import get_logger


logger = get_logger(__name__)


async def migrate_existing_tables():
    """
    扫描所有数据源 schema，为现有表创建 rawdata 视图
    
    按优先级处理：tushare -> akshare -> 其他
    """
    # 获取数据库连接字符串（从环境或配置）
    from alphahome.common.config_manager import get_database_url

    connection_string = get_database_url()
    if not connection_string:
        raise ValueError("数据库连接字符串未配置。请检查配置文件中的 database.url 设置。")
    db_manager = create_async_manager(connection_string)
    
    try:
        await db_manager.connect()
        logger.info("数据库连接成功，开始迁移现有表的 rawdata 视图...")
        
        # 数据源优先级（从高到低）
        schemas = ['tushare', 'akshare', 'ifind', 'pytdx']
        created_views = []
        skipped_views = []
        failed_views = []
        
        for schema in schemas:
            logger.info(f"\n扫描 schema: {schema}")
            
            # 获取该 schema 中的所有表
            try:
                tables = await db_manager.get_tables_in_schema(schema)
                logger.info(f"  找到 {len(tables)} 个表")
            except Exception as e:
                logger.warning(f"  读取 schema {schema} 失败: {e}")
                continue
            
            for table in tables:
                view_name = table
                
                try:
                    # tushare：总是创建/覆盖
                    if schema == 'tushare':
                        await db_manager.create_rawdata_view(
                            view_name=view_name,
                            source_schema=schema,
                            source_table=table,
                            replace=True
                        )
                        created_views.append(f"rawdata.{view_name} -> tushare.{table}")
                        logger.info(f"    [OK] 创建: rawdata.{view_name} -> tushare.{table}")
                        continue

                    # 其他源：检查是否已有视图
                    view_exists = await db_manager.view_exists('rawdata', view_name)
                    if view_exists:
                        skipped_views.append(
                            f"{schema}.{table} (rawdata.{view_name} 已存在)"
                        )
                        logger.info(f"    [SKIP] 跳过: {schema}.{table} (视图已存在)")
                        continue

                    # 创建视图
                    await db_manager.create_rawdata_view(
                        view_name=view_name,
                        source_schema=schema,
                        source_table=table,
                        replace=False
                    )
                    created_views.append(f"rawdata.{view_name} -> {schema}.{table}")
                    logger.info(f"    [OK] 创建: rawdata.{view_name} -> {schema}.{table}")
                
                except Exception as e:
                    error_msg = f"{schema}.{table} 创建视图失败: {e}"
                    failed_views.append(error_msg)
                    logger.warning(f"    ✗ 失败: {error_msg}")
        
        # 生成报告
        print("\n" + "="*70)
        print("迁移完成报告")
        print("="*70)
        print(f"[OK] 成功创建视图: {len(created_views)} 个")
        print(f"[SKIP] 跳过的表: {len(skipped_views)} 个")
        print(f"[FAIL] 失败: {len(failed_views)} 个")
        print("="*70)
        
        if created_views:
            print("\n创建的视图:")
            for view in created_views[:20]:  # 只显示前20个
                print(f"  + {view}")
            if len(created_views) > 20:
                print(f"  ... 还有 {len(created_views) - 20} 个视图")

        if skipped_views:
            print("\n跳过的表:")
            for skip in skipped_views[:10]:  # 只显示前10个
                print(f"  - {skip}")
            if len(skipped_views) > 10:
                print(f"  ... 还有 {len(skipped_views) - 10} 个表")

        if failed_views:
            print("\n失败的表:")
            for fail in failed_views:
                print(f"  [FAIL] {fail}")
        
        print("="*70)
        logger.info("迁移完成")
        
    except Exception as e:
        logger.error(f"迁移过程中出错: {e}", exc_info=True)
        raise
    finally:
        await db_manager.close()


async def main():
    """主函数"""
    try:
        await migrate_existing_tables()
    except KeyboardInterrupt:
        logger.info("\n迁移被用户中断")
    except Exception as e:
        logger.error(f"致命错误: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
