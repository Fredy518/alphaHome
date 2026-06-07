"""
重新创建 stock_sharefloat_schedule 特征

问题：老的物化视图没有 as_of_date 字段，需要删除并重新创建新结构。

使用方法：
    python scripts/maintenance/recreate_stock_sharefloat_schedule.py
"""
import asyncio
from alphahome.common.config_manager import ConfigManager
from alphahome.common.db_manager import DBManager
from alphahome.features.recipes.mv.stock.stock_sharefloat_schedule import StockSharefloatScheduleMV


async def recreate_view():
    """删除旧视图并创建新结构"""
    config = ConfigManager()
    db_url = config.get_database_url()
    db_manager = DBManager(db_url, mode="async")
    
    try:
        await db_manager.connect()
        print("✓ 数据库连接成功")
        
        # 1. 删除旧的物化视图
        print("\n1. 删除旧的物化视图...")
        await db_manager.execute(
            "DROP MATERIALIZED VIEW IF EXISTS features.mv_stock_sharefloat_schedule CASCADE;"
        )
        print("✓ 旧视图已删除")
        
        # 2. 创建新的物化视图（带 as_of_date 字段）
        print("\n2. 创建新的物化视图结构...")
        recipe = StockSharefloatScheduleMV(db_manager=db_manager)
        success = await recipe.create()
        
        if success:
            print("✓ 新视图创建成功")
        else:
            print("✗ 新视图创建失败")
            return
        
        # 3. 全量刷新数据
        print("\n3. 开始全量刷新（这可能需要几分钟）...")
        result = await recipe.refresh(strategy="full")
        
        if result.get("status") == "success":
            rows = result.get("rows_affected", "未知")
            print(f"✓ 全量刷新完成！共 {rows} 行数据")
        else:
            print(f"✗ 刷新失败: {result.get('error_message')}")
        
        # 4. 验证数据
        print("\n4. 验证数据...")
        row = await db_manager.fetch_one(
            """
            SELECT 
                COUNT(*) as total_rows,
                as_of_date,
                COUNT(*) FILTER (WHERE float_pressure_level = 'HIGH') as high_pressure_count,
                COUNT(*) FILTER (WHERE float_pressure_level = 'MEDIUM') as medium_pressure_count
            FROM features.mv_stock_sharefloat_schedule
            GROUP BY as_of_date
            """
        )
        if row:
            print(f"   计算基准日期: {row['as_of_date']}")
            print(f"   总股票数: {row['total_rows']}")
            print(f"   HIGH 压力: {row['high_pressure_count']} 只")
            print(f"   MEDIUM 压力: {row['medium_pressure_count']} 只")
        
        print("\n✓ 重建完成！")
        
    except Exception as e:
        print(f"\n✗ 错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await db_manager.close()


if __name__ == "__main__":
    asyncio.run(recreate_view())
