import asyncio
import logging
from alphahome.data_module.data_checker import DataQualityChecker
from alphahome.data_module.task_factory import TaskFactory
from datetime import datetime

async def main():
    # 初始化数据库连接
    await TaskFactory.initialize()
    db_manager = TaskFactory.get_db_manager()
    
    missing_months = await check_missing_months('tushare_fund_portfolio', db_manager)
    print("缺失的月份:", missing_months)
    


async def check_missing_months(table_name, db_manager):
    """检查指定表中缺失的月份"""
    # 获取所有的 ann_date
    query = f"SELECT DISTINCT ann_date FROM {table_name}"
    ann_dates = await db_manager.fetch(query)
    
    # 提取月份
    months = {record['ann_date'].strftime('%Y-%m') for record in ann_dates}
    
    # 生成预期的月份列表
    start_date = "1998-10"  # 假设从2015年1月开始
    end_date = datetime.now().strftime('%Y-%m')
    all_months = {f"{year}-{month:02d}" for year in range(int(start_date[:4]), int(end_date[:4]) + 1)
                   for month in range(1, 13)}
    
    # 找出缺失的月份
    missing_months = sorted(all_months - months)
    
    return missing_months

# 在 main 函数中调用


# 运行主函数
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())

