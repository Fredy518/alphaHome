#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
使用真实数据库测试 StockAdjDailyProcessor。
请确保已正确配置 config.json 文件，并且数据库服务正在运行。
"""

import asyncio
import logging
import json
import os
import pandas as pd
from datetime import date, timedelta, datetime

import appdirs # 用于定位配置文件

# 假设项目根目录已添加到 PYTHONPATH
# 或者在运行时使用 python -m tests.processors.test_stock_adjdaily_processor_real_data
from alphahome.fetchers.db_manager import DBManager
from alphahome.processors.tasks.stock_adjdaily_processor import StockAdjDailyProcessor

# --- 配置 (与 alphahome/gui/controller.py 中保持一致) ---
APP_NAME = "alphahome"
APP_AUTHOR = "trademaster"
CONFIG_DIR = appdirs.user_config_dir(APP_NAME, APP_AUTHOR)
CONFIG_FILE_PATH = os.path.join(CONFIG_DIR, 'config.json')

def load_db_url_from_config() -> str | None:
    """从用户配置文件加载数据库URL。"""
    if not os.path.exists(CONFIG_FILE_PATH):
        logging.error(f"错误：配置文件 {CONFIG_FILE_PATH} 不存在。请确保已运行GUI或手动创建了配置文件。")
        logging.error(f"配置文件示例应包含：{{\"database\": {{\"url\": \"postgresql://user:pass@host:port/db\"}}}}")
        return None
    try:
        with open(CONFIG_FILE_PATH, 'r', encoding='utf-8') as f:
            config = json.load(f)
        db_config = config.get('database')
        if not db_config or 'url' not in db_config:
            logging.error(f"配置文件 {CONFIG_FILE_PATH} 中缺少 'database.url' 配置。")
            return None
        return db_config['url']
    except Exception as e:
        logging.error(f"加载或解析配置文件 {CONFIG_FILE_PATH} 时出错: {e}", exc_info=True)
        return None

async def run_real_data_test():
    """执行使用真实数据库的 StockAdjDailyProcessor 测试。"""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    logger.info("开始真实数据测试...")

    db_url = load_db_url_from_config()
    if not db_url:
        logger.error("未能获取数据库URL，测试终止。")
        return

    logger.info(f"使用数据库URL: {db_url[:db_url.find('@') + 1]}... (凭证已隐藏)") # 隐藏凭证部分

    db_manager = None
    try:
        db_manager = DBManager(db_url)
        await db_manager.connect()
        logger.info("成功连接到数据库。")

        # --- 测试参数 ---
        # !!! 注意：请选择一个非常小的数据范围和少量代码进行测试 !!!
        # !!! 避免对生产数据造成意外影响或长时间运行 !!!
        test_codes = ['000001.SZ'] # 例如，只测试一只股票
        test_start_date_str = '20230101' 
        test_end_date_str = '20230105'   
        
        # !! 重要: 如果您的真实表名与StockAdjDailyProcessor中的默认值不同，在此处指定
        # !! 考虑使用一个专门的测试结果表，或者确保测试数据范围极小且易于清理
        # !! 如果 result_table 是生产表, replace_existing=True 会清除符合条件的数据!
        processor_config = {
            # 'source_table_stock_factor': 'your_real_stock_factor_table', # 如果需要覆盖
            # 'source_table_calendar': 'your_real_calendar_table',       # 如果需要覆盖
            # 'result_table': 'stock_daily_adjusted_hfq_test_real', # 强烈建议使用测试专用表
            'block_size_codes': 1, # 测试时块大小设为1，方便观察单个代码的处理
            'default_calendar_start_date': '20221201', # 日历加载范围可以缩小以加快测试
            'default_calendar_end_date': '20230228'
        }
        # 如果不指定 result_table, 将使用 StockAdjDailyProcessor 中的默认表 'stock_daily_adjusted_hfq'
        # 请确保您了解其后果，特别是 replace_existing=True 时。

        target_result_table = processor_config.get('result_table', 'stock_daily_adjusted_hfq')
        logger.info(f"处理器将尝试写入表: {target_result_table}")
        logger.warning(f"重要：测试将针对表 '{target_result_table}'。如果 replace_existing=True (默认为True, 如果未在execute中指定为False)，则 codes='{test_codes}', dates='{test_start_date_str}-{test_end_date_str}' 范围内的数据将被清除并重写。")

        processor = StockAdjDailyProcessor(db_connection=db_manager, config=processor_config)

        logger.info(f"执行处理器: codes={test_codes}, start_date={test_start_date_str}, end_date={test_end_date_str}")
        
        # 在 BlockProcessor.execute 中，save_result 默认为 True。
        # 在 StockAdjDailyProcessor 的父类 BlockProcessor 的 execute 实现中，
        # _clear_existing_results 会在其内部被调用（如果 save_result=True 且 replace_existing=True (默认)）。
        # StockAdjDailyProcessor 重写了 _clear_existing_results 以基于块的实际数据范围进行清除。
        results = await processor.execute(
            codes=test_codes,
            start_date=test_start_date_str,
            end_date=test_end_date_str,
            replace_existing=True # 确保测试的幂等性，会清除本次测试范围内的数据
        )
        logger.info(f"处理器执行完成。结果: {results}")

        # --- 数据验证 --- 
        logger.info(f"开始从真实数据库表 '{target_result_table}' 进行数据验证...")
        validation_passed = True
        for code in test_codes:
            # 注意：这里的参数化查询语法 $1, $2, $3 是 asyncpg 的风格
            # 如果您的DBManager或数据库驱动使用不同的占位符（如 %s 或 ?），请相应调整
            query = f"SELECT ts_code, trade_date, open_hfq, close_hfq, volume_hfq, amount_hfq, adj_factor, is_trade FROM {target_result_table} WHERE ts_code = $1 AND trade_date >= $2 AND trade_date <= $3 ORDER BY trade_date"
            
            # 将字符串日期转换为date对象进行比较（如果数据库列是date类型）
            start_date_obj = datetime.strptime(test_start_date_str, '%Y%m%d').date()
            end_date_obj = datetime.strptime(test_end_date_str, '%Y%m%d').date()

            fetched_rows = await db_manager.fetch(query, code, start_date_obj, end_date_obj)

            if fetched_rows:
                df = pd.DataFrame([dict(row) for row in fetched_rows])
                logger.info(f"-- 验证 {code} ---")
                logger.info(f"从表 '{target_result_table}' 中为代码 {code} 和日期范围 {test_start_date_str}-{test_end_date_str} 获取了 {len(df)} 行数据:")
                print(df.to_string())

                # 1. 检查日期是否都在请求范围内
                df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
                if not df['trade_date'].between(start_date_obj, end_date_obj, inclusive='both').all():
                    logger.error(f"验证失败 ({code}): 有些数据的 trade_date 超出了请求范围 {start_date_obj}-{end_date_obj}")
                    validation_passed = False
                
                # 2. 检查是否有 is_trade = 0 的情况 (如果源数据中有停牌且日历包含这些天)
                #    这需要对照源数据和日历进行更复杂的验证，此处仅作示例
                if 0 in df['is_trade'].values:
                    logger.info(f"验证 ({code}): 数据中包含 is_trade=0 的行，可能表示停牌日被正确填充。")
                else:
                    logger.info(f"验证 ({code}): 数据中未发现 is_trade=0 的行。这可能是因为该时段无停牌，或日历不包含这些非交易日。")

                # 3. 简单检查数值列是否存在 (更复杂的检查需要预期值)
                for col_to_check in ['close_hfq', 'volume_hfq', 'adj_factor']:
                    if col_to_check not in df.columns:
                        logger.error(f"验证失败 ({code}): 结果数据中缺少列 {col_to_check}")
                        validation_passed = False
                        break
                    if df[col_to_check].isnull().all():
                         logger.warning(f"验证注意 ({code}): 列 {col_to_check} 全部为NULL。请检查源数据和处理逻辑。")

            else:
                logger.warning(f"验证 ({code}): 未能在表 '{target_result_table}' 中找到处理结果。这可能是预期的（如果没有源数据），也可能是一个问题。")
                # 如果预期一定有数据，这里应该 validation_passed = False

        if validation_passed:
            logger.info("所有基本数据验证通过！")
        else:
            logger.error("某些数据验证失败，请检查日志。")

    except Exception as e:
        logger.error(f"真实数据测试过程中发生严重错误: {e}", exc_info=True)
    finally:
        if db_manager:
            await db_manager.close()
            logger.info("数据库连接已关闭。")
    logger.info("真实数据测试结束。")

if __name__ == '__main__':
    # 确保项目根目录在 PYTHONPATH 中，或者从项目根目录使用以下命令运行:
    # python -m tests.processors.test_stock_adjdaily_processor_real_data
    asyncio.run(run_real_data_test()) 