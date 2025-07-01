#!/usr/bin/env python3
"""
iFindAPI 增强功能演示
展示改进后的 API 接口，特别是 basic_data_service 的易用性提升
"""

import asyncio
import sys
import os

# 添加项目根目录到路径
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from alphahome.common.config_manager import ConfigManager
from alphahome.fetchers.sources.ifind.ifind_api import iFindAPI

async def demo_basic_data_service():
    """演示改进后的 basic_data_service API"""
    print("=== iFindAPI 增强功能演示 ===\n")
    
    config_manager = ConfigManager()
    async with iFindAPI(config_manager) as api:
        
        print("1. 简单用法 - 传入指标列表")
        try:
            result = await api.basic_data_service(
                codes="000001.SZ,600519.SH",
                indicators=["ths_stock_short_name_stock", "ths_pe_ttm_stock"]
            )
            print(f"✅ 成功获取数据，errorcode: {result.get('errorcode')}")
            if result.get('tables'):
                print(f"   返回 {len(result['tables'])} 只股票的数据")
                for table in result['tables'][:2]:  # 只显示前2个
                    print(f"   - {table.get('thscode')}: {table.get('table', {})}")
        except Exception as e:
            print(f"❌ 请求失败: {e}")
        
        print("\n" + "="*50 + "\n")
        
        print("2. 字符串用法 - 分号分隔指标")
        try:
            result = await api.basic_data_service(
                codes="000001.SZ",
                indicators="ths_stock_short_name_stock;ths_listed_market_stock"
            )
            print(f"✅ 成功获取数据，errorcode: {result.get('errorcode')}")
            if result.get('tables'):
                print(f"   股票数据: {result['tables'][0].get('table', {})}")
        except Exception as e:
            print(f"❌ 请求失败: {e}")
        
        print("\n" + "="*50 + "\n")
        
        print("3. 高级用法 - 带参数的指标")
        try:
            result = await api.basic_data_service(
                codes="000001.SZ",
                indicators=["ths_close_price_stock", "ths_pe_ttm_stock"],
                indiparams=[["", "100", ""], [""]]  # 第一个指标有参数，第二个没有
            )
            print(f"✅ 成功获取数据，errorcode: {result.get('errorcode')}")
            if result.get('tables'):
                print(f"   股票数据: {result['tables'][0].get('table', {})}")
        except Exception as e:
            print(f"❌ 请求失败: {e}")
        
        print("\n" + "="*50 + "\n")
        
        print("4. 其他便捷API方法示例")
        print("   📊 high_frequency - 分钟级数据")
        print("   📈 real_time_quotation - 实时行情")
        print("   📉 cmd_history_quotation - 历史行情")
        print("   📅 date_sequence - 多日数据序列")
        print("   📋 data_pool - 专题报表")
        print("   🏦 edb_service - 经济数据库")
        print("   ⚡ snap_shot - tick数据")
        print("   📰 report_query - 公告查询")
        print("   🎯 smart_stock_picking - 智能选股")
        print("   📆 get_trade_dates - 交易日查询")
        
        print("\n所有方法都支持类型提示和详细的文档说明！")

if __name__ == "__main__":
    print("请确保已在 config.json 中配置了有效的 iFind API 信息")
    print("按 Enter 键开始演示...")
    input()
    
    asyncio.run(demo_basic_data_service()) 