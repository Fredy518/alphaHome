#!/usr/bin/env python3
"""
Hikyuu 导出器复权支持示例

演示如何使用更新后的 HikyuuH5Exporter 导出前复权数据
"""

import pandas as pd
from alphahome.providers.tools.hikyuu_h5_exporter import HikyuuH5Exporter

def create_sample_data():
    """创建示例数据"""
    
    # 原始价格数据
    raw_data = pd.DataFrame({
        'ts_code': ['000001.SZ', '000001.SZ', '000001.SZ'],
        'trade_date': ['2023-01-01', '2023-01-02', '2023-01-03'],
        'open': [10.0, 10.5, 11.0],
        'high': [10.8, 11.2, 11.5],
        'low': [9.8, 10.2, 10.8],
        'close': [10.5, 11.0, 11.2],
        'vol': [1000000, 1200000, 1100000],
        'amount': [10500000, 13200000, 12320000]
    })
    
    # 复权因子数据
    adj_data = pd.DataFrame({
        'ts_code': ['000001.SZ', '000001.SZ', '000001.SZ'],
        'trade_date': ['2023-01-01', '2023-01-02', '2023-01-03'],
        'adj_factor': [1.0, 0.95, 0.90]  # 模拟分红导致的价格调整
    })
    
    return raw_data, adj_data

def main():
    """主函数"""
    
    print("=== Hikyuu 导出器复权支持示例 ===")
    
    # 创建示例数据
    raw_data, adj_data = create_sample_data()
    
    print("原始价格数据:")
    print(raw_data)
    print("\n复权因子数据:")
    print(adj_data)
    
    # 创建导出器
    exporter = HikyuuH5Exporter("E:/stock")
    
    # 导出前复权数据
    print("\n导出前复权数据...")
    exporter.export_day_incremental(raw_data, adj_data)
    
    print("导出完成！")
    print("\n复权计算说明:")
    print("- 前复权价格 = 原始价格 × 复权因子")
    print("- 复权因子 < 1.0 表示分红送股等导致的价格调整")
    print("- 导出到 Hikyuu 的数据已经是前复权价格，可直接用于回测")

if __name__ == "__main__":
    main()
