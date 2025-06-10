#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
最终版同步回测演示

解决方案特点：
1. 完全同步的数据库管理器
2. 每次查询使用独立的连接和事件循环
3. 彻底避免与backtrader的异步冲突
4. 统一配置管理
"""

import sys
import os
from datetime import date, datetime
import backtrader as bt
import pandas as pd

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from alphahome.backtesting.strategies.examples.dual_moving_average import DualMovingAverageStrategy
from alphahome.common.db_manager import create_sync_manager
from alphahome.common.config_manager import get_database_url, get_backtesting_config


class SyncPostgreSQLDataFeed(bt.feeds.DataBase):
    """
    完全同步的PostgreSQL数据源
    专为backtrader设计，避免异步冲突
    """
    
    params = (
        ('ts_code', None),
        ('table_name', 'tushare_stock_daily'),
        ('sync_db_manager', None),
        ('start_date', None),
        ('end_date', None),
    )
    
    def __init__(self):
        super().__init__()
        self._data_df = None
        self._current_index = 0
        self._total_rows = 0
    
    def start(self):
        """启动数据源，同步加载数据"""
        print(f"   正在加载数据: {self.p.ts_code}")
        
        try:
            # 构建SQL查询
            sql = f"""
            SELECT trade_date, open, high, low, close, volume
            FROM {self.p.table_name} 
            WHERE ts_code = $1
            """
            params = [self.p.ts_code]
            
            if self.p.start_date:
                sql += " AND trade_date >= $2"
                params.append(self.p.start_date)
                
            if self.p.end_date:
                if len(params) == 2:
                    sql += " AND trade_date <= $3"
                else:
                    sql += " AND trade_date <= $2"
                params.append(self.p.end_date)
                
            sql += " ORDER BY trade_date ASC"
            
            # 同步查询数据
            records = self.p.sync_db_manager.fetch(sql, *params)
            
            if not records:
                raise Exception(f"未找到数据: {self.p.ts_code}")
            
            # 转换为DataFrame
            data_list = []
            for record in records:
                data_list.append({
                    'datetime': pd.to_datetime(record['trade_date']),
                    'open': float(record['open']),
                    'high': float(record['high']),
                    'low': float(record['low']),
                    'close': float(record['close']),
                    'volume': float(record['volume'])
                })
            
            self._data_df = pd.DataFrame(data_list)
            self._data_df = self._data_df.sort_values('datetime').reset_index(drop=True)
            self._total_rows = len(self._data_df)
            
            print(f"   ✅ {self.p.ts_code}: 加载 {self._total_rows} 条记录")
            
        except Exception as e:
            print(f"   ❌ {self.p.ts_code}: 数据加载失败 - {e}")
            raise
    
    def _load(self):
        """加载下一条数据"""
        if self._current_index >= self._total_rows:
            return False  # 数据结束
        
        row = self._data_df.iloc[self._current_index]
        
        # 设置OHLCV数据
        self.lines.datetime[0] = bt.date2num(row['datetime'])
        self.lines.open[0] = row['open']
        self.lines.high[0] = row['high']
        self.lines.low[0] = row['low']
        self.lines.close[0] = row['close']
        self.lines.volume[0] = row['volume']
        
        self._current_index += 1
        return True
    
    def islive(self):
        """返回False，表示这是历史数据"""
        return False


def main():
    """主函数 - 完全同步的回测演示"""
    print("🎯 AlphaHome 最终版同步回测演示")
    print("=" * 60)
    print("✨ 使用完全同步的数据库管理器")
    print("✨ 彻底解决异步冲突问题")
    print()
    
    try:
        # 1. 获取配置
        print("📋 加载配置...")
        connection_string = get_database_url()
        if not connection_string:
            print("⚠️  未找到数据库配置")
            return
        
        bt_config = get_backtesting_config()
        cash = bt_config.get('default_cash', 100000)
        commission = bt_config.get('default_commission', 0.001)
        start_date = datetime.strptime(bt_config.get('default_start_date', '2023-01-01'), '%Y-%m-%d').date()
        end_date = datetime.strptime(bt_config.get('default_end_date', '2023-12-31'), '%Y-%m-%d').date()
        table = bt_config.get('default_table', 'tushare_stock_daily')
        
        print(f"   初始资金: {cash:,}")
        print(f"   手续费率: {commission:.3%}")
        print(f"   回测周期: {start_date} 至 {end_date}")
        
        # 2. 创建同步数据库管理器
        print("\n🔧 创建同步数据库管理器...")
        sync_db_manager = create_sync_manager(connection_string)
        
        # 测试连接
        if not sync_db_manager.test_connection():
            print("❌ 数据库连接测试失败")
            return
        print("✅ 数据库连接成功")
        
        # 3. 创建Cerebro引擎
        print("\n🧠 创建回测引擎...")
        cerebro = bt.Cerebro()
        cerebro.broker.setcash(cash)
        cerebro.broker.setcommission(commission=commission)
        
        # 添加分析器
        cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
        cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
        cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')
        
        # 4. 添加数据源
        print("\n📈 添加数据源...")
        ts_codes = ['000001.SZ', '000002.SZ']  # 平安银行、万科A
        
        for ts_code in ts_codes:
            # 检查数据是否存在
            count_sql = f"""
            SELECT COUNT(*) as count 
            FROM {table} 
            WHERE ts_code = $1 AND trade_date BETWEEN $2 AND $3
            """
            result = sync_db_manager.fetch_one(count_sql, ts_code, start_date, end_date)
            count = result['count'] if result else 0
            
            if count == 0:
                print(f"   ⚠️  {ts_code}: 未找到数据，跳过")
                continue
            
            print(f"   ✅ {ts_code}: 找到 {count} 条数据")
            
            # 创建同步数据源
            data_feed = SyncPostgreSQLDataFeed(
                ts_code=ts_code,
                sync_db_manager=sync_db_manager,
                start_date=start_date,
                end_date=end_date,
                table_name=table
            )
            
            cerebro.adddata(data_feed, name=ts_code)
        
        # 5. 添加策略
        print("\n📈 添加交易策略...")
        cerebro.addstrategy(
            DualMovingAverageStrategy,
            fast_period=5,
            slow_period=20,
            printlog=False
        )
        
        # 6. 运行回测
        print("\n🚀 开始回测...")
        initial_value = cerebro.broker.getvalue()
        print(f"   初始资金: {initial_value:,.2f}")
        
        start_time = datetime.now()
        results = cerebro.run()
        end_time = datetime.now()
        
        duration = end_time - start_time
        print(f"✅ 回测完成，耗时: {duration.total_seconds():.1f}秒")
        
        # 7. 分析结果
        print("\n📊 回测结果分析")
        print("=" * 60)
        
        strategy = results[0]
        final_value = cerebro.broker.getvalue()
        total_return = (final_value - initial_value) / initial_value
        
        print(f"初始资金: {initial_value:,.2f}")
        print(f"最终价值: {final_value:,.2f}")
        print(f"总收益率: {total_return:.2%}")
        print(f"绝对收益: {final_value - initial_value:,.2f}")
        
        # 分析器结果
        try:
            sharpe = strategy.analyzers.sharpe.get_analysis()
            drawdown = strategy.analyzers.drawdown.get_analysis()
            trades = strategy.analyzers.trades.get_analysis()
            
            print(f"\n📈 风险指标:")
            sharpe_ratio = sharpe.get('sharperatio', 0)
            if sharpe_ratio:
                print(f"夏普比率: {sharpe_ratio:.3f}")
            else:
                print("夏普比率: 无法计算")
                
            max_drawdown = drawdown.get('max', {}).get('drawdown', 0)
            if max_drawdown:
                print(f"最大回撤: {max_drawdown:.2%}")
            else:
                print("最大回撤: 0.00%")
            
            print(f"\n🎯 交易统计:")
            total_trades = trades.get('total', {}).get('total', 0)
            won_trades = trades.get('won', {}).get('total', 0)
            lost_trades = trades.get('lost', {}).get('total', 0)
            
            print(f"总交易次数: {total_trades}")
            print(f"盈利交易: {won_trades}")
            print(f"亏损交易: {lost_trades}")
            
            if total_trades > 0:
                win_rate = won_trades / total_trades
                print(f"胜率: {win_rate:.1%}")
            else:
                print("胜率: 无交易")
                
        except Exception as e:
            print(f"⚠️  分析结果获取部分失败: {e}")
        
        print("\n🎉 演示完成！")
        print("\n💡 解决方案总结:")
        print("   ✅ 专用的同步数据库管理器")
        print("   ✅ 每次查询使用独立连接")
        print("   ✅ 避免事件循环冲突")
        print("   ✅ 完全兼容backtrader")
        print("   ✅ 统一配置管理")
        print("   ✅ 简洁可靠的架构")
        
    except KeyboardInterrupt:
        print("\n⏹️  用户中断演示")
    except Exception as e:
        print(f"\n❌ 演示过程中发生错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main() 