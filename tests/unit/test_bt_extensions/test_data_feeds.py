import unittest
import pandas as pd
from datetime import date
import backtrader as bt
from unittest.mock import MagicMock

from alphahome.bt_extensions.data.feeds import PostgreSQLDataFeed

class TestPostgreSQLDataFeed(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up test data"""
        cls.sample_data = pd.DataFrame({
            'trade_date': pd.to_datetime(['2024-01-02', '2024-01-03', '2024-01-04']),
            'open': [100, 102, 105],
            'high': [103, 104, 106],
            'low': [99, 101, 103],
            'close': [102, 103, 104],
            'vol': [1000, 1200, 1100],
            'amount': [100000, 122400, 114400],
            'pre_close': [98, 102, 103],
            'change': [4, 1, 1],
            'pct_chg': [4.08, 0.98, 0.97]
        })
        cls.sample_feed_data = pd.DataFrame({
            'trade_date': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03']),
            'open': [100, 101, 102],
            'high': [105, 106, 107],
            'low': [95, 96, 97],
            'close': [102, 103, 104],
            'vol': [1000, 1100, 1200],
            'amount': [100000, 112000, 124000],
            'pre_close': [98, 102, 103],
            'change': [4, 1, 1],
            'pct_chg': [4.08, 0.98, 0.97]
        })
        cls.sample_feed_data_for_mapping = pd.DataFrame({
            'trade_date': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03']),
            'open': [100, 101, 102],
            'high': [105, 106, 107],
            'low': [95, 96, 97],
            'close': [102, 103, 104],
            'vol': [1000, 1100, 1200],
            'amount': [100000, 112000, 124000],
            'pre_close': [98, 102, 103],
            'change': [4, 1, 1],
            'pct_chg': [4.08, 0.98, 0.97]
        })
        cls.db_manager = MagicMock()

    def test_feed_with_preloaded_data(self):
        """测试使用预加载数据时，不应查询数据库"""
        data_feed = PostgreSQLDataFeed(
            ts_code='000001.SZ',
            start_date='2023-01-01',
            end_date='2023-01-05',
            preloaded_data=self.sample_feed_data,
            db_manager=self.db_manager
        )
        cerebro = bt.Cerebro()

        cerebro.adddata(data_feed)
        
        # Run a simple backtest to see if it processes the data
        strats = cerebro.run()
        
        # Check if the data points were loaded correctly
        self.assertEqual(len(data_feed), 3)
        # After cerebro.run(), the data feed is at the last bar.
        # So data_feed.close[0] or data_feed.close.get(0) will get the last value.
        self.assertEqual(data_feed.close[0], 104) # Last close value
        self.assertEqual(data_feed.open[0], 102) # Last open value
        self.assertEqual(data_feed.volume[0], 1200) # Last volume value

        # 断言数据库查询未被调用
        self.db_manager.fetch.assert_not_called()

    def test_feed_with_empty_preloaded_data(self):
        """测试当预加载数据为空时，应从数据库加载"""
        # Simulate the fetch method returning a list of dictionaries
        self.db_manager.fetch.return_value = [
            {
                'ts_code': '000002.SZ', 'trade_date': '2023-01-01',
                'open': 10, 'high': 11, 'low': 9, 'close': 10.5, 'vol': 1000
            }
        ]
        data_feed = PostgreSQLDataFeed(
            ts_code='000002.SZ',
            start_date='2023-01-01',
            end_date='2023-01-01',
            preloaded_data=pd.DataFrame(), # 空的DataFrame
            db_manager=self.db_manager
        )
        cerebro = bt.Cerebro()
        cerebro.adddata(data_feed, name='000002.SZ')
        cerebro.run()

        # 断言数据库查询被调用
        self.db_manager.fetch.assert_called_once()
        # 验证加载的数据
        self.assertEqual(data_feed.close[0], 10.5)


    def test_ohlcv_mapping(self):
        """测试 OHLCV 和列名是否正确映射"""
        data_feed = PostgreSQLDataFeed(
            ts_code='000003.SZ',
            start_date='2023-01-01',
            end_date='2023-01-05',
            preloaded_data=self.sample_feed_data_for_mapping,
            db_manager=self.db_manager
        )
        cerebro = bt.Cerebro()
        cerebro.adddata(data_feed, name='000003.SZ')
        cerebro.run()

        # 验证 Backtrader line 是否与 DataFrame 的最后一行匹配
        last_row = self.sample_feed_data_for_mapping.iloc[-1]
        self.assertEqual(data_feed.datetime.date(0), pd.to_datetime(last_row['trade_date']).date())
        self.assertEqual(data_feed.open[0], last_row['open'])
        self.assertEqual(data_feed.high[0], last_row['high'])
        self.assertEqual(data_feed.low[0], last_row['low'])
        self.assertEqual(data_feed.close[0], last_row['close'])
        self.assertEqual(data_feed.volume[0], last_row['vol'])
        self.assertEqual(data_feed.openinterest[0], 0)

if __name__ == '__main__':
    unittest.main() 