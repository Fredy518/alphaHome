import yaml
import pandas as pd
import backtrader as bt
import logging
import argparse
from datetime import datetime
from pathlib import Path
import sys
import os

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

# 导入AlphaHome组件（可选）
try:
    from research.tools.context import ResearchContext
    from src.unified_data_loader import UnifiedDataLoader, load_data_for_backtrader
    ALPHAHOME_AVAILABLE = True
except ImportError as e:
    logging.warning(f"AlphaHome组件不可用: {e}")
    ALPHAHOME_AVAILABLE = False

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("StrategyResearch")

# --- Configuration Loading ---
def load_config(config_path='config.yml'):
    """Loads the project configuration from a YAML file."""
    logger.info(f"Loading configuration from {config_path}")
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logger.error(f"Configuration file not found at {config_path}")
        raise
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML file: {e}")
        raise

# --- Unified Data Loading for Backtrader ---
def load_data_for_bt_unified(config, research_context=None):
    """
    统一数据加载函数 - 支持AlphaHome和CSV两种模式

    Args:
        config: 配置字典
        research_context: 可选的ResearchContext实例

    Returns:
        backtrader数据源或数据源列表
    """
    data_config = config.get('data_source', {})

    if ALPHAHOME_AVAILABLE and research_context and 'symbols' in data_config:
        # AlphaHome模式
        logger.info("使用AlphaHome数据源")
        return load_data_for_backtrader(
            research_context=research_context,
            symbols=data_config['symbols'],
            start_date=data_config.get('start_date'),
            end_date=data_config.get('end_date')
        )
    elif 'csv_path' in data_config:
        # CSV模式
        logger.info("使用CSV数据源")
        return load_data_for_backtrader(
            csv_path=data_config['csv_path']
        )
    else:
        # 兼容旧配置格式
        logger.info("使用传统CSV数据加载")
        data_info = config['data'][0]  # 假设第一个数据源
        return load_data_for_bt_legacy(
            data_info['path'],
            config['backtest_settings']['start_date'],
            config['backtest_settings']['end_date']
        )

def load_data_for_bt_legacy(data_path, start_date, end_date):
    """传统的CSV数据加载函数（向后兼容）"""
    logger.info(f"Loading data from {data_path} for backtrader.")
    try:
        dataframe = pd.read_csv(
            data_path,
            index_col='date',
            parse_dates=True
        )
        # Filter by date range from config
        dataframe = dataframe.loc[start_date:end_date]

        # Create a Backtrader data feed
        return bt.feeds.PandasData(dataname=dataframe)
    except FileNotFoundError:
        logger.error(f"Data file not found at {data_path}")
        raise
    except Exception as e:
        logger.error(f"Error preparing data for Backtrader: {e}")
        raise

# --- Backtrader Strategy Definition ---
class MovingAverageCrossStrategy(bt.Strategy):
    """
    Example Strategy: A simple moving average crossover strategy.
    Replace this with your own strategy logic.
    """
    params = (
        ('fast_ma', 10),
        ('slow_ma', 50),
    )

    def __init__(self):
        self.fast_ma = bt.indicators.SimpleMovingAverage(
            self.data.close, period=self.params.fast_ma)
        self.slow_ma = bt.indicators.SimpleMovingAverage(
            self.data.close, period=self.params.slow_ma)
        self.crossover = bt.indicators.CrossOver(self.fast_ma, self.slow_ma)

    def next(self):
        if not self.position:  # Not in the market
            if self.crossover > 0:  # Fast ma crosses above slow ma
                self.buy()
        elif self.crossover < 0:  # Fast ma crosses below slow ma
            self.close()

# --- Backtesting Engine ---
def run_backtest(config):
    """Initializes and runs the backtrader engine with AlphaHome integration."""
    logger.info("Initializing backtesting engine...")

    # Initialize ResearchContext (if AlphaHome is available)
    research_context = None
    if ALPHAHOME_AVAILABLE:
        try:
            logger.info("初始化AlphaHome研究上下文...")
            research_context = ResearchContext(project_path=".")
            logger.info("✅ AlphaHome集成成功")
        except Exception as e:
            logger.warning(f"AlphaHome初始化失败，将使用CSV模式: {e}")

    backtest_cfg = config['backtest_settings']
    strategy_params = config['parameters']['strategy_a']

    cerebro = bt.Cerebro()

    # 1. Add Strategy
    cerebro.addstrategy(
        MovingAverageCrossStrategy,
        fast_ma=strategy_params.get('fast_ma', 10),
        slow_ma=strategy_params.get('slow_ma', 50)
    )

    # 2. Add Data (Unified Loading)
    try:
        data_feed = load_data_for_bt_unified(config, research_context)

        # 处理多个数据源的情况
        if isinstance(data_feed, list):
            for feed in data_feed:
                cerebro.adddata(feed)
            logger.info(f"添加了 {len(data_feed)} 个数据源")
        else:
            cerebro.adddata(data_feed)
            logger.info("添加了 1 个数据源")

    except Exception as e:
        logger.error(f"数据加载失败: {e}")
        # 尝试使用传统方式加载
        if 'data' in config:
            data_cfg = config['data'][0]
            data_feed = load_data_for_bt_legacy(
                data_cfg['path'],
                backtest_cfg['start_date'],
                backtest_cfg['end_date']
            )
            cerebro.adddata(data_feed)
            logger.info("使用传统CSV数据加载成功")
        else:
            raise

    # 3. Set Initial Capital and Broker Settings
    cerebro.broker.setcash(backtest_cfg['initial_cash'])
    cerebro.broker.setcommission(commission=backtest_cfg['commission'])
    
    # 4. Add Analyzers
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe_ratio')
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.TimeReturn, timeframe=bt.TimeFrame.Days, _name='daily_returns')

    logger.info("Running backtest...")
    starting_portfolio_value = cerebro.broker.getvalue()
    
    results = cerebro.run()
    
    ending_portfolio_value = cerebro.broker.getvalue()
    logger.info("Backtest complete.")

    # 5. Print Analysis Results
    logger.info(f"Starting Portfolio Value: {starting_portfolio_value:,.2f}")
    logger.info(f"Ending Portfolio Value:   {ending_portfolio_value:,.2f}")
    
    strat = results[0]
    sharpe = strat.analyzers.sharpe_ratio.get_analysis()
    returns = strat.analyzers.returns.get_analysis()
    drawdown = strat.analyzers.drawdown.get_analysis()
    daily_returns_dict = strat.analyzers.daily_returns.get_analysis()

    logger.info(f"Sharpe Ratio: {sharpe.get('sharperatio', 'N/A')}")
    logger.info(f"Annualized Return: {returns.get('rnorm100', 'N/A')}%")
    logger.info(f"Max Drawdown: {drawdown.max.drawdown}%")

    # 6. Save results to CSV for notebook analysis
    if daily_returns_dict:
        logger.info("Saving backtest results to CSV for further analysis...")
        results_df = pd.DataFrame(
            list(daily_returns_dict.items()),
            columns=['datetime', 'returns']
        )
        results_df.set_index('datetime', inplace=True)

        # Calculate portfolio value over time
        initial_cash = backtest_cfg['initial_cash']
        results_df['portfolio_value'] = initial_cash * (1 + results_df['returns']).cumprod()
        
        # Add placeholder columns to match notebook expectations
        results_df['benchmark_returns'] = 0.0  # Placeholder, can be improved
        results_df['positions'] = 0  # Placeholder
        results_df['cash'] = results_df['portfolio_value'] # Simplified placeholder
        results_df['trades'] = 0 # Placeholder

        # Ensure the data directory exists
        data_dir = Path('./data')
        data_dir.mkdir(exist_ok=True)
        
        output_path = data_dir / 'backtest_results.csv'
        results_df.to_csv(output_path)
        logger.info(f"✅ Results saved to {output_path}")

# --- Main Execution ---
def main(config_path):
    """Main entry point for the strategy research project."""
    try:
        config = load_config(config_path)
    except Exception:
        logger.error("Failed to load configuration. Aborting.")
        return

    logger.info(f"Successfully loaded project: {config.get('project_id')}")
    
    run_backtest(config)

    logger.info("Project execution finished.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run a strategy backtest project for AlphaHome.")
    parser.add_argument(
        '--config', 
        type=str, 
        default='config.yml',
        help='Path to the project configuration file.'
    )
    args = parser.parse_args()
    
    main(args.config) 