import yaml
import pandas as pd
import backtrader as bt
import logging
import argparse
from datetime import datetime

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

# --- Data Loading for Backtrader ---
def load_data_for_bt(data_path, start_date, end_date):
    """Loads data and prepares it in a Backtrader-compatible format."""
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
    """Initializes and runs the backtrader engine."""
    logger.info("Initializing backtesting engine...")
    
    backtest_cfg = config['backtest_settings']
    strategy_params = config['parameters']['strategy_a']
    data_cfg = config['data'][0]

    cerebro = bt.Cerebro()

    # 1. Add Strategy
    cerebro.addstrategy(
        MovingAverageCrossStrategy,
        fast_ma=strategy_params.get('lookback_period', 10), # Using a different param name
        slow_ma=strategy_params.get('slow_ma_period', 50) # Example of another param
    )

    # 2. Add Data
    data_feed = load_data_for_bt(
        data_cfg['path'], 
        backtest_cfg['start_date'], 
        backtest_cfg['end_date']
    )
    cerebro.adddata(data_feed)

    # 3. Set Initial Capital and Broker Settings
    cerebro.broker.setcash(backtest_cfg['initial_cash'])
    cerebro.broker.setcommission(commission=backtest_cfg['commission'])
    
    # 4. Add Analyzers
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe_ratio')
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')

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

    logger.info(f"Sharpe Ratio: {sharpe.get('sharperatio', 'N/A')}")
    logger.info(f"Annualized Return: {returns.get('rnorm100', 'N/A')}%")
    logger.info(f"Max Drawdown: {drawdown.max.drawdown}%")


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