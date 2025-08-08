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

# --- 日志配置 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("StrategyResearch")

# --- 配置加载 ---
def load_config(config_path='config.yml'):
    """
    从YAML文件加载项目配置。
    此函数负责读取策略研究项目的配置，支持相对路径和UTF-8编码，确保系统能够正确初始化。
    
    Args:
        config_path (str): 配置文件的路径，默认为 'config.yml'。
        
    Returns:
        dict: 解析后的配置字典。
        
    Raises:
        FileNotFoundError: 如果配置文件不存在。
        yaml.YAMLError: 如果YAML文件解析失败。
    """
    # 如果是相对路径，则相对于当前脚本目录构建绝对路径
    if not os.path.isabs(config_path):
        script_dir = Path(__file__).parent
        config_path = script_dir / config_path

    logger.info(f"正在从 {config_path} 加载配置。")
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logger.error(f"错误：配置文件未在 {config_path} 找到。中止。")
        raise
    except yaml.YAMLError as e:
        logger.error(f"错误：解析YAML文件失败: {e}。请检查配置文件的格式。")
        raise

# --- Backtrader 的统一数据加载 ---
def load_data_for_bt_unified(config, research_context=None):
    """
    统一数据加载函数 - 支持AlphaHome数据库和CSV文件两种模式。
    它包含一个智能数据源选择器，优先尝试从AlphaHome数据库加载数据，
    如果数据库不可用或加载失败，则优雅地降级到使用CSV备用数据源。
    此行为与 database_research 模板保持一致，提供了灵活的数据加载机制。

    Args:
        config (dict): 包含数据源配置的字典。
        research_context (ResearchContext, optional): AlphaHome ResearchContext实例，用于数据库访问。
                                                      如果为 None，则直接使用CSV模式。

    Returns:
        backtrader数据源或数据源列表: 返回一个或多个 Backtrader 数据馈送对象。
    """
    try:
        # 尝试使用智能数据源选择器来决定从何处加载数据
        from src.data_source_selector import smart_data_source_selector
        return smart_data_source_selector(config, research_context)
        
    except ImportError:
        # 如果智能数据源选择器模块不可用（例如，未安装或路径问题），则回退到传统加载模式
        logger.warning("警告：智能数据源选择器不可用，将使用传统数据加载模式。")
        
        # 回退到传统模式的数据配置
        data_config = config.get('data', {})
        
        # 尝试使用AlphaHome数据源，前提是AlphaHome可用且提供了研究上下文
        if ALPHAHOME_AVAILABLE and research_context:
            try:
                logger.info("尝试使用AlphaHome数据源...")
                research_config = config.get('research', {})
                # 从配置中获取默认股票列表和时间范围
                symbols = research_config.get('stock_pool', {}).get('default_symbols', [])
                time_range = research_config.get('time_range', {})
                
                if symbols:
                    # 如果配置了股票代码，则通过unified_data_loader加载数据
                    return load_data_for_backtrader(
                        research_context=research_context,
                        symbols=symbols,
                        start_date=time_range.get('default_start'),
                        end_date=time_range.get('default_end')
                    )
            except Exception as e:
                # 如果AlphaHome数据源加载失败，记录警告并降级到CSV
                logger.warning(f"AlphaHome数据源加载失败: {e}，正在降级到CSV模式。")
        
        # CSV备用模式：如果AlphaHome不可用或失败，则尝试从CSV文件加载
        csv_config = data_config.get('fallback_csv', {})
        if csv_config.get('file_path'):
            logger.info("正在使用CSV备用数据源...")
            return load_data_for_backtrader(
                csv_path=csv_config['file_path']
            )
        
        # 兼容旧配置格式：处理旧版配置中直接列出数据路径的情况
        logger.info("正在使用传统CSV数据加载（兼容旧配置格式）。")
        if 'data' in config and isinstance(config['data'], list):
            data_info = config['data'][0] # 假设第一个数据源是主要的
            return load_data_for_bt_legacy(
                data_info['path'],
                config['backtest_settings']['start_date'],
                config['backtest_settings']['end_date']
            )

def load_data_for_bt_legacy(data_path, start_date, end_date):
    """
    传统的CSV数据加载函数，用于向后兼容性。
    此函数直接从指定的CSV文件加载数据，并进行基本的列重命名和日期筛选，
    以适应Backtrader的数据格式要求。
    
    Args:
        data_path (str): CSV数据文件的路径。
        start_date (str): 数据加载的开始日期，格式 'YYYY-MM-DD'。
        end_date (str): 数据加载的结束日期，格式 'YYYY-MM-DD'。
        
    Returns:
        bt.feeds.PandasData: Backtrader 的 PandasData 数据馈送对象。
        
    Raises:
        FileNotFoundError: 如果CSV数据文件不存在。
        Exception: 如果准备数据时发生其他错误。
    """
    logger.info(f"正在为 Backtrader 从 {data_path} 加载数据（传统模式）。")
    try:
        # 确保使用绝对路径，以便正确找到文件
        import os
        if not os.path.isabs(data_path):
            script_dir = Path(__file__).parent
            data_path = script_dir / data_path

        dataframe = pd.read_csv(
            data_path,
            index_col='trade_date', # 将 'trade_date' 列设为索引
            parse_dates=True        # 自动解析日期列
        )

        # 重命名列以匹配backtrader期望的格式
        column_mapping = {
            'ts_code': 'symbol', # 股票代码统一为 'symbol'
            'vol': 'volume'      # 成交量统一为 'volume'
        }
        dataframe = dataframe.rename(columns=column_mapping)

        # 根据配置中的日期范围筛选数据
        dataframe = dataframe.loc[start_date:end_date]

        # 创建一个 Backtrader 数据馈送对象
        return bt.feeds.PandasData(dataname=dataframe)
    except FileNotFoundError:
        logger.error(f"错误：数据文件未在 {data_path} 找到。")
        raise
    except Exception as e:
        logger.error(f"错误：为 Backtrader 准备数据失败: {e}。请检查数据格式或路径。")
        raise

# --- Backtrader 策略定义 ---
class MovingAverageCrossStrategy(bt.Strategy):
    """
    示例策略：一个简单的移动平均线交叉策略。
    此策略演示了如何在 Backtrader 中定义参数、初始化指标以及在 `next` 方法中实现交易逻辑。
    用户应替换此策略逻辑为自己的研究策略。
    """
    # 策略参数定义
    params = (
        ('fast_ma', 10), # 快速移动平均线周期
        ('slow_ma', 50), # 慢速移动平均线周期
    )

    def __init__(self):
        # 初始化策略指标
        # 创建快速移动平均线
        self.fast_ma = bt.indicators.SimpleMovingAverage(
            self.data.close, period=self.params.fast_ma)
        # 创建慢速移动平均线
        self.slow_ma = bt.indicators.SimpleMovingAverage(
            self.data.close, period=self.params.slow_ma)
        # 创建交叉信号（快速线穿过慢速线）
        self.crossover = bt.indicators.CrossOver(self.fast_ma, self.slow_ma)

    def next(self):
        # 策略的下一个Bar（K线）的处理逻辑
        if not self.position:  # 如果当前没有持仓
            if self.crossover > 0:  # 快速均线向上穿过慢速均线，形成金叉
                self.buy() # 买入操作
        elif self.crossover < 0:  # 快速均线向下穿过慢速均线，形成死叉
            self.close() # 平仓操作

# --- 回测引擎 ---
def run_backtest(config):
    """
    初始化并运行 Backtrader 回测引擎，并集成 AlphaHome 数据源。
    此函数设置回测环境，包括加载策略、添加数据、设置资金和佣金，
    并运行回测以生成绩效分析结果。
    
    Args:
        config (dict): 包含回测所有配置参数的字典。
    """
    logger.info("正在初始化回测引擎...")

    # 初始化 ResearchContext（如果AlphaHome可用）。
    # ResearchContext 提供了对 AlphaHome 数据库和其他核心工具的访问。
    research_context = None
    if ALPHAHOME_AVAILABLE:
        try:
            logger.info("正在初始化AlphaHome研究上下文...")
            research_context = ResearchContext(project_path=".")
            logger.info("✅ AlphaHome集成成功。")
        except Exception as e:
            # 如果AlphaHome初始化失败，则记录警告，回测将仅使用CSV模式
            logger.warning(f"AlphaHome初始化失败，将使用CSV模式: {e}。")

    # 从配置中获取回测和策略参数
    backtest_cfg = config['backtest_settings']
    strategy_params = config['parameters']['strategy_a']

    # 创建 Backtrader Cerebro 引擎实例
    cerebro = bt.Cerebro()

    # 1. 添加策略
    cerebro.addstrategy(
        MovingAverageCrossStrategy,
        fast_ma=strategy_params.get('fast_ma', 10), # 从配置中获取快均线周期，默认为10
        slow_ma=strategy_params.get('slow_ma', 50)  # 从配置中获取慢均线周期，默认为50
    )

    # 2. 添加数据（使用统一数据加载函数）
    try:
        data_feed = load_data_for_bt_unified(config, research_context)

        # 处理多个数据源的情况（例如，多只股票）
        if isinstance(data_feed, list):
            for feed in data_feed:
                cerebro.adddata(feed) # 逐个添加数据馈送
            logger.info(f"成功添加了 {len(data_feed)} 个数据源。")
        else:
            cerebro.adddata(data_feed) # 添加单个数据馈送
            logger.info("成功添加了 1 个数据源。")

    except Exception as e:
        logger.error(f"错误：数据加载失败: {e}。尝试使用备用数据源。")
        # 如果数据加载失败，尝试使用传统CSV方式作为最后的备用方案
        if 'data' in config and 'fallback_csv' in config['data']:
            csv_cfg = config['data']['fallback_csv']
            if 'file_path' in csv_cfg:
                data_feed = load_data_for_bt_legacy(
                    csv_cfg['file_path'],
                    backtest_cfg['start_date'],
                    backtest_cfg['end_date']
                )
                cerebro.adddata(data_feed)
                logger.info("成功使用传统CSV数据加载作为备用。")
            else:
                logger.error("错误：CSV备用数据源配置不完整，缺少 'file_path'。")
                raise
        else:
            logger.error("错误：没有可用的备用数据源（既无AlphaHome也无完整的CSV配置）。")
            raise

    # 3. 设置初始资金和券商设置
    cerebro.broker.setcash(backtest_cfg['initial_cash'])         # 设置初始资金
    cerebro.broker.setcommission(commission=backtest_cfg['commission']) # 设置交易佣金
    
    # 4. 添加分析器（Backtrader内置的绩效分析工具）
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe_ratio')    # 夏普比率
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')              # 总收益和年化收益
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')            # 最大回撤
    cerebro.addanalyzer(bt.analyzers.TimeReturn, timeframe=bt.TimeFrame.Days, _name='daily_returns') # 每日收益

    logger.info("正在运行回测...")
    starting_portfolio_value = cerebro.broker.getvalue() # 获取初始投资组合价值
    
    results = cerebro.run() # 运行回测
    
    ending_portfolio_value = cerebro.broker.getvalue() # 获取最终投资组合价值
    logger.info("回测完成。")

    # 5. 打印分析结果
    logger.info(f"初始投资组合价值: {starting_portfolio_value:,.2f}")
    logger.info(f"最终投资组合价值:   {ending_portfolio_value:,.2f}")
    
    strat = results[0] # 获取第一个策略实例的结果
    sharpe = strat.analyzers.sharpe_ratio.get_analysis() # 获取夏普比率分析结果
    returns = strat.analyzers.returns.get_analysis()     # 获取收益分析结果
    drawdown = strat.analyzers.drawdown.get_analysis()   # 获取回撤分析结果
    daily_returns_dict = strat.analyzers.daily_returns.get_analysis() # 获取每日收益字典

    logger.info(f"夏普比率: {sharpe.get('sharperatio', 'N/A')}")
    logger.info(f"年化收益率: {returns.get('rnorm100', 'N/A')}%")
    logger.info(f"最大回撤: {drawdown.max.drawdown}%")

    # 6. 将结果保存到CSV文件，供后续Jupyter Notebook分析使用
    if daily_returns_dict:
        logger.info("正在保存回测结果到CSV文件，供进一步分析...")
        results_df = pd.DataFrame(
            list(daily_returns_dict.items()),
            columns=['datetime', 'returns'] # 列名为日期和收益
        )
        results_df.set_index('datetime', inplace=True) # 设置日期为索引

        # 计算随时间变化的投资组合价值
        initial_cash = backtest_cfg['initial_cash']
        results_df['portfolio_value'] = initial_cash * (1 + results_df['returns']).cumprod()
        
        # 添加占位符列以匹配Jupyter Notebook的期望数据结构
        results_df['benchmark_returns'] = 0.0  # 占位符，可根据实际基准数据改进
        results_df['positions'] = 0            # 占位符，如果策略不记录持仓量，则保持为0
        results_df['cash'] = results_df['portfolio_value'] # 简化的现金占位符
        results_df['trades'] = 0               # 占位符，如果策略不记录交易次数，则保持为0

        # 确保数据输出目录存在，如果不存在则创建
        data_dir = Path('./data')
        data_dir.mkdir(exist_ok=True)
        
        output_path = data_dir / 'backtest_results.csv'
        results_df.to_csv(output_path) # 保存为CSV文件
        logger.info(f"✅ 回测结果已保存到 {output_path}")

# --- 主程序执行入口 ---
def main(config_path):
    """
    策略研究项目的主入口点。
    此函数负责加载配置，然后运行回测流程，并在结束后记录完成信息。
    
    Args:
        config_path (str): 配置文件的路径。
    """
    try:
        config = load_config(config_path) # 加载项目配置
    except Exception:
        logger.error("错误：加载配置失败。中止程序。")
        return

    logger.info(f"项目成功加载: {config.get('project', {}).get('name', 'strategy_research')}")
    
    run_backtest(config) # 运行回测

    logger.info("项目执行完毕。")

if __name__ == '__main__':
    # 当脚本作为主模块直接运行时执行
    parser = argparse.ArgumentParser(
        description="运行 AlphaHome 策略回测项目。此脚本是策略研究的命令行工具。"
    )
    parser.add_argument(
        '--config', 
        type=str, 
        default='config.yml',
        help='项目配置文件的路径。默认为当前目录下的 config.yml。'
    )
    args = parser.parse_args()
    
    main(args.config) 