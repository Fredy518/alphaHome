"""
智能数据源选择器
解决strategy_research的UTF-8编码问题，实现优雅降级到CSV数据源
"""

import logging
from typing import Optional, Dict, Any
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)

def test_database_connection(research_context) -> bool:
    """
    安全测试AlphaHome数据库连接
    
    Args:
        research_context: ResearchContext实例
        
    Returns:
        bool: 数据库连接是否成功
    """
    try:
        # 执行简单查询测试连接
        test_data = research_context.data_tool.get_stock_data(
            symbols=["000001.SZ"],
            start_date="2024-01-01",
            end_date="2024-01-02"
        )
        
        if test_data is not None and not test_data.empty:
            logger.info("✅ 数据库连接测试成功")
            return True
        else:
            logger.warning("⚠️ 数据库连接测试返回空数据")
            return False
            
    except UnicodeDecodeError as e:
        logger.warning(f"🚨 数据库UTF-8编码错误: {e}")
        return False
    except Exception as e:
        logger.warning(f"⚠️ 数据库连接失败: {type(e).__name__}: {e}")
        return False

def smart_data_source_selector(config: Dict[str, Any], research_context=None):
    """
    智能数据源选择器
    
    自动检测数据库可用性，失败时优雅降级到CSV数据源
    与database_research行为保持一致
    
    Args:
        config: 配置字典
        research_context: 可选的ResearchContext实例
        
    Returns:
        backtrader数据源或数据源列表
    """
    # 尝试AlphaHome数据库
    if research_context:
        logger.info("🔍 测试AlphaHome数据库连接...")
        if test_database_connection(research_context):
            logger.info("✅ 使用AlphaHome数据库")
            return _load_from_alphahome(config, research_context)
        else:
            logger.warning("⚠️ AlphaHome数据库不可用，降级到CSV模式")
    
    # 降级到CSV备用数据源
    logger.info("📊 使用CSV备用数据源")
    return _load_from_csv_backup(config)

def _load_from_alphahome(config: Dict[str, Any], research_context):
    """从AlphaHome数据库加载数据"""
    research_config = config.get('research', {})
    symbols = research_config.get('stock_pool', {}).get('default_symbols', [])
    time_range = research_config.get('time_range', {})
    
    if not symbols:
        raise ValueError("未配置股票列表")
    
    from src.unified_data_loader import load_data_for_backtrader
    return load_data_for_backtrader(
        research_context=research_context,
        symbols=symbols,
        start_date=time_range.get('default_start'),
        end_date=time_range.get('default_end')
    )

def _load_from_csv_backup(config: Dict[str, Any]):
    """从CSV备用数据源加载数据"""
    csv_config = config.get('data', {}).get('fallback_csv', {})
    csv_path = csv_config.get('file_path', 'data/market_data.csv')
    
    if not Path(csv_path).exists():
        # 创建示例CSV数据
        _create_sample_csv(csv_path)
    
    from src.unified_data_loader import load_data_for_backtrader
    return load_data_for_backtrader(csv_path=csv_path)

def _create_sample_csv(csv_path: str):
    """创建示例CSV数据文件"""
    sample_data = pd.DataFrame({
        'ts_code': ['000001.SZ', '000001.SZ', '000002.SZ', '000002.SZ'],
        'trade_date': ['2024-01-01', '2024-01-02', '2024-01-01', '2024-01-02'],
        'open': [10.0, 10.5, 20.0, 20.5],
        'high': [11.0, 11.5, 21.0, 21.5],
        'low': [9.5, 10.0, 19.5, 20.0],
        'close': [10.5, 11.0, 20.5, 21.0],
        'vol': [1000000, 1200000, 800000, 900000],
        'amount': [10500000, 13200000, 16400000, 18900000]
    })
    
    csv_file = Path(csv_path)
    csv_file.parent.mkdir(parents=True, exist_ok=True)
    sample_data.to_csv(csv_file, index=False)
    logger.info(f"📁 创建示例CSV数据文件: {csv_path}")