"""
ResearchContext - 研究项目与AlphaHome核心系统的统一接口

提供对数据库管理器和批处理计划器的标准化访问
"""

import os
import yaml
import logging
from pathlib import Path
from typing import Optional, Dict, Any

# 设置日志
logger = logging.getLogger(__name__)


class ResearchContext:
    """
    研究上下文管理器

    作为研究项目与AlphaHome核心系统沟通的桥梁，自动加载项目配置，
    并提供随时可用的db_manager和planner实例。

    简化版本：专注于核心功能，避免过度抽象
    """

    def __init__(self, project_path: Optional[Path] = None):
        """
        初始化ResearchContext

        Args:
            project_path: 项目路径，如果未提供则使用当前工作目录
        """
        self.project_path = Path(project_path) if project_path else Path.cwd()
        self.config_path = self.project_path / 'config.yml'

        # 加载配置
        self.config = self._load_config()

        # 初始化核心组件（延迟加载）
        self._db_manager = None
        self._planner = None
        self._data_tool = None  # 新增：数据访问工具

        # 分析结果存储
        self._analysis_results = {}

        logger.info(f"ResearchContext initialized for project: {self.project_path}")

    def _load_config(self) -> Dict[str, Any]:
        """加载项目配置文件"""
        if not self.config_path.exists():
            logger.warning(f"Config file not found at {self.config_path}, using default config")
            return self._get_default_config()

        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                logger.info(f"Config loaded from {self.config_path}")
                return config
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            return self._get_default_config()

    def _get_default_config(self) -> Dict[str, Any]:
        """获取默认配置"""
        return {
            'db_manager': {
                'db_type': 'postgresql',
                'host': os.getenv('DB_HOST', 'localhost'),
                'port': int(os.getenv('DB_PORT', '5432')),
                'user': os.getenv('DB_USER', 'postgres'),
                'password': os.getenv('DB_PASSWORD', 'password'),
                'db_name': os.getenv('DB_NAME', 'alphadb')
            },
            'planner': {
                'batch_size': int(os.getenv('BATCH_SIZE', '50')),
                'max_workers': int(os.getenv('MAX_WORKERS', '4'))
            }
        }

    @property
    def db_manager(self):
        """获取数据库管理器实例（延迟加载）"""
        if self._db_manager is None:
            self._db_manager = self._create_db_manager()
        return self._db_manager

    @property
    def planner(self):
        """获取批处理计划器实例（延迟加载）"""
        if self._planner is None:
            self._planner = self._create_planner()
        return self._planner

    @property
    def data_tool(self):
        """获取数据访问工具实例（延迟加载）"""
        if self._data_tool is None:
            self._data_tool = self._create_data_tool()
        return self._data_tool

    def _create_db_manager(self):
        """创建数据库管理器实例

        配置优先级：
        1. AlphaHome主配置文件
        2. 研究项目配置文件
        3. 默认值
        """
        try:
            # 动态导入以避免循环依赖
            import sys
            project_root = Path(__file__).parent.parent.parent
            if str(project_root) not in sys.path:
                sys.path.insert(0, str(project_root))

            from alphahome.common.db_manager import create_sync_manager

            # 获取合并后的数据库配置
            db_config = self._get_merged_db_config()

            # 获取连接参数并进行URL编码处理
            import urllib.parse

            user = urllib.parse.quote_plus(str(db_config.get('user', 'postgres')))
            password = urllib.parse.quote_plus(str(db_config.get('password', 'password')))
            host = str(db_config.get('host', 'localhost'))
            port = str(db_config.get('port', 5432))
            db_name = urllib.parse.quote_plus(str(db_config.get('db_name', 'alphadb')))

            # 构建连接字符串（URL编码处理特殊字符）
            connection_string = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"

            # 创建DBManager实例（使用同步模式，适合研究环境）
            db_manager = create_sync_manager(connection_string)

            logger.info("DBManager created successfully")
            return db_manager

        except Exception as e:
            logger.error(f"Failed to create DBManager: {e}")
            raise

    def _get_merged_db_config(self):
        """获取合并后的数据库配置

        简化的配置优先级：
        1. AlphaHome主配置文件（如果存在）
        2. 研究项目配置文件（覆盖特定参数）
        3. 默认值（兜底）

        Returns:
            合并后的数据库配置字典
        """
        # 默认配置
        config = {
            'db_type': 'postgresql',
            'host': 'localhost',
            'port': 5432,
            'user': 'postgres',
            'password': 'wuhao123',
            'db_name': 'alphadb'
        }

        # 加载主配置（优先级最高）
        main_config = self._load_alphahome_config()
        if main_config:
            config.update(main_config)
            logger.info("使用AlphaHome主配置文件")

        # 研究项目配置覆盖（如果有）
        research_config = self.config.get('db_manager', {})
        if research_config:
            # 只覆盖非空值
            for key, value in research_config.items():
                if value is not None and value != '':
                    config[key] = value
            logger.info("研究项目配置覆盖了部分参数")

        logger.debug(f"最终数据库配置: {self._mask_sensitive_config(config)}")
        return config

    def _load_alphahome_config(self):
        """加载AlphaHome主配置文件

        简化版本：直接加载已知位置的配置文件
        """
        import json
        import os

        # AlphaHome主配置文件的标准位置
        config_path = Path(os.path.expanduser("~")) / "AppData" / "Local" / "trademaster" / "alphahome" / "config.json"

        try:
            if not config_path.exists():
                logger.debug(f"主配置文件不存在: {config_path}")
                return None

            # 直接加载JSON配置文件
            with open(config_path, 'r', encoding='utf-8') as f:
                main_config = json.load(f)

            # 解析数据库配置
            database_config = main_config.get('database', {})
            if database_config and 'url' in database_config:
                db_config = self._parse_database_url(database_config['url'])
                logger.info(f"成功加载AlphaHome主配置: {config_path}")
                return db_config

            # 兼容旧格式
            db_config = main_config.get('db_manager', {})
            if db_config:
                logger.info(f"成功加载AlphaHome主配置 (旧格式): {config_path}")
                return db_config

            logger.debug("主配置文件中未找到数据库配置")
            return None

        except json.JSONDecodeError as e:
            logger.warning(f"主配置文件JSON格式错误: {e}")
            return None
        except Exception as e:
            logger.warning(f"加载主配置文件失败: {e}")
            return None

    def _parse_database_url(self, database_url):
        """解析PostgreSQL数据库URL

        Args:
            database_url: PostgreSQL连接URL，格式：postgresql://user:password@host:port/database

        Returns:
            解析后的数据库配置字典
        """
        import urllib.parse

        try:
            parsed = urllib.parse.urlparse(database_url)
            return {
                'db_type': 'postgresql',
                'host': parsed.hostname or 'localhost',
                'port': parsed.port or 5432,
                'user': parsed.username or 'postgres',
                'password': parsed.password or '',
                'db_name': parsed.path.lstrip('/') if parsed.path else 'alphadb'
            }
        except Exception as e:
            logger.warning(f"解析数据库URL失败: {e}")
            return {}

    def _mask_sensitive_config(self, config):
        """屏蔽敏感配置信息用于日志输出"""
        masked = config.copy()
        if 'password' in masked:
            masked['password'] = '*' * len(str(masked['password']))
        return masked

    def store_analysis_result(self, key: str, result: Any):
        """存储分析结果

        Args:
            key: 结果标识符
            result: 分析结果数据
        """
        self._analysis_results[key] = result
        logger.debug(f"存储分析结果: {key}")

    def get_analysis_result(self, key: str) -> Optional[Any]:
        """获取分析结果

        Args:
            key: 结果标识符

        Returns:
            分析结果数据，如果不存在则返回None
        """
        return self._analysis_results.get(key)

    def get_all_analysis_results(self) -> Dict[str, Any]:
        """获取所有分析结果"""
        return self._analysis_results.copy()

    def _create_planner(self):
        """创建批处理计划器实例"""
        try:
            from alphahome.common.planning.extended_batch_planner import ExtendedBatchPlanner

            planner_config = self.config.get('planner', {})

            # 创建ExtendedBatchPlanner实例
            planner = ExtendedBatchPlanner(
                db_manager=self.db_manager,
                batch_size=planner_config.get('batch_size', 50),
                max_workers=planner_config.get('max_workers', 4)
            )

            logger.info("ExtendedBatchPlanner created successfully")
            return planner

        except Exception as e:
            logger.error(f"Failed to create ExtendedBatchPlanner: {e}")
            raise

    def _create_data_tool(self):
        """创建数据访问工具实例"""
        try:
            # 动态导入数据访问工具
            import sys
            project_root = Path(__file__).parent.parent.parent
            if str(project_root) not in sys.path:
                sys.path.insert(0, str(project_root))

            from alphahome.providers import AlphaDataTool

            # 创建AlphaDataTool实例
            data_tool = AlphaDataTool(self.db_manager)

            logger.info("AlphaDataTool created successfully")
            return data_tool

        except Exception as e:
            logger.error(f"Failed to create AlphaDataTool: {e}")
            raise

    def query_dataframe(self, query: str, params=None):
        """执行SQL查询并返回DataFrame"""
        return self.db_manager.query_dataframe(query, params)

    def get_stock_list(self, market: Optional[str] = None):
        """获取股票列表

        使用providers数据提供层获取股票列表，替代直接SQL查询
        """
        # 使用data_tool获取股票基本信息
        stock_info = self.data_tool.get_stock_info(list_status='L')

        if market:
            # 如果指定了市场，进行过滤
            stock_info = stock_info[stock_info['market'] == market]

        return stock_info['ts_code'].tolist()

    def get_trading_dates(self, start_date: str, end_date: str, exchange: str = 'SSE'):
        """获取交易日列表

        使用providers数据提供层获取交易日历，替代直接SQL查询
        """
        # 使用data_tool获取交易日历
        trade_cal = self.data_tool.get_trade_dates(start_date, end_date, exchange)

        # 筛选开市日期并返回日期列表
        trading_dates = trade_cal[trade_cal['is_open'] == 1]['cal_date']
        return trading_dates.dt.strftime('%Y-%m-%d').tolist()

    # === 新增便捷方法，基于providers数据提供层 ===

    def get_stock_data(self, symbols, start_date: str, end_date: str, adjust: bool = True):
        """获取股票行情数据的便捷方法"""
        return self.data_tool.get_stock_data(symbols, start_date, end_date, adjust)

    def get_index_weights(self, index_code: str, start_date: str, end_date: str, monthly: bool = False):
        """获取指数权重数据的便捷方法"""
        return self.data_tool.get_index_weights(index_code, start_date, end_date, monthly)

    def get_industry_data(self, symbols=None, level: str = 'sw_l1'):
        """获取行业分类数据的便捷方法"""
        return self.data_tool.get_industry_data(symbols, level)

    def close(self):
        """关闭所有连接"""
        if self._db_manager:
            try:
                self._db_manager.close()
                logger.info("DBManager connection closed")
            except:
                pass

    def __enter__(self):
        """支持with语句"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出时自动关闭连接"""
        self.close()
