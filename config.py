import os
import logging
from pathlib import Path

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger('config')

# 默认配置
DEFAULT_CONFIG = {
    'DATABASE_URL': 'postgresql://postgres:wuhao123@localhost:5432/tusharedb',
    'TUSHARE_TOKEN': '',
    'LOG_LEVEL': 'INFO',
    'CONCURRENT_LIMIT': 5,
}

# 配置文件路径
CONFIG_FILE = Path.home() / '.autoDatabase' / 'config.ini'

# 尝试从环境变量加载配置
def load_config():
    config = DEFAULT_CONFIG.copy()
    
    # 从环境变量加载
    for key in config.keys():
        env_value = os.environ.get(key)
        if env_value:
            config[key] = env_value
            logger.debug(f"从环境变量加载配置: {key}")
    
    # 如果配置文件存在，从配置文件加载
    if CONFIG_FILE.exists():
        try:
            import configparser
            parser = configparser.ConfigParser()
            parser.read(CONFIG_FILE)
            
            if 'DEFAULT' in parser:
                for key, value in parser['DEFAULT'].items():
                    key = key.upper()
                    if key in config:
                        config[key] = value
                        logger.debug(f"从配置文件加载配置: {key}")
        except Exception as e:
            logger.warning(f"加载配置文件失败: {e}")
    
    return config

# 全局配置对象
config = load_config()

# 获取数据库连接字符串
def get_database_url():
    return config['DATABASE_URL']

# 获取Tushare API令牌
def get_tushare_token():
    return config['TUSHARE_TOKEN']

# 获取并发限制
def get_concurrent_limit():
    return int(config['CONCURRENT_LIMIT'])

# 保存配置到文件
def save_config(new_config=None):
    if new_config:
        for key, value in new_config.items():
            if key in config:
                config[key] = value
    
    # 确保目录存在
    CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    # 写入配置文件
    try:
        import configparser
        parser = configparser.ConfigParser()
        parser['DEFAULT'] = {k.lower(): str(v) for k, v in config.items()}
        
        with open(CONFIG_FILE, 'w') as f:
            parser.write(f)
        
        logger.info(f"配置已保存到: {CONFIG_FILE}")
        return True
    except Exception as e:
        logger.error(f"保存配置失败: {e}")
        return False
