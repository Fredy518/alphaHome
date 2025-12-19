"""
配置管理模块

提供统一的配置获取接口，支持通过命令行参数显式指定配置文件。
"""

import os
from typing import Optional
from alphahome.common.config_manager import ConfigManager

def get_config_manager(config_path: Optional[str] = None) -> ConfigManager:
    """
    获取配置管理器实例。
    
    Args:
        config_path: 显式指定的配置文件路径（可选）。
                    如果提供，会优先使用。
                    
    Returns:
        配置管理器实例
        
    Raises:
        RuntimeError: 如果配置文件不存在或无法读取
    """
    if config_path:
        # 检查文件存在性
        if not os.path.exists(config_path):
            raise RuntimeError(f"配置文件不存在: {config_path}")
        # 通过环境变量临时覆盖（ConfigManager 会读取）
        old_config = os.environ.get('ALPHAHOME_CONFIG')
        os.environ['ALPHAHOME_CONFIG'] = config_path
        try:
            return ConfigManager()
        finally:
            if old_config:
                os.environ['ALPHAHOME_CONFIG'] = old_config
            else:
                os.environ.pop('ALPHAHOME_CONFIG', None)
    
    return ConfigManager()
