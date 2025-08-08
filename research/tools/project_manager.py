import os
import shutil
import logging
import argparse
from pathlib import Path

# --- 日志配置 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("ProjectManager")

# --- 核心功能函数 ---
def get_available_templates(template_dir: Path) -> list:
    """
    扫描模板目录并返回所有可用模板的名称列表。
    此函数旨在帮助用户发现可用于创建新项目的模板。
    
    Args:
        template_dir (Path): 模板目录的路径。
        
    Returns:
        list: 可用模板名称的列表。如果目录不存在或没有模板，则返回空列表。
    """
    if not template_dir.is_dir():
        # 如果指定的模板目录不存在，则没有可用模板
        return []
    # 遍历目录中的所有子目录，每个子目录被视为一个模板
    return [d.name for d in template_dir.iterdir() if d.is_dir()]

def create_project(project_name: str, template: str, base_path: str = 'research/projects'):
    """
    从指定的模板创建一个新的研究项目。
    此功能是项目管理的核心，允许用户基于预定义结构快速启动新项目，
    确保项目的一致性和规范性。
    
    Args:
        project_name (str): 新项目的名称。
        template (str): 用于创建项目的模板名称（例如 'strategy_research'）。
        base_path (str): 创建新项目的基础路径，默认为 'research/projects'。
        
    Returns:
        bool: 项目创建成功返回 True，否则返回 False。
    """
    # 构建新项目和模板的完整路径
    project_path = Path(base_path) / project_name
    template_path = Path('research/templates') / template

    logger.info(f"尝试使用模板 '{template}' 在 '{project_path}' 创建项目 '{project_name}'。")

    # 1. 验证模板是否存在
    if not template_path.is_dir():
        logger.error(f"错误：模板 '{template}' 在 '{template_path}' 处未找到。中止操作。")
        available = get_available_templates(template_path.parent)
        logger.info(f"可用模板列表：{available}")
        return False

    # 2. 检查项目目录是否已存在，避免覆盖现有项目
    if project_path.exists():
        logger.warning(f"警告：项目目录 '{project_path}' 已存在。中止操作以防止数据丢失。")
        return False

    # 3. 复制模板目录到新项目位置
    try:
        shutil.copytree(template_path, project_path)
        logger.info(f"成功将模板复制到 '{project_path}'。")
    except OSError as e:
        logger.error(f"复制模板目录时发生操作系统错误：{e}。请检查权限或路径。")
        return False

    logger.info(f"项目 '{project_name}' 创建成功。")
    return True

# --- 命令行接口 (CLI) ---
def main():
    """
    项目管理器的主命令行接口。
    它解析用户输入的命令（如 'create' 或 'list'），并调用相应的函数执行操作。
    """
    parser = argparse.ArgumentParser(
        description="""AlphaHome 研究项目管理器。
用于创建和管理量化研究项目的命令行工具。""",
        formatter_class=argparse.RawTextHelpFormatter
    )

    # 为不同的命令设置子解析器
    subparsers = parser.add_subparsers(
        dest='command', 
        required=True, 
        help='可用命令'
    )

    # 'create' 命令：用于创建新项目
    parser_create = subparsers.add_parser(
        'create', 
        help='创建一个新的研究项目。'
    )
    parser_create.add_argument(
        'project_name', 
        type=str, 
        help='新项目的名称。'
    )
    parser_create.add_argument(
        '--template',
        type=str,
        default='default_project', # 默认使用 'default_project' 模板
        help='用于新项目的模板名称。'
    )

    # 'list' 命令：用于列出所有可用模板
    parser_list = subparsers.add_parser(
        'list', 
        help='列出所有可用的项目模板。'
    )

    args = parser.parse_args()

    # 定义模板目录的路径
    template_dir = Path('research/templates')

    # 根据用户选择的命令执行相应的操作
    if args.command == 'create':
        logger.info(f"正在执行 'create' 命令，为项目 '{args.project_name}' 创建新项目。")
        create_project(args.project_name, args.template)
    
    elif args.command == 'list':
        logger.info("正在执行 'list' 命令，列出可用模板。")
        available_templates = get_available_templates(template_dir)
        if available_templates:
            print("可用项目模板：")
            for template in available_templates:
                print(f"  - {template}")
        else:
            logger.warning(f"警告：在 '{template_dir}' 目录中未找到任何模板。请确保模板目录结构正确。")

if __name__ == '__main__':
    # 当脚本作为主程序运行时，调用 main 函数
    main() 