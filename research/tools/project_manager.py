import os
import shutil
import logging
import argparse
from pathlib import Path

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("ProjectManager")

# --- Core Functions ---
def get_available_templates(template_dir: Path) -> list:
    """Scans the template directory and returns a list of available template names."""
    if not template_dir.is_dir():
        return []
    return [d.name for d in template_dir.iterdir() if d.is_dir()]

def create_project(project_name: str, template: str, base_path: str = 'research/projects'):
    """
    Creates a new research project from a specified template.
    """
    project_path = Path(base_path) / project_name
    template_path = Path('research/templates') / template

    logger.info(f"Attempting to create project '{project_name}' at '{project_path}' using template '{template}'.")

    # 1. Validate template existence
    if not template_path.is_dir():
        logger.error(f"Template '{template}' not found at '{template_path}'. Aborting.")
        available = get_available_templates(template_path.parent)
        logger.info(f"Available templates are: {available}")
        return False

    # 2. Check for existing project directory
    if project_path.exists():
        logger.warning(f"Project directory '{project_path}' already exists. Aborting.")
        return False

    # 3. Copy the template directory
    try:
        shutil.copytree(template_path, project_path)
        logger.info(f"Successfully copied template to '{project_path}'.")
    except OSError as e:
        logger.error(f"Error copying template directory: {e}")
        return False

    logger.info(f"Project '{project_name}' created successfully.")
    return True

# --- Main CLI ---
def main():
    """Main command-line interface for the project manager."""
    parser = argparse.ArgumentParser(
        description="AlphaHome Research Project Manager.",
        formatter_class=argparse.RawTextHelpFormatter
    )

    # Subparsers for different commands
    subparsers = parser.add_subparsers(dest='command', required=True, help='Available commands')

    # 'create' command
    parser_create = subparsers.add_parser('create', help='Create a new research project.')
    parser_create.add_argument('project_name', type=str, help='The name for the new project.')
    parser_create.add_argument(
        '--template',
        type=str,
        default='default_project',
        help='The template to use for the new project.'
    )

    # 'list' command
    parser_list = subparsers.add_parser('list', help='List all available project templates.')

    args = parser.parse_args()

    template_dir = Path('research/templates')

    if args.command == 'create':
        logger.info(f"Executing 'create' command for project '{args.project_name}'.")
        create_project(args.project_name, args.template)
    
    elif args.command == 'list':
        logger.info("Executing 'list' command.")
        available_templates = get_available_templates(template_dir)
        if available_templates:
            print("Available project templates:")
            for template in available_templates:
                print(f"  - {template}")
        else:
            logger.warning(f"No templates found in '{template_dir}'.")

if __name__ == '__main__':
    main() 