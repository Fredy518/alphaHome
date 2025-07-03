import yaml
import pandas as pd
import logging
import argparse

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(config_path='config.yml'):
    """Loads the project configuration from a YAML file."""
    logging.info(f"Loading configuration from {config_path}")
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logging.error(f"Configuration file not found at {config_path}")
        return None
    except yaml.YAMLError as e:
        logging.error(f"Error parsing YAML file: {e}")
        return None

def load_data(data_config):
    """Loads data based on the data source configuration."""
    source_type = data_config.get('source_type')
    path = data_config.get('path')
    logging.info(f"Loading data from {path} (type: {source_type})")
    
    if source_type == 'local_file':
        try:
            # Assuming CSV for simplicity, can be extended
            return pd.read_csv(path)
        except FileNotFoundError:
            logging.error(f"Data file not found at {path}")
            return None
    else:
        logging.warning(f"Data source type '{source_type}' is not yet supported.")
        return None

def run_analysis(data, params):
    """Placeholder for the main analysis or strategy logic."""
    logging.info("Running analysis...")
    if data is None or data.empty:
        logging.warning("Data is empty, skipping analysis.")
        return

    # Example: Accessing parameters
    lookback = params.get('strategy_a', {}).get('lookback_period', 20)
    logging.info(f"Using lookback period: {lookback}")
    
    # --- Your analysis code here ---
    # For example, calculate a simple moving average
    data['sma'] = data['close'].rolling(window=lookback).mean()
    logging.info("Analysis complete. Result preview:")
    logging.info(f"\n{data.tail()}")
    # -------------------------------

def main(config_path):
    """Main entry point for the research project."""
    config = load_config(config_path)
    if not config:
        return

    logging.info(f"Successfully loaded project: {config.get('project_id')}")

    # Load the first data source defined in the config
    if config.get('data'):
        market_data = load_data(config['data'][0])
    else:
        logging.warning("No data sources defined in config.")
        market_data = None

    # Run the analysis
    run_analysis(market_data, config.get('parameters', {}))
    
    logging.info("Project execution finished.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run a research project for AlphaHome.")
    parser.add_argument(
        '--config', 
        type=str, 
        default='config.yml',
        help='Path to the project configuration file.'
    )
    args = parser.parse_args()
    
    main(args.config) 