"""
Running the Xetra ETL application
Entrypoint of the program
"""

import logging
import logging.config

import yaml

def main():
    """
    Entry point to run the xetra ETL job
    """
    # Parsing Yaml file
    config_path = '/home/tgou1055/xetra-ETL-project/configs/xetra_report1_config.yml'
    config = yaml.safe_load(open(config_path))
    # Configure logging
    log_config = config['logging']
    logging.config.dictConfig(log_config)  # load it as a dictionary
    logger = logging.getLogger(__name__) # __name__, neglect the name of the file
    logger.info("This is a test of logging!")


if __name__ == '__main__':
    main()