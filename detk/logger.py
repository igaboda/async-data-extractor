import os
import json
import sys
import logging


logger = logging.getLogger('mylogger')


def setup_logger(output_path: str):
    """Sets up logger based on logging.json configuration file. Sets log output path to given app directory."""
    config_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'logging.json')

    with open(config_path, 'rt') as f:
        log_config = json.load(f)
    log_config['handlers']['file_handler']['filename'] = output_path
    logging.config.dictConfig(log_config)

    sys.excepthook = log_exception


def log_exception(exc_type, exc_value, exc_traceback):
    """Log errors if they occur."""
    logger.exception("Exception occured: ", exc_info=(exc_type, exc_value, exc_traceback))

