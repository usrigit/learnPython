import logging
import os
import commons
from logging.handlers import RotatingFileHandler


def get_log_info():
    """
    Function get all log variables from config file
    :return: user defined log variables
    """
    log_path = commons.get_prop('common', 'log-path')
    log_size = int(commons.get_prop('common', 'log-size'))
    log_format = commons.get_prop('common', 'log-format')
    log_backup = int(commons.get_prop('common', 'log-backup'))
    log_level = commons.get_prop('common', 'log-level')
    date_format = commons.get_prop('common', 'date-format')

    return log_path, log_size, log_format, log_backup, log_level, date_format


def init(log_name, log_type):
    log_path, log_size, log_format, log_backup, log_level, date_format = get_log_info()
    if not os.path.isdir(log_path):
        os.makedirs(log_path, exist_ok=True)

    log_info = os.path.join(log_path, 'Trade_info.log')
    logger = logging.getLogger(log_name)
    logger.setLevel(log_level)

    log_err = os.path.join(log_path, 'Trade_error.log')
    err_logger = logging.getLogger(log_name + "_Err")
    err_logger.setLevel(log_level)

    if 'INFO' == log_type:
        log_file = log_info
    else:
        log_file = log_err

    handler = RotatingFileHandler(log_file, maxBytes=log_size, backupCount=log_backup)
    formatter = logging.Formatter(fmt=log_format, datefmt=date_format)
    handler.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(handler)

    if not err_logger.handlers:
        err_logger.addHandler(handler)
