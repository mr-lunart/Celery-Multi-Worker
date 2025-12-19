import logging
import sys
import os

def setup_logger():
    logs_path = "logs/consumer"
    logger = logging.getLogger("logger-consumer")
    logger.setLevel(logging.DEBUG)
    log_console_handler = logging.StreamHandler()
    log_file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=os.path.join(logs_path, 'consumer-history.log'),
        when='midnight',
        interval=1,
        backupCount=3, # Akan menyimpan log 7 hari terakhir
        encoding='utf-8',
    )
    logger.addHandler(log_console_handler)
    logger.addHandler(log_file_handler)
    formatter = logging.Formatter(
        "{asctime} - {levelname} - {message}",
        style="{",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log_console_handler.setFormatter(formatter)
    log_file_handler.setFormatter(formatter)
    return logger