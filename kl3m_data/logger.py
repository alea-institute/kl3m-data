"""
Logging module for the kl3m_data package.
"""

# imports
import logging
from pathlib import Path

# constants
DEFAULT_LOG_PATH = Path(__file__).parent.parent / "logs" / "kl3m_data.log"
DEFAULT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
DEFAULT_LOG_LEVEL = logging.INFO
DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


# log factory
def get_logger(
    name: str = "kl3m_data",
    log_path: Path = DEFAULT_LOG_PATH,
    log_level: int = DEFAULT_LOG_LEVEL,
    log_format: str = DEFAULT_LOG_FORMAT,
) -> logging.Logger:
    """
    Get a logger object with the specified log path, log level, and log format.

    Args:
        name (str): Logger name.
        log_path (Path): Path to the log file.
        log_level (int): Log level.
        log_format (str): Log format.

    Returns:
        logging.Logger: A logger object.
    """
    # create logger
    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    # create file handler
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(log_level)

    # create formatter
    formatter = logging.Formatter(log_format)
    file_handler.setFormatter(formatter)

    # add file handler to logger
    logger.addHandler(file_handler)

    return logger


# create default logger
LOGGER = get_logger()
