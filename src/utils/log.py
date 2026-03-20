import logging

logging.basicConfig(
    filename="etl_dev.log",
    encoding="utf-8",
    filemode="a",
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def log_info(message: str) -> logging:
    """
    Args:
        message (str): message that the user wants to log

    Returns:
        logging: returns a message
    """

    return logger.info(message)

def log_error(message: str) -> logging:
    """
    Args:
        message (str): error message that the user wants to log

    Returns:
        logging: returns an error message
    """

    return logger.error(message)