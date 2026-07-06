"""
logger.py
=========
Centralized logging setup used by every job/module in the pipeline.
Writes to both console (for Databricks driver logs) and a rotating file
(useful for local/Docker runs).
"""

import logging
import os
from logging.handlers import RotatingFileHandler


def get_logger(name: str, log_file: str = "logs/pipeline.log",
                level: str = "INFO") -> logging.Logger:
    """Return a configured logger.

    Args:
        name: usually __name__ of the calling module.
        log_file: path to the log file (created if missing).
        level: logging level string, e.g. "INFO", "DEBUG".

    Returns:
        A configured logging.Logger instance.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        # Avoid attaching duplicate handlers if called multiple times
        # (common in notebooks / repeated job invocations).
        return logger

    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)
    logger.addHandler(console_handler)

    try:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_handler = RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=3)
        file_handler.setFormatter(fmt)
        logger.addHandler(file_handler)
    except OSError:
        # In read-only or serverless environments (e.g. some Databricks clusters)
        # file logging may not be writable - console logging still works.
        logger.warning("Could not attach file handler for logging; using console only.")

    return logger
