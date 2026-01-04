"""Logging configuration for the data cleaning pipeline."""

import logging
import sys
from pathlib import Path
from typing import Optional

from .config import LOG_LEVEL, LOG_FORMAT, LOG_FILE


def setup_logger(
    name: str,
    log_file: Optional[Path] = None,
    level: str = LOG_LEVEL,
    log_format: str = LOG_FORMAT
) -> logging.Logger:
    """
    Set up a logger with both file and console handlers.
    
    Args:
        name: Name of the logger
        log_file: Path to log file (default: uses config.LOG_FILE)
        level: Logging level (default: INFO)
        log_format: Log message format
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))
    
    # Avoid adding handlers multiple times
    if logger.handlers:
        return logger
    
    formatter = logging.Formatter(log_format)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, level.upper()))
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler
    if log_file is None:
        log_file = LOG_FILE
    
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(getattr(logging, level.upper()))
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger
