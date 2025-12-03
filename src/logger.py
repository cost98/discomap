"""
Logging Configuration for DiscoMap

Centralized logging setup with file and console handlers.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional


class Logger:
    """Centralized logging configuration."""

    _loggers = {}

    @classmethod
    def setup(
        cls,
        name: str,
        log_dir: Optional[Path] = None,
        level: int = logging.INFO,
        console: bool = True,
        file: bool = True,
    ) -> logging.Logger:
        """
        Setup and configure a logger.

        Args:
            name: Logger name (usually module name)
            log_dir: Directory for log files
            level: Logging level
            console: Enable console output
            file: Enable file output

        Returns:
            Configured logger instance
        """
        if name in cls._loggers:
            return cls._loggers[name]

        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.propagate = False

        # Avoid duplicate handlers
        if logger.handlers:
            return logger

        # Formatter
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )

        # Console handler
        if console:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(level)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        # File handler
        if file and log_dir:
            log_dir = Path(log_dir)
            log_dir.mkdir(parents=True, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d")
            log_file = log_dir / f"{name}_{timestamp}.log"

            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        cls._loggers[name] = logger
        return logger

    @classmethod
    def get_logger(cls, name: str) -> logging.Logger:
        """Get existing logger or create default one."""
        if name not in cls._loggers:
            return cls.setup(name, file=False)
        return cls._loggers[name]


def get_logger(name: str, log_dir: Optional[Path] = None) -> logging.Logger:
    """
    Convenience function to get a logger.

    Args:
        name: Logger name
        log_dir: Optional log directory

    Returns:
        Logger instance
    """
    return Logger.setup(name, log_dir=log_dir)
