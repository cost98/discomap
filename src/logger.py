"""
Logging Configuration for DiscoMap

Centralized logging setup with file and console handlers with color support.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional


# ANSI color codes for terminal output
class Colors:
    """ANSI color codes for terminal formatting."""
    RESET = "\033[0m"
    BOLD = "\033[1m"
    
    # Regular colors
    BLACK = "\033[30m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"
    
    # Bright colors
    BRIGHT_BLACK = "\033[90m"
    BRIGHT_RED = "\033[91m"
    BRIGHT_GREEN = "\033[92m"
    BRIGHT_YELLOW = "\033[93m"
    BRIGHT_BLUE = "\033[94m"
    BRIGHT_MAGENTA = "\033[95m"
    BRIGHT_CYAN = "\033[96m"
    BRIGHT_WHITE = "\033[97m"


class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors and emojis."""
    
    # Level to color and emoji mapping
    LEVEL_COLORS = {
        logging.DEBUG: Colors.BRIGHT_BLACK,
        logging.INFO: Colors.BRIGHT_CYAN,
        logging.WARNING: Colors.BRIGHT_YELLOW,
        logging.ERROR: Colors.BRIGHT_RED,
        logging.CRITICAL: Colors.RED + Colors.BOLD,
    }
    
    LEVEL_EMOJIS = {
        logging.DEBUG: "🔍",
        logging.INFO: "ℹ️ ",
        logging.WARNING: "⚠️ ",
        logging.ERROR: "❌",
        logging.CRITICAL: "🔥",
    }
    
    def format(self, record):
        # Add color to level name
        levelname = record.levelname
        if record.levelno in self.LEVEL_COLORS:
            color = self.LEVEL_COLORS[record.levelno]
            emoji = self.LEVEL_EMOJIS[record.levelno]
            record.levelname = f"{color}{emoji} {levelname}{Colors.RESET}"
            
            # Color the entire message based on level
            if record.levelno >= logging.ERROR:
                record.msg = f"{color}{record.msg}{Colors.RESET}"
            elif record.levelno == logging.WARNING:
                record.msg = f"{color}{record.msg}{Colors.RESET}"
        
        # Format timestamp in cyan
        original_asctime = self.formatTime(record, self.datefmt)
        record.asctime = f"{Colors.CYAN}{original_asctime}{Colors.RESET}"
        
        # Format module name in bright black (gray)
        record.name = f"{Colors.BRIGHT_BLACK}{record.name}{Colors.RESET}"
        
        return super().format(record)


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

        # Plain formatter for file (no colors)
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s", 
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        
        # Colored formatter for console
        console_formatter = ColoredFormatter(
            "%(asctime)s %(name)s %(levelname)s %(message)s",
            datefmt="%H:%M:%S"
        )

        # Console handler
        if console:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(level)
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)

        # File handler
        if file and log_dir:
            log_dir = Path(log_dir)
            log_dir.mkdir(parents=True, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d")
            log_file = log_dir / f"{name}_{timestamp}.log"

            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            file_handler.setLevel(level)
            file_handler.setFormatter(file_formatter)
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
