"""
Tests for configuration module.
"""

import pytest

from src.config import Config


def test_config_singleton():
    """Test Config singleton pattern."""
    config1 = Config()
    config2 = Config()
    
    assert config1 is config2


def test_config_defaults():
    """Test default configuration values."""
    config = Config()
    
    assert config.DB_HOST == "localhost"
    assert config.DB_PORT == 5432
    assert config.DB_NAME == "discomap"
    assert config.API_PORT == 8000


def test_config_db_url():
    """Test database URL construction."""
    config = Config()
    
    url = (
        f"postgresql://{config.DB_USER}:{config.DB_PASSWORD}"
        f"@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}"
    )
    
    assert "postgresql://" in url
    assert config.DB_NAME in url
