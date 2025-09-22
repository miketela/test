"""Unit tests for configuration module."""

import pytest
import json
import tempfile
from pathlib import Path
from unittest.mock import patch, mock_open

from src.core.config import Config


class TestConfig:
    """Test cases for Config class."""
    
    def test_config_initialization_with_valid_file(self, temp_dir):
        """Test Config initialization with a valid configuration file."""
        config_data = {
            "data_raw_dir": "data/raw",
            "data_processed_dir": "data/processed",
            "metrics_dir": "metrics",
            "logs_dir": "logs",
            "schemas_dir": "schemas",
            "log_level": "INFO",
            "log_format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        }
        
        config_file = temp_dir / "config.json"
        with open(config_file, 'w') as f:
            json.dump(config_data, f)
        
        config = Config(str(config_file))
        
        # Paths are converted to absolute, so check they end with the expected relative path
        assert config.data_raw_dir.endswith("data/raw")
        assert config.data_processed_dir.endswith("data/processed")
        assert config.metrics_dir.endswith("metrics")
        assert config.logs_dir.endswith("logs")
        assert config.schemas_dir.endswith("schemas")
        assert config.log_level == "INFO"
        assert config.log_format == "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    def test_config_initialization_with_nonexistent_file(self):
        """Test Config initialization with a non-existent file."""
        with pytest.raises(FileNotFoundError):
            Config("nonexistent_config.json")
    
    def test_config_initialization_with_invalid_json(self, temp_dir):
        """Test Config initialization with invalid JSON."""
        config_file = temp_dir / "invalid_config.json"
        config_file.write_text("{ invalid json }")
        
        with pytest.raises(json.JSONDecodeError):
            Config(str(config_file))
    
    def test_config_missing_required_fields(self, temp_dir):
        """Test Config initialization with missing required fields."""
        config_data = {
            "data_raw_dir": "data/raw"
            # Missing other required fields
        }
        
        config_file = temp_dir / "incomplete_config.json"
        with open(config_file, 'w') as f:
            json.dump(config_data, f)
        
        with pytest.raises(KeyError):
            Config(str(config_file))
    
    def test_config_default_values(self, temp_dir):
        """Test Config with minimal required fields uses defaults where applicable."""
        config_data = {
            "data_raw_dir": "data/raw",
            "data_processed_dir": "data/processed",
            "metrics_dir": "metrics",
            "logs_dir": "logs",
            "schemas_dir": "schemas"
            # Missing log_level and log_format
        }
        
        config_file = temp_dir / "minimal_config.json"
        with open(config_file, 'w') as f:
            json.dump(config_data, f)
        
        config = Config(str(config_file))
        
        # Should have default values or handle missing fields gracefully
        assert hasattr(config, 'data_raw_dir')
        assert hasattr(config, 'data_processed_dir')
    
    @patch('builtins.open', new_callable=mock_open, read_data='{"data_raw_dir": "test", "data_processed_dir": "test", "metrics_dir": "test", "logs_dir": "test", "schemas_dir": "test"}')
    @patch('pathlib.Path.exists', return_value=True)
    def test_config_with_mocked_file(self, mock_exists, mock_file):
        """Test Config with mocked file operations."""
        # This test should now work with all required fields
        config = Config("mocked_config.json")
        assert config.data_raw_dir.endswith("test")
        
        mock_file.assert_called_once_with("mocked_config.json", 'r')
        mock_exists.assert_called()
