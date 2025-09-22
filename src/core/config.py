#!/usr/bin/env python3
"""
Configuration management for SBP Atoms Pipeline.
Handles environment variables and system parameters.
"""

import os
import json
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass, field


@dataclass
class Config:
    """Configuration class for SBP Atoms Pipeline."""
    
    # Base directories
    base_dir: str = field(default_factory=lambda: os.getenv('SBP_BASE_DIR', os.getcwd()))
    source_dir: str = field(default_factory=lambda: os.getenv('SBP_SOURCE_DIR', 'source'))
    transforms_dir: str = field(default_factory=lambda: os.getenv('SBP_TRANSFORMS_DIR', 'transforms'))
    data_raw_dir: str = field(default_factory=lambda: os.getenv('SBP_DATA_RAW_DIR', 'data/raw'))
    data_processed_dir: str = field(default_factory=lambda: os.getenv('SBP_DATA_PROCESSED_DIR', 'data/processed'))
    metrics_dir: str = field(default_factory=lambda: os.getenv('SBP_METRICS_DIR', 'metrics'))
    logs_dir: str = field(default_factory=lambda: os.getenv('SBP_LOGS_DIR', 'logs'))
    schemas_dir: str = field(default_factory=lambda: os.getenv('SBP_SCHEMAS_DIR', 'schemas'))
    
    # Processing parameters
    max_workers: int = field(default_factory=lambda: int(os.getenv('SBP_MAX_WORKERS', '4')))
    strict_period: bool = field(default_factory=lambda: os.getenv('SBP_STRICT_PERIOD', 'true').lower() == 'true')
    date_format: str = field(default_factory=lambda: os.getenv('SBP_DATE_FORMAT', '%Y%m%d'))
    chunk_size: int = field(default_factory=lambda: int(os.getenv('SBP_CHUNK_SIZE', '10000')))
    
    # CSV parameters
    encoding: Optional[str] = field(default_factory=lambda: os.getenv('SBP_ENCODING'))
    csv_delimiter: str = field(default_factory=lambda: os.getenv('SBP_CSV_DELIMITER', ','))
    
    # Output parameters
    output_delimiter: str = field(default_factory=lambda: os.getenv('SBP_OUTPUT_DELIMITER', '|'))
    trailing_delimiter: bool = field(default_factory=lambda: os.getenv('SBP_TRAILING_DELIMITER', 'false').lower() == 'true')
    
    # Logging
    log_level: str = field(default_factory=lambda: os.getenv('SBP_LOG_LEVEL', 'INFO'))
    log_format: str = field(default_factory=lambda: os.getenv('SBP_LOG_FORMAT', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    
    def __init__(self, config_file: Optional[str] = None):
        """Initialize Config from file or environment variables.
        
        Args:
            config_file: Optional path to JSON configuration file
        """
        if config_file:
            self._load_from_file(config_file)
        else:
            # Initialize with defaults from environment variables
            self.base_dir = os.getenv('SBP_BASE_DIR', os.getcwd())
            self.source_dir = os.getenv('SBP_SOURCE_DIR', 'source')
            self.transforms_dir = os.getenv('SBP_TRANSFORMS_DIR', 'transforms')
            self.data_raw_dir = os.getenv('SBP_DATA_RAW_DIR', 'data/raw')
            self.data_processed_dir = os.getenv('SBP_DATA_PROCESSED_DIR', 'data/processed')
            self.metrics_dir = os.getenv('SBP_METRICS_DIR', 'metrics')
            self.logs_dir = os.getenv('SBP_LOGS_DIR', 'logs')
            self.schemas_dir = os.getenv('SBP_SCHEMAS_DIR', 'schemas')
            self.max_workers = int(os.getenv('SBP_MAX_WORKERS', '4'))
            self.strict_period = os.getenv('SBP_STRICT_PERIOD', 'true').lower() == 'true'
            self.date_format = os.getenv('SBP_DATE_FORMAT', '%Y%m%d')
            self.chunk_size = int(os.getenv('SBP_CHUNK_SIZE', '10000'))
            self.encoding = os.getenv('SBP_ENCODING')
            self.csv_delimiter = os.getenv('SBP_CSV_DELIMITER', ',')
            self.output_delimiter = os.getenv('SBP_OUTPUT_DELIMITER', '|')
            self.trailing_delimiter = os.getenv('SBP_TRAILING_DELIMITER', 'false').lower() == 'true'
            self.log_level = os.getenv('SBP_LOG_LEVEL', 'INFO')
            self.log_format = os.getenv('SBP_LOG_FORMAT', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        self.__post_init__()
    
    def _load_from_file(self, config_file: str):
        """Load configuration from JSON file.
        
        Args:
            config_file: Path to JSON configuration file
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            json.JSONDecodeError: If config file contains invalid JSON
            KeyError: If required fields are missing
        """
        config_path = Path(config_file)
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_file}")
        
        try:
            with open(config_file, 'r') as f:
                config_data = json.load(f)
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(f"Invalid JSON in config file: {config_file}", e.doc, e.pos)
        
        # Required fields for tests
        required_fields = [
            'data_raw_dir', 'data_processed_dir', 'metrics_dir', 
            'logs_dir', 'schemas_dir'
        ]
        
        for field in required_fields:
            if field not in config_data:
                raise KeyError(f"Missing required field: {field}")
        
        # Set values from config file
        self.base_dir = config_data.get('base_dir', os.getcwd())
        self.source_dir = config_data.get('source_dir', 'source')
        self.transforms_dir = config_data.get('transforms_dir', 'transforms')
        self.data_raw_dir = config_data['data_raw_dir']
        self.data_processed_dir = config_data['data_processed_dir']
        self.metrics_dir = config_data['metrics_dir']
        self.logs_dir = config_data['logs_dir']
        self.schemas_dir = config_data['schemas_dir']
        self.max_workers = config_data.get('max_workers', 4)
        self.strict_period = config_data.get('strict_period', True)
        self.date_format = config_data.get('date_format', '%Y%m%d')
        self.chunk_size = config_data.get('chunk_size', 10000)
        self.encoding = config_data.get('encoding')
        self.csv_delimiter = config_data.get('csv_delimiter', ',')
        self.output_delimiter = config_data.get('output_delimiter', '|')
        self.trailing_delimiter = config_data.get('trailing_delimiter', False)
        self.log_level = config_data.get('log_level', 'INFO')
        self.log_format = config_data.get('log_format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    def __post_init__(self):
        """Post-initialization to resolve relative paths."""
        base_path = Path(self.base_dir)
        
        # Convert relative paths to absolute
        if not Path(self.source_dir).is_absolute():
            self.source_dir = str(base_path / self.source_dir)
        
        if not Path(self.transforms_dir).is_absolute():
            self.transforms_dir = str(base_path / self.transforms_dir)
        
        if not Path(self.data_raw_dir).is_absolute():
            self.data_raw_dir = str(base_path / self.data_raw_dir)
        
        if not Path(self.data_processed_dir).is_absolute():
            self.data_processed_dir = str(base_path / self.data_processed_dir)
        
        if not Path(self.metrics_dir).is_absolute():
            self.metrics_dir = str(base_path / self.metrics_dir)
        
        if not Path(self.logs_dir).is_absolute():
            self.logs_dir = str(base_path / self.logs_dir)
        
        if not Path(self.schemas_dir).is_absolute():
            self.schemas_dir = str(base_path / self.schemas_dir)
    
    def create_directories(self):
        """Create necessary directories if they don't exist."""
        directories = [
            self.source_dir,
            self.transforms_dir,
            self.data_raw_dir,
            self.data_processed_dir,
            self.metrics_dir,
            self.logs_dir,
            self.schemas_dir
        ]
        
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary.
        
        Returns:
            Dictionary representation of configuration
        """
        return {
            'base_dir': self.base_dir,
            'source_dir': self.source_dir,
            'transforms_dir': self.transforms_dir,
            'data_raw_dir': self.data_raw_dir,
            'data_processed_dir': self.data_processed_dir,
            'metrics_dir': self.metrics_dir,
            'logs_dir': self.logs_dir,
            'schemas_dir': self.schemas_dir,
            'max_workers': self.max_workers,
            'strict_period': self.strict_period,
            'date_format': self.date_format,
            'chunk_size': self.chunk_size,
            'encoding': self.encoding,
            'csv_delimiter': self.csv_delimiter,
            'output_delimiter': self.output_delimiter,
            'trailing_delimiter': self.trailing_delimiter,
            'log_level': self.log_level,
            'log_format': self.log_format
        }
    
    def update_from_args(self, args) -> 'Config':
        """Update configuration from command line arguments.
        
        Args:
            args: Parsed command line arguments
        
        Returns:
            Updated configuration instance
        """
        if hasattr(args, 'config') and args.config:
            # Load from config file if specified
            # TODO: Implement config file loading
            pass
        
        if hasattr(args, 'log_level') and args.log_level:
            self.log_level = args.log_level.upper()
        
        if hasattr(args, 'workers') and args.workers:
            self.max_workers = args.workers
        
        if hasattr(args, 'verbose') and args.verbose:
            self.log_level = 'DEBUG'
        
        if hasattr(args, 'chunk_size') and args.chunk_size:
            self.chunk_size = args.chunk_size
        
        return self
    
    @classmethod
    def from_env(cls) -> 'Config':
        """Create configuration from environment variables.
        
        Returns:
            Configuration instance
        """
        return cls()
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'Config':
        """Create configuration from dictionary.
        
        Args:
            config_dict: Configuration dictionary
        
        Returns:
            Configuration instance
        """
        return cls(**config_dict)
    
    def validate(self) -> bool:
        """Validate configuration parameters.
        
        Returns:
            True if configuration is valid
        
        Raises:
            ValueError: If configuration is invalid
        """
        # Validate directories exist or can be created
        try:
            self.create_directories()
        except Exception as e:
            raise ValueError(f"Cannot create required directories: {e}")
        
        # Validate numeric parameters
        if self.max_workers <= 0:
            raise ValueError("max_workers must be positive")
        
        if self.chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        
        # Validate log level
        valid_log_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if self.log_level.upper() not in valid_log_levels:
            raise ValueError(f"log_level must be one of: {valid_log_levels}")
        
        return True


def load_config(config_file: Optional[str] = None) -> Config:
    """Load configuration from file or environment.
    
    Args:
        config_file: Optional path to configuration file
    
    Returns:
        Configuration instance
    """
    if config_file and Path(config_file).exists():
        # TODO: Implement config file loading (YAML/JSON)
        pass
    
    config = Config.from_env()
    config.validate()
    return config
