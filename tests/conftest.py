"""Pytest configuration and shared fixtures."""

import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock
import json
from datetime import datetime

from src.core.config import Config
from src.core.log import setup_logging


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    temp_path = Path(tempfile.mkdtemp())
    yield temp_path
    shutil.rmtree(temp_path)


@pytest.fixture
def sample_config(temp_dir):
    """Create a sample configuration for testing."""
    config_data = {
        "data_raw_dir": str(temp_dir / "data" / "raw"),
        "data_processed_dir": str(temp_dir / "data" / "processed"),
        "metrics_dir": str(temp_dir / "metrics"),
        "reports_dir": str(temp_dir / "reports"),
        "logs_dir": str(temp_dir / "logs"),
        "schemas_dir": "schemas",
        "log_level": "INFO",
        "log_format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        "csv_delimiter": ",",
        "output_delimiter": "|"
    }
    
    config_file = temp_dir / "config.json"
    with open(config_file, 'w') as f:
        json.dump(config_data, f, indent=2)
    
    return Config(str(config_file))


@pytest.fixture
def sample_csv_content():
    """Sample CSV content for testing."""
    return """Fecha,Codigo_Banco,Numero_Prestamo,Numero_Cliente,Tipo_Credito,Moneda,Importe,Status_Garantia
2024-01-31,001,LOAN001,CLIENT001,COMERCIAL,PEN,50000.00,VIGENTE
2024-01-31,001,LOAN002,CLIENT002,CONSUMO,USD,25000.00,VIGENTE
"""


@pytest.fixture
def sample_csv_file(temp_dir, sample_csv_content):
    """Create a sample CSV file for testing."""
    csv_file = temp_dir / "BASE_AT12_20240131.CSV"
    csv_file.parent.mkdir(parents=True, exist_ok=True)
    csv_file.write_text(sample_csv_content)
    return csv_file


@pytest.fixture
def sample_metrics_data():
    """Sample metrics data for testing."""
    return {
        "exploration_id": "AT12_202401__run-202401",
        "run_id": "AT12_202401__run-202401",
        "timestamp": "2024-01-31T10:00:00",
        "atom": "AT12",
        "atom_type": "AT12",
        "period": "202401",
        "total_files": 1,
        "total_records": 2,
        "files_analyzed": 1,
        "file_metrics": {
            "BASE_AT12_20240131__run-202401.CSV": {
                "file_size": 256,
                "sha256": "abc123def456",
                "row_count": 2,
                "column_count": 8,
                "headers": [
                    "Fecha", "Codigo_Banco", "Numero_Prestamo", 
                    "Numero_Cliente", "Tipo_Credito", "Moneda", 
                    "Importe", "Status_Garantia"
                ],
                "column_metrics": [
                    {
                        "name": "Fecha",
                        "data_type": "datetime",
                        "null_count": 0,
                        "null_percentage": 0.0,
                        "unique_count": 1
                    },
                    {
                        "name": "Codigo_Banco",
                        "data_type": "object",
                        "null_count": 0,
                        "null_percentage": 0.0,
                        "unique_count": 1
                    }
                ]
            }
        }
    }


@pytest.fixture
def sample_metrics_file(temp_dir, sample_metrics_data):
    """Create a sample metrics JSON file for testing."""
    metrics_file = temp_dir / "metrics" / "test_metrics.json"
    metrics_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(metrics_file, 'w') as f:
        json.dump(sample_metrics_data, f, indent=2)
    
    return metrics_file


@pytest.fixture
def mock_logger():
    """Mock logger for testing."""
    return Mock()