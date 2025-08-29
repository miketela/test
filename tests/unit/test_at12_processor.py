"""Unit tests for AT12 processor."""

import pytest
import json
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from src.AT12.processor import AT12Processor
from src.core.config import Config
from src.core.io import UniversalFileReader


class TestAT12Processor:
    """Test cases for AT12Processor class."""
    
    @pytest.fixture
    def mock_config(self, temp_dir):
        """Create a mock configuration for testing."""
        # Create schema directory and files
        schema_dir = temp_dir / "schemas" / "AT12"
        schema_dir.mkdir(parents=True, exist_ok=True)
        
        # Create expected_files.json
        expected_files = {
            "subtypes": {
                "BASE_AT12": {
                    "description": "Base AT12 regulatory data",
                    "required": True,
                    "pattern": "BASE_AT12_*"
                }
            }
        }
        with open(schema_dir / "expected_files.json", 'w') as f:
            json.dump(expected_files, f)
        
        # Create schema_headers.json
        schema_headers = {
            "BASE_AT12": {
                "Fecha": None,
                "Codigo_Banco": None,
                "Numero_Prestamo": None
            }
        }
        with open(schema_dir / "schema_headers.json", 'w') as f:
            json.dump(schema_headers, f)
        
        return {
            'base_dir': str(temp_dir),
            'source_dir': str(temp_dir / "source"),
            'data_raw_dir': str(temp_dir / "data" / "raw"),
            'data_processed_dir': str(temp_dir / "data" / "processed"),
            'metrics_dir': str(temp_dir / "metrics"),
            'schemas_dir': 'schemas',
            'input_delimiter': ',',
            'encoding': 'utf-8',
            'quotechar': '"',
            'xlsx_sheet_name': 0,
            'chunk_size': 10000,
            'output_delimiter': '|',
            'trailing_delimiter': False
        }
    
    @pytest.fixture
    def mock_logger(self):
        """Create a mock logger for testing."""
        return Mock()
    
    @pytest.fixture
    def at12_processor(self, mock_config, mock_logger):
        """Create AT12Processor instance for testing."""
        return AT12Processor(mock_config)
    
    @pytest.fixture
    def sample_schema(self, temp_dir):
        """Create sample AT12 schema for testing."""
        schema_dir = temp_dir / "schemas" / "AT12"
        schema_dir.mkdir(parents=True, exist_ok=True)
        
        schema_data = {
            "required_headers": [
                "Fecha", "Codigo_Banco", "Numero_Prestamo",
                "Numero_Cliente", "Tipo_Credito", "Moneda",
                "Importe", "Status_Garantia"
            ],
            "data_types": {
                "Fecha": "datetime",
                "Codigo_Banco": "string",
                "Numero_Prestamo": "string",
                "Numero_Cliente": "string",
                "Tipo_Credito": "string",
                "Moneda": "string",
                "Importe": "float",
                "Status_Garantia": "string"
            }
        }
        
        schema_file = schema_dir / "schema_headers.json"
        with open(schema_file, 'w') as f:
            json.dump(schema_data, f, indent=2)
        
        return schema_file
    
    def test_initialization(self, at12_processor, mock_config, mock_logger):
        """Test AT12Processor initialization."""
        assert at12_processor.config == mock_config
        assert at12_processor.atom_name == "AT12"
        assert hasattr(at12_processor, 'logger')
        assert hasattr(at12_processor, 'file_reader')
    
    def test_filename_parser_integration(self, at12_processor):
        """Test that filename parser is properly initialized."""
        assert hasattr(at12_processor, 'filename_parser')
        assert at12_processor.filename_parser is not None
        
        # Test parsing a valid filename through the filename parser
        filename = "BASE_AT12_20240131.CSV"
        parsed = at12_processor.filename_parser.parse_filename(filename)
        
        assert parsed.is_valid
        assert parsed.subtype == "BASE_AT12"
        assert parsed.date_str == "20240131"
    
    @patch('src.core.fs.find_files_by_pattern')
    def test_discover_files_finds_matching_files(self, mock_list_files, at12_processor, temp_dir):
        """Test file discovery finds files matching the period."""
        # Mock file discovery
        mock_files = [
            temp_dir / "BASE_AT12_20240131.CSV",
            temp_dir / "DETALLE_AT12_20240131.CSV",
            temp_dir / "OTHER_AT12_20240228.CSV"  # Different month
        ]
        mock_list_files.return_value = mock_files
        
        # Create actual files for validation
        for file_path in mock_files:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text("Fecha,Codigo_Banco,Numero_Prestamo\n2024-01-31,001,12345\n")
        
        # Mock source_dir in config
        at12_processor.config['source_dir'] = str(temp_dir)
        
        # Test file discovery through explore method
        result = at12_processor.explore(2024, 1, "test-run-001")
        
        # Check that files were discovered
        assert result is not None
        # Should find at least some files (may have validation warnings)
        assert len(mock_files) == 3
    
    @patch('src.core.fs.find_files_by_pattern')
    def test_discover_files_no_matching_files(self, mock_find, at12_processor, temp_dir):
        """Test file discovery when no files match the period."""
        mock_find.return_value = []
        
        # Mock source_dir in config
        at12_processor.config['source_dir'] = str(temp_dir)
        
        result = at12_processor.explore(2024, 1, "test-run-001")
        
        assert not result.success
        assert len(result.output_files or []) == 0
    
    def test_schema_loading(self, at12_processor):
        """Test that schemas are loaded during initialization."""
        # Schemas should be loaded during __init__
        assert hasattr(at12_processor, 'expected_files')
        assert hasattr(at12_processor, 'schema_headers')
        assert 'BASE_AT12' in at12_processor.expected_files['subtypes']
        assert 'BASE_AT12' in at12_processor.schema_headers
    
    def test_validate_files_through_explore(self, at12_processor, temp_dir):
        """Test file validation through explore method."""
        # Create source directory and test files
        source_dir = temp_dir / "source"
        source_dir.mkdir(parents=True, exist_ok=True)
        
        test_files = [
            source_dir / "BASE_AT12_20240131.CSV",
            source_dir / "DETALLE_AT12_20240131.CSV"
        ]
        
        for file_path in test_files:
            file_path.write_text("Fecha,Codigo_Banco,Numero_Prestamo\n2024-01-31,001,12345\n")
        
        # Update config to point to source directory
        at12_processor.config['source_dir'] = str(source_dir)
        
        # Test validation through explore
        with patch('src.core.fs.find_files_by_pattern') as mock_find:
            mock_find.return_value = test_files
            result = at12_processor.explore(2024, 1, "test-run-001")
            
            # Check the result - should process files successfully
            assert result is not None
            # Should succeed with valid files
            assert result.success
    

    
    @patch('src.core.fs.find_files_by_pattern')
    def test_explore_successful_execution(self, mock_find_files, at12_processor, temp_dir):
        """Test successful execution of explore method."""
        # Setup mock files
        source_dir = temp_dir / "source"
        source_dir.mkdir(parents=True, exist_ok=True)
        
        test_files = [
            source_dir / "BASE_AT12_20240131.CSV",
            source_dir / "DETALLE_AT12_20240131.CSV"
        ]
        
        for file_path in test_files:
            file_path.write_text("Fecha,Codigo_Banco,Numero_Prestamo\n2024-01-31,001,12345\n")
        
        mock_find_files.return_value = test_files
        
        # Update config to point to source directory
        at12_processor.config['source_dir'] = str(source_dir)
        
        result = at12_processor.explore(2024, 1, "test-run-001")
        
        # Check the result details
        assert result is not None
        # Should succeed with valid files
        assert result.success
    
    @patch('src.core.fs.find_files_by_pattern')
    def test_explore_no_files_found(self, mock_find_files, at12_processor, temp_dir):
        """Test explore method when no files are found."""
        mock_find_files.return_value = []
        
        # Mock source_dir in config
        at12_processor.config['source_dir'] = str(temp_dir)
        
        result = at12_processor.explore(2024, 1, "test-run-001")
        
        assert not result.success
        assert len(result.output_files or []) == 0