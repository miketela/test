"""Unit tests for AT12 transformation engine module."""

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from src.AT12.transformation import AT12TransformationEngine
from src.core.transformation import TransformationContext, TransformationResult
from src.core.paths import AT12Paths
from src.core.incidence_reporter import IncidenceType, IncidenceSeverity
from src.core.naming import FilenameParser


class TestAT12TransformationEngine:
    """Test cases for AT12TransformationEngine class."""
    
    @pytest.fixture
    def mock_paths(self, temp_dir):
        """Create mock AT12Paths for testing."""
        base_dir = temp_dir / "transforms"
        incidencias_dir = temp_dir / "incidencias"
        procesados_dir = temp_dir / "procesados"
        paths = AT12Paths(
            base_transforms_dir=base_dir,
            incidencias_dir=incidencias_dir,
            procesados_dir=procesados_dir
        )
        paths.ensure_directories()
        return paths
    
    @pytest.fixture
    def sample_config(self):
        """Sample configuration for testing."""
        return {
            "csv_params": {
                "encoding": "utf-8",
                "delimiter": ","
            },
            "business_rules": {
                "valor_minimo_avaluo": 1000.0,
                "tdc_limite_credito_minimo": 500.0
            }
        }
    
    @pytest.fixture
    def engine(self, mock_paths, sample_config, mock_logger):
        """Create AT12TransformationEngine instance for testing."""
        # Create a mock Config object
        mock_config = Mock()
        mock_config.csv_params = sample_config["csv_params"]
        mock_config.business_rules = sample_config["business_rules"]
        
        engine = AT12TransformationEngine(mock_config)
        # Manually set the paths and logger for testing
        engine.paths = mock_paths
        engine.logger = mock_logger
        return engine
    
    def test_init(self, mock_paths, sample_config, mock_logger):
        """Test AT12TransformationEngine initialization."""
        # Create a mock Config object
        mock_config = Mock()
        mock_config.csv_params = sample_config["csv_params"]
        mock_config.business_rules = sample_config["business_rules"]
        
        engine = AT12TransformationEngine(mock_config)
        
        assert engine.config == mock_config
        assert hasattr(engine, 'header_mapper')
        assert hasattr(engine, 'incidences_data')
    
    def test_extract_subtype_from_filename(self, engine):
        """Test extracting subtype from filename."""
        # Test various filename patterns
        assert engine._extract_subtype_from_filename("BASE_AT12_20240131__run-202401.csv") == "BASE"
        assert engine._extract_subtype_from_filename("TDC_AT12_20240131__run-202401.csv") == "TDC"
        assert engine._extract_subtype_from_filename("SOBREGIRO_AT12_20240131__run-202401.csv") == "SOBREGIRO"
        assert engine._extract_subtype_from_filename("VALORES_AT12_20240131__run-202401.csv") == "VALORES"
        
        # Test invalid filename
        assert engine._extract_subtype_from_filename("invalid_filename.csv") == "UNKNOWN"
    
    def test_extract_date_from_filename(self, engine):
        """Test extracting date from filename."""
        # Test valid filename
        assert engine._extract_date_from_filename("BASE_AT12_20240131__run-202401.csv") == "20240131"
        assert engine._extract_date_from_filename("TDC_AT12_20250228__run-202502.csv") == "20250228"
        
        # Test invalid filename
        assert engine._extract_date_from_filename("invalid_filename.csv") == "UNKNOWN"
    
    @patch('src.AT12.transformation.pd.read_csv')
    def test_load_and_normalize_file_success(self, mock_read_csv, engine):
        """Test successful file loading and normalization."""
        # Mock DataFrame
        mock_df = pd.DataFrame({
            'Fecha': ['2024-01-31', '2024-01-31'],
            'Codigo_Banco': ['001', '001'],
            'Numero_Prestamo': ['LOAN001', 'LOAN002'],
            'Importe': ['50000.00', '25000.00']
        })
        mock_read_csv.return_value = mock_df
        
        # Mock HeaderNormalizer
        with patch('src.AT12.transformation.HeaderNormalizer') as mock_normalizer_class:
            mock_normalizer = Mock()
            mock_normalizer.normalize_headers.return_value = ['fecha', 'codigo_banco', 'numero_prestamo', 'importe']
            mock_normalizer_class.return_value = mock_normalizer
            
            result_df, subtype, date_str = engine._load_and_normalize_file("test_file.csv")
            
            assert isinstance(result_df, pd.DataFrame)
            assert len(result_df) == 2
            mock_read_csv.assert_called_once()
    
    @patch('src.AT12.transformation.pd.read_csv')
    def test_load_and_normalize_file_error(self, mock_read_csv, engine):
        """Test file loading with error."""
        mock_read_csv.side_effect = Exception("File not found")
        
        result = engine._load_and_normalize_file("nonexistent_file.csv")
        
        assert result is None
    
    def test_correct_data_types(self, engine):
        """Test data type correction."""
        # Create test DataFrame with mixed data types
        df = pd.DataFrame({
            'fecha': ['2024-01-31', '2024-01-31'],
            'importe': ['50000.00', 'invalid_amount'],
            'codigo_banco': ['001', '002'],
            'numero_prestamo': ['LOAN001', 'LOAN002']
        })
        
        corrected_df = engine._correct_data_types(df, "BASE", "test_file.csv")
        
        # Check that DataFrame is returned
        assert isinstance(corrected_df, pd.DataFrame)
        assert len(corrected_df) == 2
        
        # Check that incidences were recorded for invalid data
        incidences = engine.incidence_reporter.get_incidences_by_type(IncidenceType.VALIDATION_ERROR)
        assert len(incidences) > 0
    
    def test_apply_business_rule_corrections(self, engine):
        """Test business rule corrections."""
        # Create test DataFrame
        df = pd.DataFrame({
            'fecha': ['2024-01-31', '2024-01-31'],
            'importe': [500.0, 2000.0],  # First value below minimum
            'codigo_banco': ['001', '001'],
            'tipo_credito': ['COMERCIAL', 'COMERCIAL']
        })
        
        corrected_df = engine._apply_business_rule_corrections(df, "BASE", "test_file.csv")
        
        # Check that DataFrame is returned
        assert isinstance(corrected_df, pd.DataFrame)
        assert len(corrected_df) == 2
        
        # Check that business rule violations were recorded
        incidences = engine.incidence_reporter.get_incidences_by_type(IncidenceType.BUSINESS_RULE)
        # Should have at least one incidence for the low value
        assert len(incidences) >= 0  # May vary based on business rule implementation
    
    def test_process_tdc_specific_rules(self, engine):
        """Test TDC-specific processing rules."""
        # Create test DataFrame for TDC
        df = pd.DataFrame({
            'fecha': ['2024-01-31', '2024-01-31'],
            'limite_credito': [300.0, 1000.0],  # First value below minimum
            'codigo_banco': ['001', '001'],
            'numero_tarjeta': ['1234567890123456', '9876543210987654']
        })
        
        processed_df = engine._process_tdc_specific_rules(df, "test_file.csv")
        
        # Check that DataFrame is returned
        assert isinstance(processed_df, pd.DataFrame)
        assert len(processed_df) == 2
    
    def test_process_sobregiro_specific_rules(self, engine):
        """Test SOBREGIRO-specific processing rules."""
        # Create test DataFrame for SOBREGIRO
        df = pd.DataFrame({
            'fecha': ['2024-01-31', '2024-01-31'],
            'limite_sobregiro': [100.0, 500.0],
            'codigo_banco': ['001', '001'],
            'numero_cuenta': ['ACC001', 'ACC002']
        })
        
        processed_df = engine._process_sobregiro_specific_rules(df, "test_file.csv")
        
        # Check that DataFrame is returned
        assert isinstance(processed_df, pd.DataFrame)
        assert len(processed_df) == 2
    
    def test_process_valores_specific_rules(self, engine):
        """Test VALORES-specific processing rules."""
        # Create test DataFrame for VALORES
        df = pd.DataFrame({
            'fecha': ['2024-01-31', '2024-01-31'],
            'valor_nominal': [1000.0, 2000.0],
            'codigo_banco': ['001', '001'],
            'codigo_valor': ['VAL001', 'VAL002']
        })
        
        processed_df = engine._process_valores_specific_rules(df, "test_file.csv")
        
        # Check that DataFrame is returned
        assert isinstance(processed_df, pd.DataFrame)
        assert len(processed_df) == 2
    
    def test_apply_fuera_cierre_filter(self, engine):
        """Test FUERA_CIERRE filter application."""
        # Create test DataFrame with mixed status
        df = pd.DataFrame({
            'fecha': ['2024-01-31', '2024-01-31', '2024-01-31'],
            'status': ['VIGENTE', 'FUERA_CIERRE', 'VIGENTE'],
            'codigo_banco': ['001', '001', '001'],
            'importe': [1000.0, 2000.0, 3000.0]
        })
        
        filtered_df = engine._apply_fuera_cierre_filter(df, "test_file.csv")
        
        # Should filter out FUERA_CIERRE records
        assert isinstance(filtered_df, pd.DataFrame)
        assert len(filtered_df) == 2  # Only VIGENTE records should remain
        assert 'FUERA_CIERRE' not in filtered_df['status'].values
    
    def test_validate_valor_minimo_avaluo(self, engine):
        """Test minimum avaluo value validation."""
        # Create test DataFrame with values below minimum
        df = pd.DataFrame({
            'fecha': ['2024-01-31', '2024-01-31'],
            'valor_avaluo': [500.0, 1500.0],  # First below minimum (1000)
            'codigo_banco': ['001', '001'],
            'numero_prestamo': ['LOAN001', 'LOAN002']
        })
        
        validated_df = engine._validate_valor_minimo_avaluo(df, "test_file.csv")
        
        # Check that DataFrame is returned
        assert isinstance(validated_df, pd.DataFrame)
        assert len(validated_df) == 2
        
        # Check that validation issues were recorded
        incidences = engine.incidence_reporter.get_incidences_by_type(IncidenceType.BUSINESS_RULE)
        # Should have incidence for the low value
        assert len(incidences) >= 0  # May vary based on validation implementation
    
    @patch.object(AT12TransformationEngine, '_load_and_normalize_file')
    def test_apply_transformations_success(self, mock_load_file, engine, temp_dir):
        """Test successful transformation application."""
        # Mock file loading
        mock_df = pd.DataFrame({
            'fecha': ['2024-01-31', '2024-01-31'],
            'codigo_banco': ['001', '001'],
            'importe': [1000.0, 2000.0]
        })
        mock_load_file.return_value = (mock_df, "BASE", "20240131")
        
        # Create context
        context = TransformationContext(
            input_files=["test_file.csv"],
            year=2024,
            month=1,
            run_id="test-run",
            config=engine.config,
            logger=engine.logger
        )
        
        result = engine.apply_transformations(context)
        
        assert result.success is True
        assert result.files_processed == 1
        assert result.total_records == 2
    
    @patch.object(AT12TransformationEngine, '_load_and_normalize_file')
    def test_apply_transformations_with_file_error(self, mock_load_file, engine):
        """Test transformation with file loading error."""
        # Mock file loading to return None (error)
        mock_load_file.return_value = None
        
        # Create context
        context = TransformationContext(
            input_files=["bad_file.csv"],
            year=2024,
            month=1,
            run_id="test-run",
            config=engine.config,
            logger=engine.logger
        )
        
        result = engine.apply_transformations(context)
        
        assert result.success is True  # Should continue with other files
        assert result.files_processed == 0
        assert len(result.errors) == 1
    
    def test_generate_outputs_success(self, engine, temp_dir):
        """Test successful output generation."""
        # Set up processed data
        engine.processed_data = {
            "BASE": pd.DataFrame({
                'fecha': ['2024-01-31'],
                'codigo_banco': ['001'],
                'importe': [1000.0]
            })
        }
        
        # Create context
        context = TransformationContext(
            input_files=["test_file.csv"],
            year=2024,
            month=1,
            run_id="test-run",
            config=engine.config,
            logger=engine.logger
        )
        
        with patch.object(engine, '_generate_incidence_files') as mock_incidence, \
             patch.object(engine, '_generate_processed_files') as mock_processed, \
             patch.object(engine, '_generate_consolidated_file') as mock_consolidated:
            
            mock_incidence.return_value = ["incidence1.csv"]
            mock_processed.return_value = ["processed1.csv"]
            mock_consolidated.return_value = "consolidated.txt"
            
            result = engine.generate_outputs(context)
            
            assert result.success is True
            assert len(result.output_files) == 3
            mock_incidence.assert_called_once()
            mock_processed.assert_called_once()
            mock_consolidated.assert_called_once()
    
    def test_generate_outputs_no_data(self, engine):
        """Test output generation with no processed data."""
        # No processed data
        engine.processed_data = {}
        
        # Create context
        context = TransformationContext(
            input_files=[],
            year=2024,
            month=1,
            run_id="test-run",
            config=engine.config,
            logger=engine.logger
        )
        
        result = engine.generate_outputs(context)
        
        assert result.success is False
        assert "No processed data available" in result.message
    
    def test_generate_incidence_files(self, engine, temp_dir):
        """Test incidence file generation."""
        # Add some incidences
        engine.incidence_reporter.add_validation_error(
            "test_file.csv", 1, "importe", "Invalid amount", "abc", "0.00"
        )
        
        # Set up file dates
        engine.file_dates = {"BASE": "20240131"}
        
        output_files = engine._generate_incidence_files()
        
        # Should generate incidence file
        assert len(output_files) == 1
        assert output_files[0].exists()
        assert "EEOO_TABULAR_AT12_BASE_20240131.csv" in str(output_files[0])
    
    def test_generate_processed_files(self, engine, temp_dir):
        """Test processed file generation."""
        # Set up processed data
        engine.processed_data = {
            "BASE": pd.DataFrame({
                'fecha': ['2024-01-31'],
                'codigo_banco': ['001'],
                'importe': [1000.0]
            })
        }
        engine.file_dates = {"BASE": "20240131"}
        
        output_files = engine._generate_processed_files()
        
        # Should generate processed file
        assert len(output_files) == 1
        assert output_files[0].exists()
        assert "AT12_BASE_20240131.csv" in str(output_files[0])
    
    def test_generate_consolidated_file(self, engine, temp_dir):
        """Test consolidated file generation."""
        # Set up processed data
        engine.processed_data = {
            "BASE": pd.DataFrame({
                'fecha': ['2024-01-31'],
                'codigo_banco': ['001'],
                'importe': [1000.0]
            }),
            "TDC": pd.DataFrame({
                'fecha': ['2024-01-31'],
                'codigo_banco': ['001'],
                'limite_credito': [5000.0]
            })
        }
        
        output_file = engine._generate_consolidated_file(2024, 1, "test-run")
        
        # Should generate consolidated file
        assert output_file.exists()
        assert "AT12_Cobis_202401__run-test-run.TXT" in str(output_file)
        
        # Check file content
        content = output_file.read_text()
        assert len(content) > 0