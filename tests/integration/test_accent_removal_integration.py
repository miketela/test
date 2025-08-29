"""Integration tests for accent removal in CSV header validation."""

import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, MagicMock

from src.AT12.processor import AT12Processor
from src.core.naming import HeaderNormalizer


class TestAccentRemovalIntegration:
    """Integration tests for accent removal in the validation flow."""
    
    @pytest.fixture
    def temp_csv_with_accents(self):
        """Create a temporary CSV file with accented headers."""
        csv_content = '''"Número_Préstamo","Código_Banco","País_Emisión","Fecha"
"12345","001","Colombia","2024-01-31"
"67890","002","México","2024-01-31"
'''
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='_BASE_AT12_20240131.csv', delete=False, encoding='utf-8') as f:
            f.write(csv_content)
            temp_file = f.name
        
        yield temp_file
        
        # Cleanup
        if os.path.exists(temp_file):
            os.unlink(temp_file)
    
    @pytest.fixture
    def mock_config(self):
        """Mock configuration for AT12Processor."""
        return {
            'base_dir': '/Users/macbook/Documents/newAtoms',
            'source_dir': '/tmp',
            'csv_delimiter': ',',
            'encoding': 'utf-8',
            'chunk_size': 10000,
            'output_delimiter': '|',
            'trailing_delimiter': False
        }
    
    @pytest.fixture
    def mock_schema_headers(self):
        """Mock schema headers without accents (normalized)."""
        return {
            'BASE_AT12': {
                'Numero_Prestamo': 'string',
                'Codigo_Banco': 'string', 
                'Pais_Emision': 'string',
                'Fecha': 'date'
            }
        }
    
    def test_header_normalization_removes_accents(self):
        """Test that header normalization removes accents correctly."""
        headers_with_accents = [
            "Número_Préstamo",
            "Código_Banco", 
            "País_Emisión",
            "Fecha"
        ]
        
        expected_normalized = [
            "Numero_Prestamo",
            "Codigo_Banco",
            "Pais_Emision", 
            "Fecha"
        ]
        
        result = HeaderNormalizer.normalize_headers(headers_with_accents)
        assert result == expected_normalized
    
    def test_validation_with_accented_headers_passes(self, temp_csv_with_accents):
        """Test that CSV files with accented headers can be read and normalized correctly."""
        from src.core.io import StrictCSVReader
        
        # Test that we can read the CSV file with accented headers
        csv_reader = StrictCSVReader(delimiter=',', encoding='utf-8')
        
        # Read the file and get headers
        df_sample = csv_reader.read_sample(Path(temp_csv_with_accents), sample_size=10)
        actual_headers = list(df_sample.columns)
        
        # Verify the original headers contain accents
        assert "Número_Préstamo" in actual_headers
        assert "Código_Banco" in actual_headers
        assert "País_Emisión" in actual_headers
        
        # Test normalization removes accents
        normalized_headers = HeaderNormalizer.normalize_headers(actual_headers)
        expected_normalized = ["Numero_Prestamo", "Codigo_Banco", "Pais_Emision", "Fecha"]
        
        assert normalized_headers == expected_normalized
        
        # Test validation against schema (both normalized)
        schema_headers = ["Numero_Prestamo", "Codigo_Banco", "Pais_Emision", "Fecha"]
        validation_result = HeaderNormalizer.validate_headers_against_schema(
            normalized_headers, schema_headers, order_strict=False
        )
        
        assert validation_result['is_valid'] == True
        assert len(validation_result['errors']) == 0
    
    def test_validation_with_mixed_accent_scenarios(self):
        """Test header validation with various accent scenarios using HeaderNormalizer directly."""
        test_scenarios = [
            # (CSV headers, expected normalized, description)
            (["Numero_Prestamo", "Codigo_Banco", "Pais_Emision", "Fecha"], 
             ["Numero_Prestamo", "Codigo_Banco", "Pais_Emision", "Fecha"], "No accents"),
            (["Número_Préstamo", "Código_Banco", "País_Emisión", "Fecha"], 
             ["Numero_Prestamo", "Codigo_Banco", "Pais_Emision", "Fecha"], "With accents"),
            (["NÚMERO_PRÉSTAMO", "CÓDIGO_BANCO", "PAÍS_EMISIÓN", "FECHA"], 
             ["NUMERO_PRESTAMO", "CODIGO_BANCO", "PAIS_EMISION", "FECHA"], "Uppercase with accents"),
            (["número_préstamo", "código_banco", "país_emisión", "fecha"], 
             ["numero_prestamo", "codigo_banco", "pais_emision", "fecha"], "Lowercase with accents"),
        ]
        
        for headers, expected_normalized, description in test_scenarios:
            result = HeaderNormalizer.normalize_headers(headers)
            assert result == expected_normalized, f"Scenario '{description}' failed. Expected {expected_normalized}, got {result}"
            
            # Test that normalized headers can be validated against schema
            # Use the same case as the normalized result for validation
            if description == "Uppercase with accents":
                schema_headers = ["NUMERO_PRESTAMO", "CODIGO_BANCO", "PAIS_EMISION", "FECHA"]
            elif description == "Lowercase with accents":
                schema_headers = ["numero_prestamo", "codigo_banco", "pais_emision", "fecha"]
            else:
                schema_headers = ["Numero_Prestamo", "Codigo_Banco", "Pais_Emision", "Fecha"]
            
            validation_result = HeaderNormalizer.validate_headers_against_schema(
                result, schema_headers, order_strict=False
            )
            assert validation_result['is_valid'] == True, f"Validation failed for scenario '{description}': {validation_result['errors']}"
    
    def test_accent_removal_preserves_non_accented_characters(self):
        """Test that accent removal doesn't affect non-accented characters."""
        test_cases = [
            ("Fecha_Inicio", "Fecha_Inicio"),  # No change expected
            ("Codigo_123", "Codigo_123"),      # Numbers preserved
            ("Status_OK", "Status_OK"),        # English preserved
            ("Valor_USD", "Valor_USD"),        # Currency codes preserved
        ]
        
        for input_text, expected in test_cases:
            result = HeaderNormalizer.remove_accents(input_text)
            assert result == expected, f"Expected '{expected}' but got '{result}' for input '{input_text}'"
    
    def test_full_normalization_pipeline(self):
        """Test the complete header normalization pipeline."""
        # Headers as they might appear in a real CSV file
        raw_headers = [
            "  Número de Préstamo  ",  # Spaces, accents
            "Código-Banco@2024",      # Special chars, accents
            "País__de___Emisión",     # Multiple underscores, accents
            "FECHA (ACTUALIZACIÓN)",  # Parentheses, uppercase, accents
        ]
        
        expected_normalized = [
            "Numero_de_Prestamo",
            "Codigo_Banco_2024",
            "Pais_de_Emision",
            "FECHA_ACTUALIZACION"
        ]
        
        result = HeaderNormalizer.normalize_headers(raw_headers)
        assert result == expected_normalized