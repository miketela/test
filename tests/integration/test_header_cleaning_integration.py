import pytest
import tempfile
import os
from unittest.mock import Mock, patch
from src.core.naming import HeaderNormalizer
from src.AT12.processor import AT12Processor


class TestHeaderCleaningIntegration:
    """Integration tests for header cleaning functionality."""
    
    def test_header_cleaning_removes_parenthetical_numbers(self):
        """Test that header cleaning removes parenthetical numbers like (0), (1), etc."""
        input_headers = [
            "Fecha (0)",
            "Código Banco (1)", 
            "País Emisión (2)",
            "Número Préstamo (10)",
            "Campo Extra (999)"
        ]
        
        result = HeaderNormalizer.normalize_headers(input_headers)
        
        expected = [
            "Fecha",
            "Codigo_Banco",
            "Pais_Emision", 
            "Numero_Prestamo",
            "Campo_Extra"
        ]
        
        assert result == expected
    
    def test_header_cleaning_handles_spaces_correctly(self):
        """Test that header cleaning handles various space scenarios."""
        input_headers = [
            "  Fecha Renovación  ",
            "Código    Banco", 
            "País   de   Emisión",
            "\tNúmero\t\tPréstamo\n",
            "Campo-Especial@Test"
        ]
        
        result = HeaderNormalizer.normalize_headers(input_headers)
        
        expected = [
            "Fecha_Renovacion",
            "Codigo_Banco",
            "Pais_de_Emision", 
            "Numero_Prestamo",
            "Campo_Especial_Test"
        ]
        
        assert result == expected
    
    def test_complete_header_cleaning_pipeline(self):
        """Test complete header cleaning with all features combined."""
        input_headers = [
            "  Fecha Renovación (0)  ",
            "Código    Banco (1)", 
            "País   de   Emisión (2)",
            "\tNúmero\t\tPréstamo (10)\n",
            "Campo-Especial@Test (999)"
        ]
        
        result = HeaderNormalizer.normalize_headers(input_headers)
        
        expected = [
            "Fecha_Renovacion",
            "Codigo_Banco",
            "Pais_de_Emision", 
            "Numero_Prestamo",
            "Campo_Especial_Test"
        ]
        
        assert result == expected
    
    def test_validation_with_cleaned_headers_passes(self):
        """Test that validation passes after header cleaning."""
        # Headers with parenthetical numbers and spaces
        csv_headers = [
            "Fecha (0)",
            "Código Banco (1)", 
            "País Emisión (2)",
            "Número Préstamo (10)"
        ]
        
        # Expected schema headers (normalized)
        schema_headers = [
            "Fecha",
            "Codigo_Banco",
            "Pais_Emision", 
            "Numero_Prestamo"
        ]
        
        # Normalize headers
        normalized_headers = HeaderNormalizer.normalize_headers(csv_headers)
        
        # Validate against schema
        validation_result = HeaderNormalizer.validate_headers_against_schema(
            normalized_headers, schema_headers
        )
        
        assert validation_result['is_valid'] is True
        assert len(validation_result['missing_headers']) == 0
        assert len(validation_result['extra_headers']) == 0
    
    def test_mixed_cleaning_scenarios(self):
        """Test various mixed scenarios of header cleaning."""
        test_cases = [
            {
                'input': ["Campo Normal", "Campo (1)", "  Campo Espacios  ", "Campo_Guión (2)"],
                'expected': ["Campo_Normal", "Campo", "Campo_Espacios", "Campo_Guion"]
            },
            {
                'input': ["Año (0)", "Número de Cuenta (1)", "País de Origen (2)"],
                'expected': ["Ano", "Numero_de_Cuenta", "Pais_de_Origen"]
            },
            {
                'input': ["Código   Único (999)", "  Fecha   Última   (0)  "],
                'expected': ["Codigo_Unico", "Fecha_Ultima"]
            }
        ]
        
        for case in test_cases:
            result = HeaderNormalizer.normalize_headers(case['input'])
            assert result == case['expected'], f"Failed for input: {case['input']}"