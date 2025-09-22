"""Unit tests for naming utilities."""

import pytest
from datetime import datetime

from src.core.naming import FilenameParser, HeaderNormalizer, ParsedFilename


class TestHeaderNormalizer:
    """Test cases for HeaderNormalizer class."""
    
    def test_remove_accents_basic(self):
        """Test basic accent removal functionality."""
        # Test common Spanish accents and tildes
        test_cases = [
            ("Número", "Numero"),
            ("Código", "Codigo"),
            ("País", "Pais"),
            ("Región", "Region"),
            ("Garantía", "Garantia"),
            ("Última", "Ultima"),
            ("Actualización", "Actualizacion"),
            ("Emisión", "Emision"),
            ("Calificación", "Calificacion"),
            ("Descripción", "Descripcion")
        ]
        
        for input_text, expected in test_cases:
            result = HeaderNormalizer.remove_accents(input_text)
            assert result == expected, f"Expected '{expected}' but got '{result}' for input '{input_text}'"
    
    def test_clean_header_text_parenthetical_numbers(self):
        """Test removal of parenthetical numbers."""
        test_cases = [
            ("Fecha (0)", "Fecha"),
            ("Código Banco (1)", "Código Banco"),
            ("País Emisión (2)", "País Emisión"),
            ("Número Préstamo (10)", "Número Préstamo"),
            ("Campo (999)", "Campo")
        ]
        
        for input_text, expected in test_cases:
            result = HeaderNormalizer.clean_header_text(input_text)
            assert result == expected, f"Expected '{expected}', got '{result}' for input '{input_text}'"
    
    def test_clean_header_text_extra_spaces(self):
        """Test removal of extra spaces."""
        test_cases = [
            ("  Fecha  ", "Fecha"),
            ("Código    Banco", "Código Banco"),
            ("   País   Emisión   ", "País Emisión"),
            ("Número\t\tPréstamo", "Número Préstamo"),
            ("Campo\n\nTexto", "Campo Texto")
        ]
        
        for input_text, expected in test_cases:
            result = HeaderNormalizer.clean_header_text(input_text)
            assert result == expected, f"Expected '{expected}', got '{result}' for input '{input_text}'"
    
    def test_clean_header_text_combined(self):
        """Test combined cleaning of parenthetical numbers and spaces."""
        test_cases = [
            ("  Fecha (0)  ", "Fecha"),
            ("Código    Banco (1)", "Código Banco"),
            ("   País   Emisión (2)   ", "País Emisión"),
            ("Número\t\tPréstamo (10)  ", "Número Préstamo"),
            ("  Campo   (999)   Texto  ", "Campo Texto")
        ]
        
        for input_text, expected in test_cases:
            result = HeaderNormalizer.clean_header_text(input_text)
            assert result == expected, f"Expected '{expected}', got '{result}' for input '{input_text}'"
    
    def test_remove_accents_mixed_case(self):
        """Test accent removal with mixed case."""
        test_cases = [
            ("NÚMERO_PRÉSTAMO", "NUMERO_PRESTAMO"),
            ("código_región", "codigo_region"),
            ("Fecha_Última_Actualización", "Fecha_Ultima_Actualizacion")
        ]
        
        for input_text, expected in test_cases:
            result = HeaderNormalizer.remove_accents(input_text)
            assert result == expected, f"Expected '{expected}' but got '{result}' for input '{input_text}'"
    
    def test_remove_accents_no_accents(self):
        """Test that text without accents remains unchanged."""
        test_cases = [
            "Fecha",
            "Codigo_Banco",
            "Numero_Prestamo",
            "Status_Garantia",
            "Tipo_Credito"
        ]
        
        for text in test_cases:
            result = HeaderNormalizer.remove_accents(text)
            assert result == text, f"Text without accents should remain unchanged: '{text}' -> '{result}'"
    
    def test_remove_accents_empty_string(self):
        """Test accent removal with empty string."""
        result = HeaderNormalizer.remove_accents("")
        assert result == ""
    
    def test_normalize_headers_with_accents(self):
        """Test complete header normalization including accent removal."""
        input_headers = [
            "Número de Préstamo",
            "Código del Banco",
            "País de Emisión",
            "Región Geográfica",
            "Última Actualización",
            "Descripción de la Garantía"
        ]
        
        expected_headers = [
            "Numero_de_Prestamo",
            "Codigo_del_Banco",
            "Pais_de_Emision",
            "Region_Geografica",
            "Ultima_Actualizacion",
            "Descripcion_de_la_Garantia"
        ]
        
        result = HeaderNormalizer.normalize_headers(input_headers)
        assert result == expected_headers
    
    def test_normalize_headers_complex_cases(self):
        """Test normalization with complex cases including special characters and accents."""
        input_headers = [
            "  Número-Préstamo  ",  # Leading/trailing spaces, hyphen, accents
            "Código@Banco#2024",    # Special characters with accents
            "País__de___Emisión",   # Multiple underscores with accents
            "Región (Geográfica)",  # Parentheses with accents
            "Última/Actualización", # Slash with accents
        ]
        
        expected_headers = [
            "Numero_Prestamo",
            "Codigo_Banco_2024",
            "Pais_de_Emision",
            "Region_Geografica",
            "Ultima_Actualizacion"
        ]
        
        result = HeaderNormalizer.normalize_headers(input_headers)
        assert result == expected_headers
    
    def test_normalize_headers_real_at12_examples(self):
        """Test with real AT12 header examples that might contain accents."""
        input_headers = [
            "Fecha",
            "Código_Banco",
            "Número_Préstamo",
            "Número_Cliente",
            "Tipo_Crédito",
            "Valor_Garantía",
            "Calificación_Emisor",
            "País_Emisión",
            "Fecha_Última_Actualización",
            "Código_Región"
        ]
        
        expected_headers = [
            "Fecha",
            "Codigo_Banco",
            "Numero_Prestamo",
            "Numero_Cliente",
            "Tipo_Credito",
            "Valor_Garantia",
            "Calificacion_Emisor",
            "Pais_Emision",
            "Fecha_Ultima_Actualizacion",
            "Codigo_Region"
        ]
        
        result = HeaderNormalizer.normalize_headers(input_headers)
        assert result == expected_headers
    
    def test_validate_headers_against_schema_with_accents(self):
        """Test header validation when both actual and expected headers have accents."""
        # Headers from CSV file (with accents)
        actual_headers = ["Número_Préstamo", "Código_Banco", "País_Emisión"]
        
        # Expected headers from schema (normalized, without accents)
        expected_headers = ["Numero_Prestamo", "Codigo_Banco", "Pais_Emision"]
        
        # Normalize both sets
        normalized_actual = HeaderNormalizer.normalize_headers(actual_headers)
        normalized_expected = HeaderNormalizer.normalize_headers(expected_headers)
        
        result = HeaderNormalizer.validate_headers_against_schema(
            normalized_actual, normalized_expected, order_strict=False
        )
        
        assert result['is_valid'] == True
        assert len(result['errors']) == 0
        assert len(result['missing_headers']) == 0
        assert len(result['extra_headers']) == 0
    
    def test_normalize_headers_integration(self):
        """Test complete header normalization with accents."""
        input_headers = ["Número_Préstamo", "Código_Banco", "País_Emisión", "Fecha"]
        expected_normalized = ["Numero_Prestamo", "Codigo_Banco", "Pais_Emision", "Fecha"]
        
        result = HeaderNormalizer.normalize_headers(input_headers)
        assert result == expected_normalized
    
    def test_normalize_headers_with_parenthetical_numbers(self):
        """Test normalization with parenthetical numbers and spaces."""
        input_headers = [
            "Fecha (0)",
            "Código Banco (1)", 
            "País   Emisión (2)",
            "  Número   Préstamo (10)  ",
            "Campo    Extra (999)"
        ]
        expected_normalized = [
            "Fecha",
            "Codigo_Banco",
            "Pais_Emision", 
            "Numero_Prestamo",
            "Campo_Extra"
        ]
        
        result = HeaderNormalizer.normalize_headers(input_headers)
        assert result == expected_normalized
    
    def test_normalize_headers_real_at12_example(self):
        """Test normalization with real AT12 header examples."""
        input_headers = [
            "Número Préstamo",
            "Código Banco", 
            "País de Emisión",
            "Última Actualización",
            "Año de Proceso"
        ]
        expected_normalized = [
            "Numero_Prestamo",
            "Codigo_Banco",
            "Pais_de_Emision", 
            "Ultima_Actualizacion",
            "Ano_de_Proceso"
        ]
        
        result = HeaderNormalizer.normalize_headers(input_headers)
        assert result == expected_normalized
    
    def test_normalize_headers_complete_cleaning(self):
        """Test complete normalization with all cleaning features."""
        input_headers = [
            "  Fecha Renovación (0)  ",
            "Código    Banco (1)", 
            "País   de   Emisión (2)",
            "\tNúmero\t\tPréstamo (10)\n",
            "Campo-Especial@Test (999)"
        ]
        expected_normalized = [
            "Fecha_Renovacion",
            "Codigo_Banco",
            "Pais_de_Emision", 
            "Numero_Prestamo",
            "Campo_Especial_Test"
        ]
        
        result = HeaderNormalizer.normalize_headers(input_headers)
        assert result == expected_normalized


class TestFilenameParser:
    """Test cases for FilenameParser class."""
    
    def test_filename_parser_initialization(self):
        """Test FilenameParser initialization."""
        expected_subtypes = ["BASE_AT12", "TDC_AT12", "VALORES_AT12"]
        parser = FilenameParser(expected_subtypes)
        for subtype in expected_subtypes:
            assert subtype in parser.expected_subtypes
        # Aliases should be registered automatically
        assert 'GARANTIAS_AUTOS_AT12' in parser.expected_subtypes
        assert parser.alias_map['GARANTIAS_AUTOS_AT12'] == 'GARANTIA_AUTOS_AT12'
    
    def test_normalize_filename(self):
        """Test filename normalization to uppercase."""
        parser = FilenameParser(["BASE_AT12"])
        
        test_cases = [
            ("base_at12_20240131.csv", "BASE_AT12_20240131.CSV"),
            ("Base_AT12_20240131.CSV", "BASE_AT12_20240131.CSV"),
            ("BASE_at12_20240131.Csv", "BASE_AT12_20240131.CSV")
        ]
        
        for input_filename, expected in test_cases:
            result = parser.normalize_filename(input_filename)
            assert result == expected
    
    def test_parse_valid_filename(self):
        """Test parsing valid filenames."""
        expected_subtypes = ["BASE_AT12", "TDC_AT12"]
        parser = FilenameParser(expected_subtypes)
        
        filename = "BASE_AT12_20240131.CSV"
        result = parser.parse_filename(filename)
        
        assert result.is_valid == True
        assert result.subtype == "BASE_AT12"
        assert result.date_str == "20240131"
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 31
        assert result.extension == "CSV"
        assert len(result.errors) == 0

    def test_parse_filename_with_alias(self):
        """Ensure filename parser maps known aliases to canonical subtype."""
        parser = FilenameParser(["GARANTIA_AUTOS_AT12"])
        filename = "GARANTIAS_AUTOS_AT12_20250831.csv"
        result = parser.parse_filename(filename)

        assert result.is_valid is True
        assert result.subtype == "GARANTIA_AUTOS_AT12"
        assert result.date_str == "20250831"

    def test_parse_invalid_filename(self):
        """Test parsing invalid filenames."""
        expected_subtypes = ["BASE_AT12", "TDC_AT12"]
        parser = FilenameParser(expected_subtypes)
        
        invalid_filenames = [
            "INVALID_AT12_20240131.CSV",  # Unknown subtype
            "BASE_AT12_2024013.CSV",      # Invalid date format
            "BASE_AT12.CSV",              # Missing date
            "20240131_BASE_AT12.CSV"      # Wrong order
        ]
        
        for filename in invalid_filenames:
            result = parser.parse_filename(filename)
            assert result.is_valid == False or len(result.errors) > 0
    
    def test_parse_valid_extensions(self):
        """Test that both CSV and TXT extensions are accepted."""
        expected_subtypes = ["BASE_AT12"]
        parser = FilenameParser(expected_subtypes)
        
        valid_filenames = [
            "BASE_AT12_20240131.CSV",
            "BASE_AT12_20240131.TXT"
        ]
        
        for filename in valid_filenames:
            result = parser.parse_filename(filename)
            assert result.is_valid == True
            assert len(result.errors) == 0
