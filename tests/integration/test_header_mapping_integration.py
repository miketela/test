import unittest
import tempfile
import os
import pandas as pd
from src.core.header_mapping import HeaderMapper
from src.core.naming import HeaderNormalizer


class TestHeaderMappingIntegration(unittest.TestCase):
    """Integration tests for HeaderMapper functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        
    def tearDown(self):
        """Clean up test fixtures."""
        # Clean up temp directory
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        
    def test_at02_cuentas_header_mapping_flow(self):
        """Test complete AT02_CUENTAS header mapping and validation flow."""
        # Original headers with parenthetical numbers and special characters
        original_headers = [
            '(1) Fecha',
            '(2) Cod_banco',
            '(3) Cod_Subsidiaria',
            '(4) Tipo_Depósito'
        ]
        
        # Expected schema headers
        expected_headers = [
            'Fecha',
            'Cod_banco', 
            'Cod_Subsidiaria',
            'Tipo_Deposito'
        ]
        
        # Test mapping - AT02_CUENTAS replaces with predefined schema headers
        mapped_headers = HeaderMapper.map_headers(original_headers, 'AT02_CUENTAS')
        
        # Verify mapping results - should return first 4 schema headers
        self.assertEqual(len(mapped_headers), 4)
        self.assertEqual(mapped_headers[0], 'Fecha')
        self.assertEqual(mapped_headers[1], 'Cod_banco')
        self.assertEqual(mapped_headers[2], 'Cod_Subsidiaria')
        self.assertEqual(mapped_headers[3], 'Tipo_Deposito')
        
        # Test validation
        validation_result = HeaderMapper.validate_mapped_headers(
            original_headers, 'AT02_CUENTAS', expected_headers
        )
        
        self.assertTrue(validation_result['is_valid'])
        self.assertEqual(len(validation_result['missing_headers']), 0)
        self.assertEqual(len(validation_result['extra_headers']), 0)
        
    def test_mixed_mapping_methods(self):
        """Test AT02_CUENTAS direct replacement behavior."""
        # AT02_CUENTAS headers - all get replaced with schema headers
        original_headers = [
            '(1) Fecha',  # Will be replaced with FECHA
            '(2) Cod_banco',  # Will be replaced with COD_BANCO
            'Unknown Header (3)',  # Will be replaced with COD_SUBSIDIARIA
            'Another Unknown'  # Will be replaced with TIPO_DEPOSITO
        ]
        
        mapped_headers = HeaderMapper.map_headers(original_headers, 'AT02_CUENTAS')
        
        # AT02_CUENTAS replaces all headers with first N schema headers
        expected = ['Fecha', 'Cod_banco', 'Cod_Subsidiaria', 'Tipo_Deposito']
        self.assertEqual(mapped_headers, expected)
        
        # Test mapping report - all should be direct for AT02_CUENTAS
        report = HeaderMapper.get_mapping_report(original_headers, 'AT02_CUENTAS')
        
        self.assertEqual(report['total_headers'], 4)
        self.assertEqual(report['direct_mappings'], 4)
        self.assertEqual(report['normalized_mappings'], 0)
        
    def test_mapping_report_generation(self):
        """Test detailed mapping report generation."""
        original_headers = [
            '(1) Fecha',
            'Custom Header (2)'
        ]
        
        report = HeaderMapper.get_mapping_report(original_headers, 'AT02_CUENTAS')
        
        # Check summary - AT02_CUENTAS uses direct replacement
        self.assertEqual(report['total_headers'], 2)
        self.assertEqual(report['direct_mappings'], 2)
        self.assertEqual(report['normalized_mappings'], 0)
        self.assertIn('mappings', report)
        self.assertEqual(len(report['mappings']), 2)
        
        # Check individual mappings
        first_mapping = report['mappings'][0]
        self.assertEqual(first_mapping['original'], '(1) Fecha')
        self.assertEqual(first_mapping['mapped'], 'Fecha')
        self.assertEqual(first_mapping['method'], 'direct')
        
    def test_fallback_to_standard_normalization(self):
        """Test fallback to standard normalization for non-AT02_CUENTAS subtypes."""
        original_headers = [
            'Header',
            'Another Header',
            'Header with Ñ and (5)'
        ]
        
        # Use a different subtype that doesn't have specific mapping
        mapped_headers = HeaderMapper.map_headers(original_headers, 'AT01_SOMETHING')
        
        # Should use standard normalization and convert to uppercase
        expected = ['HEADER', 'ANOTHER_HEADER', 'HEADER_WITH_N_AND']
        self.assertEqual(mapped_headers, expected)
        
    def test_validation_with_partial_match(self):
        """Test validation when headers don't match expected count."""
        # Only 2 input headers but expecting 3
        original_headers = [
            '(1) Fecha',  # Will be replaced with FECHA
            '(2) Cod_banco',  # Will be replaced with COD_BANCO
            # Missing third header
        ]
        
        expected_headers = [
            'Fecha',
            'Cod_banco',
            'Cod_Subsidiaria'  # This will be missing since we only have 2 input headers
        ]
        
        validation_result = HeaderMapper.validate_mapped_headers(
            original_headers, 'AT02_CUENTAS', expected_headers
        )
        
        # Should be invalid because Cod_Subsidiaria is missing
        self.assertFalse(validation_result['is_valid'])
        self.assertIn('Cod_Subsidiaria', validation_result['missing_headers'])
        
    def test_case_insensitive_mapping(self):
        """Test that mapping works regardless of case in original headers."""
        original_headers = [
            '(1) fecha',  # lowercase
            '(2) COD_BANCO',  # uppercase
            '(3) Cod_Subsidiaria'  # mixed case
        ]
        
        mapped_headers = HeaderMapper.map_headers(original_headers, 'AT02_CUENTAS')
        
        expected = ['Fecha', 'Cod_banco', 'Cod_Subsidiaria']
        self.assertEqual(mapped_headers, expected)
        
    def test_special_characters_handling(self):
        """Test AT02_CUENTAS direct replacement with special characters."""
        original_headers = [
            '(4) Tipo_Depósito',  # Has accent
            '(17) Identificación del cliente',  # Has accent and spaces
            '(21) Género'  # Has accent
        ]
        
        mapped_headers = HeaderMapper.map_headers(original_headers, 'AT02_CUENTAS')
        
        # AT02_CUENTAS replaces with first 3 schema headers regardless of input content
        expected = ['Fecha', 'Cod_banco', 'Cod_Subsidiaria']
        self.assertEqual(mapped_headers, expected)


if __name__ == '__main__':
    unittest.main()