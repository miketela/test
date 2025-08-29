import unittest
from src.core.header_mapping import HeaderMapper


class TestHeaderMapper(unittest.TestCase):
    """Test cases for HeaderMapper class."""
    
    def test_get_mapping_for_subtype_at02_cuentas(self):
        """Test that AT02_CUENTAS mapping is available."""
        mapping = HeaderMapper.get_mapping_for_subtype('AT02_CUENTAS')
        self.assertIsNotNone(mapping)
        self.assertIsInstance(mapping, list)
        self.assertEqual(len(mapping), 31)  # Should have 31 mappings
        
    def test_get_mapping_for_subtype_nonexistent(self):
        """Test that non-existent subtype returns empty dict."""
        mapping = HeaderMapper.get_mapping_for_subtype('NONEXISTENT')
        self.assertEqual(mapping, {})
        
    def test_map_headers_direct_replacement(self):
        """Test header replacement for AT02_CUENTAS."""
        original_headers = [
            '(1) Fecha',
            '(2) Cod_banco',
            '(3) Cod_Subsidiaria'
        ]
        
        mapped_headers = HeaderMapper.map_headers(original_headers, 'AT02_CUENTAS')
        
        expected = ['Fecha', 'Cod_banco', 'Cod_Subsidiaria']
        self.assertEqual(mapped_headers, expected)
        
    def test_map_headers_at02_cuentas_full_list(self):
        """Test header replacement for AT02_CUENTAS with more headers."""
        original_headers = [
            '(1) Fecha',
            '(2) Cod_banco',
            '(3) Cod_Subsidiaria',
            '(4) Tipo_Depósito',
            '(5) Tipo_Cliente'
        ]
        
        mapped_headers = HeaderMapper.map_headers(original_headers, 'AT02_CUENTAS')
        
        expected = ['Fecha', 'Cod_banco', 'Cod_Subsidiaria', 'Tipo_Deposito', 'Tipo_Cliente']
        self.assertEqual(mapped_headers, expected)
        
    def test_map_headers_no_subtype_mapping(self):
        """Test header mapping for subtype without specific mapping."""
        original_headers = [
            'Header (1)',
            'Another Header (2)'
        ]
        
        mapped_headers = HeaderMapper.map_headers(original_headers, 'NONEXISTENT')
        
        # Should use standard normalization
        expected = ['HEADER', 'ANOTHER_HEADER']
        self.assertEqual(mapped_headers, expected)
        
    def test_validate_mapped_headers_success(self):
        """Test successful header validation with mapping."""
        original_headers = [
            '(1) Fecha',
            '(2) Cod_banco',
            '(3) Cod_Subsidiaria'
        ]
        expected_headers = ['Fecha', 'Cod_banco', 'Cod_Subsidiaria']
        
        result = HeaderMapper.validate_mapped_headers(
            original_headers, 'AT02_CUENTAS', expected_headers
        )
        
        self.assertTrue(result['is_valid'])
        self.assertEqual(len(result['missing_headers']), 0)
        self.assertEqual(len(result['extra_headers']), 0)
        
    def test_validate_mapped_headers_missing(self):
        """Test header validation with missing headers."""
        original_headers = [
            '(1) Fecha',
            '(2) Cod_banco'
        ]
        expected_headers = ['Fecha', 'Cod_banco', 'Cod_Subsidiaria']
        
        result = HeaderMapper.validate_mapped_headers(
            original_headers, 'AT02_CUENTAS', expected_headers
        )
        
        self.assertFalse(result['is_valid'])
        self.assertIn('Cod_Subsidiaria', result['missing_headers'])
        
    def test_validate_mapped_headers_extra(self):
        """Test header validation with extra headers for AT02_CUENTAS."""
        # For AT02_CUENTAS, we have 4 input headers but only expect 3 schema headers
        original_headers = [
            '(1) Fecha',
            '(2) Cod_banco',
            '(3) Cod_Subsidiaria',
            '(4) Extra_column'
        ]
        expected_headers = ['Fecha', 'Cod_banco', 'Cod_Subsidiaria']
        
        result = HeaderMapper.validate_mapped_headers(
            original_headers, 'AT02_CUENTAS', expected_headers
        )
        
        # AT02_CUENTAS maps to first 4 schema headers, so Tipo_Deposito will be extra
        self.assertTrue(result['is_valid'])  # Extra headers are warnings, not errors
        self.assertIn('Tipo_Deposito', result['extra_headers'])
        
    def test_get_mapping_report(self):
        """Test mapping report generation."""
        original_headers = [
            '(1) Fecha',
            '(2) Cod_banco',
            '(3) Cod_Subsidiaria'
        ]
        
        report = HeaderMapper.get_mapping_report(
            original_headers, 'AT02_CUENTAS'
        )
        
        self.assertEqual(report['total_headers'], 3)  # Number of input headers
        self.assertEqual(report['direct_mappings'], 3)
        self.assertEqual(report['normalized_mappings'], 0)
        self.assertIn('mappings', report)
        self.assertEqual(len(report['mappings']), 3)
        
        # Check first mapping
        first_mapping = report['mappings'][0]
        self.assertEqual(first_mapping['original'], '(1) Fecha')
        self.assertEqual(first_mapping['mapped'], 'Fecha')
        self.assertEqual(first_mapping['method'], 'direct')


if __name__ == '__main__':
    unittest.main()