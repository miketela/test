"""Unit tests for IO module."""

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import Mock, patch

from src.core.io import StrictCSVReader


class TestStrictCSVReader:
    """Test cases for StrictCSVReader class."""
    
    def test_initialization_with_valid_file(self, sample_csv_file):
        """Test StrictCSVReader initialization with valid CSV file."""
        reader = StrictCSVReader()
        
        assert reader.encoding == 'utf-8'
        assert reader.delimiter == ','
    
    def test_initialization_with_custom_parameters(self, sample_csv_file):
        """Test StrictCSVReader initialization with custom parameters."""
        reader = StrictCSVReader(
            encoding='latin-1', 
            delimiter=';'
        )
        
        assert reader.encoding == 'latin-1'
        assert reader.delimiter == ';'
    
    def test_initialization_with_nonexistent_file(self, temp_dir):
        """Test StrictCSVReader initialization with non-existent file."""
        nonexistent_file = temp_dir / "nonexistent.csv"
        reader = StrictCSVReader()
        
        # The validation should fail with an error for non-existent file
        result = reader.validate_csv(nonexistent_file)
        assert not result.is_valid
        assert len(result.errors) > 0
        assert any("error" in error.lower() for error in result.errors)
    
    def test_validate_csv_with_valid_file(self, sample_csv_file):
        """Test validate_csv method with valid CSV file."""
        reader = StrictCSVReader()
        
        # Should not raise any exception
        result = reader.validate_csv(sample_csv_file)
        assert result.is_valid
    
    def test_validate_csv_with_invalid_file(self, temp_dir):
        """Test validate_csv method with invalid CSV file."""
        invalid_csv = temp_dir / "invalid.csv"
        invalid_csv.write_text("invalid,csv,content\nwith,mismatched\ncolumn,counts,here,extra")
        
        reader = StrictCSVReader()
        result = reader.validate_csv(invalid_csv)
        
        assert result.is_valid  # File is readable, but has warnings
        assert len(result.warnings) > 0
    
    def test_read_csv_returns_dataframe(self, sample_csv_file):
        """Test read_csv method returns a pandas DataFrame."""
        reader = StrictCSVReader()
        
        df = reader.read_csv(sample_csv_file)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert len(df.columns) > 0
    
    def test_read_csv_with_expected_columns(self, sample_csv_file):
        """Test read_csv method with expected column structure."""
        reader = StrictCSVReader()
        
        df = reader.read_csv(sample_csv_file)
        
        expected_columns = [
            'Fecha', 'Codigo_Banco', 'Numero_Prestamo', 
            'Numero_Cliente', 'Tipo_Credito', 'Moneda', 
            'Importe', 'Status_Garantia'
        ]
        
        for col in expected_columns:
            assert col in df.columns
    
    def test_read_csv_chunks_returns_iterator(self, sample_csv_file):
        """Test read_csv_chunks method returns an iterator."""
        reader = StrictCSVReader()
        
        chunks = reader.read_csv_chunks(sample_csv_file)
        
        # Should be able to iterate over chunks
        chunk_list = list(chunks)
        assert len(chunk_list) > 0
        
        for chunk in chunk_list:
            assert isinstance(chunk, pd.DataFrame)
    
    def test_read_sample_returns_limited_rows(self, sample_csv_file):
        """Test read_sample method returns limited number of rows."""
        reader = StrictCSVReader()
        
        sample_df = reader.read_sample(sample_csv_file, sample_size=1)
        
        assert isinstance(sample_df, pd.DataFrame)
        assert len(sample_df) <= 1
    
    def test_read_sample_with_larger_n_than_file(self, sample_csv_file):
        """Test read_sample method when n_rows is larger than file size."""
        reader = StrictCSVReader()
        
        # Request more rows than available
        sample_df = reader.read_sample(sample_csv_file, sample_size=1000)
        
        assert isinstance(sample_df, pd.DataFrame)
        # Should return all available rows, not more
        full_df = reader.read_csv(sample_csv_file)
        assert len(sample_df) <= len(full_df)
    
    def test_count_records_returns_integer(self, sample_csv_file):
        """Test count_records method returns correct count."""
        reader = StrictCSVReader()
        
        count = reader.count_records(sample_csv_file)
        
        assert isinstance(count, int)
        assert count >= 0
        
        # Verify against actual DataFrame length
        df = reader.read_csv(sample_csv_file)
        assert count == len(df)
    
    def test_count_records_excludes_header(self, sample_csv_file):
        """Test that count_records excludes header row."""
        reader = StrictCSVReader()
        
        count = reader.count_records(sample_csv_file)
        
        # Count should be data rows only, not including header
        with open(sample_csv_file, 'r') as f:
            total_lines = len(f.readlines())
        
        assert count == total_lines - 1  # Exclude header
    
    @patch('pandas.read_csv')
    def test_read_csv_with_pandas_error(self, mock_read_csv, sample_csv_file):
        """Test read_csv method handles pandas errors gracefully."""
        mock_read_csv.side_effect = pd.errors.EmptyDataError("No data")
        
        reader = StrictCSVReader()
        
        with pytest.raises(pd.errors.EmptyDataError):
            reader.read_csv(sample_csv_file)
    
    def test_encoding_detection_fallback(self, temp_dir):
        """Test encoding detection and fallback behavior."""
        # Create a file with special characters
        special_csv = temp_dir / "special.csv"
        content = "Nombre,Descripción\nJosé,Niño\n"
        special_csv.write_bytes(content.encode('latin-1'))
        
        # Should handle encoding gracefully
        reader = StrictCSVReader(encoding='latin-1')
        df = reader.read_csv(special_csv)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0