"""Unit tests for filesystem utilities."""

import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, Mock

from src.core.fs import (
    ensure_directory,
    calculate_sha256,
    get_file_info,
    copy_with_versioning,
    find_files_by_pattern
)


class TestFilesystemUtilities:
    """Test cases for filesystem utility functions."""
    
    def test_ensure_directory_creates_new_directory(self, temp_dir):
        """Test creating a new directory."""
        new_dir = temp_dir / "new_directory"
        assert not new_dir.exists()
        
        result = ensure_directory(new_dir)
        
        assert new_dir.exists()
        assert new_dir.is_dir()
        assert result == new_dir
    
    def test_ensure_directory_existing_directory(self, temp_dir):
        """Test with existing directory."""
        existing_dir = temp_dir / "existing"
        existing_dir.mkdir()
        
        result = ensure_directory(existing_dir)
        
        assert existing_dir.exists()
        assert result == existing_dir
    
    def test_ensure_directory_nested_directories(self, temp_dir):
        """Test creating nested directories."""
        nested_dir = temp_dir / "level1" / "level2" / "level3"
        assert not nested_dir.exists()
        
        result = ensure_directory(nested_dir)
        
        assert nested_dir.exists()
        assert nested_dir.is_dir()
        assert result == nested_dir
    
    def test_calculate_sha256_with_valid_file(self, temp_dir):
        """Test calculate_sha256 with a valid file."""
        test_file = temp_dir / "test.txt"
        test_content = "Hello, World!"
        test_file.write_text(test_content)
        
        file_hash = calculate_sha256(test_file)
        
        assert isinstance(file_hash, str)
        assert len(file_hash) == 64  # SHA256 hash length
        
        # Test consistency - same file should produce same hash
        second_hash = calculate_sha256(test_file)
        assert file_hash == second_hash
    
    def test_calculate_sha256_with_nonexistent_file(self, temp_dir):
        """Test calculate_sha256 with non-existent file."""
        nonexistent_file = temp_dir / "nonexistent.txt"
        
        with pytest.raises(FileNotFoundError):
            calculate_sha256(nonexistent_file)
    
    def test_get_file_info_with_valid_file(self, temp_dir):
        """Test get_file_info with a valid file."""
        test_file = temp_dir / "test.txt"
        test_content = "Hello, World!"  # 13 characters
        test_file.write_text(test_content)
        
        file_info = get_file_info(test_file)
        
        assert isinstance(file_info, dict)
        assert 'size' in file_info
        assert file_info['size'] == len(test_content.encode('utf-8'))
    
    def test_get_file_info_with_nonexistent_file(self, temp_dir):
        """Test get_file_info with non-existent file."""
        nonexistent_file = temp_dir / "nonexistent.txt"
        
        with pytest.raises(FileNotFoundError):
            get_file_info(nonexistent_file)
    
    def test_copy_with_versioning(self, temp_dir):
        """Test copy_with_versioning function."""
        source_file = temp_dir / "source.txt"
        dest_file = temp_dir / "destination.txt"
        test_content = "Test file content"
        run_id = "test-run-001"
        
        source_file.write_text(test_content)
        
        result_path, was_versioned = copy_with_versioning(source_file, dest_file, run_id)
        
        assert dest_file.exists()
        assert dest_file.read_text() == test_content
        assert result_path == dest_file
        assert not was_versioned
        
        # Check that file sizes match
        source_info = get_file_info(source_file)
        dest_info = get_file_info(dest_file)
        assert source_info['size'] == dest_info['size']
    
    def test_copy_with_versioning_creates_directories(self, temp_dir):
        """Test that copy_with_versioning creates destination directories."""
        source_file = temp_dir / "source.txt"
        dest_file = temp_dir / "nested" / "dir" / "destination.txt"
        test_content = "Test file content"
        run_id = "test-run-002"
        
        source_file.write_text(test_content)
        
        result_path, was_versioned = copy_with_versioning(source_file, dest_file, run_id)
        
        assert dest_file.exists()
        assert dest_file.read_text() == test_content
        assert result_path == dest_file
        assert not was_versioned
    
    def test_find_files_by_pattern(self, temp_dir):
        """Test find_files_by_pattern function."""
        # Create test files
        (temp_dir / "file1.csv").write_text("csv content")
        (temp_dir / "file2.csv").write_text("CSV content")
        (temp_dir / "file3.txt").write_text("txt content")
        (temp_dir / "subdir").mkdir()
        (temp_dir / "subdir" / "file4.csv").write_text("nested csv")
        
        # Test CSV pattern (case-sensitive)
        csv_files = find_files_by_pattern(temp_dir, "*.csv")
        
        assert len(csv_files) >= 2  # Should find .csv files
        csv_names = [f.name for f in csv_files]
        assert "file1.csv" in csv_names
        assert "file2.csv" in csv_names
    
    def test_find_files_by_pattern_recursive(self, temp_dir):
        """Test find_files_by_pattern with recursive search."""
        # Create nested structure
        (temp_dir / "root.csv").write_text("root csv")
        nested_dir = temp_dir / "nested"
        nested_dir.mkdir()
        (nested_dir / "nested.csv").write_text("nested csv")
        
        # Test recursive search
        all_csv_files = find_files_by_pattern(temp_dir, "**/*.csv")
        
        assert len(all_csv_files) >= 2
        csv_names = [f.name for f in all_csv_files]
        assert "root.csv" in csv_names
        assert "nested.csv" in csv_names
    
    def test_find_files_by_pattern_empty_directory(self, temp_dir):
        """Test find_files_by_pattern with empty directory."""
        empty_dir = temp_dir / "empty"
        empty_dir.mkdir()
        
        files = find_files_by_pattern(empty_dir, "*.csv")
        
        assert len(files) == 0
        assert isinstance(files, list)