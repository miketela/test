"""Unit tests for paths module."""

import pytest
from pathlib import Path
from unittest.mock import Mock, patch

from src.core.paths import AT12Paths


class TestAT12Paths:
    """Test cases for AT12Paths class."""
    
    def test_init(self, temp_dir):
        """Test AT12Paths initialization."""
        base_transforms = temp_dir / "transforms" / "AT12"
        incidencias_dir = base_transforms / "incidencias"
        procesados_dir = base_transforms / "procesados"
        
        paths = AT12Paths(
            base_transforms_dir=base_transforms,
            incidencias_dir=incidencias_dir,
            procesados_dir=procesados_dir
        )
        
        assert paths.base_transforms_dir == base_transforms
        assert paths.incidencias_dir == incidencias_dir
        assert paths.procesados_dir == procesados_dir
    
    def test_ensure_directories(self, temp_dir):
        """Test directory creation."""
        base_transforms = temp_dir / "transforms" / "AT12"
        incidencias_dir = base_transforms / "incidencias"
        procesados_dir = base_transforms / "procesados"
        
        paths = AT12Paths(
            base_transforms_dir=base_transforms,
            incidencias_dir=incidencias_dir,
            procesados_dir=procesados_dir
        )
        
        # Directories should not exist initially
        assert not paths.incidencias_dir.exists()
        assert not paths.procesados_dir.exists()
        
        # Ensure directories
        paths.ensure_directories()
        
        # Directories should now exist
        assert paths.incidencias_dir.exists()
        assert paths.procesados_dir.exists()
        assert paths.incidencias_dir.is_dir()
        assert paths.procesados_dir.is_dir()
    
    def test_get_incidence_path(self, temp_dir):
        """Test incidence file path generation."""
        base_transforms = temp_dir / "transforms" / "AT12"
        incidencias_dir = base_transforms / "incidencias"
        procesados_dir = base_transforms / "procesados"
        
        paths = AT12Paths(
            base_transforms_dir=base_transforms,
            incidencias_dir=incidencias_dir,
            procesados_dir=procesados_dir
        )
        
        # Test with standard filename
        filename = "BASE_AT12_20250131.csv"
        incidence_path = paths.get_incidencia_path(filename)
        
        expected_path = paths.incidencias_dir / "EEOO_TABULAR_BASE_AT12_20250131.csv"
        assert incidence_path == expected_path
    
    def test_get_processed_path(self, temp_dir):
        """Test processed file path generation."""
        base_transforms = temp_dir / "transforms" / "AT12"
        incidencias_dir = base_transforms / "incidencias"
        procesados_dir = base_transforms / "procesados"
        
        paths = AT12Paths(
            base_transforms_dir=base_transforms,
            incidencias_dir=incidencias_dir,
            procesados_dir=procesados_dir
        )
        
        filename = "AT12_BASE_20250131.xlsx"
        processed_path = paths.get_procesado_path(filename)

        expected_path = paths.procesados_dir / filename
        assert processed_path == expected_path
    
    def test_get_consolidated_path(self, temp_dir):
        """Test consolidated file path generation."""
        base_transforms = temp_dir / "transforms" / "AT12"
        incidencias_dir = base_transforms / "incidencias"
        procesados_dir = base_transforms / "procesados"
        
        paths = AT12Paths(
            base_transforms_dir=base_transforms,
            incidencias_dir=incidencias_dir,
            procesados_dir=procesados_dir
        )
        
        filename = "AT12_Cobis_202501__run-202501.TXT"
        consolidated_path = paths.get_consolidated_path(filename)
        
        expected_path = paths.procesados_dir / filename
        assert consolidated_path == expected_path
    
    def test_list_incidence_files(self, temp_dir):
        """Test listing incidence files."""
        base_transforms = temp_dir / "transforms" / "AT12"
        incidencias_dir = base_transforms / "incidencias"
        procesados_dir = base_transforms / "procesados"
        
        paths = AT12Paths(
            base_transforms_dir=base_transforms,
            incidencias_dir=incidencias_dir,
            procesados_dir=procesados_dir
        )
        paths.ensure_directories()
        
        # Create some test files
        (paths.incidencias_dir / "EEOO_TABULAR_AT12_BASE_20240131.csv").touch()
        (paths.incidencias_dir / "EEOO_TABULAR_AT12_TDC_20240131.csv").touch()
        (paths.incidencias_dir / "other_file.txt").touch()
        
        # List files
        files = paths.list_incidencias()
        
        # Should only return CSV files
        assert len(files) == 2
        file_names = [f.name for f in files]
        assert "EEOO_TABULAR_AT12_BASE_20240131.csv" in file_names
        assert "EEOO_TABULAR_AT12_TDC_20240131.csv" in file_names
        assert "other_file.txt" not in file_names
    
    def test_list_processed_files(self, temp_dir):
        """Test listing processed files."""
        base_transforms = temp_dir / "transforms" / "AT12"
        incidencias_dir = base_transforms / "incidencias"
        procesados_dir = base_transforms / "procesados"
        
        paths = AT12Paths(
            base_transforms_dir=base_transforms,
            incidencias_dir=incidencias_dir,
            procesados_dir=procesados_dir
        )
        paths.ensure_directories()
        
        # Create some test files
        (paths.procesados_dir / "AT12_BASE_20240131.xlsx").touch()
        (paths.procesados_dir / "AT12_TDC_20240131.xlsx").touch()
        (paths.procesados_dir / "AT12_Cobis_202401__run-202401.TXT").touch()
        (paths.procesados_dir / "other_file.txt").touch()

        # List Excel files
        excel_files = paths.list_procesados("*.xlsx")
        assert len(excel_files) == 2

        # List TXT files
        txt_files = paths.list_procesados("*.TXT")
        assert len(txt_files) == 1
        
        # List all files
        all_files = paths.list_procesados()
        assert len(all_files) == 4
    
    def test_clean_incidencias(self, temp_dir):
        """Test cleaning incidencias directory."""
        base_transforms = temp_dir / "transforms" / "AT12"
        incidencias_dir = base_transforms / "incidencias"
        procesados_dir = base_transforms / "procesados"
        
        paths = AT12Paths(
            base_transforms_dir=base_transforms,
            incidencias_dir=incidencias_dir,
            procesados_dir=procesados_dir
        )
        paths.ensure_directories()
        
        # Create some test files
        file1 = paths.incidencias_dir / "file1.csv"
        file2 = paths.incidencias_dir / "file2.csv"
        file1.touch()
        file2.touch()
        
        # Verify files exist
        assert file1.exists()
        assert file2.exists()
        
        # Clean directory
        paths.clean_directories(keep_consolidated=False)
        
        # Verify files are removed
        assert not file1.exists()
        assert not file2.exists()
        assert paths.incidencias_dir.exists()  # Directory should still exist
    
    def test_clean_procesados(self, temp_dir):
        """Test cleaning procesados directory."""
        base_transforms = temp_dir / "transforms" / "AT12"
        incidencias_dir = base_transforms / "incidencias"
        procesados_dir = base_transforms / "procesados"
        
        paths = AT12Paths(
            base_transforms_dir=base_transforms,
            incidencias_dir=incidencias_dir,
            procesados_dir=procesados_dir
        )
        paths.ensure_directories()
        
        # Create some test files
        file1 = paths.procesados_dir / "file1.xlsx"
        file2 = paths.procesados_dir / "file2.TXT"
        file1.touch()
        file2.touch()
        
        # Verify files exist
        assert file1.exists()
        assert file2.exists()
        
        # Clean directory (without keeping consolidated files)
        paths.clean_directories(keep_consolidated=False)
        
        # Verify files are removed
        assert not file1.exists()
        assert not file2.exists()
        assert paths.procesados_dir.exists()  # Directory should still exist
