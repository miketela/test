"""Path utilities for AT12 transformation system.

This module provides centralized path management for the simplified
transforms/AT12 directory structure, eliminating year/month subdirectories.

New Structure:
- transforms/AT12/incidencias/  # All incidence CSVs
- transforms/AT12/procesados/   # Corrected CSVs and consolidated TXT
"""

from pathlib import Path
from typing import Optional
from dataclasses import dataclass

from .config import Config
from .naming import FilenameParser


@dataclass
class AT12Paths:
    """Centralized path management for AT12 transformations."""
    
    base_transforms_dir: Path
    incidencias_dir: Path
    procesados_dir: Path
    
    @classmethod
    def from_config(cls, config: Config) -> 'AT12Paths':
        """Create AT12Paths from configuration.
        
        Args:
            config: Configuration instance
            
        Returns:
            AT12Paths instance with resolved directories
        """
        base_transforms = Path(config.data_processed_dir) / "transforms" / "AT12"
        
        return cls(
            base_transforms_dir=base_transforms,
            incidencias_dir=base_transforms / "incidencias",
            procesados_dir=base_transforms / "procesados"
        )
    
    def ensure_directories(self) -> None:
        """Create all required directories if they don't exist."""
        self.incidencias_dir.mkdir(parents=True, exist_ok=True)
        self.procesados_dir.mkdir(parents=True, exist_ok=True)
    
    def get_incidencia_path(self, filename: str) -> Path:
        """Get full path for an incidence file.
        
        Args:
            filename: Original filename (e.g., 'BASE_AT12_20250131.csv')
            
        Returns:
            Full path in incidencias directory
        """
        # Convert to incidence format: EEOO_TABULAR_[SUBTYPE]_[YYYYMMDD].csv
        # Define expected subtypes for AT12
        expected_subtypes = ['BASE_AT12', 'TDC_AT12', 'SOBREGIRO_AT12', 'VALORES_AT12', 
                           'GARANTIA_AUTOS_AT12', 'POLIZA_HIPOTECAS_AT12', 'AFECTACIONES_AT12', 
                           'VALOR_MINIMO_AVALUO_AT12']
        parser = FilenameParser(expected_subtypes)
        parsed = parser.parse_filename(filename)
        
        if parsed and parsed.is_valid:
            incidence_filename = f"EEOO_TABULAR_{parsed.subtype}_{parsed.date_str}.csv"
        else:
            # Fallback to original filename if parsing fails
            incidence_filename = filename
            
        return self.incidencias_dir / incidence_filename
    
    def get_procesado_path(self, filename: str) -> Path:
        """Get full path for a processed file.
        
        Args:
            filename: Processed filename (e.g., 'AT12_BASE_20250131.csv')
            
        Returns:
            Full path in procesados directory
        """
        return self.procesados_dir / filename
    
    def get_consolidated_path(self, consolidated_filename: str) -> Path:
        """Get full path for consolidated TXT file.
        
        Args:
            consolidated_filename: Consolidated filename (e.g., 'AT12_Cobis_202501__run-202501.TXT')
            
        Returns:
            Full path in procesados directory
        """
        return self.procesados_dir / consolidated_filename
    
    def list_incidencias(self, pattern: str = "*.csv") -> list[Path]:
        """List all incidence files matching pattern.
        
        Args:
            pattern: File pattern to match (default: '*.csv')
            
        Returns:
            List of incidence file paths
        """
        if not self.incidencias_dir.exists():
            return []
        return list(self.incidencias_dir.glob(pattern))
    
    def list_procesados(self, pattern: str = "*") -> list[Path]:
        """List all processed files matching pattern.
        
        Args:
            pattern: File pattern to match (default: '*')
            
        Returns:
            List of processed file paths
        """
        if not self.procesados_dir.exists():
            return []
        return list(self.procesados_dir.glob(pattern))
    
    def clean_directories(self, keep_consolidated: bool = True) -> None:
        """Clean transformation directories.
        
        Args:
            keep_consolidated: Whether to preserve consolidated TXT files
        """
        # Clean incidencias
        for file_path in self.list_incidencias():
            file_path.unlink()
        
        # Clean procesados (optionally keeping consolidated files)
        for file_path in self.list_procesados():
            if keep_consolidated and file_path.suffix.upper() == '.TXT':
                continue
            file_path.unlink()


def get_at12_paths(config: Optional[Config] = None) -> AT12Paths:
    """Convenience function to get AT12Paths instance.
    
    Args:
        config: Optional configuration instance. If None, loads default config.
        
    Returns:
        AT12Paths instance
    """
    if config is None:
        config = Config()
    
    return AT12Paths.from_config(config)