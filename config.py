#!/usr/bin/env python3
"""
Configuration management for SBP Atoms Pipeline.
Handles environment variables, CLI arguments, and default settings.
"""

import os
from pathlib import Path
from typing import Optional, Any


class Config:
    """Configuration class for the SBP Atoms pipeline."""
    
    def __init__(self):
        """Initialize configuration with defaults and environment variables."""
        # Base paths
        self.base_dir = Path.cwd()
        self.source_dir = Path(os.getenv("SOURCE_DIR", self.base_dir / "source"))
        self.raw_dir = Path(os.getenv("RAW_DIR", self.base_dir / "data" / "raw"))
        self.reports_dir = Path(os.getenv("REPORTS_DIR", self.base_dir / "reports"))
        self.metrics_dir = Path(os.getenv("METRICS_DIR", self.base_dir / "metrics"))
        self.logs_dir = Path(os.getenv("LOGS_DIR", self.base_dir / "logs"))
        self.schemas_dir = Path(os.getenv("SCHEMAS_DIR", self.base_dir / "schemas"))
        
        # Processing parameters
        self.workers = int(os.getenv("PIPELINE_WORKERS", "1"))
        self.strict_period = os.getenv("STRICT_PERIOD", "true").lower() == "true"
        self.date_fmt = os.getenv("DATE_FMT", "%Y%m%d")
        self.chunk_rows = int(os.getenv("CHUNK_ROWS", "1000000"))
        
        # Input file parameters
        self.input_delimiter = os.getenv("INPUT_DELIMITER", ",")
        self.csv_delimiter = os.getenv("CSV_DELIMITER", ",")  # Backward compatibility
        self.csv_encoding = os.getenv("CSV_ENCODING", "utf-8")
        self.csv_quotechar = os.getenv("CSV_QUOTECHAR", '"')
        
        # XLSX parameters
        self.xlsx_sheet_name = os.getenv("XLSX_SHEET_NAME", "0")  # Default to first sheet
        # Convert to int if it's a number, otherwise keep as string
        try:
            self.xlsx_sheet_name = int(self.xlsx_sheet_name)
        except ValueError:
            pass  # Keep as string for sheet name
        
        # Output parameters
        self.output_delimiter = os.getenv("OUTPUT_DELIMITER", "|")
        self.trailing_delimiter = os.getenv("TRAILING_DELIMITER", "false").lower() == "true"

        # Sequences and IDs
        # Starting point for TDC Numero_Garantia sequence (overridable per env)
        try:
            self.tdc_sequence_start = int(os.getenv("TDC_SEQUENCE_START", "1"))
        except ValueError:
            self.tdc_sequence_start = 1
        try:
            self.valores_sequence_start = int(os.getenv("VALORES_SEQUENCE_START", "1"))
        except ValueError:
            self.valores_sequence_start = 1
        
        # Logging
        self.log_level = os.getenv("LOG_LEVEL", "INFO")
        self.verbose = False
        
        # Ensure directories exist
        self._create_directories()
    
    def _create_directories(self):
        """Create necessary directories if they don't exist."""
        for directory in [self.source_dir, self.raw_dir, self.reports_dir, 
                         self.metrics_dir, self.logs_dir]:
            directory.mkdir(parents=True, exist_ok=True)
    
    def update_from_args(self, args):
        """Update configuration from command line arguments."""
        if hasattr(args, 'workers') and args.workers:
            self.workers = args.workers
        
        if hasattr(args, 'verbose') and args.verbose:
            self.verbose = True
            self.log_level = "DEBUG"
        
        if hasattr(args, 'strict_period') and args.strict_period is not None:
            self.strict_period = args.strict_period
    
    def get_atom_source_dir(self, atom: str, year: int, month: int) -> Path:
        """Get source directory for a specific atom and period."""
        return self.source_dir / f"{year:04d}" / f"{month:02d}" / atom
    
    def get_atom_raw_dir(self, atom: str, year: int, month: int) -> Path:
        """Get raw data directory for a specific atom and period."""
        return self.raw_dir / f"{year:04d}" / f"{month:02d}" / atom
    
    def get_atom_metrics_dir(self, atom: str, year: int, month: int) -> Path:
        """Get metrics directory for a specific atom and period."""
        return self.metrics_dir / f"{year:04d}" / f"{month:02d}" / atom
    
    def get_atom_reports_dir(self, atom: str, year: int, month: int) -> Path:
        """Get reports directory for a specific atom and period."""
        return self.reports_dir / f"{year:04d}" / f"{month:02d}" / atom
    
    def get_atom_logs_dir(self, atom: str, year: int, month: int) -> Path:
        """Get logs directory for a specific atom and period."""
        return self.logs_dir / f"{year:04d}" / f"{month:02d}" / atom
    
    def get_schema_path(self, atom: str, schema_type: str) -> Path:
        """Get path to schema file for a specific atom."""
        return self.schemas_dir / atom / f"{schema_type}.json"
