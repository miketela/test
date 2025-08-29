#!/usr/bin/env python3
"""
Metrics calculation utilities for SBP Atoms Pipeline.
Computes comprehensive statistics for CSV files.
"""

import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict

from .io import StrictCSVReader, UniversalFileReader, infer_data_types
from typing import Union
from .fs import get_file_info


@dataclass
class ColumnMetrics:
    """Metrics for a single column."""
    name: str
    data_type: str
    null_count: int
    null_percentage: float
    unique_count: int
    top_values: List[tuple]  # (value, count) pairs
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    avg_length: Optional[float] = None
    min_value: Optional[str] = None
    max_value: Optional[str] = None
    mean_value: Optional[float] = None
    std_value: Optional[float] = None


@dataclass
class FileMetrics:
    """Comprehensive metrics for a CSV file."""
    file_path: str
    file_name: str
    file_size: int
    file_mtime: str
    file_sha256: str
    row_count: int
    column_count: int
    headers: List[str]
    column_metrics: List[ColumnMetrics]
    validation_warnings: List[str]
    validation_errors: List[str]
    processing_time: float


class MetricsCalculator:
    """Calculator for comprehensive file metrics."""
    
    def __init__(self, file_reader: Union[StrictCSVReader, UniversalFileReader], top_n_values: int = 10):
        """Initialize metrics calculator.
        
        Args:
            file_reader: File reader instance (CSV or Universal)
            top_n_values: Number of top values to track per column
        """
        self.file_reader = file_reader
        # Keep backward compatibility
        if hasattr(file_reader, 'csv_reader'):
            self.csv_reader = file_reader.csv_reader
        else:
            self.csv_reader = file_reader
        self.top_n_values = top_n_values
    
    def calculate_file_metrics(self, file_path: Path) -> FileMetrics:
        """Calculate comprehensive metrics for a CSV file.
        
        Args:
            file_path: Path to CSV file
        
        Returns:
            FileMetrics object with all calculated metrics
        """
        import time
        start_time = time.time()
        
        # Get file information
        file_info = get_file_info(file_path)
        
        # Validate file structure
        if hasattr(self.file_reader, 'validate_file'):
            validation_result = self.file_reader.validate_file(file_path)
        else:
            validation_result = self.csv_reader.validate_csv(file_path)
        
        if not validation_result.is_valid:
            # Return basic metrics if file is invalid
            return FileMetrics(
                file_path=str(file_path),
                file_name=file_path.name,
                file_size=file_info['size'],
                file_mtime=file_info['mtime'],
                file_sha256=file_info['sha256'],
                row_count=0,
                column_count=0,
                headers=[],
                column_metrics=[],
                validation_warnings=validation_result.warnings,
                validation_errors=validation_result.errors,
                processing_time=time.time() - start_time
            )
        
        # Read file data
        try:
            if hasattr(self.file_reader, 'read_file'):
                df = self.file_reader.read_file(file_path)
            else:
                df = self.csv_reader.read_csv(file_path)
        except Exception as e:
            validation_result.errors.append(f"Failed to read file: {e}")
            return FileMetrics(
                file_path=str(file_path),
                file_name=file_path.name,
                file_size=file_info['size'],
                file_mtime=file_info['mtime'],
                file_sha256=file_info['sha256'],
                row_count=0,
                column_count=0,
                headers=[],
                column_metrics=[],
                validation_warnings=validation_result.warnings,
                validation_errors=validation_result.errors,
                processing_time=time.time() - start_time
            )
        
        # Calculate column metrics
        column_metrics = []
        data_types = infer_data_types(df)
        
        for col in df.columns:
            col_metrics = self._calculate_column_metrics(df[col], col, data_types.get(col, 'string'))
            column_metrics.append(col_metrics)
        
        return FileMetrics(
            file_path=str(file_path),
            file_name=file_path.name,
            file_size=file_info['size'],
            file_mtime=file_info['mtime'],
            file_sha256=file_info['sha256'],
            row_count=len(df),
            column_count=len(df.columns),
            headers=list(df.columns),
            column_metrics=column_metrics,
            validation_warnings=validation_result.warnings,
            validation_errors=validation_result.errors,
            processing_time=time.time() - start_time
        )
    
    def _calculate_column_metrics(self, series: pd.Series, col_name: str, data_type: str) -> ColumnMetrics:
        """Calculate metrics for a single column.
        
        Args:
            series: Pandas series for the column
            col_name: Column name
            data_type: Inferred data type
        
        Returns:
            ColumnMetrics object
        """
        # Basic counts
        total_count = len(series)
        null_count = series.isna().sum() + (series == '').sum()  # Count both NaN and empty strings
        null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
        unique_count = series.nunique()
        
        # Top values
        value_counts = series.value_counts().head(self.top_n_values)
        top_values = [(str(val), count) for val, count in value_counts.items()]
        
        # String length metrics
        string_lengths = series.astype(str).str.len()
        min_length = int(string_lengths.min()) if not string_lengths.empty else None
        max_length = int(string_lengths.max()) if not string_lengths.empty else None
        avg_length = float(string_lengths.mean()) if not string_lengths.empty else None
        
        # Value range metrics (for numeric/date columns)
        min_value = None
        max_value = None
        mean_value = None
        std_value = None
        
        if data_type in ['integer', 'numeric']:
            try:
                numeric_series = pd.to_numeric(series, errors='coerce')
                if not numeric_series.isna().all():
                    min_value = str(numeric_series.min())
                    max_value = str(numeric_series.max())
                    mean_value = float(numeric_series.mean())
                    std_value = float(numeric_series.std())
            except:
                pass
        elif data_type == 'datetime':
            try:
                date_series = pd.to_datetime(series, errors='coerce')
                if not date_series.isna().all():
                    min_value = str(date_series.min())
                    max_value = str(date_series.max())
            except:
                pass
        else:
            # For string columns, min/max are lexicographic
            try:
                non_null_series = series.dropna()
                if not non_null_series.empty:
                    min_value = str(non_null_series.min())
                    max_value = str(non_null_series.max())
            except:
                pass
        
        return ColumnMetrics(
            name=col_name,
            data_type=data_type,
            null_count=int(null_count),
            null_percentage=round(null_percentage, 2),
            unique_count=int(unique_count),
            top_values=top_values,
            min_length=min_length,
            max_length=max_length,
            avg_length=round(avg_length, 2) if avg_length is not None else None,
            min_value=min_value,
            max_value=max_value,
            mean_value=round(mean_value, 4) if mean_value is not None else None,
            std_value=round(std_value, 4) if std_value is not None else None
        )
    
    def export_metrics_to_dict(self, metrics: FileMetrics) -> Dict[str, Any]:
        """Export metrics to dictionary format.
        
        Args:
            metrics: FileMetrics object
        
        Returns:
            Dictionary representation of metrics
        """
        return asdict(metrics)
    
    def export_metrics_to_csv(self, metrics_list: List[FileMetrics], output_path: Path):
        """Export multiple file metrics to CSV format.
        
        Args:
            metrics_list: List of FileMetrics objects
            output_path: Output CSV file path
        """
        # Create summary DataFrame
        summary_data = []
        
        for metrics in metrics_list:
            summary_data.append({
                'file_name': metrics.file_name,
                'file_size': metrics.file_size,
                'row_count': metrics.row_count,
                'column_count': metrics.column_count,
                'validation_warnings': len(metrics.validation_warnings),
                'validation_errors': len(metrics.validation_errors),
                'processing_time': metrics.processing_time,
                'file_sha256': metrics.file_sha256
            })
        
        df = pd.DataFrame(summary_data)
        df.to_csv(output_path, index=False)