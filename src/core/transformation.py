"""Base transformation engine for regulatory data processing.

This module provides the foundation for pandas-based data transformations,
with a focus on maintainability, testability, and separation of concerns.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime
import pandas as pd
import logging

from .config import Config
from .paths import AT12Paths
from .io import UniversalFileReader
from .naming import FilenameParser


@dataclass
class TransformationResult:
    """Result of a transformation operation."""
    
    success: bool
    processed_files: List[Path]
    incidence_files: List[Path]
    consolidated_file: Optional[Path]
    metrics: Dict[str, Any]
    errors: List[str]
    warnings: List[str]
    
    @property
    def total_files_processed(self) -> int:
        """Total number of files processed."""
        return len(self.processed_files)
    
    @property
    def has_incidences(self) -> bool:
        """Whether any incidences were generated."""
        return len(self.incidence_files) > 0


@dataclass
class TransformationContext:
    """Context information for transformation operations."""
    
    run_id: str
    period: str  # YYYYMMDD format
    config: Config
    paths: AT12Paths
    source_files: List[Path]
    logger: logging.Logger
    
    @property
    def period_year_month(self) -> str:
        """Extract YYYYMM from period (YYYYMMDD)."""
        return self.period[:6] if len(self.period) >= 6 else self.period
    
    @property
    def year(self) -> str:
        """Extract year from period (YYYYMMDD)."""
        return self.period[:4] if len(self.period) >= 4 else self.period
    
    @property
    def month(self) -> str:
        """Extract month from period (YYYYMMDD)."""
        return self.period[4:6] if len(self.period) >= 6 else "01"


class TransformationEngine(ABC):
    """Abstract base class for data transformation engines.
    
    This class defines the common interface and workflow for all
    transformation engines, emphasizing pandas-based processing
    and clear separation between business logic and I/O operations.
    """
    
    def __init__(self, config: Config):
        """Initialize transformation engine.
        
        Args:
            config: Configuration instance
        """
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self._file_reader = UniversalFileReader()
        self._filename_parser = FilenameParser([])  # Default empty list, subclasses should override
    
    def transform(self, context: TransformationContext) -> TransformationResult:
        """Execute the complete transformation pipeline.
        
        Args:
            context: Transformation context with all necessary information
            
        Returns:
            TransformationResult with operation details
        """
        self.logger.info(f"Starting transformation for period {context.period}")
        
        result = TransformationResult(
            success=False,
            processed_files=[],
            incidence_files=[],
            consolidated_file=None,
            metrics={},
            errors=[],
            warnings=[]
        )
        
        try:
            # Ensure output directories exist
            context.paths.ensure_directories()
            
            # Load and validate source data
            source_data = self._load_source_data(context, result)
            if not source_data:
                result.errors.append("No valid source data found")
                return result
            
            # Apply transformations
            transformed_data = self._apply_transformations(context, source_data, result)
            
            # Generate outputs
            self._generate_outputs(context, transformed_data, result)
            
            # Calculate final metrics
            result.metrics.update(self._calculate_metrics(context, transformed_data, result))
            
            result.success = len(result.errors) == 0
            self.logger.info(f"Transformation completed. Success: {result.success}")
            
        except Exception as e:
            self.logger.error(f"Transformation failed: {str(e)}", exc_info=True)
            result.errors.append(f"Transformation failed: {str(e)}")
            result.success = False
        
        return result
    
    def _load_source_data(self, context: TransformationContext, result: TransformationResult) -> Dict[str, pd.DataFrame]:
        """Load source data files into pandas DataFrames.
        
        Args:
            context: Transformation context
            result: Result object to update with loading information
            
        Returns:
            Dictionary mapping subtype to DataFrame
        """
        source_data = {}
        
        for file_path in context.source_files:
            try:
                # Parse filename to extract subtype and date
                parsed = self._filename_parser.parse_filename(file_path.name)
                if not parsed or not parsed.is_valid:
                    result.warnings.append(f"Could not parse filename: {file_path.name}")
                    continue
                
                subtype = parsed.subtype
                file_date = parsed.date_str
                
                # Validate date matches expected period
                if file_date != context.period:
                    result.warnings.append(
                        f"File date {file_date} does not match expected period {context.period} for {file_path.name}"
                    )
                    continue
                
                # Load data with pandas
                df = self._load_dataframe(file_path)
                if df is not None and not df.empty:
                    source_data[subtype] = df
                    self.logger.info(f"Loaded {len(df)} records from {file_path.name}")
                else:
                    result.warnings.append(f"Empty or invalid data in {file_path.name}")
                    
            except Exception as e:
                error_msg = f"Failed to load {file_path.name}: {str(e)}"
                result.errors.append(error_msg)
                self.logger.error(error_msg)
        
        return source_data
    
    def _load_dataframe(self, file_path: Path) -> Optional[pd.DataFrame]:
        """Load a single file into a pandas DataFrame.
        
        Args:
            file_path: Path to the file to load
            
        Returns:
            DataFrame or None if loading fails
        """
        try:
            # Use UniversalFileReader for consistent loading
            df = self._file_reader.read_file(file_path)
            
            if df is not None and not df.empty:
                # Ensure consistent date parsing for any date columns
                # This is a placeholder - specific date column handling should be in subclasses
                return df
            
        except Exception as e:
            self.logger.error(f"Error loading {file_path}: {str(e)}")
        
        return None
    
    @abstractmethod
    def _apply_transformations(self, context: TransformationContext, 
                             source_data: Dict[str, pd.DataFrame], 
                             result: TransformationResult) -> Dict[str, pd.DataFrame]:
        """Apply business-specific transformations to the data.
        
        Args:
            context: Transformation context
            source_data: Dictionary mapping subtype to source DataFrame
            result: Result object to update with transformation information
            
        Returns:
            Dictionary mapping subtype to transformed DataFrame
        """
        pass
    
    @abstractmethod
    def _generate_outputs(self, context: TransformationContext, 
                         transformed_data: Dict[str, pd.DataFrame], 
                         result: TransformationResult) -> None:
        """Generate output files from transformed data.
        
        Args:
            context: Transformation context
            transformed_data: Dictionary mapping subtype to transformed DataFrame
            result: Result object to update with output information
        """
        pass
    
    def _calculate_metrics(self, context: TransformationContext, 
                          transformed_data: Dict[str, pd.DataFrame], 
                          result: TransformationResult) -> Dict[str, Any]:
        """Calculate transformation metrics.
        
        Args:
            context: Transformation context
            transformed_data: Dictionary mapping subtype to transformed DataFrame
            result: Result object with current state
            
        Returns:
            Dictionary with calculated metrics
        """
        metrics = {
            'transformation_timestamp': datetime.now().isoformat(),
            'period': context.period,
            'run_id': context.run_id,
            'subtypes_processed': list(transformed_data.keys()),
            'total_records_by_subtype': {
                subtype: len(df) for subtype, df in transformed_data.items()
            },
            'total_records': sum(len(df) for df in transformed_data.values()),
            'files_generated': {
                'processed': len(result.processed_files),
                'incidences': len(result.incidence_files),
                'consolidated': 1 if result.consolidated_file else 0
            }
        }
        
        return metrics
    
    def _save_dataframe_as_csv(self, df: pd.DataFrame, file_path: Path, 
                              encoding: str = 'utf-8') -> bool:
        """Save DataFrame as CSV with consistent formatting.

        Args:
            df: DataFrame to save
            file_path: Output file path
            encoding: File encoding (default: utf-8)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure parent directory exists
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save with consistent CSV parameters
            df.to_csv(
                file_path,
                index=False,
                encoding=encoding,
                sep=self.config.get('csv_separator', '|'),
                quoting=1,  # QUOTE_ALL
                date_format='%Y%m%d'  # Ensure YYYYMMDD format for dates
            )
            
            self.logger.info(f"Saved {len(df)} records to {file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save DataFrame to {file_path}: {str(e)}")
            return False

    def _save_dataframe_as_excel(self, df: pd.DataFrame, file_path: Path, *, sheet_name: str = 'Sheet1') -> bool:
        """Save DataFrame as Excel with consistent formatting."""
        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)

            with pd.ExcelWriter(file_path, engine=self.config.get('excel_engine', 'openpyxl')) as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=False)

            self.logger.info(f"Saved {len(df)} records to {file_path}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to save DataFrame to {file_path}: {str(e)}")
            return False
