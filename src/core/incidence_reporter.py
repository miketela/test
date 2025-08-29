"""Incidence reporting system for transformation processes.

This module provides a standardized way to collect, format, and report
incidences (data quality issues, validation failures, etc.) during
transformation processes using pandas for consistent CSV output.
"""

from typing import Dict, List, Optional, Any, Union
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import pandas as pd
import logging

from .config import Config
from .paths import AT12Paths


class IncidenceType(Enum):
    """Types of incidences that can be reported."""
    
    DATA_QUALITY = "DATA_QUALITY"
    VALIDATION_FAILURE = "VALIDATION_FAILURE"
    BUSINESS_RULE_VIOLATION = "BUSINESS_RULE_VIOLATION"
    TRANSFORMATION_ERROR = "TRANSFORMATION_ERROR"
    HEADER_MISMATCH = "HEADER_MISMATCH"
    TYPE_CONVERSION_ERROR = "TYPE_CONVERSION_ERROR"
    DUPLICATE_RECORD = "DUPLICATE_RECORD"
    MISSING_REQUIRED_FIELD = "MISSING_REQUIRED_FIELD"
    INVALID_FORMAT = "INVALID_FORMAT"
    THRESHOLD_VIOLATION = "THRESHOLD_VIOLATION"


class IncidenceSeverity(Enum):
    """Severity levels for incidences."""
    
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


@dataclass
class Incidence:
    """Individual incidence record."""
    
    # Core identification
    incidence_id: str
    timestamp: str
    period: str  # YYYYMMDD format
    run_id: str
    
    # Source information
    subtype: str
    source_file: Optional[str] = None
    record_index: Optional[int] = None
    
    # Incidence details
    incidence_type: IncidenceType = IncidenceType.DATA_QUALITY
    severity: IncidenceSeverity = IncidenceSeverity.MEDIUM
    rule_name: Optional[str] = None
    column_name: Optional[str] = None
    
    # Values and context
    original_value: Optional[str] = None
    expected_value: Optional[str] = None
    corrected_value: Optional[str] = None
    
    # Description and resolution
    description: str = ""
    resolution_action: Optional[str] = None
    
    # Additional metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert incidence to dictionary for DataFrame creation.
        
        Returns:
            Dictionary representation of the incidence
        """
        return {
            'incidence_id': self.incidence_id,
            'timestamp': self.timestamp,
            'period': self.period,
            'run_id': self.run_id,
            'subtype': self.subtype,
            'source_file': self.source_file,
            'record_index': self.record_index,
            'incidence_type': self.incidence_type.value,
            'severity': self.severity.value,
            'rule_name': self.rule_name,
            'column_name': self.column_name,
            'original_value': self.original_value,
            'expected_value': self.expected_value,
            'corrected_value': self.corrected_value,
            'description': self.description,
            'resolution_action': self.resolution_action,
            'metadata': str(self.metadata) if self.metadata else None
        }


class IncidenceReporter:
    """Centralized incidence reporting system.
    
    Collects, manages, and exports incidences in standardized CSV format
    using pandas for consistent data handling.
    """
    
    def __init__(self, config: Config, run_id: str, period: str):
        """Initialize incidence reporter.
        
        Args:
            config: Configuration instance
            run_id: Current run identifier
            period: Processing period in YYYYMMDD format
        """
        self.config = config
        self.run_id = run_id
        self.period = period
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Storage for incidences by subtype
        self.incidences: Dict[str, List[Incidence]] = {}
        self._incidence_counter = 0
    
    def add_incidence(self, subtype: str, incidence_type: IncidenceType, 
                     description: str, severity: IncidenceSeverity = IncidenceSeverity.MEDIUM,
                     **kwargs) -> str:
        """Add a new incidence.
        
        Args:
            subtype: Data subtype (e.g., 'BASE', 'TDC', etc.)
            incidence_type: Type of incidence
            description: Human-readable description
            severity: Severity level
            **kwargs: Additional incidence fields
            
        Returns:
            Generated incidence ID
        """
        self._incidence_counter += 1
        incidence_id = f"{self.run_id}_{self.period}_{subtype}_{self._incidence_counter:06d}"
        
        incidence = Incidence(
            incidence_id=incidence_id,
            timestamp=datetime.now().isoformat(),
            period=self.period,
            run_id=self.run_id,
            subtype=subtype,
            incidence_type=incidence_type,
            severity=severity,
            description=description,
            **kwargs
        )
        
        if subtype not in self.incidences:
            self.incidences[subtype] = []
        
        self.incidences[subtype].append(incidence)
        
        self.logger.debug(f"Added incidence {incidence_id}: {description}")
        return incidence_id
    
    def add_validation_failure(self, subtype: str, rule_name: str, 
                             record_index: Optional[int] = None,
                             column_name: Optional[str] = None,
                             original_value: Optional[str] = None,
                             expected_value: Optional[str] = None,
                             description: Optional[str] = None) -> str:
        """Add a validation failure incidence.
        
        Args:
            subtype: Data subtype
            rule_name: Name of the validation rule that failed
            record_index: Index of the failing record
            column_name: Name of the column with the issue
            original_value: Original value that failed validation
            expected_value: Expected value or format
            description: Optional custom description
            
        Returns:
            Generated incidence ID
        """
        if description is None:
            description = f"Validation rule '{rule_name}' failed"
            if column_name:
                description += f" for column '{column_name}'"
            if original_value is not None:
                description += f" with value '{original_value}'"
        
        return self.add_incidence(
            subtype=subtype,
            incidence_type=IncidenceType.VALIDATION_FAILURE,
            description=description,
            severity=IncidenceSeverity.HIGH,
            rule_name=rule_name,
            record_index=record_index,
            column_name=column_name,
            original_value=str(original_value) if original_value is not None else None,
            expected_value=str(expected_value) if expected_value is not None else None
        )
    
    def add_data_quality_issue(self, subtype: str, issue_type: str,
                              record_index: Optional[int] = None,
                              column_name: Optional[str] = None,
                              original_value: Optional[str] = None,
                              corrected_value: Optional[str] = None,
                              description: Optional[str] = None) -> str:
        """Add a data quality issue incidence.
        
        Args:
            subtype: Data subtype
            issue_type: Type of data quality issue
            record_index: Index of the problematic record
            column_name: Name of the column with the issue
            original_value: Original problematic value
            corrected_value: Value after correction (if applicable)
            description: Optional custom description
            
        Returns:
            Generated incidence ID
        """
        if description is None:
            description = f"Data quality issue: {issue_type}"
            if column_name:
                description += f" in column '{column_name}'"
        
        return self.add_incidence(
            subtype=subtype,
            incidence_type=IncidenceType.DATA_QUALITY,
            description=description,
            severity=IncidenceSeverity.MEDIUM,
            rule_name=issue_type,
            record_index=record_index,
            column_name=column_name,
            original_value=str(original_value) if original_value is not None else None,
            corrected_value=str(corrected_value) if corrected_value is not None else None,
            resolution_action="CORRECTED" if corrected_value is not None else "FLAGGED"
        )
    
    def add_business_rule_violation(self, subtype: str, rule_name: str,
                                   record_index: Optional[int] = None,
                                   column_name: Optional[str] = None,
                                   original_value: Optional[str] = None,
                                   threshold: Optional[Union[str, float, int]] = None,
                                   description: Optional[str] = None) -> str:
        """Add a business rule violation incidence.
        
        Args:
            subtype: Data subtype
            rule_name: Name of the business rule that was violated
            record_index: Index of the violating record
            column_name: Name of the column involved
            original_value: Value that violated the rule
            threshold: Threshold or limit that was violated
            description: Optional custom description
            
        Returns:
            Generated incidence ID
        """
        if description is None:
            description = f"Business rule violation: {rule_name}"
            if threshold is not None:
                description += f" (threshold: {threshold})"
        
        metadata = {}
        if threshold is not None:
            metadata['threshold'] = threshold
        
        return self.add_incidence(
            subtype=subtype,
            incidence_type=IncidenceType.BUSINESS_RULE_VIOLATION,
            description=description,
            severity=IncidenceSeverity.HIGH,
            rule_name=rule_name,
            record_index=record_index,
            column_name=column_name,
            original_value=str(original_value) if original_value is not None else None,
            metadata=metadata
        )
    
    def get_incidences_by_subtype(self, subtype: str) -> List[Incidence]:
        """Get all incidences for a specific subtype.
        
        Args:
            subtype: Data subtype
            
        Returns:
            List of incidences for the subtype
        """
        return self.incidences.get(subtype, [])
    
    def get_all_incidences(self) -> List[Incidence]:
        """Get all incidences across all subtypes.
        
        Returns:
            List of all incidences
        """
        all_incidences = []
        for incidences_list in self.incidences.values():
            all_incidences.extend(incidences_list)
        return all_incidences
    
    def get_incidence_summary(self) -> Dict[str, Any]:
        """Get summary statistics of incidences.
        
        Returns:
            Dictionary with incidence statistics
        """
        all_incidences = self.get_all_incidences()
        
        summary = {
            'total_incidences': len(all_incidences),
            'by_subtype': {subtype: len(incidences) for subtype, incidences in self.incidences.items()},
            'by_type': {},
            'by_severity': {},
            'period': self.period,
            'run_id': self.run_id
        }
        
        # Count by type
        for incidence in all_incidences:
            inc_type = incidence.incidence_type.value
            summary['by_type'][inc_type] = summary['by_type'].get(inc_type, 0) + 1
        
        # Count by severity
        for incidence in all_incidences:
            severity = incidence.severity.value
            summary['by_severity'][severity] = summary['by_severity'].get(severity, 0) + 1
        
        return summary
    
    def export_incidences_to_csv(self, paths: AT12Paths) -> List[Path]:
        """Export incidences to CSV files using pandas.
        
        Args:
            paths: AT12Paths instance for output directory management
            
        Returns:
            List of generated incidence file paths
        """
        exported_files = []
        
        for subtype, incidences in self.incidences.items():
            if not incidences:
                continue
            
            try:
                # Convert incidences to DataFrame
                incidence_data = [inc.to_dict() for inc in incidences]
                df = pd.DataFrame(incidence_data)
                
                # Generate filename following the standard pattern
                filename = f"EEOO_TABULAR_{subtype}_AT12_{self.period}.csv"
                file_path = paths.get_incidencia_path(filename)
                
                # Ensure directory exists
                file_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Export to CSV with consistent formatting
                df.to_csv(
                    file_path,
                    index=False,
                    encoding='utf-8',
                    sep=self.config.csv_delimiter,
                    quoting=1,  # QUOTE_ALL
                    date_format='%Y%m%d'
                )
                
                exported_files.append(file_path)
                self.logger.info(f"Exported {len(incidences)} incidences to {file_path}")
                
            except Exception as e:
                self.logger.error(f"Failed to export incidences for {subtype}: {str(e)}")
        
        return exported_files
    
    def export_summary_to_csv(self, paths: AT12Paths) -> Optional[Path]:
        """Export incidence summary to CSV file.
        
        Args:
            paths: AT12Paths instance for output directory management
            
        Returns:
            Path to generated summary file or None if export fails
        """
        try:
            summary = self.get_incidence_summary()
            
            # Create summary DataFrame
            summary_data = []
            
            # Overall summary
            summary_data.append({
                'metric': 'total_incidences',
                'category': 'OVERALL',
                'value': summary['total_incidences'],
                'period': self.period,
                'run_id': self.run_id
            })
            
            # By subtype
            for subtype, count in summary['by_subtype'].items():
                summary_data.append({
                    'metric': 'incidences_by_subtype',
                    'category': subtype,
                    'value': count,
                    'period': self.period,
                    'run_id': self.run_id
                })
            
            # By type
            for inc_type, count in summary['by_type'].items():
                summary_data.append({
                    'metric': 'incidences_by_type',
                    'category': inc_type,
                    'value': count,
                    'period': self.period,
                    'run_id': self.run_id
                })
            
            # By severity
            for severity, count in summary['by_severity'].items():
                summary_data.append({
                    'metric': 'incidences_by_severity',
                    'category': severity,
                    'value': count,
                    'period': self.period,
                    'run_id': self.run_id
                })
            
            df = pd.DataFrame(summary_data)
            
            # Generate summary filename
            filename = f"INCIDENCES_SUMMARY_AT12_{self.period}.csv"
            file_path = paths.get_incidencia_path(filename)
            
            # Export to CSV
            df.to_csv(
                file_path,
                index=False,
                encoding='utf-8',
                sep=self.config.csv_delimiter,
                quoting=1
            )
            
            self.logger.info(f"Exported incidence summary to {file_path}")
            return file_path
            
        except Exception as e:
            self.logger.error(f"Failed to export incidence summary: {str(e)}")
            return None
    
    def clear_incidences(self, subtype: Optional[str] = None) -> None:
        """Clear incidences for a specific subtype or all subtypes.
        
        Args:
            subtype: Specific subtype to clear, or None to clear all
        """
        if subtype is not None:
            if subtype in self.incidences:
                del self.incidences[subtype]
                self.logger.info(f"Cleared incidences for subtype: {subtype}")
        else:
            self.incidences.clear()
            self._incidence_counter = 0
            self.logger.info("Cleared all incidences")