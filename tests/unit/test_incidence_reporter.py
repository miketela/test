"""Unit tests for incidence reporter module."""

import pytest
import pandas as pd
from pathlib import Path
from datetime import datetime
from unittest.mock import Mock

from src.core.incidence_reporter import (
    IncidenceType,
    IncidenceSeverity,
    Incidence,
    IncidenceReporter
)


class TestIncidenceType:
    """Test cases for IncidenceType enum."""
    
    def test_enum_values(self):
        """Test that all expected enum values exist."""
        assert IncidenceType.VALIDATION_FAILURE.value == "VALIDATION_FAILURE"
        assert IncidenceType.DATA_QUALITY.value == "DATA_QUALITY"
        assert IncidenceType.BUSINESS_RULE_VIOLATION.value == "BUSINESS_RULE_VIOLATION"
        assert IncidenceType.TRANSFORMATION_ERROR.value == "TRANSFORMATION_ERROR"
        assert IncidenceType.HEADER_MISMATCH.value == "HEADER_MISMATCH"


class TestIncidenceSeverity:
    """Test cases for IncidenceSeverity enum."""
    
    def test_enum_values(self):
        """Test that all expected enum values exist."""
        assert IncidenceSeverity.LOW.value == "LOW"
        assert IncidenceSeverity.MEDIUM.value == "MEDIUM"
        assert IncidenceSeverity.HIGH.value == "HIGH"
        assert IncidenceSeverity.CRITICAL.value == "CRITICAL"


class TestIncidence:
    """Test cases for Incidence dataclass."""
    
    def test_incidence_creation(self):
        """Test creating an Incidence instance."""
        timestamp = datetime.now().isoformat()
        incidence = Incidence(
            incidence_id="test-id",
            timestamp=timestamp,
            period="202401",
            run_id="test-run",
            subtype="test-subtype",
            source_file="test.csv",
            record_index=5,
            column_name="test_column",
            incidence_type=IncidenceType.VALIDATION_FAILURE,
            severity=IncidenceSeverity.HIGH,
            description="Test validation error",
            original_value="invalid_value",
            expected_value="valid_value",
            corrected_value="valid_value"
        )
        
        assert incidence.incidence_id == "test-id"
        assert incidence.timestamp == timestamp
        assert incidence.period == "202401"
        assert incidence.run_id == "test-run"
        assert incidence.subtype == "test-subtype"
        assert incidence.source_file == "test.csv"
        assert incidence.record_index == 5
        assert incidence.column_name == "test_column"
        assert incidence.incidence_type == IncidenceType.VALIDATION_FAILURE
        assert incidence.severity == IncidenceSeverity.HIGH
        assert incidence.description == "Test validation error"
        assert incidence.original_value == "invalid_value"
        assert incidence.expected_value == "valid_value"
        assert incidence.corrected_value == "valid_value"

    def test_incidence_optional_fields(self):
        """Test creating an Incidence with optional fields as None."""
        timestamp = datetime.now().isoformat()
        incidence = Incidence(
            incidence_id="test-id-2",
            timestamp=timestamp,
            period="202401",
            run_id="test-run",
            subtype="test-subtype-2",
            description="Test data quality issue"
        )
        
        assert incidence.source_file is None
        assert incidence.record_index is None
        assert incidence.column_name is None
        assert incidence.original_value is None
        assert incidence.expected_value is None
        assert incidence.corrected_value is None


class TestIncidenceReporter:
    """Test cases for IncidenceReporter class."""
    
    def test_init(self, sample_config):
        """Test IncidenceReporter initialization."""
        reporter = IncidenceReporter(
            config=sample_config,
            run_id="test-run",
            period="202401"
        )
        assert len(reporter.incidences) == 0
        assert reporter.config == sample_config
        assert reporter.run_id == "test-run"
        assert reporter.period == "202401"
        assert isinstance(reporter.logger, logging.Logger)
    
    def test_add_incidence(self, sample_config):
        """Test adding an incidence."""
        reporter = IncidenceReporter(
            config=sample_config,
            run_id="test-run",
            period="202401"
        )
        
        reporter.add_incidence(
            file_name="test.csv",
            incidence_type=IncidenceType.VALIDATION_ERROR,
            severity=IncidenceSeverity.HIGH,
            description="Test validation error"
        )
        
        assert len(reporter.incidences) == 1
        incidence = reporter.incidences[0]
        assert incidence.file_name == "test.csv"
        assert incidence.incidence_type == IncidenceType.VALIDATION_ERROR
        assert incidence.severity == IncidenceSeverity.HIGH
        assert incidence.description == "Test validation error"
        assert incidence.row_number == 1
        assert incidence.column_name == "test_col"
        assert isinstance(incidence.timestamp, datetime)
    
    def test_add_validation_error(self, sample_config):
        """Test adding a validation error."""
        reporter = IncidenceReporter(
            config=sample_config,
            run_id="test-run",
            period="202401"
        )
        
        reporter.add_validation_error(
            file_name="test.csv",
            row_number=5,
            column_name="amount",
            description="Invalid amount format",
            original_value="abc",
            corrected_value="0.00"
        )
        
        assert len(reporter.incidences) == 1
        incidence = reporter.incidences[0]
        assert incidence.incidence_type == IncidenceType.VALIDATION_ERROR
        assert incidence.severity == IncidenceSeverity.HIGH
        assert incidence.original_value == "abc"
        assert incidence.corrected_value == "0.00"
    
    def test_add_data_quality_issue(self, sample_config):
        """Test adding a data quality issue."""
        reporter = IncidenceReporter(
            config=sample_config,
            run_id="test-run",
            period="202401"
        )
        
        reporter.add_data_quality_issue(
            file_name="test.csv",
            description="Missing required field",
            severity=IncidenceSeverity.MEDIUM,
            row_number=10,
            column_name="required_field"
        )
        
        assert len(reporter.incidences) == 1
        incidence = reporter.incidences[0]
        assert incidence.incidence_type == IncidenceType.DATA_QUALITY
        assert incidence.severity == IncidenceSeverity.MEDIUM
    
    def test_add_business_rule_violation(self, sample_config):
        """Test adding a business rule violation."""
        reporter = IncidenceReporter(
            config=sample_config,
            run_id="test-run",
            period="202401"
        )
        
        reporter.add_business_rule_violation(
            file_name="test.csv",
            rule_name="VALOR_MINIMO_AVALUO",
            description="Value below minimum threshold",
            row_number=3,
            original_value="100",
            corrected_value="1000"
        )
        
        assert len(reporter.incidences) == 1
        incidence = reporter.incidences[0]
        assert incidence.incidence_type == IncidenceType.BUSINESS_RULE
        assert incidence.severity == IncidenceSeverity.MEDIUM
        assert "VALOR_MINIMO_AVALUO" in incidence.description
    
    def test_get_incidences_by_type(self, sample_config):
        """Test filtering incidences by type."""
        reporter = IncidenceReporter(
            config=sample_config,
            run_id="test-run",
            period="202401"
        )
        
        # Add different types of incidences
        reporter.add_validation_error("test1.csv", 1, "col1", "Error 1")
        reporter.add_data_quality_issue("test2.csv", "Quality issue")
        reporter.add_validation_error("test3.csv", 2, "col2", "Error 2")
        
        # Filter by validation errors
        validation_errors = reporter.get_incidences_by_type(IncidenceType.VALIDATION_ERROR)
        assert len(validation_errors) == 2
        
        # Filter by data quality issues
        quality_issues = reporter.get_incidences_by_type(IncidenceType.DATA_QUALITY)
        assert len(quality_issues) == 1
    
    def test_get_incidences_by_severity(self, sample_config):
        """Test filtering incidences by severity."""
        reporter = IncidenceReporter(
            config=sample_config,
            run_id="test-run",
            period="202401"
        )
        
        # Add incidences with different severities
        reporter.add_validation_error("test1.csv", 1, "col1", "High error")  # HIGH by default
        reporter.add_data_quality_issue("test2.csv", "Medium issue", IncidenceSeverity.MEDIUM)
        reporter.add_data_quality_issue("test3.csv", "Low issue", IncidenceSeverity.LOW)
        
        # Filter by high severity
        high_severity = reporter.get_incidences_by_severity(IncidenceSeverity.HIGH)
        assert len(high_severity) == 1
        
        # Filter by medium severity
        medium_severity = reporter.get_incidences_by_severity(IncidenceSeverity.MEDIUM)
        assert len(medium_severity) == 1
    
    def test_get_incidences_by_file(self, sample_config, mock_logger):
        """Test filtering incidences by file name."""
        reporter = IncidenceReporter(
            config=sample_config,
            run_id="test-run",
            period="202401",
            logger=mock_logger
        )
        
        # Add incidences for different files
        reporter.add_validation_error("file1.csv", 1, "col1", "Error in file1")
        reporter.add_validation_error("file2.csv", 1, "col1", "Error in file2")
        reporter.add_validation_error("file1.csv", 2, "col2", "Another error in file1")
        
        # Filter by file1.csv
        file1_incidences = reporter.get_incidences_by_file("file1.csv")
        assert len(file1_incidences) == 2
        
        # Filter by file2.csv
        file2_incidences = reporter.get_incidences_by_file("file2.csv")
        assert len(file2_incidences) == 1
    
    def test_get_summary(self, sample_config, mock_logger):
        """Test getting incidences summary."""
        reporter = IncidenceReporter(
            config=sample_config,
            run_id="test-run",
            period="202401",
            logger=mock_logger
        )
        
        # Add various incidences
        reporter.add_validation_error("test1.csv", 1, "col1", "Error 1")
        reporter.add_validation_error("test1.csv", 2, "col2", "Error 2")
        reporter.add_data_quality_issue("test2.csv", "Quality issue", IncidenceSeverity.MEDIUM)
        reporter.add_business_rule_violation("test1.csv", "RULE1", "Rule violation")
        
        summary = reporter.get_summary()
        
        # Check total counts
        assert summary['total_incidences'] == 4
        assert summary['files_affected'] == 2
        
        # Check by type
        assert summary['by_type']['VALIDATION_ERROR'] == 2
        assert summary['by_type']['DATA_QUALITY'] == 1
        assert summary['by_type']['BUSINESS_RULE'] == 1
        
        # Check by severity
        assert summary['by_severity']['HIGH'] == 2
        assert summary['by_severity']['MEDIUM'] == 2
        
        # Check by file
        assert summary['by_file']['test1.csv'] == 3
        assert summary['by_file']['test2.csv'] == 1
    
    def test_to_dataframe(self, sample_config, mock_logger):
        """Test converting incidences to DataFrame."""
        reporter = IncidenceReporter(
            config=sample_config,
            run_id="test-run",
            period="202401",
            logger=mock_logger
        )
        
        # Add some incidences
        reporter.add_validation_error(
            "test.csv", 1, "col1", "Error 1", "bad_value", "good_value"
        )
        reporter.add_data_quality_issue("test.csv", "Quality issue")
        
        df = reporter.to_dataframe()
        
        # Check DataFrame structure
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        
        expected_columns = [
            'timestamp', 'file_name', 'row_number', 'column_name',
            'incidence_type', 'severity', 'description',
            'original_value', 'corrected_value'
        ]
        assert list(df.columns) == expected_columns
        
        # Check data types
        assert df['incidence_type'].dtype == 'object'
        assert df['severity'].dtype == 'object'
    
    def test_export_to_csv(self, temp_dir, sample_config, mock_logger):
        """Test exporting incidences to CSV."""
        reporter = IncidenceReporter(
            config=sample_config,
            run_id="test-run",
            period="202401",
            logger=mock_logger
        )
        
        # Add some incidences
        reporter.add_validation_error("test.csv", 1, "col1", "Error 1")
        reporter.add_data_quality_issue("test.csv", "Quality issue")
        
        # Export to CSV
        output_path = temp_dir / "incidences.csv"
        reporter.export_to_csv(str(output_path))
        
        # Verify file was created
        assert output_path.exists()
        
        # Read back and verify content
        df = pd.read_csv(output_path)
        assert len(df) == 2
        assert 'incidence_type' in df.columns
        assert 'severity' in df.columns
    
    def test_export_summary_to_csv(self, temp_dir, sample_config, mock_logger):
        """Test exporting summary to CSV."""
        reporter = IncidenceReporter(
            config=sample_config,
            run_id="test-run",
            period="202401",
            logger=mock_logger
        )
        
        # Add some incidences
        reporter.add_validation_error("test1.csv", 1, "col1", "Error 1")
        reporter.add_validation_error("test1.csv", 2, "col2", "Error 2")
        reporter.add_data_quality_issue("test2.csv", "Quality issue")
        
        # Export summary to CSV
        output_path = temp_dir / "summary.csv"
        reporter.export_summary_to_csv(str(output_path))
        
        # Verify file was created
        assert output_path.exists()
        
        # Read back and verify content
        df = pd.read_csv(output_path)
        assert len(df) > 0
        assert 'metric' in df.columns
        assert 'value' in df.columns
    
    def test_clear(self, sample_config, mock_logger):
        """Test clearing all incidences."""
        reporter = IncidenceReporter(
            config=sample_config,
            run_id="test-run",
            period="202401",
            logger=mock_logger
        )
        
        # Add some incidences
        reporter.add_validation_error("test.csv", 1, "col1", "Error 1")
        reporter.add_data_quality_issue("test.csv", "Quality issue")
        
        assert len(reporter.incidences) == 2
        
        # Clear incidences
        reporter.clear()
        
        assert len(reporter.incidences) == 0