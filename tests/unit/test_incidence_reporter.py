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
    
    def test_add_incidence(self, sample_config):
        """Test adding an incidence."""
        reporter = IncidenceReporter(
            config=sample_config,
            run_id="test-run",
            period="202401"
        )
        
        reporter.add_incidence(
            subtype="test-subtype",
            incidence_type=IncidenceType.VALIDATION_FAILURE,
            severity=IncidenceSeverity.HIGH,
            description="Test validation error",
            source_file="test.csv",
            column_name="test_col",
            record_index=1
        )
        
        incidences = reporter.get_all_incidences()
        assert len(incidences) == 1
        incidence = incidences[0]
        assert incidence.source_file == "test.csv"
        assert incidence.incidence_type == IncidenceType.VALIDATION_FAILURE
        assert incidence.severity == IncidenceSeverity.HIGH
        assert incidence.description == "Test validation error"
        assert incidence.record_index == 1
        assert incidence.column_name == "test_col"
        assert isinstance(incidence.timestamp, str)
    
    def test_add_validation_failure(self, sample_config):
        """Test adding a validation failure."""
        reporter = IncidenceReporter(
            config=sample_config,
            run_id="test-run",
            period="202401"
        )
        
        reporter.add_validation_failure(
            subtype="test-subtype",
            rule_name="invalid_format",
            record_index=5,
            column_name="amount",
            original_value="abc",
            expected_value="float"
        )
        
        incidences = reporter.get_all_incidences()
        assert len(incidences) == 1
        incidence = incidences[0]
        assert incidence.incidence_type == IncidenceType.VALIDATION_FAILURE
        assert incidence.severity == IncidenceSeverity.HIGH
        assert incidence.original_value == "abc"
        assert incidence.expected_value == "float"
    
    def test_add_data_quality_issue(self, sample_config):
        """Test adding a data quality issue."""
        reporter = IncidenceReporter(
            config=sample_config,
            run_id="test-run",
            period="202401"
        )
        
        reporter.add_data_quality_issue(
            subtype="test-subtype",
            issue_type="missing_field",
            record_index=10,
            column_name="required_field",
            original_value=None,
            corrected_value=None
        )
        
        incidences = reporter.get_all_incidences()
        assert len(incidences) == 1
        incidence = incidences[0]
        assert incidence.incidence_type == IncidenceType.DATA_QUALITY
        assert incidence.severity == IncidenceSeverity.MEDIUM
        assert incidence.rule_name == "missing_field"
    
    def test_add_business_rule_violation(self, sample_config):
        """Test adding a business rule violation."""
        reporter = IncidenceReporter(
            config=sample_config,
            run_id="test-run",
            period="202401"
        )
        
        reporter.add_business_rule_violation(
            subtype="test-subtype",
            rule_name="VALOR_MINIMO_AVALUO",
            description="VALOR_MINIMO_AVALUO: Value below minimum threshold",
            record_index=3,
            original_value="100",
            threshold=1000
        )
        
        incidences = reporter.get_all_incidences()
        assert len(incidences) == 1
        incidence = incidences[0]
        assert incidence.incidence_type == IncidenceType.BUSINESS_RULE_VIOLATION
        assert incidence.severity == IncidenceSeverity.HIGH
        assert "VALOR_MINIMO_AVALUO" in incidence.description
    
    def test_get_incidences_by_type(self, sample_config):
        """Test filtering incidences by type."""
        reporter = IncidenceReporter(
            config=sample_config,
            run_id="test-run",
            period="202401"
        )

        # Add different types of incidences
        reporter.add_validation_failure(subtype="SUB1", rule_name="rule1", description="Error 1")
        reporter.add_data_quality_issue(subtype="SUB2", issue_type="issue1", description="Quality issue")
        reporter.add_validation_failure(subtype="SUB1", rule_name="rule2", description="Error 2")

        all_incidences = reporter.get_all_incidences()

        # Filter by validation errors
        validation_errors = [inc for inc in all_incidences if inc.incidence_type == IncidenceType.VALIDATION_FAILURE]
        assert len(validation_errors) == 2

        # Filter by data quality issues
        quality_issues = [inc for inc in all_incidences if inc.incidence_type == IncidenceType.DATA_QUALITY]
        assert len(quality_issues) == 1
    
    def test_get_incidences_by_severity(self, sample_config):
        """Test filtering incidences by severity."""
        reporter = IncidenceReporter(
            config=sample_config,
            run_id="test-run",
            period="202401"
        )

        # Add incidences with different severities
        reporter.add_incidence(subtype="SUB1", incidence_type=IncidenceType.VALIDATION_FAILURE, description="High error", severity=IncidenceSeverity.HIGH)
        reporter.add_incidence(subtype="SUB2", incidence_type=IncidenceType.DATA_QUALITY, description="Medium issue", severity=IncidenceSeverity.MEDIUM)
        reporter.add_incidence(subtype="SUB3", incidence_type=IncidenceType.DATA_QUALITY, description="Low issue", severity=IncidenceSeverity.LOW)

        all_incidences = reporter.get_all_incidences()

        # Filter by high severity
        high_severity = [inc for inc in all_incidences if inc.severity == IncidenceSeverity.HIGH]
        assert len(high_severity) == 1

        # Filter by medium severity
        medium_severity = [inc for inc in all_incidences if inc.severity == IncidenceSeverity.MEDIUM]
        assert len(medium_severity) == 1
    
    def test_get_incidences_by_file(self, sample_config):
        """Test filtering incidences by file name."""
        reporter = IncidenceReporter(
            config=sample_config,
            run_id="test-run",
            period="202401"
        )
        
        # Add incidences for different files
        reporter.add_incidence(
            subtype="SUB1", incidence_type=IncidenceType.VALIDATION_FAILURE,
            description="Error in file1", severity=IncidenceSeverity.HIGH,
            source_file="file1.csv", record_index=1, column_name="col1", rule_name="rule1"
        )
        reporter.add_incidence(
            subtype="SUB2", incidence_type=IncidenceType.VALIDATION_FAILURE,
            description="Error in file2", severity=IncidenceSeverity.HIGH,
            source_file="file2.csv", record_index=1, column_name="col1", rule_name="rule2"
        )
        reporter.add_incidence(
            subtype="SUB1", incidence_type=IncidenceType.VALIDATION_FAILURE,
            description="Another error in file1", severity=IncidenceSeverity.HIGH,
            source_file="file1.csv", record_index=2, column_name="col2", rule_name="rule3"
        )
        
        # Filter by file1.csv using get_all_incidences and filtering
        all_incidences = reporter.get_all_incidences()
        file1_incidences = [inc for inc in all_incidences if inc.source_file == "file1.csv"]
        assert len(file1_incidences) == 2
        
        # Filter by file2.csv
        file2_incidences = [inc for inc in all_incidences if inc.source_file == "file2.csv"]
        assert len(file2_incidences) == 1
    
    def test_get_summary(self, sample_config):
        """Test getting incidences summary."""
        reporter = IncidenceReporter(
            config=sample_config,
            run_id="test-run",
            period="202401"
        )
        
        # Add various incidences
        reporter.add_incidence(
            subtype="SUB1", incidence_type=IncidenceType.VALIDATION_FAILURE,
            description="Error 1", severity=IncidenceSeverity.HIGH,
            source_file="test1.csv", record_index=1, column_name="col1", rule_name="rule1"
        )
        reporter.add_incidence(
            subtype="SUB1", incidence_type=IncidenceType.VALIDATION_FAILURE,
            description="Error 2", severity=IncidenceSeverity.HIGH,
            source_file="test1.csv", record_index=2, column_name="col2", rule_name="rule2"
        )
        reporter.add_incidence(
            subtype="SUB2", incidence_type=IncidenceType.DATA_QUALITY,
            description="Quality issue", severity=IncidenceSeverity.MEDIUM,
            source_file="test2.csv", rule_name="quality_issue"
        )
        reporter.add_incidence(
            subtype="SUB1", incidence_type=IncidenceType.BUSINESS_RULE_VIOLATION,
            description="Rule violation", severity=IncidenceSeverity.HIGH,
            source_file="test1.csv", rule_name="RULE1"
        )
        
        summary = reporter.get_incidence_summary()
        
        # Check total counts
        assert summary['total_incidences'] == 4
        assert len(summary['by_subtype']) == 2
        
        # Check by type - using correct enum values
        assert summary['by_type']['VALIDATION_FAILURE'] == 2
        assert summary['by_type']['DATA_QUALITY'] == 1
        assert summary['by_type']['BUSINESS_RULE_VIOLATION'] == 1
        
        # Check by severity
        assert summary['by_severity']['HIGH'] == 3  # validation failures and business rule are HIGH by default
        assert summary['by_severity']['MEDIUM'] == 1
    
    def test_incidence_to_dict(self, sample_config):
        """Test converting incidences to dictionary format."""
        reporter = IncidenceReporter(
            config=sample_config,
            run_id="test-run",
            period="202401"
        )
        
        # Add some incidences
        reporter.add_incidence(
            subtype="SUB1", incidence_type=IncidenceType.VALIDATION_FAILURE,
            description="Error 1", severity=IncidenceSeverity.HIGH,
            source_file="test.csv", record_index=1, column_name="col1",
            original_value="bad_value", expected_value="good_value", rule_name="rule1"
        )
        reporter.add_incidence(
            subtype="SUB2", incidence_type=IncidenceType.DATA_QUALITY,
            description="Quality issue", severity=IncidenceSeverity.MEDIUM,
            source_file="test.csv", rule_name="quality_issue"
        )
        
        all_incidences = reporter.get_all_incidences()
        assert len(all_incidences) == 2
        
        # Test that incidences can be converted to dict
        incidence_dict = all_incidences[0].to_dict()
        
        assert 'incidence_id' in incidence_dict
        assert 'subtype' in incidence_dict
        assert 'incidence_type' in incidence_dict
        assert 'description' in incidence_dict
        assert 'severity' in incidence_dict
        assert 'timestamp' in incidence_dict
        assert 'run_id' in incidence_dict
        assert 'period' in incidence_dict
        assert incidence_dict['subtype'] == "SUB1"
    
    def test_export_incidences_to_csv(self, tmp_path, sample_config):
        """Test exporting incidences to CSV using AT12Paths."""
        from unittest.mock import Mock
        
        reporter = IncidenceReporter(
            config=sample_config,
            run_id="test-run",
            period="202401"
        )
        
        # Add some incidences
        reporter.add_incidence(
            subtype="SUB1", incidence_type=IncidenceType.VALIDATION_FAILURE,
            description="Error 1", severity=IncidenceSeverity.HIGH,
            source_file="test.csv", record_index=1, column_name="col1", rule_name="rule1"
        )
        reporter.add_incidence(
            subtype="SUB2", incidence_type=IncidenceType.DATA_QUALITY,
            description="Quality issue", severity=IncidenceSeverity.MEDIUM,
            source_file="test.csv", rule_name="quality_issue"
        )
        
        # Mock AT12Paths
        mock_paths = Mock()
        csv_file1 = tmp_path / "EEOO_TABULAR_SUB1_AT12_202401.csv"
        csv_file2 = tmp_path / "EEOO_TABULAR_SUB2_AT12_202401.csv"
        mock_paths.get_incidencia_path.side_effect = [csv_file1, csv_file2]
        
        # Export to CSV
        exported_files = reporter.export_incidences_to_csv(mock_paths)
        
        # Check files were created
        assert len(exported_files) == 2
        assert csv_file1.exists()
        assert csv_file2.exists()
        
        # Check file content
        df1 = pd.read_csv(csv_file1)
        assert len(df1) == 1
        assert 'incidence_id' in df1.columns
        assert 'description' in df1.columns
    
    def test_export_summary_to_csv(self, sample_config, tmp_path):
        """Test exporting the summary to a CSV file."""
        from unittest.mock import Mock
        
        reporter = IncidenceReporter(
            config=sample_config,
            run_id="test-run",
            period="202401"
        )

        # Add some incidences
        reporter.add_incidence(
            subtype="SUB1", incidence_type=IncidenceType.VALIDATION_FAILURE,
            description="Error 1", severity=IncidenceSeverity.HIGH,
            source_file="test1.csv", record_index=1, column_name="col1", rule_name="rule1"
        )
        reporter.add_incidence(
            subtype="SUB1", incidence_type=IncidenceType.VALIDATION_FAILURE,
            description="Error 2", severity=IncidenceSeverity.HIGH,
            source_file="test1.csv", record_index=2, column_name="col2", rule_name="rule2"
        )
        reporter.add_incidence(
            subtype="SUB2", incidence_type=IncidenceType.DATA_QUALITY,
            description="Quality issue", severity=IncidenceSeverity.MEDIUM,
            source_file="test2.csv", rule_name="quality_issue"
        )

        # Mock AT12Paths
        mock_paths = Mock()
        output_path = tmp_path / "INCIDENCES_SUMMARY_AT12_202401.csv"
        mock_paths.get_incidencia_path.return_value = output_path

        # Export summary to CSV
        result_path = reporter.export_summary_to_csv(mock_paths)

        # Verify file was created
        assert result_path == output_path
        assert output_path.exists()

        # Read back and verify content
        df = pd.read_csv(output_path)
        assert len(df) > 0
        assert 'metric' in df.columns
        assert 'category' in df.columns
        assert 'value' in df.columns
    
    def test_clear(self, sample_config):
        """Test clearing all incidences."""
        reporter = IncidenceReporter(
            config=sample_config,
            run_id="test-run",
            period="202401"
        )
        
        # Add some incidences
        reporter.add_incidence(
            subtype="SUB1", incidence_type=IncidenceType.VALIDATION_FAILURE,
            description="Error 1", severity=IncidenceSeverity.HIGH,
            source_file="test.csv", record_index=1, column_name="col1", rule_name="rule1"
        )
        reporter.add_incidence(
            subtype="SUB2", incidence_type=IncidenceType.DATA_QUALITY,
            description="Quality issue", severity=IncidenceSeverity.MEDIUM,
            source_file="test.csv", rule_name="quality_issue"
        )
        
        assert len(reporter.get_all_incidences()) == 2
        
        # Clear incidences
        reporter.clear_incidences()
        
        assert len(reporter.get_all_incidences()) == 0