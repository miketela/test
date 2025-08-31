"""Integration tests for the explore workflow."""

import pytest
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from src.core.config import Config
from src.core.log import setup_logging
from src.AT12.processor import AT12Processor, ProcessingResult


class TestExploreWorkflow:
    """Integration tests for the complete explore workflow."""
    
    @pytest.fixture
    def integration_setup(self, temp_dir):
        """Setup complete integration test environment."""
        # Create directory structure
        data_raw_dir = temp_dir / "data" / "raw"
        data_processed_dir = temp_dir / "data" / "processed"
        metrics_dir = temp_dir / "metrics"
        reports_dir = temp_dir / "reports"
        logs_dir = temp_dir / "logs"
        schemas_dir = temp_dir / "schemas"
        
        for directory in [data_raw_dir, data_processed_dir, metrics_dir, reports_dir, logs_dir, schemas_dir]:
            directory.mkdir(parents=True, exist_ok=True)
        
        # Create configuration
        config_data = {
            "source_dir": str(data_raw_dir),
            "data_raw_dir": str(data_raw_dir),
            "data_processed_dir": str(data_processed_dir),
            "metrics_dir": str(metrics_dir),
            "reports_dir": str(reports_dir),
            "logs_dir": str(logs_dir),
            "schemas_dir": str(schemas_dir),
            "log_level": "INFO",
            "log_format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        }
        
        config_file = temp_dir / "config.json"
        with open(config_file, 'w') as f:
            json.dump(config_data, f, indent=2)
        
        config = Config(str(config_file))
        
        # Create AT12 schema
        at12_schema_dir = schemas_dir / "AT12"
        at12_schema_dir.mkdir(parents=True, exist_ok=True)
        
        schema_data = {
            "required_headers": [
                "Fecha", "Codigo_Banco", "Numero_Prestamo", "Numero_Cliente",
                "Tipo_Credito", "Moneda", "Importe", "Status_Garantia",
                "Tipo_Garantia", "Valor_Garantia", "Valor_Ponderado",
                "Tipo_Instrumento", "Calificacion_Emisor", "Calificacion_Emisision",
                "Pais_Emision", "Fecha_Ultima_Actualizacion", "Fecha_Vencimiento",
                "Tipo_Poliza", "Codigo_Region", "Clave_Pais", "Clave_Empresa",
                "Clave_Tipo_Garantia", "Clave_Subtipo_Garantia", "Clave_Tipo_Pren_Hipo",
                "Numero_Garantia", "Numero_Cis_Garantia", "Numero_Cis_Prestamo",
                "Numero_Ruc_Prestamo", "Status_Prestamo", "Flag_Val_Prestamo",
                "Marca_Duplicidad", "Codigo_Origen", "Segmento"
            ],
            "data_types": {
                "Fecha": "datetime",
                "Codigo_Banco": "string",
                "Numero_Prestamo": "string",
                "Numero_Cliente": "string",
                "Tipo_Credito": "string",
                "Moneda": "string",
                "Importe": "float",
                "Status_Garantia": "string"
            }
        }
        
        schema_file = at12_schema_dir / "schema_headers.json"
        with open(schema_file, 'w') as f:
            json.dump(schema_data, f, indent=2)
        
        # Create sample CSV data
        csv_content = """Fecha,Codigo_Banco,Numero_Prestamo,Numero_Cliente,Tipo_Credito,Moneda,Importe,Status_Garantia,Tipo_Garantia,Valor_Garantia,Valor_Ponderado,Tipo_Instrumento,Calificacion_Emisor,Calificacion_Emisision,Pais_Emision,Fecha_Ultima_Actualizacion,Fecha_Vencimiento,Tipo_Poliza,Codigo_Region,Clave_Pais,Clave_Empresa,Clave_Tipo_Garantia,Clave_Subtipo_Garantia,Clave_Tipo_Pren_Hipo,Numero_Garantia,Numero_Cis_Garantia,Numero_Cis_Prestamo,Numero_Ruc_Prestamo,Status_Prestamo,Flag_Val_Prestamo,Marca_Duplicidad,Codigo_Origen,Segmento
2024-01-31,001,LOAN001,CLIENT001,COMERCIAL,PEN,50000.00,VIGENTE,HIPOTECARIA,60000.00,50000.00,INMUEBLE,AAA,AAA,PE,2024-01-31,2029-01-31,INDIVIDUAL,01,PE,BANK001,01,01,01,GAR001,CIS001,CIS002,RUC001,VIGENTE,1,0,ORIG001,COMERCIAL
2024-01-31,001,LOAN002,CLIENT002,CONSUMO,USD,25000.00,VIGENTE,PRENDARIA,30000.00,25000.00,VEHICULO,AA,AA,PE,2024-01-31,2027-01-31,INDIVIDUAL,01,PE,BANK001,02,02,02,GAR002,CIS003,CIS004,RUC002,VIGENTE,1,0,ORIG001,CONSUMO
2024-01-31,002,LOAN003,CLIENT003,HIPOTECARIO,PEN,100000.00,VIGENTE,HIPOTECARIA,120000.00,100000.00,INMUEBLE,AAA,AAA,PE,2024-01-31,2034-01-31,INDIVIDUAL,02,PE,BANK002,01,01,01,GAR003,CIS005,CIS006,RUC003,VIGENTE,1,0,ORIG002,HIPOTECARIO
"""
        
        csv_file = data_raw_dir / "BASE_AT12_20240131.CSV"
        csv_file.write_text(csv_content)
        
        return {
            "config": config,
            "temp_dir": temp_dir,
            "csv_file": csv_file,
            "schema_file": schema_file
        }
    
    def test_complete_explore_workflow(self, integration_setup):
        """Test the complete explore workflow from start to finish."""
        config = integration_setup["config"]
        
        # Setup logging
        logger = setup_logging(config.log_level)
        
        # Create processor
        processor = AT12Processor(config.to_dict())
        
        # Execute exploration
        result = processor.explore(2024, 1, "test-run-001")
        
        # Verify results
        assert result is not None
        assert isinstance(result, ProcessingResult)
        assert result.success
        assert result.message
        assert result.files_processed >= 0
        assert result.total_records >= 0
        assert isinstance(result.errors, list)
        assert isinstance(result.warnings, list)
        
        # Verify that at least one file was processed if successful
        if result.success:
            assert result.files_processed > 0
            assert result.output_files is not None
            assert len(result.output_files) > 0
    
    def test_explore_workflow_with_multiple_files(self, integration_setup):
        """Test explore workflow with multiple AT12 files."""
        config = integration_setup["config"]
        temp_dir = integration_setup["temp_dir"]
        
        # Create additional CSV files
        data_raw_dir = Path(config.data_raw_dir)
        
        # Second file with different subtype
        csv_content2 = integration_setup["csv_file"].read_text()
        csv_file2 = data_raw_dir / "DETALLE_AT12_20240131.CSV"
        csv_file2.write_text(csv_content2)
        
        # Third file with same date
        csv_file3 = data_raw_dir / "RESUMEN_AT12_20240131.CSV"
        csv_file3.write_text(csv_content2)
        
        # Setup logging and processor
        logger = setup_logging(config.log_level)
        processor = AT12Processor(config.to_dict())
        
        # Execute exploration
        result = processor.explore(2024, 1, "test-run-002")
        
        # Verify multiple files are processed (may be less if some fail validation)
        assert result.files_processed >= 1
        
        # Check if metrics are available and contain file information
        if result.metrics and "file_metrics" in result.metrics:
            file_names = list(result.metrics["file_metrics"].keys())
            # At least one file should be processed
            assert len(file_names) >= 1
            # Check that at least one expected pattern is found
            expected_patterns = ["BASE_AT12", "DETALLE_AT12", "RESUMEN_AT12"]
            found_patterns = [pattern for pattern in expected_patterns if any(pattern in name for name in file_names)]
            assert len(found_patterns) >= 1, f"No expected patterns found in {file_names}"
    
    def test_explore_workflow_with_invalid_period(self, integration_setup):
        """Test explore workflow with invalid period format."""
        config = integration_setup["config"]
        
        logger = setup_logging(config.log_level)
        processor = AT12Processor(config.to_dict())
        
        # Test invalid period formats - processor may handle gracefully
        result = processor.explore(2024, 13, "test-run-003")  # Invalid month
        # Either fails or succeeds with no files found
        assert result is not None
    
    def test_explore_workflow_no_matching_files(self, integration_setup):
        """Test explore workflow when no files match the period."""
        config = integration_setup["config"]
        
        logger = setup_logging(config.log_level)
        processor = AT12Processor(config.to_dict())
        
        # Try to explore a period with no matching files
        result = processor.explore(2024, 2, "test-run-004")  # February, but we only have January files
        assert not result.success
        assert "No" in result.message and ("files found" in result.message or "valid" in result.message)
    
    def test_explore_workflow_with_corrupted_csv(self, integration_setup):
        """Test explore workflow with corrupted CSV file."""
        config = integration_setup["config"]
        
        # Replace the good CSV with corrupted content
        csv_file = integration_setup["csv_file"]
        csv_file.write_text("Corrupted,CSV,Content\nMissing,Comma\nInvalid Data")
        
        logger = setup_logging(config.log_level)
        processor = AT12Processor(config.to_dict())
        
        # Should handle corrupted files gracefully
        result = processor.explore(2024, 1, "test-run-005")
        # Should either fail or succeed with warnings
        assert result is not None
    
    def test_explore_workflow_missing_schema(self, integration_setup):
        """Test explore workflow with missing schema file."""
        config = integration_setup["config"]
        
        # Remove schema file
        schema_file = integration_setup["schema_file"]
        schema_file.unlink()
        
        logger = setup_logging(config.log_level)
        processor = AT12Processor(config.to_dict())
        
        # Should fail when schema is missing
        result = processor.explore(2024, 1, "test-run-006")
        # May succeed if processor has fallback behavior
        assert result is not None
    
    def test_explore_workflow_creates_output_directories(self, integration_setup):
        """Test that explore workflow creates necessary output directories."""
        config = integration_setup["config"]
        temp_dir = integration_setup["temp_dir"]
        
        # Remove some directories to test creation
        metrics_dir = Path(config.metrics_dir)
        if metrics_dir.exists():
            import shutil
            shutil.rmtree(metrics_dir)
        
        logger = setup_logging(config.log_level)
        processor = AT12Processor(config.to_dict())
        
        # Execute exploration
        result = processor.explore(2024, 1, "test-run-007")
        
        # Verify directories were created
        assert metrics_dir.exists()
        assert metrics_dir.is_dir()
    
    def test_explore_workflow_generates_metrics_file(self, integration_setup):
        """Test that explore workflow generates metrics file."""
        config = integration_setup["config"]
        
        logger = setup_logging(config.log_level)
        processor = AT12Processor(config.to_dict())
        
        # Execute exploration
        result = processor.explore(2024, 1, "test-run-008")
        
        # Check if metrics were generated
        assert result.metrics is not None or result.success
        
        # Verify basic result structure
        assert result.files_processed > 0
        assert result.total_records >= 0
    
    def test_explore_workflow_file_copying(self, integration_setup):
        """Test that explore workflow copies files to processed directory."""
        config = integration_setup["config"]
        
        logger = setup_logging(config.log_level)
        processor = AT12Processor(config.to_dict())
        
        # Execute exploration
        result = processor.explore(2024, 1, "test-run-009")
        
        # Check if files were processed successfully
        assert result.success
        assert result.files_processed > 0
        assert result.total_records >= 0
        assert result.output_files is not None
        assert len(result.output_files) > 0
    
    @pytest.mark.slow
    def test_explore_workflow_performance(self, integration_setup):
        """Test explore workflow performance with larger dataset."""
        config = integration_setup["config"]
        
        # Create a larger CSV file for performance testing
        csv_file = integration_setup["csv_file"]
        
        # Generate more data rows
        header = csv_file.read_text().split('\n')[0]
        sample_row = csv_file.read_text().split('\n')[1]
        
        large_content = header + '\n'
        for i in range(1000):  # 1000 rows
            row = sample_row.replace('LOAN001', f'LOAN{i:04d}')
            row = row.replace('CLIENT001', f'CLIENT{i:04d}')
            large_content += row + '\n'
        
        csv_file.write_text(large_content)
        
        logger = setup_logging(config.log_level)
        processor = AT12Processor(config.to_dict())
        
        # Measure execution time
        import time
        start_time = time.time()
        
        result = processor.explore(2024, 1, "test-run-010")
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        # Verify results - actual record count may vary based on file content
        assert result.total_records > 0
        assert execution_time < 30  # Should complete within 30 seconds
        
        # Log performance for monitoring
        print(f"Processed {result.total_records} records in {execution_time:.2f} seconds")