"""Sample data fixtures for testing."""

import json
from datetime import datetime, timedelta
from pathlib import Path


class SampleDataGenerator:
    """Generator for sample test data."""
    
    @staticmethod
    def create_at12_csv_content(num_rows=10, start_date="2024-01-31"):
        """Create sample AT12 CSV content with specified number of rows."""
        headers = [
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
        ]
        
        content = ",".join(headers) + "\n"
        
        credit_types = ["COMERCIAL", "CONSUMO", "HIPOTECARIO", "MICROEMPRESA"]
        currencies = ["PEN", "USD", "EUR"]
        statuses = ["VIGENTE", "VENCIDO", "CANCELADO"]
        guarantee_types = ["HIPOTECARIA", "PRENDARIA", "FIANZA", "SIN_GARANTIA"]
        
        for i in range(num_rows):
            row_data = [
                start_date,
                f"{(i % 5) + 1:03d}",  # Codigo_Banco
                f"LOAN{i+1:06d}",  # Numero_Prestamo
                f"CLIENT{i+1:06d}",  # Numero_Cliente
                credit_types[i % len(credit_types)],  # Tipo_Credito
                currencies[i % len(currencies)],  # Moneda
                f"{(i + 1) * 10000.00:.2f}",  # Importe
                statuses[i % len(statuses)],  # Status_Garantia
                guarantee_types[i % len(guarantee_types)],  # Tipo_Garantia
                f"{(i + 1) * 12000.00:.2f}",  # Valor_Garantia
                f"{(i + 1) * 10000.00:.2f}",  # Valor_Ponderado
                "INMUEBLE" if i % 2 == 0 else "VEHICULO",  # Tipo_Instrumento
                "AAA" if i % 3 == 0 else "AA",  # Calificacion_Emisor
                "AAA" if i % 3 == 0 else "AA",  # Calificacion_Emisision
                "PE",  # Pais_Emision
                start_date,  # Fecha_Ultima_Actualizacion
                "2029-01-31",  # Fecha_Vencimiento
                "INDIVIDUAL",  # Tipo_Poliza
                f"{(i % 25) + 1:02d}",  # Codigo_Region
                "PE",  # Clave_Pais
                f"BANK{(i % 5) + 1:03d}",  # Clave_Empresa
                f"{(i % 4) + 1:02d}",  # Clave_Tipo_Garantia
                f"{(i % 4) + 1:02d}",  # Clave_Subtipo_Garantia
                f"{(i % 3) + 1:02d}",  # Clave_Tipo_Pren_Hipo
                f"GAR{i+1:06d}",  # Numero_Garantia
                f"CIS{(i*2)+1:06d}",  # Numero_Cis_Garantia
                f"CIS{(i*2)+2:06d}",  # Numero_Cis_Prestamo
                f"RUC{i+1:06d}",  # Numero_Ruc_Prestamo
                statuses[i % len(statuses)],  # Status_Prestamo
                "1" if i % 2 == 0 else "0",  # Flag_Val_Prestamo
                "0",  # Marca_Duplicidad
                f"ORIG{(i % 3) + 1:03d}",  # Codigo_Origen
                credit_types[i % len(credit_types)]  # Segmento
            ]
            
            content += ",".join(row_data) + "\n"
        
        return content
    
    @staticmethod
    def create_corrupted_csv_content():
        """Create corrupted CSV content for testing error handling."""
        return """Fecha,Codigo_Banco,Numero_Prestamo
2024-01-31,001,LOAN001
2024-01-31,002  # Missing comma and field
Invalid,Data,Here,Too,Many,Fields
"""
    
    @staticmethod
    def create_empty_csv_content():
        """Create empty CSV content for testing."""
        return "Fecha,Codigo_Banco,Numero_Prestamo\n"
    
    @staticmethod
    def create_invalid_headers_csv_content():
        """Create CSV with invalid headers for testing."""
        return """Wrong,Headers,Here
2024-01-31,001,LOAN001
2024-01-31,002,LOAN002
"""
    
    @staticmethod
    def create_at12_schema():
        """Create AT12 schema for testing."""
        return {
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
                "Status_Garantia": "string",
                "Tipo_Garantia": "string",
                "Valor_Garantia": "float",
                "Valor_Ponderado": "float",
                "Tipo_Instrumento": "string",
                "Calificacion_Emisor": "string",
                "Calificacion_Emisision": "string",
                "Pais_Emision": "string",
                "Fecha_Ultima_Actualizacion": "datetime",
                "Fecha_Vencimiento": "datetime",
                "Tipo_Poliza": "string",
                "Codigo_Region": "string",
                "Clave_Pais": "string",
                "Clave_Empresa": "string",
                "Clave_Tipo_Garantia": "string",
                "Clave_Subtipo_Garantia": "string",
                "Clave_Tipo_Pren_Hipo": "string",
                "Numero_Garantia": "string",
                "Numero_Cis_Garantia": "string",
                "Numero_Cis_Prestamo": "string",
                "Numero_Ruc_Prestamo": "string",
                "Status_Prestamo": "string",
                "Flag_Val_Prestamo": "string",
                "Marca_Duplicidad": "string",
                "Codigo_Origen": "string",
                "Segmento": "string"
            },
            "validation_rules": {
                "Fecha": {
                    "format": "YYYY-MM-DD",
                    "required": True
                },
                "Codigo_Banco": {
                    "pattern": "^[0-9]{3}$",
                    "required": True
                },
                "Importe": {
                    "min_value": 0,
                    "required": True
                },
                "Moneda": {
                    "allowed_values": ["PEN", "USD", "EUR"],
                    "required": True
                }
            }
        }
    
    @staticmethod
    def create_sample_metrics_data(exploration_id="TEST_001", num_files=1, num_records=100):
        """Create sample metrics data for testing."""
        timestamp = datetime.now().isoformat()
        
        file_metrics = {}
        for i in range(num_files):
            filename = f"BASE_AT12_20240131__run-{exploration_id}_{i}.CSV"
            file_metrics[filename] = {
                "file_size": 1024 * (i + 1),
                "sha256": f"abc123def456{i:03d}",
                "row_count": num_records // num_files,
                "column_count": 33,
                "headers": [
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
                "column_metrics": [
                    {
                        "name": "Fecha",
                        "data_type": "datetime",
                        "null_count": 0,
                        "null_percentage": 0.0,
                        "unique_count": 1,
                        "top_values": [["2024-01-31", num_records // num_files]],
                        "min_length": 10,
                        "max_length": 10,
                        "avg_length": 10.0
                    },
                    {
                        "name": "Codigo_Banco",
                        "data_type": "object",
                        "null_count": 0,
                        "null_percentage": 0.0,
                        "unique_count": 5,
                        "top_values": [["001", 20], ["002", 20], ["003", 20]],
                        "min_length": 3,
                        "max_length": 3,
                        "avg_length": 3.0
                    },
                    {
                        "name": "Importe",
                        "data_type": "float64",
                        "null_count": 0,
                        "null_percentage": 0.0,
                        "unique_count": num_records // num_files,
                        "min_value": 10000.0,
                        "max_value": (num_records // num_files) * 10000.0,
                        "mean_value": ((num_records // num_files) + 1) * 5000.0,
                        "std_value": 28867.5
                    }
                ]
            }
        
        return {
            "exploration_id": exploration_id,
            "run_id": exploration_id,
            "timestamp": timestamp,
            "atom": "AT12",
            "atom_type": "AT12",
            "period": "202401",
            "total_files": num_files,
            "total_records": num_records,
            "files_analyzed": num_files,
            "file_metrics": file_metrics,
            "processing_time": 12.34,
            "data_quality_score": 95.5,
            "validation_errors": [],
            "warnings": [
                "Date format inference applied to Fecha column",
                "Some null values detected in optional fields"
            ]
        }
    
    @staticmethod
    def create_config_data(base_dir):
        """Create configuration data for testing."""
        base_path = Path(base_dir)
        
        return {
            "data_raw_dir": str(base_path / "data" / "raw"),
            "data_processed_dir": str(base_path / "data" / "processed"),
            "metrics_dir": str(base_path / "metrics"),
            "reports_dir": str(base_path / "reports"),
            "logs_dir": str(base_path / "logs"),
            "schemas_dir": str(base_path / "schemas"),
            "log_level": "INFO",
            "log_format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        }
    
    @staticmethod
    def create_test_files_structure(base_dir, period="202401"):
        """Create a complete test files structure."""
        base_path = Path(base_dir)
        
        # Create directories
        directories = [
            "data/raw", "data/processed", "metrics", "reports", 
            "logs", "schemas/AT12"
        ]
        
        for dir_path in directories:
            (base_path / dir_path).mkdir(parents=True, exist_ok=True)
        
        # Create configuration file
        config_data = SampleDataGenerator.create_config_data(base_dir)
        config_file = base_path / "config.json"
        with open(config_file, 'w') as f:
            json.dump(config_data, f, indent=2)
        
        # Create schema file
        schema_data = SampleDataGenerator.create_at12_schema()
        schema_file = base_path / "schemas" / "AT12" / "schema_headers.json"
        with open(schema_file, 'w') as f:
            json.dump(schema_data, f, indent=2)
        
        # Create sample CSV files
        csv_files = [
            f"BASE_AT12_{period}31.CSV",
            f"DETALLE_AT12_{period}31.CSV"
        ]
        
        for csv_filename in csv_files:
            csv_content = SampleDataGenerator.create_at12_csv_content(50)
            csv_file = base_path / "data" / "raw" / csv_filename
            csv_file.write_text(csv_content)
        
        return {
            "base_dir": base_path,
            "config_file": config_file,
            "schema_file": schema_file,
            "csv_files": [base_path / "data" / "raw" / f for f in csv_files]
        }


# Convenience functions for pytest fixtures
def get_sample_at12_csv(num_rows=10):
    """Get sample AT12 CSV content."""
    return SampleDataGenerator.create_at12_csv_content(num_rows)


def get_sample_at12_schema():
    """Get sample AT12 schema."""
    return SampleDataGenerator.create_at12_schema()


def get_sample_metrics_data(exploration_id="TEST_001"):
    """Get sample metrics data."""
    return SampleDataGenerator.create_sample_metrics_data(exploration_id)


def get_corrupted_csv():
    """Get corrupted CSV content for error testing."""
    return SampleDataGenerator.create_corrupted_csv_content()