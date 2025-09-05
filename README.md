# SBP Atoms Pipeline (MVP v0.8) ✅

Regulatory atoms processing pipeline for SBP (Superintendencia Bancaria de Colombia).

## Status: PRODUCTION READY

**✅ Exploration Phase: COMPLETED**  
**✅ Header Mapping System: COMPLETED**  
**🔄 Transformation Phase: PENDING SPECIFICATIONS**

## Overview

The SBP Atoms Pipeline is a robust, auditable ETL system designed to process regulatory data files for compliance reporting. The system operates in two main phases:

### 1. Exploration Phase ✅ COMPLETED
- **Discovery**: Locate and identify regulatory files in source directories
- **Header Mapping**: Advanced header normalization and mapping system with accent removal
- **Validation**: Verify file structure, headers, and data integrity with intelligent mapping
- **Copying/Versioning**: Create versioned copies with audit trails
- **Analysis/Reporting**: Generate comprehensive metrics and PDF reports
- **Dual Format Support**: Handles both CSV and XLSX files automatically
- **Quality Metrics**: Detailed data quality analysis and statistics
- **AT02_CUENTAS Support**: Specialized handling for AT02_CUENTAS files with direct header replacement

### 2. Transformation Phase 🔄 PENDING
- **Precondition**: Validate data quality and business rules
- **Processing**: Apply join rules, deduplication, and transformations
- **Output**: Generate consolidated regulatory files
- **Status**: Awaiting functional specifications

## Architecture

- **Language**: Python 3.8+
- **CLI Orchestration**: Command-line interface for batch processing
- **Flexible Configuration**: Environment-based configuration management
- **Modularity**: Pluggable processors for different atom types
- **Auditability**: Comprehensive logging and metrics collection
- **Advanced Header Mapping**: Intelligent header normalization and mapping system

## Header Mapping System ✅

The system includes a sophisticated header mapping and normalization engine that handles various header formats and inconsistencies:

### Key Features

- **Accent Removal**: Automatically removes tildes and accents from headers (ñ → n, á → a, etc.)
- **Header Cleaning**: Removes parenthetical numbers (0), (1), extra spaces, and normalizes formatting
- **AT02_CUENTAS Specialized Mapping**: Direct header replacement for AT02_CUENTAS files using predefined schema headers
- **Fallback Normalization**: Standard normalization for other file types with uppercase conversion
- **Comprehensive Reporting**: Detailed mapping reports showing original → mapped transformations

### Supported Transformations

```python
# Examples of header transformations:
'(1) Fecha' → 'Fecha'                    # AT02_CUENTAS direct mapping
'Tipo_Depósito' → 'Tipo_Deposito'        # Accent removal
'Header (2)' → 'HEADER'                  # Standard normalization
'Identificación del cliente' → 'IDENTIFICACION_DEL_CLIENTE'  # Full normalization
```

### Usage in Processing

The header mapping system is automatically applied during file validation:

- **AT02_CUENTAS files**: Use direct replacement with predefined schema headers
- **Other file types**: Apply standard normalization (accent removal + uppercase conversion)
- **Validation reports**: Include detailed mapping information for audit trails
- **Test coverage**: Comprehensive unit and integration tests ensure reliability

## Project Structure

```
sbp-atoms/
├── src/
│   ├── core/           # Core utilities and shared components
│   │   ├── header_mapping.py  # Advanced header mapping and normalization
│   │   ├── naming.py          # Header normalization with accent removal
│   │   └── ...               # Other core utilities
│   └── AT12/           # AT12-specific processor
├── schemas/            # Data schemas and validation rules
├── source/             # Input data directory
├── data/
│   └── raw/           # Processed data with versioning
├── metrics/           # Analysis and metrics output
├── reports/           # Generated PDF reports
├── logs/              # Application logs
├── tests/             # Comprehensive test suite
│   ├── unit/          # Unit tests for individual components
│   └── integration/   # Integration tests for complete workflows
├── main.py            # Main orchestrator
├── requirements.txt   # Python dependencies
└── README.md         # This file
```

## Installation

1. Clone or download the project
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

The main script `main.py` provides four commands: `explore`, `transform`, `report`, and `tui` (interactive UI).

### Exploration Phase

To run the exploration process for a specific atom and period:

```bash
# Explore AT12 for January 2024
python main.py explore --atoms AT12 --year 2024 --month 1
```

**Arguments:**
- `--atoms`: (Optional) List of atoms to process. Default: `["AT12"]`.
- `--year`: (Optional) The year to process.
- `--month`: (Optional) The month to process (can be a number 1-12 or a name e.g., "January").
- `--workers`: (Optional) Number of parallel workers to use.
- `--verbose`: (Optional) Enable verbose logging.
- `--strict-period`: (Optional) Enforce strict period validation.

### Transformation Phase

To run the transformation process:

```bash
# Transform AT12 for January 2024
python main.py transform --atoms AT12 --year 2024 --month 1
```

**Arguments:**
- `--atoms`: (Optional) List of atoms to process. Default: `["AT12"]`.
- `--year`: (Optional) The year to process.
- `--month`: (Optional) The month to process.

### Report Generation

To generate a PDF report from a metrics file:

```bash
# Generate a report from a metrics JSON file
python main.py report --metrics-file metrics/exploration_metrics_AT12_202401__run-202401.json
```

**Arguments:**
- `--metrics-file`: (Required) Path to the metrics JSON file.
- `--output`: (Optional) Path for the output PDF file.
- `--title`: (Optional) A custom title for the report.
- `--raw-data-dir`: (Optional) Path to the raw data directory for additional analysis.

### Interactive UI (TUI)

Run the interactive terminal UI to explore, transform, and report with guided prompts, multi‑select, and search:

```bash
# Optional: ensure InquirerPy is installed for checkboxes/selects
pip install InquirerPy

# Launch the UI
    python main.py tui
```

Features:
- Explore: selecciona subtipos y archivos de `source/`, elige año/mes y ejecuta explore (usa una carpeta temporal como SOURCE_DIR).
  - Ahora incluye un submenú con opciones: “Seleccionar archivos”, “Refrescar listado” y “Volver al menú principal”.
  - El listado de archivos respeta `SBP_SOURCE_DIR` si está definido, y se refresca al volver a entrar o elegir “Refrescar listado”.
- Transform: selecciona subtipos y archivos de `data/raw` para un run y ejecuta transform (usa una carpeta temporal como RAW_DIR).
- Report: elige un JSON en `metrics/` y genera un PDF en `reports/`.
- XLSX: puedes indicar la hoja (nombre o índice 0‑based) para el conteo en resúmenes.

Notas:
- Si InquirerPy no está instalado, la UI hace fallback a prompts básicos; con InquirerPy verás checkboxes y selects interactivos.
  - Tanto la versión básica como la interactiva incluyen “Volver al menú principal” en el flujo de Explore.

#### Detailed Report Usage Guide

**Understanding Report Sections:**

1. **Análisis de Archivos**: Shows file-level information (size, rows, columns, headers)
2. **Análisis de Calidad de Datos**: Shows data quality metrics (currently limited)
3. **Análisis de Columnas**: Shows detailed column statistics (data types, null counts, unique values, etc.)

**Why Data Quality Analysis May Appear Empty:**

The "Análisis de Calidad de Datos" section may appear empty because:
- The current `MetricsCalculator` doesn't generate `quality_metrics` in the JSON file
- This section looks for specific quality metrics like completeness, accuracy, consistency scores
- Only basic file and column metrics are currently calculated

**Available Data in Reports:**
- ✅ **File Analysis**: Complete file metadata and structure information
- ✅ **Column Analysis**: Detailed statistics per column (nulls, unique values, data types, min/max values)
- ⚠️ **Data Quality Analysis**: Limited (requires enhancement to MetricsCalculator)

**To get comprehensive data quality metrics, you would need to:**
1. Enhance the `MetricsCalculator` class in `src/core/metrics.py`
2. Add quality score calculations (completeness, validity, consistency)
3. Generate `quality_metrics` dictionary in the JSON output

**Current Workaround:**
Use the "Análisis de Columnas" section which provides:
- Null count and percentage per column
- Data type validation
- Value distribution analysis
- Min/max value ranges
- Unique value counts

#### Report Generation Examples

**Basic Report Generation:**
```bash
# Generate report from exploration metrics
python main.py report --metrics-file metrics/exploration_metrics_AT12_202401__run-202401.json
```

**Custom Output Location:**
```bash
# Specify custom output path
python main.py report --metrics-file metrics/exploration_metrics_AT12_202401__run-202401.json --output reports/custom_report.pdf
```

**With Custom Title:**
```bash
# Add custom title to report
python main.py report --metrics-file metrics/exploration_metrics_AT12_202401__run-202401.json --title "AT12 Analysis Report - January 2024"
```

#### Troubleshooting Reports

**Common Issues:**

1. **"Análisis de Calidad de Datos" section is empty**
   - **Cause**: No `quality_metrics` generated in the metrics JSON
   - **Solution**: This is expected behavior with current implementation
   - **Alternative**: Check "Análisis de Columnas" for data quality insights

2. **Report generation fails**
   - **Cause**: Invalid metrics file path or corrupted JSON
   - **Solution**: Verify the metrics file exists and contains valid JSON
   ```bash
   # Check if metrics file exists
   ls -la metrics/exploration_metrics_AT12_202401__run-202401.json
   
   # Validate JSON format
   python -m json.tool metrics/exploration_metrics_AT12_202401__run-202401.json
   ```

3. **Missing file information in reports**
   - **Cause**: Files were not properly processed during exploration phase
   - **Solution**: Re-run the exploration phase first
   ```bash
   python main.py explore --atoms AT12 --year 2024 --month 1
   ```

**Report Output Location:**
- Default: `reports/exploration_metrics_[ATOM]_[PERIOD]__run-[RUN_ID]_report.pdf`
- Custom: Use `--output` parameter to specify different location

## Configuration

The system uses environment variables and configuration files:

### File Processing
- `INPUT_DELIMITER`: Input CSV delimiter (default: ',')
- `CSV_ENCODING`: CSV file encoding (default: 'utf-8')
- `CSV_QUOTECHAR`: CSV quote character (default: '"')
- `XLSX_SHEET_NAME`: Excel sheet name or index (default: 0)

### Directories
- `SBP_BASE_DIR`: Base directory for the pipeline
- `SBP_SOURCE_DIR`: Source data directory
- `SBP_OUTPUT_DELIMITER`: Output file delimiter (default: '|')
- `SBP_TRAILING_DELIMITER`: Include trailing delimiter (default: false)

## Current Status

**✅ COMPLETED (Production Ready):**
- ✅ Core infrastructure and utilities
- ✅ AT12 exploration phase (complete workflow)
- ✅ File discovery and validation
- ✅ Metrics calculation and reporting

## AT12 Transformation Highlights (TDC updates)

- TDC Número_Garantía (por run):
  - Limpia la columna y ordena por `Id_Documento` en orden ascendente.
  - Llave: (`Id_Documento`, `Tipo_Facilidad`) — se excluye `Numero_Prestamo`.
  - Asigna números secuenciales desde 855,500 y reutiliza para la misma llave.
  - Repetidos por `Numero_Prestamo` dentro de la misma llave: solo log (sin incidencias).
- Mapeo de fechas (TDC ↔ AT02_CUENTAS):
  - Join por `Id_Documento` (TDC) ↔ `identificacion_de_cuenta` (AT02).
  - `Fecha_Última_Actualización` ← `Fecha_inicio` (AT02) y `Fecha_Vencimiento` ← `Fecha_Vencimiento` (AT02).
- Inconsistencia `Tarjeta_repetida` (solo CSV):
  - Detecta duplicados excluyendo `Numero_Prestamo` con prioridad de clave: (1) `Identificacion_cliente`,`Identificacion_Cuenta`,`Tipo_Facilidad`; si no, (2) `Id_Documento`,`Tipo_Facilidad`.
  - Exporta `data/processed/transforms/AT12/incidencias/INC_REPEATED_CARD_TDC_AT12_<PERIODO>.csv` con filas completas.
- ✅ PDF report generation with detailed statistics
- ✅ Comprehensive logging and audit trails
- ✅ Dual format support (CSV + XLSX)
- ✅ Complete test suite (70 tests passing)
- ✅ Integration tests for end-to-end workflows

**🔄 PENDING:**
- ⏳ Additional atom types (AT03, etc.)
- ⏳ Extra validations per regulator updates

## AT12 Stage 1 Errors (Resumen)

- EEOR_TABULAR: limpieza de espacios (trim y colapsar múltiples espacios). Incidencia global: `EEOR_TABULAR_[SUBTIPO]_[YYYYMMDD].csv`.
- ERROR_0301 (Id_Documento, hipotecas): reglas por posición (1‑based). Exporta solo filas con cambio a `ERROR_0301_[SUBTIPO]_[YYYYMMDD].csv`. Ver detalles en `context_files/transform_context.md`.
- FECHA_CANCELACION_ERRADA: corrige `Fecha_Vencimiento` fuera de rango a `21001231` (exporta subconjunto por filas cambiadas).
- FECHA_AVALUO_ERRADA: corrige `Fecha_Ultima_Actualizacion` usando AT03 (exporta subconjunto por filas cambiadas).
- Reglas adicionales (polizas, avaluadora, etc.): ver `context_files/transform_context.md`.

## Testing

The project includes a comprehensive test suite:

```bash
# Run all tests
python -m pytest tests/ -v

# Run only unit tests
python -m pytest tests/unit/ -v

# Run only integration tests
python -m pytest tests/integration/ -v
```

**Test Coverage:**
- **Unit Tests**: 60 tests covering all core components
- **Integration Tests**: 10 tests covering complete workflows
- **Success Rate**: 100% (70 passed, 2 skipped)
- **Components Tested**: AT12 processor, I/O utilities, configuration, filesystem operations, PDF generation

## Supported File Formats

The pipeline supports both CSV and Excel formats:

### CSV Files
- **Extensions**: `.csv`
- **Encoding**: UTF-8 (configurable)
- **Delimiter**: Comma (configurable)
- **Quote Character**: Double quote (configurable)

### Excel Files
- **Extensions**: `.xlsx`, `.xls`
- **Sheet Selection**: By name or index (configurable)
- **Data Types**: All data read as strings for consistency

## Data Schema

Currently supports:
- **BASE_AT12**: Primary AT12 subtype with 85 fields including loan details, borrower information, guarantees, and financial metrics
- **File Patterns**: 
  - `BASE_AT12_[YYYYMMDD].CSV`
  - `BASE_AT12_[YYYYMMDD].XLSX`

## Contributing

This is an internal regulatory compliance tool. All changes must be reviewed and approved according to SBP guidelines.

See `AGENTS.md` for the contributor guide and agent roles.

## License

Internal use only - SBP Regulatory Compliance.

## English Context Docs

- Architecture: `context_files/en/architecture.md`
- Functional Context: `context_files/en/functional_context.md`
- Technical Context: `context_files/en/technical_context.md`
- Transform Context (AT12): `context_files/en/transform_context.md`
