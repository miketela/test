# Transform Context — AT12 Transformation Process

## Overview

The AT12 transformation process includes a robust evidence and correction system that allows tracking all changes applied to data during the transformation process. This system is designed to maintain transparency and facilitate collaboration with the operations team.

## AT12 Validation Approach

The system handles multiple types of specific validations organized into 4 main phases, each with particular behaviors. The process works as follows:

### System Characteristics
- **100% Pandas**: All transformations and joins with other tables are handled in pandas
- **Name preservation**: The main file maintains its original name
- **Complete traceability**: Each incident is recorded in separate files
- **Data joins**: Capability for validations requiring multiple tables
- **Phase processing**: Organized in 4 sequential transformation phases

## Directory Structure for Transformation

```
transforms/
├── AT12/
│   ├── incidencias/
│   │   ├── EEOO_TABULAR_AT12_BASE_20250131.csv
│   │   ├── FORMATO_FECHA_AT12_PRINCIPAL_20250131.csv
│   │   ├── DUPLICADOS_AT12_GARANTIAS_20250131.csv
│   │   └── VALORES_NULOS_AT12_SUBSIDIARIAS_20250131.csv
│   ├── procesados/
│   │   ├── AT12_BASE_20250131.csv (archivo corregido)
│   │   ├── AT12_PRINCIPAL_20250131.csv (archivo corregido)
│   │   └── AT12_GARANTIAS_20250131.csv (archivo corregido)
│   └── consolidated/
│       └── AT12_CONSOLIDATED_20250131__run-202501.TXT
└── AT03/
    └── ...
```

## AT12 Transformation Process

### Explicit Technical Guide for Validation and Correction

The AT12 transformation process is organized into 4 sequential phases, each with specific business rules implemented completely in pandas:

#### **Phase 1: Error Correction in `BASE_AT12`**

This phase corrects structural and format errors in the base file before processing specific atoms.

**1.1. EEOR TABULAR: Whitespace Errors**
*   **Objective:** To standardize the content of text fields by removing unnecessary spaces that can cause errors in data joins or length validations.
*   **Input Identification:** Any text-based field within the `BASE_AT12` table (e.g., `Id_Documento`, `Nombre_Organismo`, `Numero_Prestamo`).
*   **Detailed Process (Logic):**
    1.  Remove all leading spaces from the field.
    2.  Remove all trailing spaces from the field.
    3.  Replace any sequence of two or more spaces in the middle of the field with a single space.
*   **Final Action (Output):** The field is cleaned of all excess whitespace.

**1.2. Error `0301`: `Id_Documento` Logic for Mortgage Guarantees**
*   **Objective:** To validate and correct the length and format of the `Id_Documento` field according to complex business rules for the mortgage guarantee type.
*   **Input Identification:** Filter all records where `Tipo_Garantia` = '0301'.
*   **Detailed Process (Logic):** For each filtered record, apply the following validation sequence:
    1.  **Sub-Rule 1 (Length 10 for Specific Types):**
        *   **Condition:** `IF` the characters at positions 9 and 10 (1-based index from the left) of the `Id_Documento` are '01', '41', or '42'.
        *   **Process:** Check the string length. `IF` it is longer than 10 (e.g., `'0000000100008482'`), apply a substring function to extract the rightmost 10 characters.
        *   **Example:** `'0000000100008482'` becomes `'0100008482'`. `IF` it is shorter than 10, the record is flagged for manual review.
        *   **Output:** `Id_Documento` with exactly 10 characters.
    2.  **Sub-Rule 2 (Length 10 or 11 for Type 701):**
        *   **Condition:** `ELSE IF` the last 3 characters of the `Id_Documento` are '701'.
        *   **Process:** Check the length. `IF` the length is 10 or 11, the value is considered **valid** and is not modified. `IF` the length is 10, it must be included in a follow-up report, but the data itself is not altered.
        *   **Example:** A 10-digit `Id_Documento` ending in '701' is valid; an 11-digit one is also valid.
        *   **Output:** `Id_Documento` remains unchanged.
    3.  **Sub-Rule 3 (Length 11 for Other Types):**
        *   **Condition:** `ELSE IF` the first 3 characters of the `Id_Documento` are '100', '110', '120', '123', or '810'.
        *   **Process:** Check the length. `IF` it is longer than 11, extract the first 11 characters. `IF` it is shorter than 11, pad the string with zeros at the end (right padding) until it reaches 11 characters.
        *   **Output:** `Id_Documento` with exactly 11 characters.

**1.3. COMA EN FINCA EMPRESA**
*   **Objective:** To remove disallowed characters from the document identifier.
*   **Input Identification:** Records where `Id_Documento` contains a comma character (`,`).
*   **Detailed Process (Logic):** Apply a text replacement function to substitute all occurrences of ',' with an empty string ('').
*   **Final Action (Output):** The `Id_Documento` field is free of commas.

**1.4. Fecha Cancelación Errada (Incorrect Cancellation Date)**
*   **Objective:** To correct expiration dates that are clearly erroneous or outside a logical range.
*   **Input Identification:** Records where the year of the `Fecha_Vencimiento` field is > 2100 or < 1985.
*   **Detailed Process (Logic):**
    1.  Flag the record as an error for reporting purposes.
    2.  Overwrite the value of the `Fecha_Vencimiento` field with the constant `21001201`.
*   **Final Action (Output):** The `Fecha_Vencimiento` field contains `21001201`.

**1.5. Fecha Avalúo Errada (Incorrect Appraisal Date)**
*   **Objective:** To standardize and correct appraisal update dates that are inconsistent or improperly formatted.
*   **Input Identification:** Records that meet ANY of these conditions:
    *   `Fecha_Ultima_Actualizacion` > (the last day of the month prior to the processing date).
    *   The year of `Fecha_Ultima_Actualizacion` < 1985.
    *   `Fecha_Ultima_Actualizacion` does not follow the `YYYYMMDD` format (e.g., `'2011201'`).
*   **Detailed Process (Logic):**
    1.  Perform a `JOIN` (or `LOOKUP`) with the `AT03_CREDITOS` table using `Numero_Prestamo` as the key.
    2.  Retrieve the value from the `fec_ini_prestamo` field from the `AT03_CREDITOS` table.
    3.  Overwrite the value of the `Fecha_Ultima_Actualizacion` field in `BASE_AT12` with the retrieved value.
*   **Final Action (Output):** The `Fecha_Ultima_Actualizacion` field equals the loan's start date.

**1.6. Inmuebles sin Póliza (Properties without Policy)**
*   **Objective:** To assign a policy type to property guarantees where it is missing.
*   **Input Identification:** Records where (`Tipo_Garantia` = '0207' OR `Tipo_Garantia` = '0208') AND `Tipo_Poliza` is empty.
*   **Detailed Process (Logic):**
    *   **Case `0207`:** Perform a `JOIN` with `POLIZA_HIPOTECAS_AT12` using `Numero_Prestamo` = `numcred`. `IF` a match is found and the `seguro_incendio` field has a value, assign the corresponding value ('01' or '02') to `Tipo_Poliza`.
    *   **Case `0208`:** Assign the constant value '01' to the `Tipo_Poliza` field.
*   **Final Action (Output):** The `Tipo_Poliza` field is populated.

**1.7. Inmuebles sin Finca (Properties without Cadastral ID)**
*   **Objective:** To clean and standardize document identifiers for properties that are empty or contain junk data.
*   **Input Identification:** Records where `Tipo_Garantia` IN ('0207', '0208', '0209') AND `Id_Documento` is empty or exactly matches one of the following invalid values:
    ```
    [
      "0/0",
      "1/0",
      "1/1",
      "1",
      "9999/1",
      "0/1",
      "0"
    ]
    ```
*   **Detailed Process (Logic):** Overwrite the value of the `Id_Documento` field with the constant **'9999/9999'**.
*   **Final Action (Output):** The `Id_Documento` field contains '9999/9999'.

**1.8. Póliza Auto Comercial (Commercial Auto Policy)**
*   **Objective:** To assign an organization code to auto policies that are missing it.
*   **Input Identification:** Records where `Tipo_Garantia` = '0106' AND `Nombre_Organismo` is empty.
*   **Detailed Process (Logic):** Assign the constant value '700' to the `Nombre_Organismo` field.
*   **Final Action (Output):** The `Nombre_Organismo` field contains '700'.

**1.9. Error en Póliza de Auto (Error in Auto Policy)**
*   **Objective:** To populate the missing policy number in auto guarantees.
*   **Input Identification:** Records where `Tipo_Garantia` = '0101' AND `Id_Documento` is empty.
*   **Detailed Process (Logic):** Perform a `JOIN` with `GARANTIA_AUTOS_AT12` using `Numero_Prestamo` = `numcred`. Take the value from `num_poliza` and assign it to `Id_Documento`.
*   **Final Action (Output):** The `Id_Documento` field is populated with the policy number.

**1.10. Inmueble sin Avaluadora (Property without Appraiser)**
*   **Objective:** To assign an organization code to properties that are missing it.
*   **Input Identification:** Records where `Tipo_Garantia` IN ('0207', '0208', '0209') AND `Nombre_Organismo` is empty.
*   **Detailed Process (Logic):** Assign the constant value '774' to the `Nombre_Organismo` field.
*   **Final Action (Output):** The `Nombre_Organismo` field contains '774'.

#### **Phase 2: Input Processing to Generate and Enrich Atoms**

**2.1. `TDC_AT12` (Credit Cards)**
*   **Objective:** To generate unique guarantee codes and enrich the "atom" with dates from the source account.
*   **Detailed Process (Logic):**
    1.  **`Número_Garantía` Generation:**
        *   **Condition:** `Número_Garantía` is empty.
        *   **Logic:** For each record, create a unique key by combining `Id_Documento` + `Numero_Prestamo` + `Tipo_Facilidad`. Iterate through the records. `IF` the unique key has not been seen before, assign it the next number from the sequence (starting at 855,500) and store the key and its assigned number. `IF` the key has been seen before, assign it the previously stored number.
    2.  **Date Mapping:**
        *   Perform a `JOIN` between `TDC_AT12` and `AT02_CUENTAS` using `Identificacion_cliente` and `Identificacion_Cuenta`.
        *   Update `Fecha_Última_Actualización` in `TDC_AT12` with the value from `Fecha_proceso` in `AT02_CUENTAS`.
        *   Update `Fecha_Vencimiento` in `TDC_AT12` with the value from `Fecha_Vencimiento` in `AT02_CUENTAS`.

**2.2. `SOBREGIRO_AT12` (Overdrafts)**
*   **Objective:** To enrich the "atom" with dates from the source account.
*   **Detailed Process (Logic):** Apply the same `JOIN` and date mapping logic as described in step 2.1. The `Numero_Garantia` field is not modified.

**2.3. `VALORES_AT12` (Securities)**
*   **Objective:** To construct the securities "atom" with the complete, final column structure.
*   **Detailed Process (Logic):**
    1.  Use the complete list of required output columns as a template.
    2.  Select a reference record from `BASE_AT12` where `Tipo_Garantia` = '0507'.
    3.  Populate the new `VALORES_AT12` "atom". Fields that are missing in the original source but required in the final structure (e.g., `Clave_Pais`, `Clave_Empresa`) are filled with the values from the reference record.
*   **Result:** A new securities "atom" that complies with the final required structure.

#### **Phase 3: `FUERA_CIERRE_AT12` (Out of Closing Cycle Loans) (Definition Pending)**
*   **Objective:** To generate a report of loans outside the closing cycle for the Operations team to complete missing information.
*   **Detailed Process (Logic):**
    1.  Execute the query that generates the `AFECTACIONES_AT12` input.
    2.  Create an Excel file with three tabs: "DESEMBOLSO", "CARTERA", and "PYME".
    3.  Distribute the query data into these tabs (distribution logic is pending definition).
    4.  For each record, if `at_tipo_operacion` = '0301', fill the `Nombre_Organismo` field in the report with '182'. Other fields to be filled by Operations are left empty.

#### **Phase 4: `VALOR_MINIMO_AVALUO_AT12` (Minimum Appraisal Value) Process (Pending)**
*   **Objective:** To validate if the guarantee's value is sufficient to cover the loan's balance and apply the corresponding correction.
*   **Detailed Process (Logic):**
    1.  Generate the `VALOR_MINIMO_AVALUO_AT12` input.
    2.  Filter where `cu_tipo` = 'letras'.
    3.  Perform a `JOIN` with `AT03_CREDITOS` using `at_num_de_prestamos` = `num_cta`.
    4.  For each record, compare `saldo` (from `AT03`) with `nuevo_at_valor_garantia` (from `VALOR_MINIMO`).
        *   `IF` `saldo` > `nuevo_at_valor_garantia` (Problem): The record is reported. For the final load, the original values `at_valor_garantia` and `at_valor_pond_garantia` are used.
        *   `IF` `saldo` <= `nuevo_at_valor_garantia` (Correct): For the final load, the updated values `nuevo_at_valor_garantia` and `nuevo_at_valor_pond_garantia` are used.

### Implementation Approach

#### Pandas-Based Processing

All transformations are implemented using pandas to ensure:

1. **Efficiency**: Vectorized operations for processing large data volumes
2. **Consistency**: Use of boolean masks to identify records requiring correction
3. **Traceability**: Automatic generation of incident files for each correction applied
4. **Flexibility**: Capability to perform complex JOINs between multiple tables

#### Evidence and Incident Tracking

Each transformation phase automatically generates:
- **Processed files**: Corrected data maintaining the original name
- **Incident files**: Detailed record of each correction applied
- **Validation reports**: Summary of transformations by phase

#### Integration Points

The transformations require access to the following data sources:
- `BASE_AT12`: Main guarantee file
- `AT02_CUENTAS`: For date enrichment
- `AT03_CREDITOS`: For appraisal date correction
- `POLIZA_HIPOTECAS_AT12`: For policy assignment
- `GARANTIA_AUTOS_AT12`: For auto policy correction

### 6. Technical Implementation Guidelines

#### Pandas Operations Strategy:

1. **Vectorized Operations**: Use pandas vectorized functions for all data transformations
2. **Boolean Masking**: Identify records requiring correction using boolean masks
3. **Memory Efficiency**: Process data in chunks for large files
4. **Data Type Optimization**: Use appropriate dtypes to minimize memory usage

#### Phase Processing Pattern:

```python
# Example pattern for each phase
def process_phase(df, phase_name):
    # Create boolean mask for records needing correction
    mask = identify_records_needing_correction(df)
    
    # Apply corrections using vectorized operations
    df.loc[mask, 'column'] = apply_correction(df.loc[mask, 'column'])
    
    # Generate incident report
    incidents = create_incident_report(df[mask], phase_name)
    
    return df, incidents
```

#### Data Integration Approach:

1. **JOIN Operations**: Use pandas merge for combining data from multiple sources
2. **Data Enrichment**: Enhance BASE_AT12 with information from auxiliary tables
3. **Validation Cascading**: Apply corrections in sequence, with each phase building on previous results
4. **Output Generation**: Maintain original file structure while applying all corrections

#### Quality Assurance:

- **Before/After Comparison**: Track changes made in each phase
- **Data Integrity Checks**: Validate that corrections don't introduce new errors
- **Performance Monitoring**: Track processing time and memory usage
- **Incident Documentation**: Generate detailed reports for all corrections applied

#### Transformation Metrics and Reports:

At the end of the transformation process, a consolidated report is generated that includes:

1. **Phase-by-Phase Summary**: Results from each of the four transformation phases
2. **Data Quality Metrics**: Before/after comparison of data quality indicators
3. **Processing Performance**: Execution time and memory usage statistics
4. **Incident Summary**: Count and categorization of all corrections applied

#### Output Files Generated:

- **Processed Data**: `BASE_AT12_YYYYMMDD_processed.csv` - Main file with all corrections applied
- **Phase Incidents**: Individual CSV files for each phase documenting specific corrections
- **Consolidated Report**: JSON summary of the entire transformation process
- **Quality Dashboard**: Summary statistics for operations team review

#### Success Criteria:

- All four phases complete without critical errors
- Data integrity maintained throughout the process
- All corrections properly documented in incident files
- Output file maintains original structure and record count
- Processing completes within acceptable time limits

Example final report:
```json
{
  "run_id": "202501",
  "completion_timestamp": "2025-01-31T11:15:30Z",
  "subtypes_processed": {
    "AT12_BASE": {
      "total_records": 8420,
      "corrected_records": 145,
      "processed_file": "transforms/AT12/2025/01/procesados/AT12_BASE_20250131.csv"
    },
    "AT12_PRINCIPAL": {
      "total_records": 6200,
      "corrected_records": 89,
      "processed_file": "transforms/AT12/2025/01/procesados/AT12_PRINCIPAL_20250131.csv"
    },
    "AT12_GARANTIAS": {
      "total_records": 3800,
      "corrected_records": 23,
      "processed_file": "transforms/AT12/2025/01/procesados/AT12_GARANTIAS_20250131.csv"
    }
  },
  "validations_applied": {
    "EEOO_TABULAR": 145,
    "FORMATO_FECHA": 23,
    "DUPLICADOS": 8,
    "VALORES_NULOS": 67,
    "RANGOS_NUMERICOS": 12
  },
  "incident_files_generated": [
    "EEOO_TABULAR_AT12_BASE_20250131.csv",
    "FORMATO_FECHA_AT12_PRINCIPAL_20250131.csv",
    "DUPLICADOS_AT12_GARANTIAS_20250131.csv",
    "VALORES_NULOS_AT12_SUBSIDIARIAS_20250131.csv"
  ],
  "consolidated_file": "transforms/AT12/2025/01/consolidated/AT12_CONSOLIDATED_20250131__run-202501.TXT",
  "processing_time_seconds": 245,
  "status": "completed_with_incidents",
  "quality_summary": {
    "percent_clean_records": 85.2,
    "critical_validations_passed": true,
    "manual_review_required": false
  }
}
```

This pandas-based approach ensures efficient processing of large AT12 datasets while maintaining complete traceability and data quality standards required for regulatory compliance.

## Technical Considerations

### Pandas Performance Optimization

1. **Vectorized Operations**: Leverage pandas' vectorized functions for maximum performance
2. **Memory Management**: Use chunking for large files and optimize data types
3. **Efficient Joins**: Optimize merge operations between BASE_AT12 and auxiliary tables
4. **Index Utilization**: Set appropriate indexes for fast lookups during data enrichment

### Data Processing Architecture

1. **Sequential Phase Processing**: Execute the four phases in order, with each building on previous results
2. **Error Isolation**: Contain errors within phases to prevent cascade failures
3. **Checkpoint System**: Save intermediate results between phases for recovery
4. **Resource Monitoring**: Track memory and CPU usage during large dataset processing

### Integration Requirements

1. **File System Access**: Read/write permissions for input and output directories
2. **Data Source Connectivity**: Access to AT02_CUENTAS, AT03_CREDITOS, and auxiliary tables
3. **Logging Infrastructure**: Integration with centralized logging for audit trails
4. **Notification System**: Alerts for processing completion and error conditions

### Quality Assurance Framework

1. **Data Validation**: Pre and post-processing validation checks
2. **Regression Testing**: Automated tests to ensure consistent transformation results
3. **Performance Benchmarking**: Regular performance testing with representative datasets
4. **Compliance Monitoring**: Ensure all transformations meet regulatory requirements

### Security and Maintenance

- Evidence files do not contain sensitive PII information
- Access controls are implemented for the `transforms/` folder
- Files are encrypted at rest if they contain sensitive data
- Automatic rotation of evidence files after 24 months
- Compression of old files to optimize space
- Monitoring of `transforms/` folder size

This technical framework supports the pandas-based AT12 transformation system while ensuring scalability, reliability, and regulatory compliance.

## Transformation System Vision

The pandas-based AT12 transformation system is designed to be:

1. **Efficient**: Leverages pandas vectorized operations for optimal performance on large datasets
2. **Systematic**: Implements a four-phase approach ensuring comprehensive data correction
3. **Traceable**: Documents every correction with detailed incident reporting
4. **Reliable**: Maintains data integrity throughout the transformation process
5. **Scalable**: Handles varying data volumes through optimized memory management
6. **Compliant**: Meets regulatory requirements for financial data processing

This system transforms raw AT12 guarantee data through systematic error correction, data enrichment, and validation processes, producing high-quality datasets that support critical business decisions while maintaining complete audit trails for regulatory compliance.

### Key Benefits:

- **Data Quality**: Systematic correction of common data issues across all guarantee types
- **Operational Efficiency**: Automated processing reduces manual intervention requirements
- **Regulatory Compliance**: Complete documentation of all data transformations
- **Business Intelligence**: Clean, consistent data enables accurate reporting and analysis
- **Risk Management**: Proper validation of guarantee values and loan relationships

### Advantages of the Pandas-Based Approach

1.  **Performance**: Vectorized operations provide superior performance for large datasets
2.  **Memory Efficiency**: Optimized data types and chunking handle large files effectively
3.  **Data Integration**: Seamless joining of multiple data sources (AT02, AT03, auxiliary tables)
4.  **Systematic Processing**: Four-phase approach ensures comprehensive data correction
5.  **Transparency**: Complete documentation of all transformations and corrections
6.  **Maintainability**: Clean, readable pandas code that's easy to understand and modify
7.  **Scalability**: Efficient processing scales with data volume growth
8.  **Quality Assurance**: Built-in validation and error checking at each phase

### Operational Workflow

1. **Input Preparation**: Raw AT12 file and auxiliary data sources are validated and prepared
2. **Phase 1 - Error Correction**: Apply basic data cleaning and format corrections to BASE_AT12
3. **Phase 2 - Data Enrichment**: Integrate with AT02_CUENTAS and AT03_CREDITOS for date corrections
4. **Phase 3 - Business Logic**: Apply out-of-closing-cycle filters and policy assignments
5. **Phase 4 - Final Validation**: Validate minimum appraisal values and generate final output
6. **Output Generation**: 
   - Processed main file with all corrections applied
   - Phase-specific incident files documenting corrections
   - Consolidated transformation report
7. **Quality Review**: Operations team reviews incident files and transformation metrics
8. **Integration**: Processed file is delivered to downstream systems

This comprehensive pandas-based approach ensures that AT12 guarantee data is transformed systematically through four distinct phases, with complete traceability and quality assurance, supporting both operational efficiency and regulatory compliance requirements.

```
Input: BASE_AT12_20250131.csv + Auxiliary Data Sources
  ↓
[Phase 1: Error Correction in BASE_AT12]
  ↓
[Phase 2: Data Enrichment with AT02/AT03]
  ↓
[Phase 3: Out-of-Closing-Cycle Processing]
  ↓
[Phase 4: Minimum Appraisal Value Validation]
  ↓
Outputs:
├── BASE_AT12_20250131_processed.csv (corrected main file)
├── incidencias_phase1_error_correction_20250131.csv
├── incidencias_phase2_data_enrichment_20250131.csv
├── incidencias_phase3_business_logic_20250131.csv
├── incidencias_phase4_final_validation_20250131.csv
└── transformation_report_AT12_20250131.json
```

### Integration with Superintendence

-   **Clean Files**: Ready for submission without formatting errors.
-   **Documentation**: Incident files serve as evidence of cleaning.
-   **Compliance**: Complete traceability for audits.
-   **Efficiency**: Significant reduction in rejections due to formatting errors.

### Next Steps

1.  **Define the 20 specific validations** based on historical errors from the Superintendence.
2.  **Implement the validation engine** using pandas with the demonstrated pattern.
3.  **Create a JSON configuration** with all validation rules.
4.  **Develop a reporting system** for data quality tracking.
5.  **Integrate with the existing** AT12 exploration and processing flow.

This transformation system with incident files ensures transparency, traceability, and effective collaboration with the operations team, while maintaining the integrity and quality of processed data.

---

## Conclusion

This comprehensive pandas-based AT12 transformation system provides a robust, scalable, and traceable approach to guarantee data processing. The four-phase methodology ensures systematic error correction, data enrichment, and validation while maintaining complete audit trails for regulatory compliance.

The system's design prioritizes operational efficiency, data quality, and regulatory compliance, making it an essential component of the financial data processing infrastructure.

#### **Phase 1: Error Correction in `BASE_AT12`**

**1.1. EEOR TABULAR: Whitespace Errors**
*   **Objective:** To standardize the content of text fields by removing unnecessary spaces that can cause errors in data joins or length validations.
*   **Input Identification:** Any text-based field within the `BASE_AT12` table (e.g., `Id_Documento`, `Nombre_Organismo`, `Numero_Prestamo`).
*   **Detailed Process (Logic):**
    1.  Remove all leading spaces from the field.
    2.  Remove all trailing spaces from the field.
    3.  Replace any sequence of two or more spaces in the middle of the field with a single space.
*   **Final Action (Output):** The field is cleaned of all excess whitespace.

**1.2. Error `0301`: `Id_Documento` Logic for Mortgage Guarantees**
*   **Objective:** To validate and correct the length and format of the `Id_Documento` field according to complex business rules for the mortgage guarantee type.
*   **Input Identification:** Filter all records where `Tipo_Garantia` = '0301'.
*   **Detailed Process (Logic):** For each filtered record, apply the following validation sequence:
    1.  **Sub-Rule 1 (Length 10 for Specific Types):**
        *   **Condition:** `IF` the characters at positions 9 and 10 (1-based index from the left) of the `Id_Documento` are '01', '41', or '42'.
        *   **Process:** Check the string length. `IF` it is longer than 10 (e.g., `'0000000100008482'`), apply a substring function to extract the rightmost 10 characters.
        *   **Example:** `'0000000100008482'` becomes `'0100008482'`. `IF` it is shorter than 10, the record is flagged for manual review.
        *   **Output:** `Id_Documento` with exactly 10 characters.
    2.  **Sub-Rule 2 (Length 10 or 11 for Type 701):**
        *   **Condition:** `ELSE IF` the last 3 characters of the `Id_Documento` are '701'.
        *   **Process:** Check the length. `IF` the length is 10 or 11, the value is considered **valid** and is not modified. `IF` the length is 10, it must be included in a follow-up report, but the data itself is not altered.
        *   **Example:** A 10-digit `Id_Documento` ending in '701' is valid; an 11-digit one is also valid.
        *   **Output:** `Id_Documento` remains unchanged.
    3.  **Sub-Rule 3 (Length 11 for Other Types):**
        *   **Condition:** `ELSE IF` the first 3 characters of the `Id_Documento` are '100', '110', '120', '123', or '810'.
        *   **Process:** Check the length. `IF` it is longer than 11, extract the first 11 characters. `IF` it is shorter than 11, pad the string with zeros at the end (right padding) until it reaches 11 characters.
        *   **Output:** `Id_Documento` with exactly 11 characters.

**1.3. COMA EN FINCA EMPRESA**
*   **Objective:** To remove disallowed characters from the document identifier.
*   **Input Identification:** Records where `Id_Documento` contains a comma character (`,`).
*   **Detailed Process (Logic):** Apply a text replacement function to substitute all occurrences of ',' with an empty string ('').
*   **Final Action (Output):** The `Id_Documento` field is free of commas.

**1.4. Fecha Cancelación Errada (Incorrect Cancellation Date)**
*   **Objective:** To correct expiration dates that are clearly erroneous or outside a logical range.
*   **Input Identification:** Records where the year of the `Fecha_Vencimiento` field is > 2100 or < 1985.
*   **Detailed Process (Logic):**
    1.  Flag the record as an error for reporting purposes.
    2.  Overwrite the value of the `Fecha_Vencimiento` field with the constant `21001201`.
*   **Final Action (Output):** The `Fecha_Vencimiento` field contains `21001201`.

**1.5. Fecha Avalúo Errada (Incorrect Appraisal Date)**
*   **Objective:** To standardize and correct appraisal update dates that are inconsistent or improperly formatted.
*   **Input Identification:** Records that meet ANY of these conditions:
    *   `Fecha_Ultima_Actualizacion` > (the last day of the month prior to the processing date).
    *   The year of `Fecha_Ultima_Actualizacion` < 1985.
    *   `Fecha_Ultima_Actualizacion` does not follow the `YYYYMMDD` format (e.g., `'2011201'`).
*   **Detailed Process (Logic):**
    1.  Perform a `JOIN` (or `LOOKUP`) with the `AT03_CREDITOS` table using `Numero_Prestamo` as the key.
    2.  Retrieve the value from the `fec_ini_prestamo` field from the `AT03_CREDITOS` table.
    3.  Overwrite the value of the `Fecha_Ultima_Actualizacion` field in `BASE_AT12` with the retrieved value.
*   **Final Action (Output):** The `Fecha_Ultima_Actualizacion` field equals the loan's start date.

**1.6. Inmuebles sin Póliza (Properties without Policy)**
*   **Objective:** To assign a policy type to property guarantees where it is missing.
*   **Input Identification:** Records where (`Tipo_Garantia` = '0207' OR `Tipo_Garantia` = '0208') AND `Tipo_Poliza` is empty.
*   **Detailed Process (Logic):**
    *   **Case `0207`:** Perform a `JOIN` with `POLIZA_HIPOTECAS_AT12` using `Numero_Prestamo` = `numcred`. `IF` a match is found and the `seguro_incendio` field has a value, assign the corresponding value ('01' or '02') to `Tipo_Poliza`.
    *   **Case `0208`:** Assign the constant value '01' to the `Tipo_Poliza` field.
*   **Final Action (Output):** The `Tipo_Poliza` field is populated.

**1.7. Inmuebles sin Finca (Properties without Cadastral ID)**
*   **Objective:** To clean and standardize document identifiers for properties that are empty or contain junk data.
*   **Input Identification:** Records where `Tipo_Garantia` IN ('0207', '0208', '0209') AND `Id_Documento` is empty or exactly matches one of the following invalid values:
    ```
    [
      "0/0",
      "1/0",
      "1/1",
      "1",
      "9999/1",
      "0/1",
      "0"
    ]
    ```*   **Detailed Process (Logic):** Overwrite the value of the `Id_Documento` field with the constant **'9999/9999'**.
*   **Final Action (Output):** The `Id_Documento` field contains '9999/9999'.

**1.8. Póliza Auto Comercial (Commercial Auto Policy)**
*   **Objective:** To assign an organization code to auto policies that are missing it.
*   **Input Identification:** Records where `Tipo_Garantia` = '0106' AND `Nombre_Organismo` is empty.
*   **Detailed Process (Logic):** Assign the constant value '700' to the `Nombre_Organismo` field.
*   **Final Action (Output):** The `Nombre_Organismo` field contains '700'.

**1.9. Error en Póliza de Auto (Error in Auto Policy)**
*   **Objective:** To populate the missing policy number in auto guarantees.
*   **Input Identification:** Records where `Tipo_Garantia` = '0101' AND `Id_Documento` is empty.
*   **Detailed Process (Logic):** Perform a `JOIN` with `GARANTIA_AUTOS_AT12` using `Numero_Prestamo` = `numcred`. Take the value from `num_poliza` and assign it to `Id_Documento`.
*   **Final Action (Output):** The `Id_Documento` field is populated with the policy number.

**1.10. Inmueble sin Avaluadora (Property without Appraiser)**
*   **Objective:** To assign an organization code to properties that are missing it.
*   **Input Identification:** Records where `Tipo_Garantia` IN ('0207', '0208', '0209') AND `Nombre_Organismo` is empty.
*   **Detailed Process (Logic):** Assign the constant value '774' to the `Nombre_Organismo` field.
*   **Final Action (Output):** The `Nombre_Organismo` field contains '774'.

---

#### **Phase 2: Input Processing to Generate and Enrich Atoms**

**2.1. `TDC_AT12` (Credit Cards)**
*   **Objective:** To generate unique guarantee codes and enrich the "atom" with dates from the source account.
*   **Detailed Process (Logic):**
    1.  **`Número_Garantía` Generation:**
        *   **Condition:** `Número_Garantía` is empty.
        *   **Logic:** For each record, create a unique key by combining `Id_Documento` + `Numero_Prestamo` + `Tipo_Facilidad`. Iterate through the records. `IF` the unique key has not been seen before, assign it the next number from the sequence (starting at 855,500) and store the key and its assigned number. `IF` the key has been seen before, assign it the previously stored number.
    2.  **Date Mapping:**
        *   Perform a `JOIN` between `TDC_AT12` and `AT02_CUENTAS` using `Identificacion_cliente` and `Identificacion_Cuenta`.
        *   Update `Fecha_Última_Actualización` in `TDC_AT12` with the value from `Fecha_proceso` in `AT02_CUENTAS`.
        *   Update `Fecha_Vencimiento` in `TDC_AT12` with the value from `Fecha_Vencimiento` in `AT02_CUENTAS`.

**2.2. `SOBREGIRO_AT12` (Overdrafts)**
*   **Objective:** To enrich the "atom" with dates from the source account.
*   **Detailed Process (Logic):** Apply the same `JOIN` and date mapping logic as described in step 2.1. The `Numero_Garantia` field is not modified.

**2.3. `VALORES_AT12` (Securities)**
*   **Objective:** To construct the securities "atom" with the complete, final column structure.
*   **Detailed Process (Logic):**
    1.  Use the complete list of required output columns as a template.
    2.  Select a reference record from `BASE_AT12` where `Tipo_Garantia` = '0507'.
    3.  Populate the new `VALORES_AT12` "atom". Fields that are missing in the original source but required in the final structure (e.g., `Clave_Pais`, `Clave_Empresa`) are filled with the values from the reference record.
*   **Result:** A new securities "atom" that complies with the final required structure.

---

#### **Phase 3: `FUERA_CIERRE_AT12` (Out of Closing Cycle Loans) (Definition Pending)**
*   **Objective:** To generate a report of loans outside the closing cycle for the Operations team to complete missing information.
*   **Detailed Process (Logic):**
    1.  Execute the query that generates the `AFECTACIONES_AT12` input.
    2.  Create an Excel file with three tabs: "DESEMBOLSO", "CARTERA", and "PYME".
    3.  Distribute the query data into these tabs (distribution logic is pending definition).
    4.  For each record, if `at_tipo_operacion` = '0301', fill the `Nombre_Organismo` field in the report with '182'. Other fields to be filled by Operations are left empty.

---

#### **Phase 4: `VALOR_MINIMO_AVALUO_AT12` (Minimum Appraisal Value) Process (Pending)**
*   **Objective:** To validate if the guarantee's value is sufficient to cover the loan's balance and apply the corresponding correction.
*   **Detailed Process (Logic):**
    1.  Generate the `VALOR_MINIMO_AVALUO_AT12` input.
    2.  Filter where `cu_tipo` = 'letras'.
    3.  Perform a `JOIN` with `AT03_CREDITOS` using `at_num_de_prestamos` = `num_cta`.
    4.  For each record, compare `saldo` (from `AT03`) with `nuevo_at_valor_garantia` (from `VALOR_MINIMO`).
        *   `IF` `saldo` > `nuevo_at_valor_garantia` (Problem): The record is reported. For the final load, the original values `at_valor_garantia` and `at_valor_pond_garantia` are used.
        *   `IF` `saldo` <= `nuevo_at_valor_garantia` (Correct): For the final load, the updated values `nuevo_at_valor_garantia` and `nuevo_at_valor_pond_garantia` are used.