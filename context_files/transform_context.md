### **Transform Context — AT12 Unified Transformation Process**

#### **Overview**
The AT12 transformation is a unified ETL process designed to cleanse, enrich, and validate guarantee data. It includes a robust evidence and correction system that allows for complete traceability of all changes applied. This system ensures transparency and facilitates collaboration with the Operations team by treating all steps as part of a single, continuous workflow.

#### **AT12 Validation Approach**
The system handles multiple, specific validations as sequential stages within one ETL pipeline. Each stage builds upon the previous one, ensuring data integrity flows through the entire process.

*   **System Characteristics:**
    *   **100% Pandas:** All transformations are handled in pandas.
    *   **Traceability:** Each transformation is recorded in specific incident files.
    *   **Unified Workflow:** All transformations are stages within a single, sequential ETL process.

*   **Directory Structure for Transformation:**
    ```
    transforms/
    ├── AT12/
    │   ├── incidencias/
    │   │   ├── EEOR_TABULAR_AT12_BASE_20250131.csv
    │   │   └── ... (other incident files from each stage)
    │   ├── procesados/ (Intermediate corrected CSVs)
    │   │   └── AT12_BASE_20250131.csv 
    │   └── consolidated/ (Final TXT outputs)
    │       ├── AT12_BASE_20250131.txt
    │       ├── TDC_AT12_20250131.txt
    │       ├── SOBREGIRO_AT12_20250131.txt
    │       └── VALORES_AT12_20250131.txt
    └── AT03/
        └── ...
    ```

### **The Unified AT12 Transformation ETL Process**

The AT12 transformation is executed as a single ETL pipeline, organized into sequential stages.

---

#### **Stage 1: Initial Data Cleansing and Formatting**
This initial stage focuses on correcting structural and format errors in the `BASE_AT12` file to prepare it for subsequent business logic.

**1.1. EEOR TABULAR: Whitespace Errors**
*   **Objective:** To standardize text fields by removing unnecessary spaces that cause errors in data joins or length validations.
*   **Input Identification:** Any text-based field within the `BASE_AT12` table (e.g., `Id_Documento`, `Nombre_Organismo`, `Numero_Prestamo`).
*   **Detailed Process (Logic):**
    1.  Remove all leading spaces (from the beginning of the string).
    2.  Remove all trailing spaces (from the end of the string).
    3.  Replace any sequence of two or more spaces in the middle of the field with a single space.
*   **Final Action (Output):** The field is cleaned of all excess whitespace.

**1.2. ERROR_0301: Id_Documento Validation and Correction (Tipo_Garantia = '0301') — Derecha→Izquierda**

- Scope: Solo filas con `Tipo_Garantia = '0301'`.
- Cascada: Se aplican las reglas en orden; cuando una regla actúa (excluir/modificar/incidente), se detiene el procesamiento del documento para `ERROR_0301`.
- Indexación: Todas las posiciones se cuentan de derecha a izquierda (1‑based). Los truncados conservan los N últimos caracteres (der→izq).

- RULE_0301_01 — Posiciones 13–15 (desde la derecha) y longitud:
  - Si longitud < 15: no aplica; pasar a la siguiente.
  - Si longitud ≥ 15: extraer posiciones 13–15 (der→izq) y comparar con {'100','110','120','130','810'}.
    - Si coincide y longitud == 15: documento válido; excluir de `ERROR_0301`.
    - Si coincide y longitud > 15: truncar conservando los últimos 15; registrar original y corregido; detener.

- RULE_0301_02 — Secuencia '701' en ventanas específicas (der→izq):
  - Ventanas válidas: posiciones 11–9 o 10–8 (desde la derecha).
  - Si 701 en 11–9 y longitud ≥ 11: documento válido; excluir (sin exportar).
  - Si 701 en 10–8 y longitud = 10: documento válido; excluir y agregar al CSV de incidentes como seguimiento (tipo de error: "Secuencia 701 en posiciones 10-8 con longitud 10").

- RULE_0301_03 — Exclusión por posiciones 9–10 (der→izq) ∈ {'41','42'}:
  - Si longitud ≥ 10 y posiciones 9–10 (der→izq) ∈ {'41','42'}: válido; excluir.

- RULE_0301_04 — Restantes con '01' en posiciones 9–10 (der→izq):
  - Si longitud < 10: no modificar; incidente para revisión manual.
    - tipo de error: Longitud menor a 10 con "01" en posiciones 9-10
  - Si longitud == 10: válido; excluir.
  - Si longitud > 10: truncar conservando los últimos 10; registrar original y corregido.

- Exportes (CSV en `transforms/AT12/incidencias/`):
  - `ERROR_0301_MODIFIED_[YYYYMMDD].csv`: filas modificadas por RULE_0301_01 (truncado a 15) o RULE_0301_04 (truncado a 10). Siempre incluye columnas adyacentes `Id_Documento` y `Id_Documento_ORIGINAL`, y además `Regla`, `tipo de error` (ES) y `transformacion` (ES, p.ej., "Truncado (der→izq) a 15" / "Truncado (der→izq) a 10").
  - `ERROR_0301_INCIDENTES_[YYYYMMDD].csv`: incidentes de RULE_0301_04 (longitud < 10 con '01' en 9–10) y seguimiento de RULE_0301_02 (701 en 10–8 con longitud 10). Siempre incluye `tipo de error` (ES) y `transformacion` = "Sin cambio"; puede incluir `descripcion` (EN) si se requiere detalle adicional.

- Logging: Se emite un resumen en logs al finalizar el paso `ERROR_0301` con métricas: candidatos, modificados, incidentes, excluidos por cada regla y sin cambio.

- Post‑processing exports (CSV in `transforms/AT12/incidencias/`):
  - ERROR_0301_MODIFIED_[YYYYMMDD].csv: rows where `Id_Documento` was truncated by RULE_0301_01 o RULE_0301_04; siempre incluye: `Id_Documento_ORIGINAL`, `Id_Documento` (corregido), `Regla`, `tipo de error` (ES), y `transformacion` (ES, p.ej. "Truncado a 15" / "Truncado a 10").
  - ERROR_0301_INCIDENTES_[YYYYMMDD].csv: incidentes de RULE_0301_04 (longitud < 10 con '01' en posiciones 9–10); siempre incluye: `tipo de error` (ES) y `transformacion` = "Sin cambio"; puede incluir `descripcion` (EN) si se requiere detalle adicional.

**1.3. COMA EN FINCA EMPRESA**
*   **Objective:** To remove disallowed characters from the document identifier.
*   **Input Identification:** Records where the `Id_Documento` field contains a comma character (`,`).
*   **Detailed Process (Logic):** Apply a text replacement function to substitute all occurrences of ',' with an empty string ('').
*   **Final Action (Output):** The `Id_Documento` field is free of commas.

**1.4. Fecha Cancelación Errada (Incorrect Cancellation Date)**
*   **Objective:** To correct expiration dates that are clearly erroneous or outside a logical range.
*   **Input Identification:** Records where the year of the `Fecha_Vencimiento` field is > 2100 or < 1985.
*   **Detailed Process (Logic):** Overwrite the value of the `Fecha_Vencimiento` field with the constant `21001231`.
*   **Final Action (Output):** The `Fecha_Vencimiento` field contains the corrected date `21001231`.

**1.5. Fecha Avalúo Errada (Incorrect Appraisal Date)**
- Objetivo: Estandarizar fechas de `Fecha_Ultima_Actualizacion` inválidas reemplazándolas por `fec_ini_prestamo`.
- Alcance: Solo aplica a filas con `Tipo_Garantia` en {'0207','0208','0209'}.
- Identificación (cualquiera de):
  - `Fecha_Ultima_Actualizacion` > último día del mes de proceso.
  - Año de `Fecha_Ultima_Actualizacion` < 1985.
  - `Fecha_Ultima_Actualizacion` no cumple formato `YYYYMMDD`.
- Proceso:
  1. JOIN entre `BASE_AT12` y `AT03_CREDITOS`.
  2. Llaves: `Numero_Prestamo` ↔ `num_cta` (con clave normalizada para robustez).
  3. Para registros identificados, traer `fec_ini_prestamo`.
  4. Sobrescribir `Fecha_Ultima_Actualizacion` con `fec_ini_prestamo`.
- Acción final: `Fecha_Ultima_Actualizacion` corregida solo para `Tipo_Garantia` 0207/0208/0209.

**1.6. Inmuebles sin Póliza (Properties without Policy)**
*   **Objective:** To assign a policy type to property guarantees where it is missing.
*   **Input Identification:** Records where (`Tipo_Garantia` = '0207' or '0208') AND `Tipo_Poliza` is empty.
*   **Detailed Process (Logic):**
    *   **Case `0207`:** Perform a `JOIN` with `POLIZA_HIPOTECAS_AT12` using `Numero_Prestamo` = `numcred`. `IF` a match is found and the `seguro_incendio` field has a value, assign the corresponding value ('01' or '02') to `Tipo_Poliza`.
    *   **Case `0208`:** Assign the constant value '01' to the `Tipo_Poliza` field.

**1.7. Inmuebles sin Finca (Properties without Cadastral ID)**
*   **Objective:** To clean and standardize document identifiers for properties that are empty or contain junk data.
*   **Input Identification:** Records where `Tipo_Garantia` IN ('0207', '0208', '0209') AND `Id_Documento` is empty or exactly matches one of the following invalid values:
    ```
    ["0/0", "1/0", "1/1", "1", "9999/1", "0/1", "0"]
    ```
*   **Detailed Process (Logic):** Overwrite the `Id_Documento` field with the constant **'99999/99999'**.
*   **Final Action (Output):** `Id_Documento` is corrected to '99999/99999'.

**1.8. Póliza Auto Comercial (Commercial Auto Policy)**
*   **Objective:** To assign an organization code to auto policies that are missing it.
*   **Input Identification:** Records where `Tipo_Garantia` = '0106,0101,0102,0103,0106,0108' AND `Nombre_Organismo` is empty.
*   **Detailed Process (Logic):** Assign the constant value '700' to the `Nombre_Organismo` field.

**1.9. Error en Póliza de Auto (Error in Auto Policy)**
*   **Objective:** To populate the missing policy number in auto guarantees.
*   **Input Identification:** Records where `Tipo_Garantia` = '0101' AND `Id_Documento` is empty.
*   **Detailed Process (Logic):** `JOIN` con `GARANTIA_AUTOS_AT12` por `Numero_Prestamo = numcred` con llaves normalizadas (solo dígitos, sin ceros a la izquierda). Poblar `Id_Documento` con `num_poliza`.

**1.10. Inmueble sin Avaluadora (Property without Appraiser)**
*   **Objective:** To assign an organization code to properties that are missing it.
*   **Input Identification:** Records where `Tipo_Garantia` IN ('0207', '0208', '0209') AND `Nombre_Organismo` is empty.
*   **Detailed Process (Logic):** Assign the constant value '774' to the `Nombre_Organismo` field.

---

#### **Stage 2: Data Enrichment and Generation from Auxiliary Sources**
This stage enriches the main dataset by joining it with auxiliary files and applying specific business logic.

**2.0. Pre-processing of `Tipo_Facilidad` for TDC and Sobregiro**
This preliminary step runs before any other Stage 2 logic to ensure `Tipo_Facilidad` is correctly set based on the presence of the loan in the core credit system.

*   **Objective:** Asignar `Tipo_Facilidad` por presencia en archivos auxiliares (por subtipo).
*   **Scope:** Aplica a `TDC_AT12` y `SOBREGIRO_AT12`; cada uno con su fuente. Se omite si el archivo auxiliar requerido no está disponible.
*   **Detailed Process (Logic):**
    - `TDC_AT12` con `AT03_TDC`:
        1. Normalizar llaves en ambos lados: `Numero_Prestamo` y `num_cta` a dígitos‑solo, sin ceros a la izquierda.
        2. Regla: si `Numero_Prestamo`(norm) ∈ `AT03_TDC.num_cta`(norm) ⇒ `Tipo_Facilidad='01'`; de lo contrario `'02'`.
    3. Actualizar solo si el valor cambia. Incidencia: exportar filas cambiadas a `FACILIDAD_FROM_AT03_TDC_AT12_[YYYYMMDD].csv` con `Tipo_Facilidad_ORIGINAL`.
    - `SOBREGIRO_AT12` con `AT03_CREDITOS`:
        1. Normalizar llaves en ambos lados: `Numero_Prestamo` y `num_cta` a dígitos‑solo, sin ceros a la izquierda.
        2. Regla: si `Numero_Prestamo`(norm) ∈ `AT03_CREDITOS.num_cta`(norm) ⇒ `Tipo_Facilidad='01'`; de lo contrario `'02'`.
        3. Actualizar solo si el valor cambia. Incidencia: exportar filas cambiadas a `FACILIDAD_FROM_AT03_SOBREGIRO_AT12_[YYYYMMDD].csv` con `Tipo_Facilidad_ORIGINAL`.

**2.1. `TDC_AT12` (Credit Cards) Processing**
*   **Objective:** Generate unique guarantee numbers and enrich TDC dates; solo “Tarjeta repetida” produce incidencias (además de `FACILIDAD_FROM_AT03` en 2.0).
*   **Detailed Process (Logic):**
    1.  **`Número_Garantía` (por run):**
        *   Preparación: limpiar `Número_Garantía` (armoniza variantes como `Numero_Garantia`) y ordenar por `Id_Documento` ascendente.
        *   Llave: (`Id_Documento`, `Tipo_Facilidad`).
        *   Asignación: secuencia desde 855,500 por run; primera ocurrencia asigna, repetidas reutilizan.
        *   Repetidos por `Numero_Prestamo` dentro de la misma llave: solo log (sin incidencias).
    2.  **Date Mapping:**
        *   JOIN `Id_Documento` (TDC) ↔ `identificacion_de_cuenta` (AT02).
        *   Normalización de llaves para el JOIN: dígitos‑solo y sin ceros a la izquierda en ambos lados.
        *   Deduplicación de AT02 previa al JOIN por llave normalizada, priorizando fechas más recientes.
        *   `Fecha_Última_Actualización`/`Fecha_Ultima_Actualizacion` ← `Fecha_inicio` (AT02) y `Fecha_Vencimiento` ← `Fecha_Vencimiento` (AT02).
        *   Sin match: mantener valores originales.
    3.  **Inconsistencia `Tarjeta_repetida`:**
        *   Detectar duplicados excluyendo `Numero_Prestamo` usando prioridad de clave:
            - (`Identificacion_cliente`, `Identificacion_Cuenta`, `Tipo_Facilidad`) o,
            - (`Id_Documento`, `Tipo_Facilidad`).
        *   Llaves normalizadas (dígitos‑solo/trim) para evitar falsos positivos/negativos.
        *   Export: `INC_REPEATED_CARD_TDC_AT12_[YYYYMMDD].csv`.

*Ejemplo `Numero_Garantia` (por run)*

| Id_Documento | Numero_Prestamo | Tipo_Facilidad | Numero_Garantia |
| --- | --- | --- | --- |
| 10000 | 012312 | 01 | 855500 |
| 10000 | 012313 | 01 | 855500 |
| 10000 | 012314 | 02 | 855501 |

2.2. `SOBREGIRO_AT12` (Overdrafts) Processing Objective: To enrich the overdraft data by assigning the correct facility type (Tipo_Facilidad) and updating key dates from the master accounts file. Detailed Process (Logic):
1. `Tipo_Facilidad` Assignment from `SOBREGIRO_AT12` (FIRST):
    * A JOIN is performed between the `SOBREGIRO_AT12` input and the `AT03_CREDITO` file.
    * Keys: `Numero_Prestamo` (from `SOBREGIRO_AT12`) ↔ num_cta (from `AT03_CREDITO`).
    * Rule:
        * If a record from `SOBREGIRO_AT12` finds a match in AT03_CREDITO, its `Tipo_Facilidad` is set to '01'.
        * If no match is found, its `Tipo_Facilidad` is set to '02'.
2. Date Mapping from AT02_CUENTAS:
    * A JOIN is performed between the SOBREGIRO_AT12 data (post-step 1) and the AT02_CUENTAS master file.
    * Keys: Id_Documento (from SOBREGIRO_AT12) ↔ identificacion_de_cuenta (from AT02_CUENTAS).
    * Mapping Rules:
        * If a match is found, Fecha_Ultima_Actualizacion is overwritten with the value from Fecha_inicio (from AT02_CUENTAS).
        * If a match is found, Fecha_Vencimiento is overwritten with the value from Fecha_Vencimiento (from AT02_CUENTAS).
        * If no match is found, the original date values in the record are kept.
    * Incidence Reporting:
        * Any record whose dates are modified during the "Date Mapping" step is exported to a dedicated incident file.
        * File Name: DATE_MAPPING_CHANGES_SOBREGIRO_[YYYYMMDD].csv. Content: includes all columns of the modified rows, plus additional columns to preserve the original values for traceability: Fecha_Ultima_Actualizacion_ORIGINAL and Fecha_Vencimiento_ORIGINAL.

**2.3. `VALORES_AT12` (Securities) Generation**
*   **Objective:** To construct the securities atom with the complete, final column structure.
*   **Detailed Process (Logic):**
    1.  Select a reference record from `BASE_AT12` where `Tipo_Garantia` = '0507'.
    2.  Populate the new `VALORES_AT12` atom. Fields that are missing in the original source but required in the final structure (e.g., `Clave_Pais`, `Clave_Empresa`) are filled with the values from the reference record.

---

#### **Stage 3: Business Logic Application and Reporting**
This stage applies more complex business rules and generates specific reports for operational review.

**3.1. `FUERA_CIERRE_AT12` (Out of Closing Cycle Loans) Reporting**
*   **Objective:** To generate a structured report of loans that were not part of the main closing cycle for the Operations team to complete.
*   **Detailed Process (Logic):**
    1.  **Execute Query:** Run the SQL query that generates the `AFECTACIONES_AT12` input.
    2.  **Create Excel File & Tabs:** Create an Excel file with three tabs: `DESEMBOLSO`, `CARTERA`, `PYME`.
    3.  **Distribute Data into Tabs:**
        *   **`DESEMBOLSO` Tab:** Records where `at_fecha_inicial_prestamo` is within the last three months (inclusive).
        *   **`PYME` Tab:** Records where `Segmento` is identified as 'PYME' or 'BEC'.
        *   **`CARTERA` Tab:** All remaining records.
    4.  **Populate Fields:**
        *   Fields like `at_saldo` and `at_fecha_inicial_prestamo` are filled from the query.
        *   **Special Rule:** If `at_tipo_operacion` is '0301', `Nombre_Organismo` is populated with '182'.
        *   Other fields are left blank for Operations to complete.

---

#### **Stage 4: Final Validation and Value Correction**
This is a critical stage of the ETL pipeline where financial validations are performed and final values are set before output generation.

**4.1. `VALOR_MINIMO_AVALUO_AT12` (Minimum Appraisal Value) Validation**
*   **Objective:** To validate if a guarantee's value covers the loan's balance and apply the correct value.
*   **Detailed Process (Logic):**
    1.  **Generate Input:** Execute the SQL query to produce the `VALOR_MINIMO_AVALUO_AT12` input.
    2.  **Initial Filter:** Select records where the `cu_tipo` field contains any alphabetic characters (is not purely numeric).
    3.  **Data Enrichment:** `JOIN` with `AT03_CREDITOS` on `at_num_de_prestamos` = `num_cta` to get the current `saldo`.
    4.  **Core Validation Logic:**
        *   **Error Condition:** `saldo` > `nuevo_at_valor_garantia`.
        *   **Action if TRUE (Problem):** The record is reported to Operations. For the final load, the original `at_valor_garantia` is used (no correction is applied).
        *   **Action if FALSE (Correct):** The `Valor_Garantia` and `Valor_Ponderado` fields are updated with `nuevo_at_valor_garantia` and `nuevo_at_valor_pond_garantia`.

---

#### **Stage 5: Final Output Generation (Consolidation)**
*   **Objective:** To generate the final, regulatory-compliant text files for submission. This is the last step of the ETL process.
*   **Input Identification:** The corrected pandas DataFrames for `AT12_BASE`, `TDC_AT12`, `SOBREGIRO_AT12`, and `VALORES_AT12` after all previous transformation stages are complete.
*   **Detailed Process (Logic):**
    1.  **For the `AT12_BASE` DataFrame:**
        *   **Action:** Export the DataFrame to a text file (`.txt`).
        *   **Formatting Rules:**
            *   **Delimiter:** Use the pipe symbol (`|`) as the separator.
            *   **Header:** Do **not** include the column names in the output file.
            *   **File Path:** Save to the `consolidated/` directory.
            *   **Decimals:** Monetary fields use dot (`.`) as decimal separator (no comma).

    2.  **For the `TDC_AT12` DataFrame:**
        *   **Action:** Export the DataFrame to a text file (`.txt`).
        *   **Formatting Rules:**
            *   **Delimiter:** Use a single space (` `) as the separator.
            *   **Header:** Do **not** include the column names in the output file.
            *   **File Path:** Save to the `consolidated/` directory.
            *   **Decimals:** Monetary fields use dot (`.`) as decimal separator (no comma).

    3.  **For the `SOBREGIRO_AT12` DataFrame:**
        *   **Action:** Export the DataFrame to a text file (`.txt`).
        *   **Formatting Rules:**
            *   **Delimiter:** Use a single space (` `) as the separator.
            *   **Header:** Do **not** include the column names in the output file.
            *   **File Path:** Save to the `consolidated/` directory.
            *   **Decimals:** Monetary fields use dot (`.`) as decimal separator (no comma).

    4.  **For the `VALORES_AT12` DataFrame:**
        *   **Action:** Export the DataFrame to a text file (`.txt`).
        *   **Formatting Rules:**
            *   **Delimiter:** Use a single space (` `) as the separator.
            *   **Header:** Do **not** include the column names in the output file.
            *   **File Path:** Save to the `consolidated/` directory.
            *   **Decimals:** Monetary fields use dot (`.`) as decimal separator (no comma).

*   **Final Action (Output):** Four headerless `.txt` files are created in the `consolidated/` directory, each with its specific delimiter, representing the final output of the ETL pipeline. Internal helper columns (e.g., `__num`) are not included; all monetary fields are normalized to dot decimals across RAW, processed and consolidated outputs.

Note (Input/RAW normalization): TXT inputs (including Excel “Unicode Text”) are accepted and converted to UTF‑8 CSV in RAW with auto‑detected encoding/delimiter. During this conversion, and for CSV sources directly, all monetary fields are normalized to dot (`.`) decimals to unify downstream handling. Processed CSVs also maintain dot decimals.

### **Nomenclatura de Incidencias (CSV)**
- Subconjuntos por regla (filas completas):
  - Formato: `[REGLA]_[SUBTIPO]_[YYYYMMDD].csv` (ej.: `FECHA_AVALUO_ERRADA_BASE_AT12_20250701.csv`).
  - Incluyen columnas `_ORIGINAL` junto a cada campo corregido.
- Agregados globales (p.ej., EEOR_TABULAR):
  - Formato: `EEOR_TABULAR_[SUBTIPO]_[YYYYMMDD].csv` (incluye subtipo para evitar sobreescritura entre tipos).

### **Anexo: Esquema de Archivos de Entrada**

-   **AT03_CREDITOS**: Requerido para la validación de `valor_minimo_avaluo` y `Fecha_Avalúo_Errada`. Contiene detalles de créditos cruciales para el análisis.
-   **BASE_AT12**: Archivo principal a ser transformado.
-   **Otros archivos**: `SOBREGIRO_AT12`, `TDC_AT12`, `VALORES_AT12` son archivos auxiliares que enriquecen la transformación de AT12.

---

### **Anexo: Campos Monetarios (normalizados a punto)**
- BASE_AT12: `Valor_Inicial`, `Valor_Garantia`, `Valor_Ponderado`, `Importe`.
- TDC_AT12: `Valor_Inicial`, `Valor_Garantía`, `Valor_Ponderado`, `Importe`, `LIMITE`, `SALDO`.
- SOBREGIRO_AT12: `Valor_Inicial`, `Valor_Garantia`, `valor_ponderado`, `Importe`.
- VALORES_AT12: `Valor_Inicial`, `Valor_Garantia`, `Valor_Ponderado`, `Importe`.
- AT02_CUENTAS: `Monto`, `Monto_Pignorado`, `Intereses_por_Pagar`, `Importe`, `Importe_por_pagar`, `Tasa`.
- AT03_CREDITOS: `valor_inicial`, `intereses_x_cobrar`, `saldo`, `provision`, `provison_NIIF`, `provision_no_NIIF`, `saldo_original`, `mto_garantia_1..5`, `mto_xv30d/60d/90d/120d/180d/1a`, `Mto_xV1a5a`, `Mto_xV5a10a`, `Mto_xVm10a`, `mto_v30d/60d/90d/120d/180d/1a`, `mto_vm1a`, `mto_a_pagar`, `interes_diferido`, `tasa_interes`, `monto_ult_pago_capital`, `monto_ult_pago_interes`.
- AT03_TDC: `valor_inicial`, `intereses_x_cobrar`, `saldo`, `provision`, `provison_niif`, `provision_no_niif`, `saldo_original_2`, `mto_garantia_1..5`, `mto_xv30d/60d/90d/120d/180d/1a`, `mto_xv1a5a`, `mto_xv5a10a`, `mto_xvm10a`, `mto_v30d/60d/90d/120d/180d/1a`, `mto_vm1a`, `mto_a_pagar`, `interes_dif`, `tasa_interes`, `monto_ultimo_pago_cap`, `monto_ultimo_pago_int`.
- GARANTIA_AUTOS_AT12: `saldocapital`, `monto_asegurado`.
- POLIZA_HIPOTECAS_AT12: `saldocapital`, `seguro_incendio` (si aplica como monto).
- AFECTACIONES_AT12: `at_saldo`.
- VALOR_MINIMO_AVALUO_AT12: `at_valor_garantia`, `at_valor_pond_garantia`, `valor_garantia`, `nuevo_at_valor_garantia`, `nuevo_at_valor_pond_garantia`, `venta_rapida`, `factor`.
