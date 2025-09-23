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

**1.2. ERROR_0301: Id_Documento Validation and Correction (Tipo_Garantia = '0301') — Right→Left**

- Scope: Only rows with `Tipo_Garantia = '0301'`.
- Cascade: Apply rules in order; when a rule acts (exclude/modify/incident), stop processing that document for `ERROR_0301`.
- Indexing: Count all positions from right to left (1‑based). Truncations keep the last N characters (right→left).

- RULE_0301_01 — Positions 13–15 (from the right) and length:
  - If length < 15: not applicable; proceed to the next rule.
  - If length ≥ 15: extract positions 13–15 (right→left) and compare against {'100','110','120','130','810'}.
    - If it matches and length == 15: valid document; exclude from `ERROR_0301`.
    - If it matches and length > 15: truncate keeping the last 15; record original and corrected; stop.

- RULE_0301_02 — '701' sequence in specific windows (right→left):
  - Valid windows: positions 11–9 or 10–8 (from the right).
  - If 701 at 11–9 and length ≥ 11: valid document; exclude (no export).
  - If 701 at 10–8 and length = 10: valid document; exclude and add to the incidents CSV for follow‑up (error type: "701 sequence at positions 10–8 with length 10").

- RULE_0301_03 — Exclusion by positions 9–10 (right→left) ∈ {'41','42'}:
  - If length ≥ 10 and positions 9–10 (right→left) ∈ {'41','42'}: valid; exclude.

- RULE_0301_04 — Remaining with '01' at positions 9–10 (right→left):
  - If length < 10: do not modify; create an incident for manual review.
    - error type: Length less than 10 with "01" at positions 9–10
  - If length == 10: valid; exclude.
  - If length > 10: truncate keeping the last 10; record original and corrected.

- Exports (CSVs under `transforms/AT12/incidencias/`):
  - `ERROR_0301_MODIFIED_[YYYYMMDD].csv`: rows modified by RULE_0301_01 (truncated to 15) or RULE_0301_04 (truncated to 10). Always include adjacent columns `Id_Documento` and `Id_Documento_ORIGINAL`, plus `Regla`, `error_type` and `transformation` (e.g., "Truncated (right→left) to 15" / "Truncated (right→left) to 10").
  - `ERROR_0301_INCIDENTES_[YYYYMMDD].csv`: incidents from RULE_0301_04 (length < 10 with '01' at 9–10) and follow‑up from RULE_0301_02 (701 at 10–8 with length 10). Always include `error_type` and `transformation` = "No change"; may include `description` if additional detail is required.

- Logging: Emit a summary at the end of `ERROR_0301` with metrics: candidates, modified, incidents, excluded by each rule, and unchanged.

- Post‑processing exports (CSVs in `transforms/AT12/incidencias/`):
  - `ERROR_0301_MODIFIED_[YYYYMMDD].csv`: rows where `Id_Documento` was truncated by RULE_0301_01 or RULE_0301_04; always include: `Id_Documento_ORIGINAL`, corrected `Id_Documento`, `Regla`, `error_type`, and `transformation` (e.g., "Truncated to 15" / "Truncated to 10").
  - `ERROR_0301_INCIDENTES_[YYYYMMDD].csv`: incidents from RULE_0301_04 (length < 10 with '01' at positions 9–10); always include: `error_type` and `transformation` = "No change"; may include `description` if additional detail is required.

**1.3. COMA EN FINCA EMPRESA**
*   **Objective:** To remove disallowed characters from the document identifier.
*   **Input Identification:** Records where the `Id_Documento` field contains a comma character (`,`).
*   **Detailed Process (Logic):** Apply a text replacement function to substitute all occurrences of ',' with an empty string ('').
*   **Final Action (Output):** The `Id_Documento` field is free of commas.

**1.4. Incorrect Cancellation Date**
*   **Objective:** To correct expiration dates that are clearly erroneous or outside a logical range.
*   **Input Identification:** Records where the year of the `Fecha_Vencimiento` field is > 2100 or < 1985.
*   **Detailed Process (Logic):** Overwrite the value of the `Fecha_Vencimiento` field with the constant `21001231`.
*   **Final Action (Output):** The `Fecha_Vencimiento` field contains the corrected date `21001231`.

**1.5. Incorrect Appraisal Date**
- Objective: Standardize invalid `Fecha_Ultima_Actualizacion` by replacing with `fec_ini_prestamo`.
- Scope: Applies only to rows with `Tipo_Garantia` in {'0207','0208','0209'}.
- Identification (any of):
  - `Fecha_Ultima_Actualizacion` > last day of the processing month.
  - Year of `Fecha_Ultima_Actualizacion` < 1985.
  - `Fecha_Ultima_Actualizacion` does not conform to `YYYYMMDD` format.
- Process:
  1. JOIN between `BASE_AT12` and `AT03_CREDITOS`.
  2. Keys: `Numero_Prestamo` ↔ `num_cta` (with normalized key for robustness).
  3. For identified records, fetch `fec_ini_prestamo`.
  4. Overwrite `Fecha_Ultima_Actualizacion` with `fec_ini_prestamo`.
- Final action: `Fecha_Ultima_Actualizacion` corrected only for `Tipo_Garantia` 0207/0208/0209.

**1.6. Properties without Policy**
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

**1.8. Commercial Auto Policy**
*   **Objective:** To assign an organization code to auto policies that are missing it.
*   **Input Identification:** Records where `Tipo_Garantia` = '0106,0101,0102,0103,0106,0108' AND `Nombre_Organismo` is empty.
*   **Detailed Process (Logic):** Assign the constant value '700' to the `Nombre_Organismo` field.

**1.9. Auto Policy Error (Rule 9)**
- Objective: Populate/overwrite policy fields for auto guarantees using GARANTIA_AUTOS as source of truth.
- Input Identification: Rows where `Tipo_Garantia` ∈ {'0101','0103'} (regardless of `Id_Documento` being empty or not).
- Join Keys: Join with `GARANTIA_AUTOS_AT12` on `Numero_Prestamo` = `numcred` using normalized keys (digits only, no leading zeros). Output preserves the original formatting of `Numero_Prestamo` (no reformatting is applied).
- Updates (on successful match):
  - Id_Documento: overwrite with `num_poliza` (always, even if it had a prior value).
  - Importe and Valor_Garantia: replace with the policy amount (if available).
  - Fecha_Última_Actualización: replace with policy `Fecha_inicio`.
  - Fecha_Vencimiento: replace with policy `Fecha_Vencimiento`.
- Constraint: Accept any non-empty `num_poliza` (may include dashes/letters/symbols). If `num_poliza` is empty, do not update.
 - Exclusions: If `monto_asegurado` in `GARANTIA_AUTOS_AT12` equals any of {"Nuevo Desembolso", "PERDIDA TOTAL", "FALLECIDO"} (case-insensitive), skip all updates for that match and keep original values in `AT12_BASE` (including `Id_Documento`, dates, and amounts).

**1.10. Property without Appraiser**
*   **Objective:** To assign an organization code to properties that are missing it.
*   **Input Identification:** Records where `Tipo_Garantia` IN ('0207', '0208', '0209') AND `Nombre_Organismo` is empty.
*   **Detailed Process (Logic):** Assign the constant value '774' to the `Nombre_Organismo` field.

---

**1.11. Foreign Trustee Standardization (Fiduciaria Extranjera)**
- Objective: Standardize and enrich foreign trustee records during Stage 1 (AT12_BASE only; independent of other inputs).
- Input Identification: Rows where the trustee name field contains "FDE".
- Affected Fields: `Nombre_Fiduciaria` (selector), `Origen`/`Origen_Garantia` (set), `Cod_region`/`Codigo_Region` (set).
- Detailed Process (Logic):
  - Condition: If `Nombre_Fiduciaria` contains "FDE".
  - Actions:
    - Set `Origen` (or `Origen_Garantia`) to 'E' to indicate foreign origin.
    - Set `Cod_region` (or `Codigo_Region`) to '320' (fixed code for Guatemala).
  - Incidence Output: Export affected rows to `FDE_NOMBRE_FIDUCIARIO_<YYYYMMDD>.csv` (under `transforms/AT12/incidencias/`), preserving original values alongside updated ones for `Origen`/`Codigo_Origen` and `Cod_region`/`Codigo_Region`.
  - Notes: This rule does not reformat other fields and runs in Stage 1 as a self-contained base cleanup.

#### **Stage 2: Data Enrichment and Generation from Auxiliary Sources**
This stage enriches the main dataset by joining it with auxiliary files and applying specific business logic.

**2.0. Pre-processing of `Tipo_Facilidad` for TDC and Sobregiro**
This preliminary step runs before any other Stage 2 logic to ensure `Tipo_Facilidad` is correctly set based on the presence of the loan in the core credit system.

*   **Objective:** Assign `Tipo_Facilidad` based on presence in auxiliary files (by subtype).
*   **Scope:** Applies to `TDC_AT12` and `SOBREGIRO_AT12`; each with its source. Skip if the required auxiliary file is not available.
*   **Detailed Process (Logic):**
    - `TDC_AT12` with `AT03_TDC`:
        1. Normalize keys on both sides by stripping non-digits and leading zeros (no truncation of significant digits; blanks stay unmatched).
        2. Rule: if `Numero_Prestamo` (normalized) ∈ `AT03_TDC.num_cta` (normalized) ⇒ `Tipo_Facilidad='01'`; otherwise `'02'`.
        3. Update only if the value changes. Incidence: export changed rows to `FACILIDAD_FROM_AT03_TDC_AT12_[YYYYMMDD].csv` with `Tipo_Facilidad_ORIGINAL`.
    - `SOBREGIRO_AT12` with `AT03_CREDITOS`:
        1. Apply the same normalization rule (`Numero_Prestamo`/`num_cta` → digits only, no suffix slicing, empty keys remain unmatched).
        2. Rule: if `Numero_Prestamo` (normalized) ∈ `AT03_CREDITOS.num_cta` (normalized) ⇒ `Tipo_Facilidad='01'`; otherwise `'02'`.
        3. Update only if the value changes. Incidence: export changed rows to `FACILIDAD_FROM_AT03_SOBREGIRO_AT12_[YYYYMMDD].csv` with `Tipo_Facilidad_ORIGINAL`.

**2.1. `TDC_AT12` (Credit Cards) Processing**
*   **Objective:** Generate unique guarantee numbers and enrich TDC dates; only `Tarjeta_repetida` produces incidences (in addition to `FACILIDAD_FROM_AT03` in 2.0).
*   **Detailed Process (Logic):**
    1.  **`Numero_Garantia` (per run):**
        *   Key: (`Id_Documento`, `Tipo_Facilidad`).
        *   Assignment: sequence starting at 850,500 per run; the first occurrence assigns, repeats reuse.
        *   Overwrite: always replaces the source file value (the original is not preserved).
        *   Format: output zero‑padded 10-digit strings whenever the generated value is numeric.
        *   Duplicates by `Numero_Prestamo` within the same key: log only (no incidences).
    2.  **Date Mapping (no day/month inversion):**
        *   JOIN `Id_Documento` (TDC) ↔ `identificacion_de_cuenta` (AT02) with normalized keys (digits‑only, no leading zeros).
        *   Deduplicate AT02 before the JOIN by normalized key, prioritizing rows with the most recent dates (interpreted as day‑first to resolve ordering ambiguities, e.g., 08‑05).
        *   Assignment: `Fecha_Última_Actualización`/`Fecha_Ultima_Actualizacion` ← `Fecha_inicio` (AT02) and `Fecha_Vencimiento` ← `Fecha_Vencimiento` (AT02).
        *   Formatting: copy the date strings from AT02 as‑is; do not reformat or invert day/month.
        *   No match: keep original values.
    3.  **Inconsistency `Tarjeta_repetida`:**
        *   Detect duplicates excluding `Numero_Prestamo` using key priority:
            - (`Identificacion_cliente`, `Identificacion_Cuenta`, `Tipo_Facilidad`), or
            - (`Id_Documento`, `Tipo_Facilidad`).
        *   Normalize key parts (digits‑only/trim) to avoid false positives/negatives.
        *   Export: `INC_REPEATED_CARD_TDC_AT12_[YYYYMMDD].csv`.
    4.  **Output shaping:** enforce dot-decimal strings (e.g., `18000.00`), trim CIS/guarantee identifiers, set `País_Emisión = '591'` and drop auxiliary accounting columns (`ACMON`, `ACIMP2`, `ACNGA`, `ACCIS`, `LIMITE`, `SALDO`) so the layout ends at `Descripción de la Garantía`.

*Example `Numero_Garantia` (per run)*

| Id_Documento | Numero_Prestamo | Tipo_Facilidad | Numero_Garantia |
| --- | --- | --- | --- |
| 10000 | 012312 | 01 | 0000850500 |
| 10000 | 012313 | 01 | 0000850500 |
| 10000 | 012314 | 02 | 0000850501 |

2.2. `SOBREGIRO_AT12` (Overdrafts) Processing
Objective: To enrich the overdraft data by assigning the correct facility type (Tipo_Facilidad) and updating key dates from the master accounts file.
Detailed Process (Logic):
0. Pre‑clean: apply EEOR TABULAR (trim + collapse multiple spaces) before any Stage 2 step.
1. `Tipo_Facilidad` Assignment from `SOBREGIRO_AT12` (FIRST):
    * A JOIN is performed between the `SOBREGIRO_AT12` input and the `AT03_CREDITO` file.
    * Keys: `Numero_Prestamo` (from `SOBREGIRO_AT12`) ↔ num_cta (from `AT03_CREDITO`).
    * Rule:
        * If a record from `SOBREGIRO_AT12` finds a match in AT03_CREDITO, its `Tipo_Facilidad` is set to '01'.
        * If no match is found, its `Tipo_Facilidad` is set to '02'.
2. Date Mapping from AT02_CUENTAS (no day/month inversion):
    * A JOIN is performed between the SOBREGIRO_AT12 data (post-step 1) and the AT02_CUENTAS master file.
    * Keys: Id_Documento (from SOBREGIRO_AT12) ↔ identificacion_de_cuenta (from AT02_CUENTAS).
    * Mapping Rules:
        * If a match is found, Fecha_Ultima_Actualizacion is overwritten with the value from Fecha_inicio (from AT02_CUENTAS).
        * If a match is found, Fecha_Vencimiento is overwritten with the value from Fecha_Vencimiento (from AT02_CUENTAS).
        * Pre‑JOIN dedup of AT02 prioritizes the most recent dates; parsing for ordering assumes day‑first to resolve ambiguity, but copied strings are preserved as‑is (no re‑formatting).
        * If no match is found, the original date values in the record are kept.
    * Incidence Reporting:
        * Any record whose dates are modified during the "Date Mapping" step is exported to a dedicated incident file.
        * File Name: DATE_MAPPING_CHANGES_SOBREGIRO_[YYYYMMDD].csv. Content: includes all columns of the modified rows, plus additional columns to preserve the original values for traceability: Fecha_Ultima_Actualizacion_ORIGINAL and Fecha_Vencimiento_ORIGINAL.

Post-processing trims stray spaces in `Numero_Garantia`/`Numero_Cis_Garantia`, forces `Pais_Emision='591'` when present, and leaves all monetary fields with dot decimal notation (two digits) without thousand separators.

**2.3. `VALORES_AT12` (Securities) Generation**
*   **Objective:** Build the final VALORES dataset with the regulatory layout, enforcing 0507-specific rules.
*   **Detailed Process (Logic):**
    1.  **Raw normalization & blank-row guard:** Drop records where every field is empty/whitespace so phantom rows from Excel exports never propagate. Apply header normalization, trim strings, convert `n/a`/`na` → `NA`, and parse monetary columns tolerantly (supports comma or dot decimal formats). Helper columns `Valor_*__num` are created for numeric operations.
    2.  **Identifier standardization:**
        *   `Numero_Prestamo`: keep digits only; zero-pad to 10 when length <10, preserve longer alphanumeric IDs.
        *   `Id_Documento`: if the text matches `Linea Sobregiro de la cuenta {Numero_Prestamo}`, replace it with the normalized loan identifier; otherwise zero-pad the digits extracted from the source value.
    3.  **Tipo_Facilidad resolution:** Reuse the AT03 join rule from TDC/SOBREGIRO. Join against `AT03_CREDITOS` (and `AT03_TDC` when provided). Matches → `Tipo_Facilidad = '01'`; non-matches → `'02'`. Changes are exported as `FACILIDAD_FROM_AT03_VALORES` incidences.
    4.  **Numero_Garantia assignment:** Generate padded 10-digit numbers using the sequence registry. If TDC guarantees were assigned in the same run, start at `last_tdc + 500`; otherwise pull from the persistent registry (`valores_numero_garantia.json`). Each assignment is logged as `VALORES_NUMERO_GARANTIA_GENERATION`.
    5.  **Constants & derived fields:** Stamp `Clave_Pais=24`, `Clave_Empresa=24`, `Clave_Tipo_Garantia=3`, `Clave_Subtipo_Garantia=61`, `Clave_Tipo_Pren_Hipo=NA`, `Tipo_Instrumento=NA`, `Tipo_Poliza=NA`, `Status_Garantia=0`, `Status_Prestamo=-1`, `Calificacion_Emisor=NA`, `Calificacion_Emisision=NA`, `Segmento=PRE`, and force `Pais_Emision='591'`. Mirror identifiers: `Numero_Cis_Prestamo = Numero_Cis_Garantia`, `Numero_Ruc_Prestamo = Numero_Ruc_Garantia`.
    6.  **Importe enforcement:** Force `Importe = Valor_Garantia` using the normalized numeric series. If any discrepancy remains, the transformation aborts with a fatal error (incidence severity `error`).
    7.  **Output formatting:** Emit the 37-column regulatory layout (mirroring the target schema) with dot decimal strings (no thousand separators, no trailing zeros). Any columns missing in the source are injected as empty strings so downstream consumers always receive the enriched layout (`Clave_*`, statuses, mirrored CIS/RUC, etc.).

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
Note: The 'Importe' value from all inputs must equal the guarantee's value.
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

### **Incidence Naming (CSV)**
- Subsets per rule (full rows):
  - Format: `[RULE]_[SUBTYPE]_[YYYYMMDD].csv` (e.g., `FECHA_AVALUO_ERRADA_BASE_AT12_20250701.csv`).
  - Include `_ORIGINAL` columns next to every corrected field.
- Global aggregates (e.g., EEOR_TABULAR):
  - Format: `EEOR_TABULAR_[SUBTYPE]_[YYYYMMDD].csv` (include subtype to avoid overwriting across types).

### **Annex: Input File Schema**

-   **AT03_CREDITOS**: Required for `valor_minimo_avaluo` validation and `Fecha_Avaluo_Errada`. Contains key credit details for analysis.
-   **BASE_AT12**: Main file to be transformed.
-   **Other files**: `SOBREGIRO_AT12`, `TDC_AT12`, `VALORES_AT12` are auxiliary files that enrich the AT12 transformation.

---

### **Annex: Monetary Fields (normalized to dot)**
- BASE_AT12: `Valor_Inicial`, `Valor_Garantia`, `Valor_Ponderado`, `Importe`.
- TDC_AT12: `Valor_Inicial`, `Valor_Garantía`, `Valor_Ponderado`, `Importe`, `LIMITE`, `SALDO`.
- SOBREGIRO_AT12: `Valor_Inicial`, `Valor_Garantia`, `valor_ponderado`, `Importe`.
- VALORES_AT12: `Valor_Inicial`, `Valor_Garantia`, `Valor_Ponderado`, `Importe`.
- AT02_CUENTAS: `Monto`, `Monto_Pignorado`, `Intereses_por_Pagar`, `Importe`, `Importe_por_pagar`, `Tasa`.
- AT03_CREDITOS: `valor_inicial`, `intereses_x_cobrar`, `saldo`, `provision`, `provison_NIIF`, `provision_no_NIIF`, `saldo_original`, `mto_garantia_1..5`, `mto_xv30d/60d/90d/120d/180d/1a`, `Mto_xV1a5a`, `Mto_xV5a10a`, `Mto_xVm10a`, `mto_v30d/60d/90d/120d/180d/1a`, `mto_vm1a`, `mto_a_pagar`, `interes_diferido`, `tasa_interes`, `monto_ult_pago_capital`, `monto_ult_pago_interes`.
- AT03_TDC: `valor_inicial`, `intereses_x_cobrar`, `saldo`, `provision`, `provison_niif`, `provision_no_niif`, `saldo_original_2`, `mto_garantia_1..5`, `mto_xv30d/60d/90d/120d/180d/1a`, `mto_xv1a5a`, `mto_xv5a10a`, `mto_xvm10a`, `mto_v30d/60d/90d/120d/180d/1a`, `mto_vm1a`, `mto_a_pagar`, `interes_dif`, `tasa_interes`, `monto_ultimo_pago_cap`, `monto_ultimo_pago_int`.
- GARANTIA_AUTOS_AT12: `saldocapital`, `monto_asegurado`.
- POLIZA_HIPOTECAS_AT12: `saldocapital`, `seguro_incendio` (if applicable as amount).
- AFECTACIONES_AT12: `at_saldo`.
- VALOR_MINIMO_AVALUO_AT12: `at_valor_garantia`, `at_valor_pond_garantia`, `valor_garantia`, `nuevo_at_valor_garantia`, `nuevo_at_valor_pond_garantia`, `venta_rapida`, `factor`.
### New/Updated Rules (2025-09)

These clarifications and rules are incorporated into the unified pipeline and executed in cascade as specified below.

- BASE — Exclude Overdrafts (before other dependencies; Stage 1b):
  - Join `Numero_Prestamo` (BASE) ↔ `AT03_CREDITOS.num_cta` with normalized keys (digits‑only, no leading zeros).
  - Remove rows where `Tipo_Facilidad='02'` and the loan appears in AT03.
  - Incidences: `EXCLUDE_SOBREGIROS_BASE_<YYYYMMDD>.csv` with the removed rows.

- BASE — Obsolete Trustee Code (Stage 1a):
  - Rule: replace `Id_Fiduciaria=508` with `528`.
  - Incidences: `FIDUCIARIA_CODE_UPDATE_<YYYYMMDD>.csv` with adjacent `Codigo_Fiduciaria_ORIGINAL`.

- BASE — `Id_Documento` padding (last rule of Stage 1):
  - If `Id_Documento` is purely digits and length < 10, apply `zfill(10)`.
  - If length ≥ 10 or contains non‑numeric characters (e.g., "/"), do not change.
  - Incidences: `ID_DOCUMENTO_PADDING_BASE_AT12_<YYYYMMDD>.csv` with `Id_Documento_ORIGINAL`.

- BASE — "Contrato Privado" → `Nombre_Organismo='NA'` (Stage 1a):
  - Detect "Contrato Privado" (case‑insensitive) in candidate columns: `id_Documento`,`Tipo_Instrumento`, `Tipo_Poliza`, `Descripción de la Garantía` (and variants without accents).
  - Set `Nombre_Organismo='NA'` and export `CONTRATO_PRIVADO_NA_<YYYYMMDD>.csv` with the original value alongside.

- TDC — `Numero_Garantia` (Stage 2):
  - Sequence by key (`Id_Documento`,`Tipo_Facilidad`) starting at 850,500; always overwrite the source value; pad to 10 digits if numeric; reuse for repeated keys within the same run.

- TDC/SOBREGIRO — Date Mapping without inversion (Stage 2):
  - Date parsing is used only to order/deduplicate AT02 under a day‑first assumption; strings copied to destination fields are preserved as‑is from AT02; no re‑formatting, avoiding day↔month inversions.

- SOBREGIRO — EEOR TABULAR pre‑clean (start of Stage 2):
  - Apply trim + collapse spaces across all text columns before assigning `Tipo_Facilidad` or mapping dates. Incidences for modified rows under `EEOR_TABULAR_SOBREGIRO_AT12_<YYYYMMDD>.csv`.
