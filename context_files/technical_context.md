# Technical Context — Operational Details (MVP v0.8)

## File Conventions & Parsing
- **Input:** `.CSV` (text). **Exploration outputs:** PDFs + metrics. **Transformation output:** **one consolidated TXT** (layout pending).
- **Strict CSV read/write:**
  - `delimiter = ","`, `encoding = "utf-8"`
  - `quotechar = '"'`, `doublequote = true`
  - Commas inside data **must be quoted** (e.g., `"Michael,Portela"`); otherwise, row width mismatches are flagged.
  - If a data row has a different column count than the header → **warning** `row_width_mismatch`.
- **Decimals:** `.` (dot).  
- **Dates in content:** `YYYYMMDD` (param via `DATE_FMT = "%Y%m%d"`).
- **Filename normalization:** validate using **UPPERCASE**.
- **Header mapping:** Advanced normalization with accent removal and specialized AT02_CUENTAS handling.
- **Chunking:** `CHUNK_ROWS = 1_000_000`.

## Period Resolution
- Default: **previous month**.
- Overrides:
  - CLI: `--year 2025 --month 08` (or `--month Agosto`).
  - `STRICT_PERIOD=true` by default (enforce folder–name coherence).  
    - For tests: `--strict-period=false`.

## Presence & Duplicates
- **expected_files.json** (per atom, generic for all months): defines expected subtypes and filename pattern.
- **Duplicate subtype:** choose **most recent by date in filename**; record **warning** `duplicate_subtype`.
- **Invalid date:** **warning** `invalid_date_in_name`; ignore file.

## Header Mapping & Validation ✅

### HeaderMapper Class (`src/core/header_mapping.py`)
- **AT02_CUENTAS_MAPPING**: List of predefined schema headers for direct replacement
- **get_mapping_for_subtype()**: Returns appropriate mapping strategy for file subtype
- **map_headers()**: Applies header transformations based on subtype
- **validate_mapped_headers()**: Validates mapped headers against expected schema
- **get_mapping_report()**: Generates detailed mapping audit trail

### HeaderNormalizer Class (`src/core/naming.py`)
- **remove_accents()**: Removes tildes and accents (ñ→n, á→a, é→e, í→i, ó→o, ú→u)
- **clean_header()**: Removes parenthetical numbers (0), (1), extra spaces
- **normalize_headers()**: Full normalization pipeline (clean + accent removal + uppercase)

## Incidence Reporting System ✅

### IncidenceReporter Class (`src/core/incidence_reporter.py`)
- **Standardized incidence tracking** with fields: `incidence_id`, `timestamp`, `severity`, `incidence_type`
- **add_incidence()**: Creates incidences with proper metadata and rule tracking
- **Severity levels**: `LOW`, `MEDIUM`, `HIGH`, `CRITICAL`
- **Incidence types**: `DATA_QUALITY`, `VALIDATION_FAILURE`, `PROCESSING_ERROR`

### AT12 Transformation Engine Integration ✅
- **_add_incidence()** method standardized across all validation rules
- **Method signature**: `incidence_type`, `severity`, `rule_id`, `description`, `data`
- **Resolved duplicate method conflict** that was causing incorrect incidence types
- **All validation rules** now properly report with correct severity and metadata

### Technical Implementation
```python
# AT02_CUENTAS Direct Replacement
AT02_CUENTAS_MAPPING = [
    'Fecha', 'Cod_banco', 'Cod_Subsidiaria', 'Tipo_Deposito',
    'Tipo_Cliente', 'Numero_Cuenta', 'Identificacion_Cliente',
    # ... (30 predefined headers)
]

# Standardized Incidence Reporting
def _add_incidence(self, incidence_type: IncidenceType, severity: IncidenceSeverity, 
                   rule_id: str, description: str, data: Dict[str, Any]):
    incidence_id = self.incidence_reporter.add_incidence(
        subtype="BASE", incidence_type=incidence_type,
        description=description, severity=severity
    )
    # Set additional metadata
    last_incidence.rule_name = rule_id
    last_incidence.metadata = data

# Mapping Logic
if subtype == 'AT02_CUENTAS':
    return AT02_CUENTAS_MAPPING[:len(original_headers)]  # Direct replacement
else:
    return HeaderNormalizer.normalize_headers(original_headers)  # Standard normalization
```

### Validation Rules
- If `schema_headers.json` exists, validate **presence/order** of columns:
  - Exploration: extra columns → **warning** only.
  - Transformation: extra/missing columns → **error** (fail-fast).
- **AT02_CUENTAS**: Headers validated against predefined mapping list
- **Other subtypes**: Headers validated after normalization

## Transformation
- **Hard preconditions:** all required subtypes present for period; headers valid (if schema exists).
- **Incidence subsets:** saved under `transforms/AT12/incidencias/` using `[RULE]_[SUBTYPE]_[YYYYMMDD].csv`; corrected fields include side-by-side `_ORIGINAL` columns.
- **Global aggregates:** `EEOO_TABULAR_[YYYYMMDD].csv` (one row per event), also under `incidencias/`.
- **Final TXT:** headerless; BASE `|`, TDC/SOBREGIRO/VALORES space; written under `transforms/AT12/consolidated/`.

## Logging, Audit & Versioning
- **Console:** `INFO`, `WARNING`, `ERROR` (+ `DEBUG` with `--verbose`).
- **Exit codes:** `0` ok, `2` ok with warnings, `1` error.
- **Audit artifacts:**
  - `logs/run.json`: params, paths, counts, artifacts, duration.
  - `logs/manifest.json`: per file → `found|missing`, `reason` (`wrong_period`, `duplicate_subtype`, `invalid_date_in_name`, `missing`), size, `sha256`, `mtime`, source/raw paths.
  - `logs/events.jsonl`: timeline.
- **Raw mirror versioning:** if a same-named file arrives with a **different checksum**, keep both:
  - `NAME.csv` (original) and `NAME__run-<run_id>.csv` (new).

## Configuration (ENV & CLI)
- ENV:
  - `PIPELINE_WORKERS` (default `1`)
  - `STRICT_PERIOD` (default `true`)
  - `DATE_FMT` (default `%Y%m%d`)
  - `SOURCE_DIR`, `RAW_DIR`, `REPORTS_DIR`, `METRICS_DIR`, `LOGS_DIR`
- CLI:
  - `python main.py explore --atoms AT12 --year 2025 --month 08 --workers 4 --verbose`
  - `python main.py transform --atoms AT12 --year 2025 --month 08`

> Precedence: **CLI > ENV > defaults**.

## JSON Templates (AT12)

**schemas/AT12/expected_files.json**
```json
{
  "atom": "AT12",
  "case_insensitive": true,
  "subtypes": [
    "BASE_AT12",
    "TDC_AT12",
    "VALORES_AT12",
    "SOBREGIRO_AT12",
    "AFECTACIONES_AT12",
    "GARANTIA_AUTOS_AT12",
    "POLIZA_HIPOTECAS_AT12",
    "VALOR_MINIMO_AVALUO_AT12"
  ],
  "file_pattern": "[SUBTYPE]_[YYYYMMDD].CSV",
  "required_for": {
    "explore": ["*"],
    "transform": ["*"]
  }
}
```

**schemas/AT12/schema_headers.json** *(template; you'll fill the columns)*
```json
{
  "BASE_AT12": { "order_strict": true, "columns": [] },
  "TDC_AT12": { "order_strict": true, "columns": [] },
  "VALORES_AT12": { "order_strict": true, "columns": [] },
  "SOBREGIRO_AT12": { "order_strict": true, "columns": [] },
  "AFECTACIONES_AT12": { "order_strict": true, "columns": [] },
  "GARANTIA_AUTOS_AT12": { "order_strict": true, "columns": [] },
  "POLIZA_HIPOTECAS_AT12": { "order_strict": true, "columns": [] },
  "VALOR_MINIMO_AVALUO_AT12": { "order_strict": true, "columns": [] }
}
```

**AT03**
```json
{
  "AT03": {
    "order_strict": false,
    "columns": [
      "fec_proceso",
      "cod_banco",
      "cod_subsidiaria",
      "cod_preferencial",
      "aplica_feci",
      "tipo_credito",
      "facilidad_cred",
      "clasif_prest",
      "destino",
      "cod_region",
      "id_cliente",
      "tam_empresa",
      "cod_genero",
      "num_cta",
      "nombre_cliente",
      "id_grupo_economico",
      "tipo_relacion",
      "tipo_actividad CINU",
      "tasa_interes",
      "valor_inicial",
      "intereses_x_cobrar",
      "fec_ini_prestamo",
      "fec_vencto",
      "fec_refinan",
      "fec_renegoci",
      "num_cta_old",
      "tipo_garantia_1",
      "mto_garantia_1",
      "tipo_garantia_2",
      "mto_garantia_2",
      "tipo_garantia_3",
      "mto_garantia_3",
      "tipo_garantia_4",
      "mto_garantia_4",
      "tipo_garantia_5",
      "mto_garantia_5",
      "provision",
      "provison_NIIF",
      "provision_no_NIIF",
      "saldo",
      "cant_cuotas_xvenc",
      "mto_xv30d",
      "mto_xv60d",
      "mto_xv90d",
      "mto_xv120d",
      "mto_xv180d",
      "mto_xv1a",
      "Mto_xV1a5a",
      "Mto_xV5a10a",
      "Mto_xVm10a",
      "cant_cuotas_venc",
      "mto_v30d",
      "mto_v60d",
      "mto_v90d",
      "mto_v120d",
      "mto_v180d",
      "mto_v1a",
      "mto_vm1a",
      "nom_gpo_eco",
      "fec_prox_pgo_cap",
      "cod_per_cap",
      "fec_prox_pgo_int",
      "cod_per_int",
      "dias_mora",
      "perfil_venc",
      "mto_a_pagar",
      "num_cte",
      "cve_mes",
      "clave_promotor",
      "modalidad",
      "fecha_nacimiento",
      "clave_sexo",
      "cve_mon",
      "saldo_original",
      "metodologia_int",
      "interes_diferido",
      "fecha_ult_pago_capital",
      "monto_ult_pago_capital",
      "fecha_ult_pago_interes",
      "monto_ult_pago_interes",
      "tipo",
      "DETALLE CUENTA SB08",
      "CODIGO BANCO",
      "CODIGO AF"
    ]
  }
}
```

## Retention & Security
- **Retention:** 24 months (logs & reports).
- **PII:** no masking for now (revisit before production).
- **Permissions:** restrict `reports/` and `logs/` to authorized users (OS/ACLs).
