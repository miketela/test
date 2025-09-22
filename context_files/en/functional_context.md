# Functional Context — SBP Atoms (MVP v0.8)

## Vision
Build a **modular, scalable, and auditable** monthly pipeline for SBP regulatory “Atoms”. The active focus is **AT12**, while AT03 remains in draft to be handled separately.

## Objectives
- Run monthly (default period: **previous month**; CLI can override via `--year` and `--month`).
- Auto-discover inputs directly in the `source/` directory.
- Produce machine-readable artifacts per run (PDF generation removed to reduce noise):
  - Exploration metrics (`metrics/*.json`), manifests, and logs for audit trails.
  - Transformation outputs under `transforms/AT12/` (incidences, consolidated TXT, follow-up Excel).
- Maintain strong **traceability** and **auditability** through logs, manifests, and checksums.
- Preserve an architecture that can support a future **UI layer**.

## Scope (MVP)
- **Active atom:** AT12.
- **Expected subtypes** (exactly one file per month, case-insensitive):
  - `BASE_AT12_[YYYYMMDD].CSV`
  - `TDC_AT12_[YYYYMMDD].CSV`
  - `VALORES_AT12_[YYYYMMDD].CSV`
  - `SOBREGIRO_AT12_[YYYYMMDD].CSV`
  - `AFECTACIONES_AT12_[YYYYMMDD].CSV`
  - `GARANTIA_AUTOS_AT12_[YYYYMMDD].CSV`
  - `POLIZA_HIPOTECAS_AT12_[YYYYMMDD].CSV`
  - `VALOR_MINIMO_AVALUO_AT12_[YYYYMMDD].CSV`
  - `AT02_CUENTAS_[YYYYMMDD].CSV` *(specialized handling)*

> The system normalizes filenames to **UPPERCASE** before validation.
> **AT02_CUENTAS** receives dedicated header mapping rules.

## Key Business Rules
- **Default period:** previous month (overridable via CLI using `--year` and `--month`; accepts `MM` or month name, default is numeric `MM`).
- **Period coherence:** if the `YYYYMM` in the filename differs from the expected period, register a **warning** during Exploration and exclude the file from Transformation (count as **missing**).
- **Duplicate subtype in the same month:** keep the **most recent by `YYYYMMDD`** and register a **warning**.
- **Invalid date in filename:** register a **warning** and ignore the file.
- **Missing subtypes:**
  - **Exploration:** allowed, emits a warning, and highlights the subtype in the checklist.
  - **Transformation:** **fails** (non-zero exit code) and lists the missing files.

## Header Mapping System ✅

### AT02_CUENTAS Specialized Handling
- **Direct schema replacement:** replace headers with the predefined schema regardless of input names.
- **Position-based alignment:** first *N* input headers map to the first *N* schema headers.
- **Mixed-case compatibility:** accepts headers in uppercase, lowercase, or mixed case.
- **Parenthetical cleaning:** removes numbering such as `(1)`, `(2)`, etc.

### Standard Header Normalization (Other Files)
- **Accent removal:** converts characters like ñ → n, á → a, é → e, í → i, ó → o, ú → u.
- **Case normalization:** transforms headers to uppercase.
- **Whitespace handling:** replaces spaces with underscores.
- **Special character cleanup:** removes parenthetical numbers and extra spaces.

### Mapping Reports
- **Detailed tracking:** logs original → mapped transformations.
- **Method classification:** labels whether the mapping was “direct” or “normalized”.
- **Audit trail:** retains mapping data for inclusion in validation reports.

## Functional Processes

### 1) Exploration
- Discover inputs and copy them to `data/raw/` (controlled mirror). If a file with the same name already exists but a different checksum is detected, create a **versioned** copy using the suffix `__run-<run_id>.CSV`.
- Compute per-file metrics including:
  - row and column counts, percentage of nulls, inferred dtypes, top-N unique values,
  - string length profiles, file size, modification time, **SHA256**,
  - header presence and ordering checks when a schema is available.
- Outputs:
  - `metrics/*.csv|json`
  - `logs/run.json`, `logs/manifest.json`, `logs/events.jsonl` (PDF export removed)

### 2) Transformation
- **Precondition:** all required subtypes for the processing month must be present.
- Apply header normalization and the defined business rules (detailed rule set evolving).
- Outputs:
  - **Single consolidated TXT** per month: `AT12_CONSOLIDATED_<YYYYMM>__run-<run_id>.TXT` (layout defined by the SBP specification)
  - Incidence CSVs and Excel follow-up artifacts under `transforms/AT12/` (no PDF summary)

## Traceability & Audit
- **CLI logging:** defaults to INFO level; `--verbose` enables DEBUG.
- **Exit codes:** `0` success without warnings, `2` success with warnings, `1` failure.
- **Persistent artifacts:** `run.json`, `manifest.json` (records missing files with `reason`), `events.jsonl`.
- **Retention policy:** keep metrics and logs for 24 months.

## Data Dependencies
- **AT03 → AT12:** AT03 provides the loan `saldo` (balance) required for validating `valor_minimo_avaluo` and the `fec_ini_prestamo` (loan start date) needed to correct `Fecha_Avaluo_Errada`.

## Out of Immediate Scope
- UI delivery (architecture prepared but not implemented in this MVP).
