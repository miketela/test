# Functional Context — SBP Atoms (MVP v0.8)

## Vision
Build a **modular, scalable, and auditable** monthly pipeline for SBP regulatory “Atoms”. In this phase the focus is **AT12** (AT03 remains in “draft” to be handled separately).

## Objectives
- Run monthly (default: **previous month**).
- Auto-discover inputs directly in `source/` directory.
- Produce two business artifacts per run:
  - **Exploration PDF** (metrics + checklist).
  - **Transformation PDF** (summary of rules applied + outputs).
- Maintain strong **traceability** and **auditability** (logs, manifests, checksums).
- Keep the architecture ready for a **future UI**.

## Scope (MVP)
- **Active atom:** AT12.
- **Expected subtypes (exactly one file per month, case-insensitive):**
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
> **AT02_CUENTAS** files receive specialized header mapping treatment.

## Key Business Rules
- **Default period:** previous month (override via `--year --month`; accepts `MM` or month name; default `MM`).
- **Period coherence:** if `YYYYMM` in the filename ≠ expected period → **warning** in Exploration and **excluded** from Transformation (counted as **missing**).
- **Duplicate subtype (same month):** take the **most recent by date in the name** (`YYYYMMDD`), record a **warning**.
- **Invalid date in name:** record a **warning** and ignore the file.
- **Missing subtypes:**
  - **Exploration:** allowed (warning + appears in checklist).
  - **Transformation:** **fail** (exit code ≠ 0) listing which ones are missing.

## Header Mapping System ✅

The system includes an advanced header mapping and normalization engine:

### AT02_CUENTAS Specialized Handling
- **Direct Replacement:** Headers are replaced with predefined schema headers regardless of input content
- **Position-Based Mapping:** First N input headers map to first N schema headers
- **Mixed-Case Support:** Handles headers in various case formats (uppercase, lowercase, mixed)
- **Parenthetical Cleaning:** Removes numbers in parentheses like (1), (2), etc.

### Standard Header Normalization (Other Files)
- **Accent Removal:** Converts ñ → n, á → a, é → e, í → i, ó → o, ú → u
- **Case Normalization:** Converts to uppercase
- **Space Handling:** Replaces spaces with underscores
- **Special Character Cleaning:** Removes parenthetical numbers and extra spaces

### Mapping Reports
- **Detailed Tracking:** Records original → mapped transformations
- **Method Classification:** Identifies whether mapping was 'direct' or 'normalized'
- **Audit Trail:** Provides complete mapping information for validation reports

## Functional Processes

### 1) Exploration
- Discover and copy to `data/raw` (controlled mirror). If a same-named file already exists with a different checksum → **version** using suffix `__run-<run_id>.CSV`.
- Compute per-file metrics:
  - rows, columns, % nulls, inferred dtypes, top-N uniques, basic stats,
  - string lengths, file size, mtime, **SHA256**,
  - (if headers schema available) presence/order check.
- Outputs:
  - **Exploration PDF** (cover, executive summary, per-file “cards”).
  - `metrics/*.csv|json`, `logs/run.json`, `logs/manifest.json`, `logs/events.jsonl`.

### 2) Transformation
- **Precondition:** all required subtypes present for the processed month.
- Apply minimal normalizations (headers) and the defined rules (details pending).
- Outputs:
  - **One consolidated TXT** per month: `AT12_CONSOLIDATED_<YYYYMM>__run-<run_id>.TXT` (exact layout pending).
  - **Transformation PDF** (steps applied, row counts, discards/joins).

## Traceability & Audit
- **Console:** INFO by default; `--verbose` enables DEBUG.
- **Exit codes:** `0` success no warnings, `2` success with warnings, `1` error.
- **Persistent audit:** `run.json`, `manifest.json` (includes **missing** with `reason`), `events.jsonl`.
- **Retention:** 24 months (reports & logs).

## Out of Immediate Scope
- AT03 (left in “draft”: expected/schema placeholders).
- UI (architecture ready; not implemented now).
