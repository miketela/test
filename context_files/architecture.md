# Architecture — Layers, Modules, Artifacts (MVP v0.8)

## Logical Layers
1. **Orchestrator (`main.py`)**
   - Resolves period (default: previous month; CLI overrides).
   - Discovers files by atom/subtype.
   - Invokes **explore** / **transform** processes.
   - Manages exit codes and top-level logging.

2. **Configuration (`config.py`)**
   - Base paths & parameters (CSV rules, date fmt, chunking, workers).
   - Default period and override parsing.
   - Logging severity control.

3. **Per-atom Modules (`src/AT12/processor.py`)**
   - Unified interface:
     - `discover_files(period) -> list[Path]`
     - `explore(files) -> artifacts`
     - `transform(files) -> artifacts`
   - Logic for header normalization, presence validation, duplicates, etc.

4. **Core utils (`src/core/`)**
   - `io.py` (strict CSV read/write, chunking)
   - `fs.py` (copy, checksum, versioning)
   - `time_utils.py` (period resolution, dates)
   - `log.py` (console + structured audit logs)
   - `metrics.py` (exploration metrics computation)
   - `naming.py` (UPPERCASE normalization, parse `[SUBTYPE]_[YYYYMMDD]`, header normalization)
   - `header_mapping.py` (advanced header mapping system with AT02_CUENTAS support)

5. **Schemas (`schemas/AT012/...`)**
   - `expected_files.json` (subtype catalog + pattern)
   - `schema_headers.json` (columns only, optional)

## Folder Structure
```
repo/
├─ README.md
├─ .env
├─ context/
│  ├─ functional_context.md
│  ├─ technical_context.md
│  ├─ architecture.md
│  └─ pending.md
├─ main.py
├─ config.py
├─ src/
│  ├─ core/
│  │  ├─ io.py
│  │  ├─ fs.py
│  │  ├─ time_utils.py
│  │  ├─ pdf.py
│  │  ├─ log.py
│  │  └─ metrics.py
│  └─ AT12/
     └─ processor.py
├─ schemas/
│  ├─ AT12/
│  │  ├─ expected_files.json
│  │  └─ schema_headers.json
│  └─ AT03/
│     └─ draft.json
├─ source/
│  └─ *.csv (archivos AT12 con formato [SUBTYPE]_[YYYYMMDD].csv)
├─ data/
│  └─ raw/
│     └─ *.csv (archivos copiados con versionado)
├─ metrics/
│  └─ exploration_metrics_*.json
└─ logs/
   └─ *.log
```

## Header Mapping System ✅

### Architecture Overview
The header mapping system provides intelligent header transformation with specialized handling for different file subtypes:

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Raw Headers   │───▶│  HeaderMapper    │───▶│ Mapped Headers  │
│ (from CSV file) │    │                  │    │ (normalized)    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌──────────────────┐
                       │ Mapping Strategy │
                       │                  │
                       │ AT02_CUENTAS:    │
                       │ Direct Replace   │
                       │                  │
                       │ Others:          │
                       │ Normalize        │
                       └──────────────────┘
```

### Components
- **HeaderMapper**: Main orchestrator for header transformations
- **HeaderNormalizer**: Text normalization utilities (accents, case, cleaning)
- **AT02_CUENTAS_MAPPING**: Predefined schema with 30 headers for direct replacement
- **Mapping Reports**: Detailed audit trail with original→mapped transformations

## Data Flow Diagram

```
┌─────────────┐
│   source/   │ ──┐
│ *.csv files │   │
└─────────────┘   │
                  │
                  ▼
            ┌──────────────┐
            │ File Discovery│
            │ & Validation  │
            └──────────────┘
                  │
                  ▼
            ┌──────────────┐
            │ Header       │
            │ Mapping      │ ──┐
            └──────────────┘   │
                  │            │
                  ▼            ▼
            ┌──────────────┐ ┌──────────────┐
            │ Data         │ │ Mapping      │
            │ Processing   │ │ Reports      │
            └──────────────┘ └──────────────┘
                  │            │
                  ▼            │
            ┌──────────────┐   │
            │ Metrics      │   │
            │ Calculation  │   │
            └──────────────┘   │
                  │            │
                  ▼            │
            ┌──────────────┐   │
            │ File Copy    │   │
            │ to data/raw/ │   │
            └──────────────┘   │
                  │            │
                  ▼            │
            ┌──────────────┐   │
            │ Report       │◀──┘
            │ Generation   │
            └──────────────┘
                  │
                  ▼
    ┌─────────────────────────────────┐
    │         Outputs:                │
    │ • data/raw/*.csv (versioned)    │
    │ • metrics/*.json               │
    │ • logs/*.log                   │
    └─────────────────────────────────┘
```

## High-level Flow

**Exploration**
1. Resolve period → `YYYY/MM`.
2. Load `expected_files.json` (AT12).
3. Discover CSV files directly in `source/` directory.
4. Parse filenames with format `[SUBTYPE]_[YYYYMMDD]` and validate period coherence.
5. **Header Mapping**: Apply intelligent header transformation based on subtype:
   - AT02_CUENTAS: Direct replacement with predefined schema
   - Other files: Standard normalization (accent removal, case normalization)
6. Handle duplicates by keeping most recent version / validate file structure.
7. Copy to `data/raw` with run versioning (e.g., `filename__run-YYYYMM.csv`).
8. Compute metrics and validate mapped headers against `schema_headers.json`.
9. Generate mapping reports for audit trail.
10. Write `metrics/*.json` and `logs/*.log` (PDF export removed).

**Transformation**
1. Verify preconditions (missing ⇒ fail).
2. Normalize headers; reorder columns (if schema exists).
3. Apply join/order/derived rules (pending).
4. Write **consolidated TXT** (no PDF summary).

## Key Interfaces
- **CLI**
  - `python main.py explore --atoms AT12 --year 2025 --month 08 --workers 4 --verbose`
  - `python main.py transform --atoms AT12 --year 2025 --month 08`
- **ENV**
  - `PIPELINE_WORKERS=1`, `STRICT_PERIOD=true`, `DATE_FMT=%Y%m%d`

## Performance Considerations
- **Chunking** by rows (`CHUNK_ROWS=1_000_000`).
- **Parallelism** by subtype (default 1, configurable via ENV/CLI).
- Sequential I/O with hashing (SHA256) for auditability.

## Observability
- Console logs + structured audit (JSON/JSONL).
- Artifacts carry `run_id` in names for traceability.
