# Architecture — Layers, Modules, Artifacts (MVP v0.8)

## Logical Layers
1. Orchestrator (`main.py`)
- Resolves period, discovers files, invokes explore/transform, manages exit codes/logging.
2. Configuration (`config.py`)
- Centralizes base paths, CSV/XLSX params, workers, strict-period, logging.
3. Per-atom Modules (`src/AT12/processor.py`)
- Public interface: `discover_files`, `explore`, `transform`.
4. Core Utilities (`src/core/`)
- `io.py`, `fs.py`, `time_utils.py`, `log.py`, `metrics.py`, `naming.py`, `header_mapping.py`.
5. Schemas (`schemas/<ATOM>/`)
- `expected_files.json`, `schema_headers.json`.

## Directory Structure
```
transforms/
└─ AT12/
   ├─ incidencias/    # Incidence CSVs
   ├─ procesados/     # Corrected CSVs and TXT outputs
   └─ (optional) consolidated/  # Final TXT outputs (kept for future)
```

## Header Mapping System
- AT02_CUENTAS: direct header replacement by schema order.
- Others: normalization (accent removal, uppercase, underscores, remove `(n)`).
- Audit: mapping reports and metrics JSON.

## Data Flow (High-level)
- Explore: discover → map headers → copy to `data/raw` with run versioning → metrics (PDF export removed).
- Transform: stage pipeline (cleaning → enrichment → business reporting → validation → outputs).
- Outputs: `metrics/*.json`, `transforms/AT12/procesados/*.txt`, logs.
