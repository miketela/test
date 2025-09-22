# Technical Context — Operational Details (MVP v0.8)

## Files & Parsing
- Input: CSV; Exploration outputs: metrics (PDF export removed); Transformation: final TXT(s).
- CSV params: delimiter `,`, encoding `utf-8`, quote `"`, strictly validate row width.
- Dates: content `YYYYMMDD`; filename validated in UPPERCASE.
- Chunking: `CHUNK_ROWS=1_000_000`.

## Period Resolution
- Default: previous month. CLI overrides (`--year`, `--month`); `STRICT_PERIOD=true` by default.

## Presence & Duplicates
- `expected_files.json` defines subtypes and patterns.
- Duplicate subtype: keep the most recent by `YYYYMMDD`; warn.
- Invalid date in name: warn and ignore.

## Header Mapping & Validation
- HeaderMapper: AT02_CUENTAS direct mapping; others normalized.
- Validate presence/order against `schema_headers.json` when available.

## Incidence Reporting
- Standard fields: type, severity, rule_id, description, metadata.
- Per‑rule subsets: `[RULE]_[SUBTYPE]_[YYYYMMDD].csv` with `_ORIGINAL` columns next to corrected fields.
- Global aggregates: `EEOR_TABULAR_[YYYYMMDD].csv`.

## Consolidation
- Final TXT are headerless; delimiters: BASE `|`, TDC/SOBREGIRO/VALORES space.
- Written under `transforms/AT12/consolidated/` (processed CSVs under `transforms/AT12/procesados/`).
