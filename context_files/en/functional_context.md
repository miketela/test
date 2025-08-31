# Functional Context — SBP Atoms (MVP v0.8)

## Vision
Build a modular, scalable, auditable monthly pipeline for regulatory Atoms. Active atom: AT12.

## Objectives
- Default period: previous month (CLI overrides).
- Discover inputs in `source/` and produce:
  - Exploration PDF (metrics + checklist).
  - Transformation PDF (rules summary + outputs).
- Strong traceability (logs, checksums, manifests).

## Scope (MVP)
- AT12 subtypes (one per month): `BASE_AT12`, `TDC_AT12`, `VALORES_AT12`, `SOBREGIRO_AT12`, `AFECTACIONES_AT12`, `GARANTIA_AUTOS_AT12`, `POLIZA_HIPOTECAS_AT12`, `VALOR_MINIMO_AVALUO_AT12`, plus `AT02_CUENTAS` (special headers).
- Filenames validated in UPPERCASE: `[SUBTYPE]_[YYYYMMDD].CSV`.

## Business Rules Summary
- Period coherence: mismatched files → warning in Exploration; excluded in Transformation.
- Duplicates: keep most recent by `YYYYMMDD`; warn.
- Missing subtypes: allowed in Exploration (warn), fail in Transformation.

## Header Mapping
- AT02_CUENTAS: direct replacement by schema.
- Other files: normalization (accents→ASCII, uppercase, underscores, remove `(n)`).

## Outputs
- `metrics/*.json`, `reports/*.pdf`, `transforms/AT12/incidencias/*.csv`, `transforms/AT12/procesados/*.txt`.
