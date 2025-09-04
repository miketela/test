# Transform Context — AT12 Unified Transformation Process

## Overview
A single, auditable ETL pipeline for AT12: cleaning → enrichment → business reports → validation → final outputs. All steps are traceable via incidences and artifacts.

## Stage 1: Initial Cleansing (BASE_AT12)
1. EEOR TABULAR — Whitespace cleanup
- Trim leading/trailing spaces and collapse internal multiple spaces across all text columns.
2. Error 0301 — Id_Documento rules (1-based indexing)
- Input: rows where `Tipo_Garantia = '0301'`.
- Sub‑Rule 1 (Length 10 for specific types)
  - Condition: characters at positions 9–10 are in {'01','41','42'}.
  - Process: ensure final length = 10; if longer, take first 10; if shorter, create manual‑review incidence (no modification).
- Sub‑Rule 2 (Type 701)
  - Condition: length is 10 or 11; positions 8–10 (len=10) or 9–11 (len=11) form '701'.
  - Process: value is valid; for len=10, include in follow‑up report; do not modify.
- Sub‑Rule 3 (Length 15 for specific types)
  - Process: if length > 15, truncate to first 15 and report; if length < 15, report (no modification); if length = 15, validate positions 13–15 ∈ {'100','110','120','123','810'} (no modification).
3. COMA EN FINCA EMPRESA — Remove commas in `Id_Documento`.
4. Fecha Cancelación Errada — If `Fecha_Vencimiento` year > 2100 or < 1985 or invalid format, set `21001231`.
5. Fecha Avalúo Errada — Applies only to BASE_AT12 in Stage 1. Flag and correct only when out of bounds (year < 1985, > last day of the processing month, or invalid format). Join with `AT03_CREDITOS` by `Numero_Prestamo` ↔ `num_cta` using normalized keys (digits-only, strip leading zeros) for join only. Export only corrected rows; include `Fecha_Ultima_Actualizacion_ORIGINAL`.
6–10. Additional fixes — Missing policy/registry, policy normalizations.

## Stage 2: Enrichment and Atom Generation

### 2.0. Tipo_Facilidad pre-processing (per subtype)
- Objective: set `Tipo_Facilidad` based on presence in the appropriate auxiliary file per subtype.
- Scope: applies to `TDC_AT12` (using `AT03_TDC`) and `SOBREGIRO_AT12` (using `AT03_CREDITOS`); each runs only if its auxiliary file is available.
- Logic:
  - TDC_AT12 + AT03_TDC
    1) Normalize keys: digits-only, strip leading zeros on `Numero_Prestamo` and `num_cta`.
    2) If `Numero_Prestamo` (normalized) ∈ `AT03_TDC.num_cta` (normalized) ⇒ `Tipo_Facilidad='01'`, else `'02'`.
    3) Update rows only when the new value differs.
    - Incidence: export changed rows only to `FACILIDAD_FROM_AT03_TDC_AT12_[YYYYMMDD].csv`, preserving all columns and adding `Tipo_Facilidad_ORIGINAL`.
  - SOBREGIRO_AT12 + AT03_CREDITOS
    1) Normalize keys as above; membership check on `AT03_CREDITOS.num_cta`.
    2) Apply the same rule (`'01'` if present, else `'02'`), updating only when changed.
    - Incidence: export changed rows only to `FACILIDAD_FROM_AT03_SOBREGIRO_AT12_[YYYYMMDD].csv`, preserving all columns and adding `Tipo_Facilidad_ORIGINAL`.

### TDC_AT12 (Credit Cards)
- Numero_Garantia: sequential from 855,500 by key `(Id_Documento, Tipo_Facilidad)`; clear the target column first and sort by `Id_Documento`. Repeats within the same key reuse the number. Repeated `Numero_Prestamo` within the same key is logged only (no incidence).
- Date mapping: JOIN `Id_Documento` (TDC) ↔ `identificacion_de_cuenta` (AT02).
  - Normalize join keys (digits-only, strip leading zeros) on both sides.
  - Deduplicate AT02 on the normalized key, preferring the most recent dates.
  - Set `Fecha_Última_Actualización`/`Fecha_Ultima_Actualizacion` from `Fecha_inicio` and `Fecha_Vencimiento` from `Fecha_Vencimiento`. No change when no match.
- Inconsistency Repeated Card: detect duplicates excluding `Numero_Prestamo` with key priority:
  1) (`Identificacion_cliente`, `Identificacion_Cuenta`, `Tipo_Facilidad`), else
  2) (`Id_Documento`, `Tipo_Facilidad`).
  - Normalize key parts to avoid false positives/negatives.
  - Export: `INC_REPEATED_CARD_TDC_AT12_[YYYYMMDD].csv`.

### SOBREGIRO_AT12
- Date mapping from AT02_CUENTAS:
  - Preferred: single-key join `Id_Documento` ↔ `Identificacion_Cuenta` (normalized like TDC).
  - Fallback: dual-key join (`Identificacion_cliente`, `Identificacion_Cuenta`) when available.
  - Set `Fecha_Ultima_Actualizacion` from `Fecha_proceso` and `Fecha_Vencimiento` from `Fecha_Vencimiento`, with fallback to base values when missing.
- Money fields: normalized and formatted to comma decimals where applicable; schema preserved (e.g., `valor_ponderado`).

### VALORES_AT12
- Use reference from BASE_AT12 where `Tipo_Garantia='0507'` to populate fields like `Clave_Pais`, `Clave_Empresa`; generate `Numero_Garantia` padded when required by specification.

## Stage 3: Business Reporting — FUERA_CIERRE_AT12
- Create Excel with tabs: DESEMBOLSO (last three months), PYME (Segmento ∈ {PYME,BEC}), CARTERA (rest).
- Special rule: if `at_tipo_operacion='0301'`, set `Nombre_Organismo='182'`.
- Optionally filter out loans listed in FUERA_CIERRE.

## Stage 4: Validation — VALOR_MINIMO_AVALUO_AT12
- Initial filter: keep rows where `cu_tipo` contains alphabetic characters (not purely numeric).
- Join with `AT03_CREDITOS` by `at_num_de_prestamos = num_cta`.
- If `saldo > nuevo_at_valor_garantia`: report (keep original values). Else: update `at_valor_garantia` and `at_valor_pond_garantia`.

## Stage 5: Final Outputs (Consolidation)
- Export headerless TXT:
  - `BASE_AT12` delimited by `|`.
  - `TDC_AT12`, `SOBREGIRO_AT12`, `VALORES_AT12` delimited by a single space.
- Files written under `transforms/AT12/procesados/`.

## Incidence CSV Naming
- Per-rule subsets (full rows): `[RULE]_[SUBTYPE]_[YYYYMMDD].csv` (e.g., `FECHA_AVALUO_ERRADA_BASE_AT12_20250701.csv`). Each corrected field includes a side-by-side `_ORIGINAL` column.
- Global aggregates (e.g., EEOR_TABULAR): `EEOR_TABULAR_[YYYYMMDD].csv`.
