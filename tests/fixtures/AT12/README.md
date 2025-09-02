AT12 sample CSVs (fixtures)

How to use
- Copy the pair of CSVs for a given period into `source/` then run:
  - `python main.py explore --atoms AT12 --year 2024 --month 1`
  - `python main.py transform --atoms AT12 --year 2024 --month 1`

Included cases
- TDC_AT12_20240131.CSV + AT02_CUENTAS_20240131.CSV
  - Purpose: trigger `INC_REPEATED_CARD` in TDC and verify date mapping with AT02 that has duplicates per account (dedup picks the most recent).
  - Expected: `transforms/AT12/incidencias/INC_REPEATED_CARD_TDC_AT12_20240131.csv` exists and contains the two rows for Id_Documento 000123/123 with Tipo_Facilidad 01. TDC dates updated from AT02 for those keys. No row multiplication.

