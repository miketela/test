CSV Dialect Detection and Validation — Options (Research)

Goals
- Robustly detect CSV dialects (delimiter, quotechar, header) reliably across inputs (comma, semicolon, pipe, tab).
- Validate rows strictly (no silent drops), produce actionable error reports, and fail fast for malformed files.
- Keep performance reasonable on large files; integrate cleanly with our Pandas pipeline.

Candidates
- Python csv.Sniffer (builtin)
  - Pros: Zero dependency, quick to apply on a sample.
  - Cons: Not always reliable; fails on messy data; limited diagnostics.

- Pandas read_csv with sep=None (python engine)
  - Pros: Convenience autodetect.
  - Cons: Deprecated/slow on large files; limited control; not ideal for strict validation.

- Frictionless (https://github.com/frictionlessdata/frictionless-py)
  - Pros: Can infer Dialect (delimiter, quote), Schema, and run validations (row/field, types); rich error reports; mature.
  - Cons: Additional dependency; overhead of learning curve; performance depends on configuration.
  - Fit: Strong candidate to detect dialect and validate without loading into Pandas first. Use Detector to infer dialect, then read via Pandas using inferred parameters. Use frictionless.validate to fail-fast and export row errors.

- csvkit / agate (https://github.com/wireservice/csvkit)
  - Pros: CLI tools (in2csv) handle messy CSV; underlying agate library.
  - Cons: Best for CLI pipelines; integrating library portions adds dependencies, and validation/error reporting is less structured than Frictionless.

- petl (https://petl.readthedocs.io/)
  - Pros: Can sniff dialects; general ETL transforms.
  - Cons: Another ETL layer; validation/error reporting less formalized compared to Frictionless.

- PyArrow CSV (https://arrow.apache.org/)
  - Pros: Very fast CSV reader; integrates with Pandas.
  - Cons: Does not auto-detect delimiter; requires specifying. Good as a future performance path once dialect is known.

Recommended Approach
1) Detection: Prefer Frictionless Detector to infer Dialect (delimiter, quotechar, header) from a small sample.
2) Validation: Run Frictionless validate with an inferred/declared Schema to catch malformed rows (unbalanced quotes, width mismatch) and produce a row-level error report before Pandas load.
3) Loading: Use Pandas read_csv with the inferred delimiter/quote; optionally consider engine='pyarrow' for performance once stable.
4) Fallback: If Frictionless not installed, fallback to our enhanced Sniffer + heuristics (already implemented), still fail-fast on validation.

Deliverables for Spike
- Add optional integration utility (src/core/csv_dialect.py) that tries Frictionless if available; otherwise uses current builtin detection.
- Provide a validator that returns a structured list of errors; if non-empty, we write CSV_FORMAT_ERRORS and abort Transform.
- Keep dependency optional (soft-import), guarded by config flag (SBP_CSV_DIALECT_ENGINE=frictionless|builtin).

Open Questions
- Performance on large files (benchmark Frictionless detect/validate on >1M rows).
- How strict should validation be (e.g., stop on first error vs collect N errors)?
- Error classification for operations (row width mismatch, unbalanced quotes, invalid UTF-8 sequences, etc.).

