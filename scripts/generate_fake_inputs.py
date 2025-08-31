#!/usr/bin/env python3
import argparse
import csv
import json
import os
import random
import re
from datetime import datetime


SCHEMAS_DIR = os.path.join("schemas")
AT12_EXPECTED = os.path.join(SCHEMAS_DIR, "AT12", "expected_files.json")
AT12_HEADERS = os.path.join(SCHEMAS_DIR, "AT12", "schema_headers.json")
AT03_HEADERS = os.path.join(SCHEMAS_DIR, "AT03", "schema_headers.json")
SOURCE_DIR = os.path.join("source")


def load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def choose_headers(subtype: str, at12_schema: dict, at03_schema: dict):
    # AT03_CREDITOS should use schemas/AT03/schema_headers.json -> key "AT03" -> columns
    if subtype == "AT03_CREDITOS":
        cols = at03_schema.get("AT03", {}).get("columns", [])
        if not cols:
            raise ValueError("AT03 schema columns not found")
        return cols
    # Otherwise, prefer AT12 schema headers with same subtype key
    block = at12_schema.get(subtype)
    if not isinstance(block, dict):
        raise ValueError(f"Headers for {subtype} not found in AT12 schema")
    # Preserve insertion order of keys
    return list(block.keys())


def fake_value(col: str, row_idx: int, ymd: str):
    # Simple heuristics for realistic-ish values
    name = col.lower()

    # Dates
    if re.search(r"fecha|fec_", name):
        # Return YYYY-MM-DD for readability
        try:
            dt = datetime.strptime(ymd, "%Y%m%d").date()
            return dt.isoformat()
        except Exception:
            return "2024-01-31"

    # Monetary / numeric amounts
    if any(k in name for k in [
        "monto", "mto", "valor", "saldo", "importe", "tasa", "interes", "provision", "cuotas", "dias", "dpd"
    ]):
        base = (row_idx + 1) * 100
        # percent-like
        if "tasa" in name:
            return round(5 + (row_idx % 10) * 0.5, 2)
        return base

    # Codes and identifiers
    if any(k in name for k in [
        "cod", "codigo", "id_", "ident", "num", "numero", "clave", "cve", "cuenta"
    ]):
        return f"{row_idx+1:06d}"

    # Boolean-ish flags
    if any(k in name for k in ["aplica", "flag", "marca", "beneficiario", "status", "estatus"]):
        return random.choice(["S", "N"])  # Sí/No

    # Text fields
    if any(k in name for k in ["nombre", "detalle", "pais", "segmento", "producto", "actividad", "origen", "destino", "modalidad", "genero", "tamano", "tam_empresa", "tipo", "descripcion"]):
        return f"{col}_VAL_{row_idx+1}"

    # Fallback
    return f"{row_idx+1}"


def make_filename(subtype: str, ymd: str) -> str:
    # Use uppercase extension to match patterns
    return f"{subtype}_{ymd}.CSV"


def write_csv(path: str, headers: list, rows: int, ymd: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for i in range(rows):
            writer.writerow([fake_value(h, i, ymd) for h in headers])


def main():
    parser = argparse.ArgumentParser(description="Generate fake inputs from schemas")
    parser.add_argument("--date", dest="date", default="20240131", help="Date in YYYYMMDD for filenames and dates")
    parser.add_argument("--rows", dest="rows", type=int, default=10, help="Rows per file")
    parser.add_argument("--only", nargs="*", help="Optional list of subtypes to generate (defaults to all)")
    args = parser.parse_args()

    expected = load_json(AT12_EXPECTED)
    at12_schema = load_json(AT12_HEADERS)
    at03_schema = load_json(AT03_HEADERS)

    subtypes = list(expected.get("subtypes", {}).keys())
    if args.only:
        subtypes = [s for s in subtypes if s in args.only]
        if not subtypes:
            raise SystemExit("No matching subtypes in --only list")

    generated = []
    for subtype in subtypes:
        headers = choose_headers(subtype, at12_schema, at03_schema)
        filename = make_filename(subtype, args.date)
        out_path = os.path.join(SOURCE_DIR, filename)
        write_csv(out_path, headers, args.rows, args.date)
        generated.append(out_path)

    print("Generated files:")
    for p in generated:
        print(" -", p)


if __name__ == "__main__":
    main()

