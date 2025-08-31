#!/usr/bin/env python3
"""
Header diff utility for AT12 subtypes (TDC_AT12, SOBREGIRO_AT12).

Compares real file headers against schemas/AT12/schema_headers.json and reports:
- Missing (expected but not in file)
- Unexpected (in file but not expected)
- Mapping suggestions (fuzzy) for mismatches

Examples:
  python3 scripts/diff_headers.py --subtype TDC_AT12 --file "source/TDC_AT12_20250701.csv"
  python3 scripts/diff_headers.py --subtype SOBREGIRO_AT12 --file "source/SOBREGIRO_AT12_20250701.csv"
  python3 scripts/diff_headers.py --auto --glob "source/*20250701*.csv"
"""

import argparse
import json
import sys
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import pandas as pd


def normalize(name: str) -> str:
    if name is None:
        return ""
    s = str(name)
    # remove accents
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    # unify separators
    for ch in [" ", "-", "/", ".", ",", ";", ":", "(", ")", "[", "]"]:
        s = s.replace(ch, "_")
    # squeeze consecutive underscores
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_").upper()


def fuzzy_match_one(target: str, candidates: List[str]) -> Tuple[Optional[str], float]:
    best = None
    best_ratio = 0.0
    for c in candidates:
        r = SequenceMatcher(None, target, c).ratio()
        if r > best_ratio:
            best_ratio = r
            best = c
    return best, best_ratio


def load_schema(schema_path: Path, subtype: str) -> List[str]:
    data = json.loads(schema_path.read_text(encoding="utf-8"))
    block = data.get(subtype)
    if not isinstance(block, dict):
        raise SystemExit(f"Subtype '{subtype}' not found in schema {schema_path}")
    return list(block.keys())


def read_headers(file_path: Path) -> List[str]:
    # Reads only header row
    df = pd.read_csv(file_path, nrows=0)
    return list(df.columns)


def diff_headers(expected: List[str], actual: List[str]) -> Dict[str, List[str]]:
    exp_norm = {normalize(c): c for c in expected}
    act_norm = {normalize(c): c for c in actual}

    missing_norm = [n for n in exp_norm.keys() if n not in act_norm]
    unexpected_norm = [n for n in act_norm.keys() if n not in exp_norm]

    missing = [exp_norm[n] for n in missing_norm]
    unexpected = [act_norm[n] for n in unexpected_norm]

    return {
        "missing": missing,
        "unexpected": unexpected,
        "missing_norm": missing_norm,
        "unexpected_norm": unexpected_norm,
        "expected_norm_keys": list(exp_norm.keys()),
    }


def print_report(subtype: str, file_path: Path, expected: List[str], actual: List[str]) -> None:
    print(f"\n=== {subtype} vs {file_path.name} ===")
    print(f"Expected columns: {len(expected)} | Actual columns: {len(actual)}")

    res = diff_headers(expected, actual)
    if not res["missing"] and not res["unexpected"]:
        print("OK: headers match after normalization (only minor formatting differences)")
        return

    if res["missing"]:
        print("\nMissing (in schema, not in file):")
        for c in res["missing"]:
            print(f"  - {c}")

    if res["unexpected"]:
        print("\nUnexpected (in file, not in schema):")
        for c in res["unexpected"]:
            print(f"  - {c}")

    # Suggestions for unexpected -> expected
    if res["unexpected"]:
        print("\nSuggestions (best fuzzy match for unexpected columns):")
        exp_norm_keys = res["expected_norm_keys"]
        for u in res["unexpected"]:
            u_norm = normalize(u)
            best, ratio = fuzzy_match_one(u_norm, exp_norm_keys)
            if best and ratio >= 0.75:
                print(f"  ~ {u} -> {best} (score={ratio:.2f})")
            else:
                print(f"  ~ {u} -> (no strong match)")


def infer_subtype_from_name(name: str) -> Optional[str]:
    up = name.upper()
    for st in ("TDC_AT12", "SOBREGIRO_AT12"):
        if up.startswith(st):
            return st
    return None


def main():
    ap = argparse.ArgumentParser(description="Diff headers for TDC_AT12/SOBREGIRO_AT12 against schema")
    ap.add_argument("--subtype", choices=["TDC_AT12", "SOBREGIRO_AT12"], help="Subtype to validate")
    ap.add_argument("--file", action="append", help="Path to input CSV file (can be repeated)")
    ap.add_argument("--schema", default="schemas/AT12/schema_headers.json", help="Path to schema JSON")
    ap.add_argument("--auto", action="store_true", help="Infer subtype from filename (prefix) if --subtype not given")
    ap.add_argument("--glob", help="Glob pattern to find files (e.g., 'source/*20250701*.csv')")
    args = ap.parse_args()

    files: List[Path] = []
    if args.file:
        files.extend(Path(f) for f in args.file)
    if args.glob:
        files.extend(Path(p) for p in Path().glob(args.glob))
    if not files:
        print("No files provided. Use --file or --glob.")
        sys.exit(2)

    schema_path = Path(args.schema)
    if not schema_path.exists():
        print(f"Schema file not found: {schema_path}")
        sys.exit(2)

    for f in files:
        if not f.exists():
            print(f"Skipping (not found): {f}")
            continue
        subtype = args.subtype
        if not subtype and args.auto:
            subtype = infer_subtype_from_name(f.name)
        if not subtype:
            print(f"Cannot infer subtype for {f.name}. Use --subtype.")
            continue

        try:
            expected = load_schema(schema_path, subtype)
            actual = read_headers(f)
            print_report(subtype, f, expected, actual)
        except Exception as e:
            print(f"Error processing {f}: {e}")


if __name__ == "__main__":
    main()

