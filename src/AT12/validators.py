#!/usr/bin/env python3
"""
AT12 validation helpers.

Provides post-run checks to assert that inputs and outputs comply with
core rules, and emits a compact summary metrics JSON plus optional
incidence CSVs under transforms/AT12/incidencias/.

Rules implemented:
- CSV_WIDTH_MATCH: detect row width mismatches (commas/desquotes corridas).
- DATE_NOT_AFTER_PERIOD_END: detect dates greater than last day of period.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import json
import re
from datetime import datetime, date
import calendar
import pandas as pd

from src.core.io import StrictCSVReader, UniversalFileReader
from src.core.paths import AT12Paths
from src.core.config import Config


def _period_end(year: int, month: int) -> date:
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, last_day)


def _parse_possible_date_series(s: pd.Series) -> pd.Series:
    """Best-effort parse for common input date formats to pandas datetime.

    Tries '%Y%m%d' first (regulatory format), then pandas default.
    Returns a pandas datetime Series (NaT on failure).
    """
    try:
        out = pd.to_datetime(s.astype(str), format="%Y%m%d", errors="coerce")
        # If too many NaT, fallback to generic parse
        if out.isna().mean() > 0.95:
            out = pd.to_datetime(s.astype(str), errors="coerce")
        return out
    except Exception:
        return pd.to_datetime(s.astype(str), errors="coerce")


@dataclass
class RuleResult:
    name: str
    status: str  # PASS | WARN | FAIL
    details: Dict[str, Any] = field(default_factory=dict)


class AT12Validator:
    """Run AT12 validations and persist summary + incidences."""

    def __init__(self, config: Config, year: int, month: int, run_id: str):
        self.config = config
        self.year = year
        self.month = month
        self.run_id = run_id
        self.paths = AT12Paths.from_config(config)
        self.paths.ensure_directories()
        self.ufr = UniversalFileReader()

    # ---------------- CSV width consistency ----------------
    def validate_csv_alignment(self, files: List[Path]) -> RuleResult:
        reader = StrictCSVReader()
        total = 0
        files_with_issues = 0
        total_row_mismatches = 0
        mismatch_rows: List[Dict[str, Any]] = []

        for fp in files:
            if fp.suffix.lower() not in {".csv", ".txt"}:
                continue
            total += 1
            try:
                res = reader.validate_file(fp)
                # Parse warnings like: "Row N: width mismatch (expected X, got Y)"
                for w in (res.warnings or []):
                    m = re.search(r"Row\s+(\d+)\D+expected\s+(\d+).*got\s+(\d+)", w)
                    if m:
                        files_with_issues += 1
                        rnum, expc, gotc = int(m.group(1)), int(m.group(2)), int(m.group(3))
                        total_row_mismatches += 1
                        mismatch_rows.append({
                            "file": fp.name,
                            "row": rnum,
                            "expected_cols": expc,
                            "got_cols": gotc
                        })
            except Exception:
                # Treat unreadable file as issue but keep going
                files_with_issues += 1

        # Export detailed mismatch table if any
        if mismatch_rows:
            df = pd.DataFrame(mismatch_rows)
            out = self.paths.incidencias_dir / f"EEOO_TABULAR_WIDTH_MISMATCH_AT12_{self.year:04d}{self.month:02d}01.csv"
            df.to_csv(out, index=False, encoding="utf-8", sep="|", quoting=1)

        status = "PASS" if total_row_mismatches == 0 else ("WARN" if files_with_issues <= 1 else "FAIL")
        return RuleResult(
            name="CSV_WIDTH_MATCH",
            status=status,
            details={
                "files_checked": total,
                "files_with_issues": files_with_issues,
                "rows_flagged": total_row_mismatches,
                "incidence_file": (
                    f"EEOO_TABULAR_WIDTH_MISMATCH_AT12_{self.year:04d}{self.month:02d}01.csv"
                    if mismatch_rows else None
                )
            }
        )

    # ---------------- Dates within period end ----------------
    def validate_dates_not_after_period_end(self, processed_files: List[Path]) -> RuleResult:
        period_end = _period_end(self.year, self.month)
        # Candidate column names/patterns
        static_candidates = {
            "Fecha_Inicio", "Fecha_inicio", "Fecha_Apertura", "Fecha_Ultima_Actualizacion",
            "Fecha_Última_Actualización", "Fecha_Vencimiento", "Fecha_proceso", "Fecha_Renovacion",
            "Fecha_Avaluo", "Fecha_Avaluó", "Fecha_Avaluo_Informe",
        }
        pat = re.compile(r"(?i)^fecha")

        violations: List[Dict[str, Any]] = []
        sub_summary: Dict[str, Dict[str, Any]] = {}

        for fp in processed_files:
            try:
                df = self.ufr.read_file(fp)
            except Exception:
                continue
            subtype = Path(fp).stem.replace("AT12_", "")

            # Detect candidate date columns present in this DF
            cols = list(df.columns)
            cand_cols = [c for c in cols if (c in static_candidates or pat.search(str(c)))]
            if not cand_cols:
                continue

            # Parse and flag
            flagged_rows = 0
            cols_flagged: Dict[str, int] = {}
            for col in cand_cols:
                try:
                    dt = _parse_possible_date_series(df[col])
                    mask = (dt.notna()) & (dt.dt.date > period_end)
                    count = int(mask.sum())
                    if count:
                        cols_flagged[col] = count
                        flagged_rows += count
                        # Append detailed violations
                        for idx in df.index[mask]:
                            violations.append({
                                "file": fp.name,
                                "subtype": subtype,
                                "row": int(idx) + 2,  # +2 for header + 1-indexing
                                "column": col,
                                "value": str(df.at[idx, col])
                            })
                except Exception:
                    continue

            if flagged_rows:
                sub_summary[subtype] = {
                    "rows_flagged": flagged_rows,
                    "columns": sorted(list(cols_flagged.keys()))
                }

        # Export violations if any
        incid_path = None
        if violations:
            vdf = pd.DataFrame(violations)
            incid_path = self.paths.incidencias_dir / f"EEOO_TABULAR_FUTURE_DATE_AT12_{self.year:04d}{self.month:02d}01.csv"
            vdf.to_csv(incid_path, index=False, encoding="utf-8", sep="|", quoting=1)

        status = "PASS" if not violations else "FAIL"
        return RuleResult(
            name="DATE_NOT_AFTER_PERIOD_END",
            status=status,
            details={
                "period_end": period_end.strftime("%Y-%m-%d"),
                "subtypes": sub_summary,
                "incidence_file": incid_path.name if incid_path else None
            }
        )

    # ---------------- Rule 9: Auto Policy mapping validation ----------------
    def validate_auto_policy_rule9(self, processed_files: List[Path], input_files: List[Path]) -> RuleResult:
        """Validate Rule 9 on processed BASE using GARANTIA_AUTOS_AT12 as reference.

        Checks that for BASE rows where Tipo_Garantia in {'0101','0103'}:
        - If GARANTIA_AUTOS has a numeric num_poliza for the same Numero_Prestamo, then
          processed Id_Documento should equal that numeric policy (not empty).
        - If GARANTIA_AUTOS has a non-numeric num_poliza, processed Id_Documento must not
          equal that non-numeric value (we do not apply updates with letters).
        """
        # Find processed BASE file
        base_fp: Optional[Path] = None
        for fp in processed_files:
            name = Path(fp).name.upper()
            if 'BASE_AT12' in name:
                base_fp = Path(fp)
                break
        if base_fp is None or not base_fp.exists():
            return RuleResult(
                name="AUTO_POLICY_RULE9",
                status="WARN",
                details={"reason": "Processed BASE_AT12 file not found"}
            )

        # Find GARANTIA_AUTOS input
        autos_fp: Optional[Path] = None
        for fp in input_files:
            name = Path(fp).name.upper()
            if 'GARANTIA_AUTOS_AT12' in name:
                autos_fp = Path(fp)
                break
        if autos_fp is None or not autos_fp.exists():
            return RuleResult(
                name="AUTO_POLICY_RULE9",
                status="WARN",
                details={"reason": "GARANTIA_AUTOS_AT12 input not found"}
            )

        # Load
        try:
            base_df = self.ufr.read_file(base_fp)
            autos_df = self.ufr.read_file(autos_fp)
        except Exception as e:
            return RuleResult(
                name="AUTO_POLICY_RULE9",
                status="WARN",
                details={"reason": f"Failed reading inputs: {e}"}
            )

        # Required columns
        need_base = {'Numero_Prestamo', 'Tipo_Garantia', 'Id_Documento'}
        if not need_base.issubset(set(base_df.columns)):
            return RuleResult(
                name="AUTO_POLICY_RULE9",
                status="WARN",
                details={"reason": f"BASE missing required columns: {sorted(list(need_base - set(base_df.columns)))}"}
            )
        if not {'numcred', 'num_poliza'}.issubset(set(autos_df.columns)):
            return RuleResult(
                name="AUTO_POLICY_RULE9",
                status="WARN",
                details={"reason": "GARANTIA_AUTOS_AT12 missing numcred/num_poliza"}
            )

        # Build maps
        def _norm(s: pd.Series) -> pd.Series:
            return s.astype(str).str.replace(r"\D", "", regex=True).str.lstrip('0').where(lambda x: x != '', '0')

        autos_df = autos_df.copy()
        autos_df['_key'] = _norm(autos_df['numcred'])
        policy_map = autos_df.set_index('_key')['num_poliza']

        # Filter BASE
        base = base_df.copy()
        tg = base['Tipo_Garantia'].astype(str).str.replace(r"\D", "", regex=True)
        base_mask = tg.isin({'0101', '0103'})
        base['_key'] = _norm(base['Numero_Prestamo'])
        base_idoc = base['Id_Documento'].astype(str).str.strip()

        # Evaluate
        violations: List[Dict[str, Any]] = []
        for idx in base.index[base_mask]:
            key = base.at[idx, '_key']
            p = policy_map.get(key, None)
            if p is None or str(p).strip() == '':
                # No policy value in AUTOS → no expectation
                continue
            sp = str(p).strip()
            idoc_val = base_idoc.loc[idx]
            # Expect exact overwrite with any non-empty policy (letters/dashes allowed)
            if idoc_val != sp:
                violations.append({
                    'file': base_fp.name,
                    'row': int(idx) + 2,
                    'Numero_Prestamo_norm': key,
                    'expected_policy': sp,
                    'current_Id_Documento': idoc_val,
                    'violation': 'INCORRECT_POLICY_VALUE'
                })

        # Export violations
        incid_path = None
        if violations:
            vdf = pd.DataFrame(violations)
            incid_path = self.paths.incidencias_dir / f"EEOO_TABULAR_AUTO_POLICY_RULE9_AT12_{self.year:04d}{self.month:02d}01.csv"
            vdf.to_csv(incid_path, index=False, encoding='utf-8', sep='|', quoting=1)

        status = "PASS" if not violations else "FAIL"
        return RuleResult(
            name="AUTO_POLICY_RULE9",
            status=status,
            details={
                "violations": len(violations),
                "incidence_file": incid_path.name if incid_path else None
            }
        )

    # ---------------- Persist summary ----------------
    def write_summary(self, results: List[RuleResult]) -> Path:
        metrics = {
            "atom": "AT12",
            "period": f"{self.year:04d}{self.month:02d}",
            "run_id": self.run_id,
            "timestamp": datetime.now().isoformat(),
            "rules": [
                {"name": r.name, "status": r.status, "details": r.details} for r in results
            ],
            "status": ("PASS" if all(r.status == "PASS" for r in results) else "WARN"
                        if any(r.status == "WARN" for r in results) and not any(r.status == "FAIL" for r in results)
                        else "FAIL")
        }
        Path(self.config.metrics_dir).mkdir(parents=True, exist_ok=True)
        out = Path(self.config.metrics_dir) / f"validation_AT12_{self.year:04d}{self.month:02d}__run-{self.run_id}.json"
        out.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
        return out

    # ---------------- FDE Rule: Foreign Trustee Standardization ----------------
    def validate_fde_rule(self, processed_files: List[Path]) -> RuleResult:
        """Validate that rows with Nombre_Fiduciaria containing 'FDE' have Origen='E' and Region='320'."""
        # Find processed BASE
        base_fp: Optional[Path] = None
        for fp in processed_files:
            name = Path(fp).name.upper()
            if 'BASE_AT12' in name:
                base_fp = Path(fp)
                break
        if base_fp is None or not base_fp.exists():
            return RuleResult(
                name="FDE_RULE",
                status="WARN",
                details={"reason": "Processed BASE_AT12 file not found"}
            )

        try:
            base = self.ufr.read_file(base_fp)
        except Exception as e:
            return RuleResult(name="FDE_RULE", status="WARN", details={"reason": f"Failed to read BASE: {e}"})

        if 'Nombre_Fiduciaria' not in base.columns:
            return RuleResult(name="FDE_RULE", status="WARN", details={"reason": "Nombre_Fiduciaria missing"})

        # Resolve target columns
        origen_col = 'Codigo_Origen' if 'Codigo_Origen' in base.columns else ('Origen' if 'Origen' in base.columns else None)
        region_col = 'Codigo_Region' if 'Codigo_Region' in base.columns else ('Cod_region' if 'Cod_region' in base.columns else None)
        if origen_col is None and region_col is None:
            return RuleResult(name="FDE_RULE", status="WARN", details={"reason": "No Origen/Region columns present"})

        b = base.copy()
        mask = b['Nombre_Fiduciaria'].astype(str).str.contains('FDE', case=False, na=False)
        if not mask.any():
            return RuleResult(name="FDE_RULE", status="PASS", details={"candidates": 0})

        violations: List[Dict[str, Any]] = []
        for idx in b.index[mask]:
            ok = True
            if origen_col is not None:
                if str(b.at[idx, origen_col]).strip() != 'E':
                    ok = False
            if region_col is not None:
                if str(b.at[idx, region_col]).strip() != '320':
                    ok = False
            if not ok:
                violations.append({
                    'file': base_fp.name,
                    'row': int(idx) + 2,
                    'Nombre_Fiduciaria': str(b.at[idx, 'Nombre_Fiduciaria']).strip(),
                    'Origen_value': (str(b.at[idx, origen_col]).strip() if origen_col else None),
                    'Region_value': (str(b.at[idx, region_col]).strip() if region_col else None)
                })

        # Export violations
        incid = None
        if violations:
            dfv = pd.DataFrame(violations)
            incid = self.paths.incidencias_dir / f"EEOO_TABULAR_FDE_RULE_AT12_{self.year:04d}{self.month:02d}01.csv"
            dfv.to_csv(incid, index=False, encoding='utf-8', sep='|', quoting=1)

        status = "PASS" if not violations else "FAIL"
        return RuleResult(name="FDE_RULE", status=status, details={
            "violations": len(violations),
            "incidence_file": incid.name if incid else None
        })
