from typing import Dict, List, Optional, Any, Sequence, Union
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
import calendar

from src.core.transformation import TransformationEngine, TransformationContext, TransformationResult
from src.core.incidence_reporter import IncidenceReporter, IncidenceType, IncidenceSeverity
from src.core.naming import FilenameParser


class AT12TransformationEngine(TransformationEngine):
    """AT12 Transformation Engine for processing AT12 data files."""
    
    def __init__(self, config):
        super().__init__(config)
        self.atom_type = "AT12"
        self.incidences_data: Dict[str, List[Dict]] = {}
        self.incidence_reporter: Optional[IncidenceReporter] = None
        # AT12 expected subtypes
        expected_subtypes = ['TDC', 'SOBREGIRO', 'VALORES']
        self._filename_parser = FilenameParser(expected_subtypes)
        # Track last generated TDC Numero_Garantía (numeric) for VALORES assignment rule
        self._last_tdc_num_garantia: Optional[int] = None

    # -------------------- TDC helpers (normalization/enrichment) --------------------
    def _normalize_tdc_basic(self, df: pd.DataFrame) -> pd.DataFrame:
        """Basic normalization for TDC: trim text and normalize monetary fields.

        - Trim whitespace for object columns.
        - Monetary fields: remove thousands '.', convert ',' to '.' for internal numeric use.
        """
        if df is None or df.empty:
            return df
        df = df.copy()
        # Trim object/string columns and normalize NA tokens
        na_tokens = {"n/a", "na"}
        for col in df.columns:
            if pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
                original_nan_mask = df[col].isna()
                series = df[col].astype(str).str.strip()
                series = series.mask(original_nan_mask, '')
                lowered = series.str.lower()
                if lowered.isin(na_tokens).any():
                    series = series.mask(lowered.isin(na_tokens), 'NA')
                df[col] = series
        # Monetary fields
        money_cols = ['Valor_Inicial', 'Valor_Garantía', 'Valor_Garantia', 'Valor_Ponderado', 'Importe']
        for col in money_cols:
            if col in df.columns:
                raw = df[col].astype(str).str.strip()

                def _normalize_amount(text: str) -> str:
                    """Make a permissive numeric normalization for monetary strings.

                    - Remove currency symbols and any non digit/sep char except sign.
                    - Handle (x) as negative x.
                    - If both '.' and ',' exist: assume '.' are thousands and ',' is decimal.
                    - If only ',' exists: treat as decimal and replace with '.'.
                    - Otherwise, keep '.' as decimal.
                    """
                    import re as _re
                    if text is None:
                        return ''
                    val = str(text).strip()
                    if val == '':
                        return ''
                    neg = False
                    if '(' in val and ')' in val:
                        neg = True
                    # Remove spaces and any non-numeric/sep characters (keep digits, comma, dot, minus)
                    val = val.replace(' ', '')
                    val = _re.sub(r"[^0-9,\.\-]", "", val)
                    # Normalize separators
                    if val.count(',') > 0 and val.count('.') > 0:
                        # assume '.' as thousands sep
                        val = val.replace('.', '').replace(',', '.')
                    elif val.count(',') > 0:
                        val = val.replace(',', '.')
                    # Apply sign if needed
                    if neg and val and not val.startswith('-'):
                        val = '-' + val
                    return val

                normalized = raw.map(_normalize_amount)
                df[col + '__num'] = pd.to_numeric(normalized, errors='coerce')
        return df

    def _drop_blank_records(self, df: pd.DataFrame, subtype: str) -> pd.DataFrame:
        """Remove rows that are entirely blank after trimming string columns."""
        if df is None or df.empty:
            return df

        object_cols = df.select_dtypes(include=['object'])
        if object_cols.empty:
            return df

        stripped = object_cols.fillna('').apply(lambda col: col.astype(str).str.strip())
        empty_mask = stripped.eq('').all(axis=1)
        if empty_mask.any():
            count = int(empty_mask.sum())
            try:
                self.logger.info(f"{subtype}: dropping {count} blank row(s) prior to transformation")
            except Exception:
                pass
            df = df.loc[~empty_mask].copy()
        return df

    def _normalize_tdc_keys(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize Numero_Prestamo and Id_Documento according to rules.

        - Numero_Prestamo: digits only; zfill(10) if < 10; keep if >= 10.
        - Id_Documento: extract digits; if empty → Numero_Prestamo; zfill(10).
        """
        import re as _re
        if df is None or df.empty:
            return df
        df = df.copy()

        def _digits(s: str) -> str:
            return ''.join(ch for ch in s if ch.isdigit())

        # Numero_Prestamo
        if 'Numero_Prestamo' in df.columns:
            np_raw = df['Numero_Prestamo'].astype(str)
            np_digits = np_raw.map(_digits)
            def _norm_np(s: str) -> str:
                if not s:
                    return ''
                return s if len(s) >= 10 else s.zfill(10)
            df['Numero_Prestamo'] = np_digits.map(_norm_np)

        # Id_Documento
        if 'Id_Documento' in df.columns:
            id_raw = df['Id_Documento'].astype(str)
            id_digits = id_raw.map(_digits)
            # fallback to Numero_Prestamo when empty
            if 'Numero_Prestamo' in df.columns:
                fallback = df['Numero_Prestamo'].astype(str)
            else:
                fallback = ''
            def _norm_id(doc: str, fb: str) -> str:
                doc = doc or ''
                if not doc:
                    doc = fb or ''
                return doc if len(doc) >= 10 else doc.zfill(10)
            df['Id_Documento'] = [
                _norm_id(d, f)
                for d, f in zip(id_digits.tolist(), (fallback if isinstance(fallback, pd.Series) else [fallback]*len(df)))
            ]
        return df

    def _normalize_join_key(self, s: pd.Series) -> pd.Series:
        """Normalize keys for joins only (non-destructive): digits-only and strip leading zeros.

        Empty-like values remain missing (`pd.NA`) so they never match auxiliary references by accident.
        """
        try:
            out = s.astype(str).str.replace(r"\D", "", regex=True)
            out = out.str.lstrip('0')
            out = out.where(out != '', pd.NA)
            return out
        except Exception:
            return s.astype(str)

    def _normalize_tipo_garantia_series(self, s: pd.Series) -> pd.Series:
        """Normalize Tipo_Garantia codes to 4-digit strings (e.g., '207' -> '0207')."""
        try:
            out = s.astype(str).str.replace(r"\D", "", regex=True)
            # Take last 4 digits if longer, pad to 4 if 3
            out = out.map(lambda x: (x[-4:] if len(x) >= 4 else (x.zfill(4) if len(x) == 3 else x)))
            return out
        except Exception:
            return s.astype(str)

    def _is_empty_like(self, s: pd.Series) -> pd.Series:
        """Return a boolean mask for empty-like strings: '', NA, 'NA', 'N/A', 'NULL', 'NONE', '-'."""
        try:
            up = s.astype(str).str.strip()
            tokens = {"", "NA", "N/A", "NULL", "NONE", "N.D", "N/D", "-"}
            return s.isna() | up.eq("") | up.str.upper().isin(tokens)
        except Exception:
            return s.isna()

    def _enrich_tdc_0507(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply constants and derived fields for Tipo_Garantia = '0507'."""
        if df is None or df.empty:
            return df
        df = df.copy()
        mask = df.get('Tipo_Garantia').astype(str) == '0507'
        if not mask.any():
            return df
        # Constants
        df.loc[mask, 'Clave_Pais'] = '24'
        df.loc[mask, 'Clave_Empresa'] = '24'
        df.loc[mask, 'Clave_Tipo_Garantia'] = '3'
        df.loc[mask, 'Clave_Subtipo_Garantia'] = '61'
        df.loc[mask, 'Clave_Tipo_Pren_Hipo'] = 'NA'
        df.loc[mask, 'Tipo_Instrumento'] = 'NA'
        df.loc[mask, 'Tipo_Poliza'] = 'NA'
        df.loc[mask, 'Status_Garantia'] = '0'
        df.loc[mask, 'Status_Prestamo'] = '-1'
        df.loc[mask, 'Calificacion_Emisor'] = 'NA'
        df.loc[mask, 'Calificacion_Emisision'] = 'NA'
        # Derived
        if 'Numero_Cis_Garantia' in df.columns:
            df.loc[mask, 'Numero_Cis_Prestamo'] = df.loc[mask, 'Numero_Cis_Garantia']
        if 'Numero_Ruc_Garantia' in df.columns:
            df.loc[mask, 'Numero_Ruc_Prestamo'] = df.loc[mask, 'Numero_Ruc_Garantia']
        # Segmento rule: 02 -> PREMIRA, else PRE
        df.loc[mask, 'Segmento'] = 'PRE'
        # Importe = Valor_Garantia (use numeric internal if present)
        if 'Valor_Garantia__num' in df.columns:
            df.loc[mask, 'Importe__num'] = df.loc[mask, 'Valor_Garantia__num']
        elif 'Valor_Garantía__num' in df.columns:
            df.loc[mask, 'Importe__num'] = df.loc[mask, 'Valor_Garantía__num']
        return df

    def _format_money_comma(self, s: pd.Series) -> pd.Series:
        """Format numeric series with comma decimal as string (no thousand sep)."""
        return s.map(lambda x: ('' if pd.isna(x) else f"{float(x):.2f}".replace('.', ',')))

    def _format_money_dot(self, s: pd.Series) -> pd.Series:
        """Format numeric series with dot decimal separator, two decimals, no thousand separators."""
        from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

        def _fmt(value: Any) -> str:
            if pd.isna(value) or value == '':
                return ''
            try:
                dec = Decimal(str(value))
                quantized = dec.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                return format(quantized, '.2f')
            except (InvalidOperation, ValueError):
                return str(value)

        return s.map(_fmt)

    def _enforce_dot_decimal_strings(self, df: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
        """Ensure specified monetary columns use dot decimal format even if stored as strings."""
        if df is None or df.empty:
            return df

        import re as _re
        from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

        df = df.copy()
        for col in columns:
            if col not in df.columns:
                continue

            cleaned_values: List[str] = []
            series = df[col]

            for raw_val in series:
                if pd.isna(raw_val):
                    cleaned_values.append('')
                    continue

                text = str(raw_val).strip()
                if text == '' or _re.search(r'[A-Za-z]', text):
                    cleaned_values.append(text)
                    continue

                # Normalize separators and strip unexpected characters
                sign = ''
                if text.startswith('-'):
                    sign = '-'
                    text = text[1:]

                text = text.replace(',', '.')
                text = _re.sub(r'[^0-9\.]+', '', text)

                if text.count('.') > 1:
                    parts = text.split('.')
                    text = ''.join(parts[:-1]) + '.' + parts[-1]

                if text == '' or text == '.':
                    cleaned_values.append('')
                    continue

                try:
                    candidate = sign + text if sign else text
                    dec = Decimal(candidate)
                    normalized = dec.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                    cleaned_values.append(format(normalized, '.2f'))
                    continue
                except InvalidOperation:
                    pass

                if '.' in text:
                    whole, frac = text.split('.', 1)
                else:
                    whole, frac = text, ''

                whole = whole.lstrip('0')
                if whole == '':
                    whole = '0'

                frac = (frac + '00')[:2]
                cleaned_values.append(f"{sign}{whole}.{frac}")

            df[col] = cleaned_values

        return df

    def _get_expected_headers(self, context: TransformationContext, subtype: str) -> list:
        """Load expected headers for a subtype from schema_headers.json.

        Returns an empty list if schema is unavailable.
        """
        try:
            import json, os
            from pathlib import Path as _Path
            schemas_dir = getattr(context.config, 'schemas_dir', None)
            base_dir = getattr(context.config, 'base_dir', os.getcwd())
            root = _Path(schemas_dir) if schemas_dir else _Path(base_dir) / 'schemas'
            schema_file = root / 'AT12' / 'schema_headers.json'
            if schema_file.exists():
                data = json.loads(schema_file.read_text(encoding='utf-8'))
                if isinstance(data, dict) and subtype in data:
                    return list(data[subtype].keys())
        except Exception:
            pass
        return []

    def _enforce_dot_decimal(self, df: pd.DataFrame) -> pd.DataFrame:
        """Force dot decimals in common monetary columns across subtypes for output files.

        Replaces comma "," with dot "." in string representations for known amount columns.
        """
        if df is None or df.empty:
            return df
        df = df.copy()
        # Static candidates by common naming
        static_candidates = set([
            'Valor_Inicial', 'Valor_Garantia', 'Valor_Garantía', 'Valor_Ponderado', 'valor_ponderado', 'Importe',
            'Monto', 'Monto_Pignorado', 'Intereses_por_Pagar', 'Importe_por_pagar',
            'valor_inicial', 'intereses_x_cobrar', 'saldo', 'provision', 'provison_NIIF', 'provision_no_NIIF',
            'mto_garantia_1', 'mto_garantia_2', 'mto_garantia_3', 'mto_garantia_4', 'mto_garantia_5',
            'mto_xv30d', 'mto_xv60d', 'mto_xv90d', 'mto_xv120d', 'mto_xv180d', 'mto_xv1a',
            'Mto_xV1a5a', 'Mto_xV5a10a', 'Mto_xVm10a',
            'mto_v30d', 'mto_v60d', 'mto_v90d', 'mto_v120d', 'mto_v180d', 'mto_v1a', 'mto_vm1a',
            'mto_a_pagar', 'saldo_original', 'saldo_original_2', 'saldocapital', 'monto_asegurado',
            'LIMITE', 'SALDO', 'interes_diferido', 'interes_dif', 'tasa_interes', 'Tasa'
        ])

        # Regex-based candidates by pattern (case-insensitive)
        import re as _re
        patterns = [
            r'^(?i)mto_.*',
            r'^(?i)monto_.*',
            r'(?i)valor',
            r'(?i)importe',
            r'(?i)saldo',
            r'(?i)provis',
            r'(?i)interes',
            r'(?i)^tasa(_|$)'
        ]
        # Build full candidate set
        cols = list(df.columns)
        dynamic = set()
        for c in cols:
            cs = str(c)
            for pat in patterns:
                try:
                    if _re.search(pat, cs):
                        dynamic.add(c)
                        break
                except Exception:
                    continue
        candidates = static_candidates.union(dynamic)

        for col in candidates:
            if col in df.columns:
                try:
                    df[col] = df[col].astype(str).str.replace(',', '.', regex=False)
                except Exception:
                    pass
        return df

    def _sanitize_output_whitespace(self, df: pd.DataFrame, subtype: str = "") -> pd.DataFrame:
        """Strip leading/trailing whitespace and remove hidden space-like symbols before export."""
        if df is None or df.empty:
            return df

        import re as _re
        df = df.copy()

        replacements = {
            '\u00a0': ' ',  # non-breaking space
            '\u1680': ' ',
            '\u180e': ' ',
            '\u2000': ' ',
            '\u2001': ' ',
            '\u2002': ' ',
            '\u2003': ' ',
            '\u2004': ' ',
            '\u2005': ' ',
            '\u2006': ' ',
            '\u2007': ' ',
            '\u2008': ' ',
            '\u2009': ' ',
            '\u200a': ' ',
            '\u202f': ' ',
            '\u205f': ' ',
            '\u3000': ' ',
            '\u200b': '',  # zero width space
            '\u200c': '',
            '\u200d': '',
            '\ufeff': '',
            '\u00ad': '',  # soft hyphen
            '\u00ff': '',  # y with diaeresis
            '\u0178': ''   # Y with diaeresis
        }

        disallowed_pattern = _re.compile(r"[\u00a0\u1680\u180e\u2000-\u200f\u2028\u2029\u202f\u205f\u2060\u3000\ufeff\u00ad\u00ff\u0178]")

        text_columns = [
            col for col in df.columns
            if pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col])
        ]

        total_modifications = 0
        for col in text_columns:
            series = df[col]
            try:
                str_series = series.astype('string')
            except Exception:
                str_series = series.astype(str)
            str_series = str_series.fillna('')
            original = str_series.copy()
            for target, replacement in replacements.items():
                str_series = str_series.str.replace(target, replacement, regex=False)
            str_series = str_series.str.strip()

            diff_mask = original != str_series
            total_modifications += int(diff_mask.sum())

            df[col] = str_series.astype(object)

            leftover_mask = str_series.str.contains(disallowed_pattern, na=False)
            if leftover_mask.any():
                sample = str_series[leftover_mask].iloc[0]
                msg = (
                    f"Output sanitization failed for {subtype or 'UNKNOWN'} column {col}: "
                    f"disallowed whitespace character detected (value={repr(sample)})"
                )
                self.logger.error(msg)
                raise RuntimeError(msg)

        if total_modifications > 0:
            try:
                self.logger.info(
                    f"Sanitized {total_modifications} value(s) with stray whitespace for {subtype or 'output'}"
                )
            except Exception:
                pass

        return df

    def _zero_out_valor_ponderado(self, df: pd.DataFrame) -> pd.DataFrame:
        """Set any Valor_Ponderado/valor_ponderado column to literal '0' (string).

        Applies case-insensitively, and does not create columns if absent.
        """
        if df is None or df.empty:
            return df
        df = df.copy()
        import re as _re
        for col in list(df.columns):
            try:
                if _re.fullmatch(r"(?i)valor_ponderado", str(col).strip(), flags=_re.IGNORECASE):
                    df[col] = '0'
            except Exception:
                continue
        return df

    def _export_error_subset(self, df: pd.DataFrame, mask: pd.Series, subtype: str, rule_name: str,
                              context: TransformationContext, result: Optional[TransformationResult],
                              original_columns: Optional[Dict[str, pd.Series]] = None) -> None:
        """Export a CSV containing only rows that match the error mask, preserving all columns.

        If `original_columns` is provided (mapping: column_name -> original_series aligned to df index),
        insert side-by-side `<column_name>_ORIGINAL` columns with the pre-correction values.
        """
        try:
            out_df = df.loc[mask].copy()
            if out_df.empty:
                return
            # Include subtype to avoid overwriting when multiple subtypes export the same rule
            # Pattern: [RULE]_[SUBTYPE]_[YYYYMMDD].csv
            safe_subtype = subtype or 'BASE_AT12'
            filename = f"{rule_name}_{safe_subtype}_{context.period}.csv"
            out_path = context.paths.incidencias_dir / filename
            out_path.parent.mkdir(parents=True, exist_ok=True)

            # Insert original columns next to corrected ones if provided
            if original_columns:
                cols = list(out_df.columns)
                new_order = []
                for col in cols:
                    new_order.append(col)
                    if col in original_columns:
                        orig_col = f"{col}_ORIGINAL"
                        try:
                            out_df[orig_col] = original_columns[col].reindex(out_df.index)
                        except Exception:
                            out_df[orig_col] = original_columns[col]
                        new_order.append(orig_col)
                try:
                    out_df = out_df[new_order]
                except Exception:
                    pass

            out_df.to_csv(
                out_path,
                index=False,
                encoding='utf-8',
                sep=getattr(context.config, 'output_delimiter', '|'),
                quoting=1,
                date_format='%Y%m%d'
            )
            if result is not None and hasattr(result, 'incidence_files'):
                result.incidence_files.append(out_path)
            # Log concise: RULE -> filename
            try:
                self.logger.info(f"{rule_name} -> {out_path.name} ({len(out_df)} records)")
            except Exception:
                self.logger.info(f"{rule_name} -> {out_path} ({len(out_df)} records)")
        except Exception as e:
            self.logger.warning(f"Failed to export error subset for {rule_name}: {e}")

    def _add_incidence(self, incidence_type: IncidenceType, severity: IncidenceSeverity, rule_id: str, description: str, data: Dict[str, Any]):
        """Helper to add a standardized incidence."""
        if not self.incidence_reporter:
            self.logger.warning("Incidence reporter not initialized. Skipping incidence.")
            return

        # Use the standard add_incidence method without problematic kwargs
        incidence_id = self.incidence_reporter.add_incidence(
            subtype="BASE",
            incidence_type=incidence_type,
            description=description,
            severity=severity
        )
        
        # Manually set the additional fields on the created incidence
        if "BASE" in self.incidence_reporter.incidences and self.incidence_reporter.incidences["BASE"]:
            last_incidence = self.incidence_reporter.incidences["BASE"][-1]
            last_incidence.rule_name = rule_id
            last_incidence.metadata = data

    def _apply_transformations(self, df: pd.DataFrame, context: TransformationContext, 
                             result: TransformationResult, source_data: Dict[str, pd.DataFrame],
                             subtype: str = "") -> pd.DataFrame:
        """Apply AT12-specific transformations using a two-phase approach.
        
        Phase 1a: Operations that don't require AT02/AT03 dependencies
        Phase 1b: Operations that require AT02/AT03 dependencies (gated)
        
        Args:
            df: Input DataFrame
            context: Transformation context
            result: Result object to update
            source_data: Dictionary of source DataFrames
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting AT12 two-phase transformation pipeline")
        
        # Initialize IncidenceReporter
        self.incidence_reporter = IncidenceReporter(
            config=self.config,
            run_id=context.run_id,
            period=context.period
        )
        
        # Check availability of dependency files
        has_at02 = 'AT02_CUENTAS' in source_data and not source_data['AT02_CUENTAS'].empty
        has_at03 = 'AT03_CREDITOS' in source_data and not source_data['AT03_CREDITOS'].empty
        
        self.logger.info(f"Dependency availability - AT02: {has_at02}, AT03: {has_at03}")
        
        # Phase 1a: Independent operations (no AT02/AT03 dependencies)
        df = self._phase1a_independent_operations(df, context, result, subtype)
        
        # Phase 1b: Dependent operations (operate opportunistically by dependency)
        # - Fecha Avaluo correction runs only if AT03 is available (gated inside function)
        # - Poliza-related corrections check for their own inputs and skip if absent
        df = self._phase1b_dependent_operations(df, context, result, source_data, subtype, has_at02, has_at03)
        
        # Stage 2: Data Enrichment and Generation from Auxiliary Sources
        df = self._stage2_enrichment(df, context, result, source_data, subtype)
        
        # Stage 3: Business Logic Application and Reporting
        df = self._stage3_business_logic(df, context, result, source_data)
        
        # Stage 4: Data Validation and Quality Assurance (only when applicable and dependencies available)
        try:
            if ('Numero_Prestamo' in df.columns or 'at_num_de_prestamos' in df.columns) and has_at03:
                df = self._stage4_validation(df, context, result, source_data)
            else:
                reason = "required identifier column not present" if not ('Numero_Prestamo' in df.columns or 'at_num_de_prestamos' in df.columns) else "AT03_CREDITOS dependency not available"
                self.logger.info(f"Skipping Stage 4: {reason}")
        except Exception as e:
            self.logger.warning(f"Stage 4 skipped due to error: {e}")
        
        # QA: Verify Tipo_Garantia formatting did not lose leading zeros (log-only)
        try:
            self._qa_verify_tipo_garantia_format(df, context, subtype)
        except Exception:
            pass

        self.logger.info("AT12 two-phase transformation pipeline completed")
        return df

    def _qa_verify_tipo_garantia_format(self, df: pd.DataFrame, context: TransformationContext, subtype: str = "") -> None:
        """QA check: ensure Tipo_Garantia keeps 4-digit numeric codes; log anomalies only.

        - bad_len: numeric values with length != 4 (potential lost leading zero like '207').
        - non_digit: non-numeric codes (excluding empty).
        """
        if df is None or df.empty or 'Tipo_Garantia' not in df.columns:
            return
        s = df['Tipo_Garantia'].astype(str).str.strip()
        # Identify empties (do not count as non_digit)
        empties = s.eq("")
        digit_mask = s.str.fullmatch(r"\d+")
        bad_len_mask = digit_mask & (s.str.len() != 4)
        non_digit_mask = (~digit_mask) & (~empties)
        bad_len = int(bad_len_mask.sum())
        non_digit = int(non_digit_mask.sum())
        if bad_len or non_digit:
            # Sample a few offending values for quick diagnostics
            try:
                sample_bad = s[bad_len_mask].unique().tolist()[:5]
                sample_nd = s[non_digit_mask].unique().tolist()[:5]
            except Exception:
                sample_bad, sample_nd = [], []
            self.logger.warning(
                f"QA Tipo_Garantia anomalies in {subtype or 'BASE_AT12'}: bad_len={bad_len}, non_digit={non_digit}, "
                f"samples_bad={sample_bad}, samples_non_digit={sample_nd}"
            )
        else:
            self.logger.info(f"QA Tipo_Garantia OK in {subtype or 'BASE_AT12'}: all numeric 4-digit codes or empty")
    
    def _phase1a_independent_operations(self, df: pd.DataFrame, context: TransformationContext, 
                                      result: TransformationResult, 
                                      subtype: str = "") -> pd.DataFrame:
        """Phase 1a: Operations that don't require AT02/AT03 dependencies.

        Note: This phase intentionally does not accept or use source_data to
        ensure it can run when AT02/AT03 are unavailable.
        """
        self.logger.info("Executing Phase 1a: Independent operations")
        
        # Harmonize common accented/non-accented headers for BASE_AT12 to maximize rule coverage
        try:
            df = self._harmonize_base_headers(df, subtype)
        except Exception as e:
            self.logger.debug(f"Header harmonization skipped due to error: {e}")
        
        # Standardize base Fecha column to month-end (context driven)
        df = self._apply_base_fecha_last_day(df, context, subtype=subtype)

        # Replace legacy 'Y' separators in Id_Documento with '/'
        df = self._apply_id_documento_y_to_slash(df, context, subtype=subtype)

        # Apply error correction rules that don't require AT02/AT03
        df = self._apply_eeor_tabular_cleaning(df, context, subtype=subtype)
        df = self._apply_error_0301_correction(df, context, subtype=subtype, result=result)
        df = self._apply_coma_finca_empresa_correction(df, context)
        df = self._apply_fecha_cancelacion_correction(df, context)
        df = self._apply_inmuebles_sin_finca_correction(df, context)
        df = self._apply_poliza_auto_comercial_correction(df, context)
        df = self._apply_fiduciaria_extranjera_standardization(df, context, subtype=subtype)
        df = self._apply_codigo_fiduciaria_update(df, context, subtype=subtype)
        df = self._apply_contrato_privado_na(df, context, subtype=subtype)
        df = self._apply_inmueble_sin_avaluadora_correction(df, context)
        # Final rule in cascade for Stage 1a: pad Id_Documento to 10 when purely numeric and short
        df = self._apply_id_documento_padding(df, context, subtype=subtype)
        
        self.logger.info("Completed Phase 1a: Independent operations")
        return df

    def _harmonize_base_headers(self, df: pd.DataFrame, subtype: str = "") -> pd.DataFrame:
        """Create alias columns to handle accented/non-accented variants commonly seen in BASE_AT12.

        This is non-destructive: when only one side exists, create the counterpart so rules can operate
        regardless of input header accents.
        """
        if df is None or df.empty:
            return df
        # Only for BASE-like inputs (defensive; harmless otherwise)
        if subtype and str(subtype).upper() not in {"BASE_AT12", "BASE", "AT12_BASE"}:
            return df

        df = df.copy()
        pairs = [
            ("Numero_Prestamo", "Número_Préstamo"),
            ("Tipo_Garantia", "Tipo_Garantía"),
            ("Valor_Garantia", "Valor_Garantía"),
            ("Fecha_Ultima_Actualizacion", "Fecha_Última_Actualización"),
            ("Codigo_Banco", "Código_Banco"),
            ("Codigo_Region", "Código_Región"),
            ("Numero_Garantia", "Número_Garantía"),
            ("Numero_Cis_Garantia", "Número_Cis_Garantía"),
            ("Numero_Ruc_Garantia", "Número_Ruc_Garantía"),
        ]
        created = []
        for a, b in pairs:
            a_in = a in df.columns
            b_in = b in df.columns
            if a_in and not b_in:
                try:
                    df[b] = df[a]
                    created.append(b)
                except Exception:
                    pass
            elif b_in and not a_in:
                try:
                    df[a] = df[b]
                    created.append(a)
                except Exception:
                    pass
        if created:
            try:
                self.logger.debug(f"Header harmonization created aliases: {created}")
            except Exception:
                pass
        return df

    def _apply_base_fecha_last_day(self, df: pd.DataFrame, context: TransformationContext, subtype: str = "") -> pd.DataFrame:
        """Set BASE_AT12 `Fecha` column to the last day of the processing month (YYYYMMDD)."""
        if df is None or df.empty:
            return df
        if subtype and str(subtype).upper() not in {"BASE_AT12", "BASE", "AT12_BASE"}:
            return df
        if 'Fecha' not in df.columns:
            return df

        target_date: Optional[datetime] = None
        period = getattr(context, 'period', None)
        if isinstance(period, str):
            candidate = period.strip()
            if len(candidate) == 8 and candidate.isdigit():
                try:
                    target_date = datetime.strptime(candidate, "%Y%m%d")
                except ValueError:
                    target_date = None

        if target_date is None:
            year = getattr(context, 'year', None)
            month = getattr(context, 'month', None)
            try:
                if year is not None and month is not None:
                    target_date = datetime(int(year), int(month), 1)
            except Exception:
                target_date = None

        if target_date is None:
            return df

        try:
            last_day = calendar.monthrange(target_date.year, target_date.month)[1]
            fecha_value = f"{target_date.year}{target_date.month:02d}{last_day:02d}"
        except Exception:
            return df

        # Avoid unnecessary copy if already correct
        if df['Fecha'].astype(str).eq(fecha_value).all():
            return df

        df = df.copy()
        df['Fecha'] = fecha_value
        return df

    def _apply_id_documento_y_to_slash(self, df: pd.DataFrame, context: TransformationContext, subtype: str = "") -> pd.DataFrame:
        """Replace literal 'Y' with '/' in Id_Documento for BASE_AT12 to fix malformed separators."""
        if df is None or df.empty:
            return df
        if subtype and str(subtype).upper() not in {"BASE_AT12", "BASE", "AT12_BASE"}:
            return df
        if 'Id_Documento' not in df.columns:
            return df

        series = df['Id_Documento'].astype(str)
        mask = series.str.contains('Y', na=False)
        if not mask.any():
            return df

        df = df.copy()
        try:
            original_series = pd.Series(index=df.index, dtype=object)
        except Exception:
            original_series = None

        replaced = series.str.replace('Y', '/', regex=False)
        df.loc[mask, 'Id_Documento'] = replaced.loc[mask]
        if original_series is not None:
            original_series.loc[mask] = series.loc[mask]

        try:
            original_columns = {'Id_Documento': original_series} if original_series is not None else None
            self._export_error_subset(df, mask, 'BASE_AT12', 'ID_DOCUMENTO_Y_TO_SLASH', context, None, original_columns=original_columns)
        except Exception:
            pass

        return df

    def _phase1b_dependent_operations(self, df: pd.DataFrame, context: TransformationContext, 
                                    result: TransformationResult, source_data: Dict[str, pd.DataFrame], 
                                    subtype: str = "", has_at02: bool = False, has_at03: bool = False) -> pd.DataFrame:
        """Phase 1b: Operations that require AT02/AT03 dependencies."""
        self.logger.info("Executing Phase 1b: Dependent operations")
        
        # Apply corrections that require AT03_CREDITOS
        if has_at03 and (subtype == 'BASE_AT12' or subtype == '' or subtype is None):
            df = self._apply_fecha_avaluo_correction(df, context, source_data, subtype=subtype or 'BASE_AT12')
        elif (subtype == 'BASE_AT12' or subtype == '' or subtype is None):
            try:
                self.logger.info("Skipping FECHA_AVALUO_ERRADA: AT03_CREDITOS not available")
            except Exception:
                pass
        
        # Apply corrections that require POLIZA_HIPOTECAS_AT12 (from source_data)
        df = self._apply_inmuebles_sin_poliza_correction(df, context, source_data)
        df = self._apply_error_poliza_auto_correction(df, context, source_data)
        
        self.logger.info("Completed Phase 1b: Dependent operations")
        return df
    
    def _stage1_initial_cleansing(self, df: pd.DataFrame, context: TransformationContext, result: TransformationResult, source_data: Dict[str, pd.DataFrame], subtype: str = "") -> pd.DataFrame:
        """Stage 1: Initial Data Cleansing and Formatting (legacy method for compatibility)"""
        return self._phase1_error_correction(df, context, result, source_data, subtype=subtype)

    def _stage2_enrichment(self, df: pd.DataFrame, context: TransformationContext, result: TransformationResult, source_data: Dict[str, pd.DataFrame], subtype: str = "") -> pd.DataFrame:
        """Stage 2: Data Enrichment and Generation from Auxiliary Sources"""
        return self._phase2_input_processing(df, context, result, source_data, subtype_hint=subtype)

    def _stage3_business_logic(self, df: pd.DataFrame, context: TransformationContext, result: TransformationResult, source_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Stage 3: Business Logic Application and Reporting"""
        return self._phase3_filter_fuera_cierre(df, context, result, source_data)

    def _stage4_validation(self, df: pd.DataFrame, context: TransformationContext, result: TransformationResult, source_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Stage 4: Data Validation and Quality Assurance"""
        return self._phase4_valor_minimo_avaluo(df, context, result, source_data)
    
    def _stage5_output_generation(self, context: TransformationContext, transformed_data: Dict[str, pd.DataFrame], result: TransformationResult) -> None:
        """Stage 5: Output Generation and File Creation"""
        self._generate_outputs(context, transformed_data, result)
    
    def transform(self, context: TransformationContext, source_data: Dict[str, pd.DataFrame]) -> TransformationResult:
        """Transform AT12 data with five-stage pipeline.
        
        Args:
            context: Transformation context
            source_data: Dictionary of source DataFrames
            
        Returns:
            TransformationResult with processed data and metadata
        """
        # Initialize result with required fields from core dataclass
        result = TransformationResult(
            success=False,
            processed_files=[],
            incidence_files=[],
            consolidated_file=None,
            metrics={},
            errors=[],
            warnings=[]
        )
        transformed_data = {}
        
        try:
            # Process each data subtype
            for subtype, df in source_data.items():
                if df is None or df.empty:
                    continue
                # Process only relevant AT12 subtypes
                if subtype not in ('BASE_AT12', 'TDC_AT12', 'SOBREGIRO_AT12', 'VALORES_AT12'):
                    continue
                self.logger.info(f"Processing {subtype} with {len(df)} records")
                
                # Apply transformation pipeline
                transformed_df = self._apply_transformations(df, context, result, source_data, subtype)
                transformed_data[subtype] = transformed_df
                
                self.logger.info(f"Completed processing {subtype}: {len(transformed_df)} records")
            
            # Stage 5: Generate outputs
            self._stage5_output_generation(context, transformed_data, result)
            
            result.success = True
            # Store summary for reporting
            result.metrics.update({'subtypes_processed': list(transformed_data.keys())})
            # Ensure message attribute exists for downstream consumers/tests
            result.message = f"AT12 transformation completed successfully. Processed {len(transformed_data)} subtypes."
            
        except Exception as e:
            error_msg = f"AT12 transformation failed: {str(e)}"
            result.errors.append(error_msg)
            result.success = False
            result.message = error_msg
            self.logger.error(error_msg, exc_info=True)
        
        return result
    

        
        self.logger.info("Completed Phase 1b: Dependent error corrections")
        return df
    
    def _phase1_error_correction(self, df: pd.DataFrame, context: TransformationContext, 
                               result: TransformationResult, source_data: Dict[str, pd.DataFrame], subtype: str = "") -> pd.DataFrame:
        """Phase 1: Apply error correction rules to the data (legacy method for compatibility)."""
        self.logger.info("Executing Phase 1: Initial Data Cleansing and Formatting")
        
        # Apply all error correction rules in sequence
        df = self._apply_eeor_tabular_cleaning(df, context, subtype=subtype)
        df = self._apply_error_0301_correction(df, context, subtype=subtype, result=result)
        df = self._apply_coma_finca_empresa_correction(df, context)
        df = self._apply_fecha_cancelacion_correction(df, context)
        
        # This method requires source_data for AT03_CREDITOS lookup, and applies only for BASE_AT12
        if source_data and (subtype == 'BASE_AT12' or subtype == '' or subtype is None):
            df = self._apply_fecha_avaluo_correction(df, context, source_data, subtype=subtype or 'BASE_AT12')
        
        df = self._apply_inmuebles_sin_poliza_correction(df, context, source_data)
        df = self._apply_inmuebles_sin_finca_correction(df, context)
        df = self._apply_poliza_auto_comercial_correction(df, context)
        df = self._apply_error_poliza_auto_correction(df, context, source_data)
        df = self._apply_codigo_fiduciaria_update(df, context, subtype=subtype)
        df = self._apply_contrato_privado_na(df, context, subtype=subtype)
        df = self._apply_inmueble_sin_avaluadora_correction(df, context)
        # Final rule in cascade for legacy Phase 1: pad Id_Documento to 10 when purely numeric and short
        df = self._apply_id_documento_padding(df, context, subtype=subtype)
        
        self.logger.info("Completed Phase 1: Error correction")
        return df
    
    def _phase2_input_processing(self, df: pd.DataFrame, context: TransformationContext, 
                               result: TransformationResult, source_data: Dict[str, pd.DataFrame], subtype_hint: str = "") -> pd.DataFrame:
        """Phase 2: Process input data based on subtype with dependency checking."""
        self.logger.info("Executing Phase 2: Input Processing")
        
        # Check dependency availability
        has_at02 = 'AT02_CUENTAS' in source_data and not source_data['AT02_CUENTAS'].empty
        has_at03 = 'AT03_CREDITOS' in source_data and not source_data['AT03_CREDITOS'].empty
        
        # Determine subtype from hint or DataFrame characteristics
        subtype = subtype_hint or self._determine_subtype(df, context)
        
        if subtype == 'TDC_AT12':
            return self._process_tdc_data_gated(df, context, result, source_data, has_at02, has_at03)
        elif subtype == 'SOBREGIRO_AT12':
            return self._process_sobregiro_data_gated(df, context, result, source_data, has_at02, has_at03)
        elif subtype == 'VALORES_AT12':
            return self._process_valores_data(df, context, result, source_data)
        else:
            self.logger.warning(f"Unknown subtype: {subtype}. Applying generic processing.")
            return df
    
    def _determine_subtype(self, df: pd.DataFrame, context: TransformationContext) -> str:
        """Determine the subtype of the data based on columns or context."""
        # This is a placeholder implementation
        # In practice, you would determine subtype based on column names, context, or other criteria
        if 'TDC' in str(df.columns).upper():
            return 'TDC_AT12'
        elif 'SOBREGIRO' in str(df.columns).upper():
            return 'SOBREGIRO_AT12'
        elif 'VALORES' in str(df.columns).upper():
            return 'VALORES_AT12'
        else:
            return 'UNKNOWN'
    
    def _process_tdc_data_gated(self, df: pd.DataFrame, context: TransformationContext, 
                              result: TransformationResult, source_data: Dict[str, pd.DataFrame],
                              has_at02: bool = False, has_at03: bool = False) -> pd.DataFrame:
        """Process TDC (Tarjeta de Crédito) specific data with dependency gating.

        Note: Tipo_Facilidad pre-processing for TDC uses AT03_TDC and is required.
        """
        self.logger.info("Processing TDC_AT12 data - Stage 2 (gated)")

        # Step 0: Ensure Tipo_Facilidad using AT03_TDC (required for TDC)
        has_at03_tdc = ('AT03_TDC' in source_data) and (not source_data['AT03_TDC'].empty)
        if has_at03_tdc:
            df = self._ensure_tipo_facilidad_from_at03(
                df,
                'TDC_AT12',
                context,
                result,
                source_data,
                at03_key='AT03_TDC',
                require=True
            )
        else:
            self.logger.info("FACILIDAD_FROM_AT03 for TDC skipped: AT03_TDC not available")

        # Step 1: Generate Número_Garantía (in-process, no incidences)
        df = self._generate_numero_garantia_tdc(df, context)

        # Step 2: Date Mapping with AT02_CUENTAS (only if AT02 available)
        if has_at02:
            df = self._apply_date_mapping_tdc(df, context, source_data)
        else:
            self.logger.info("Skipping date mapping for TDC: AT02_CUENTAS not available")

        # Step 3: Business rule - Tarjeta repetida (detect duplicates excluding Numero_Prestamo)
        try:
            df = self._validate_tdc_tarjeta_repetida(df, context)
        except Exception as e:
            self.logger.warning(f"TDC 'Tarjeta_repetida' validation skipped due to error: {e}")

        df = self._enforce_dot_decimal_strings(
            df,
            ('Valor_Inicial', 'Valor_Garantia', 'Valor_Garantía', 'Valor_Ponderado', 'Importe')
        )

        df = self._finalize_tdc_output(df)

        return df
    
    def _process_tdc_data(self, df: pd.DataFrame, context: TransformationContext, 
                        result: TransformationResult, source_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Process TDC (Tarjeta de Crédito) specific data (legacy method for compatibility).

        Note: Tipo_Facilidad pre-processing for TDC uses AT03_TDC and is required.
        """
        self.logger.info("Processing TDC_AT12 data - Stage 2")

        # Step 0: Ensure Tipo_Facilidad from AT03_TDC (required)
        has_at03_tdc = ('AT03_TDC' in source_data) and (not source_data['AT03_TDC'].empty)
        if has_at03_tdc:
            df = self._ensure_tipo_facilidad_from_at03(
                df,
                'TDC_AT12',
                context,
                result,
                source_data,
                at03_key='AT03_TDC',
                require=False
            )
        else:
            # Legacy method: skip if auxiliary source not present (tests may call without aux sources)
            self.logger.info("AT03_TDC not available; skipping FACILIDAD_FROM_AT03 in legacy TDC path")

        # Step 1: Generate Número_Garantía (in-process, no incidences)
        df = self._generate_numero_garantia_tdc(df, context)

        # Step 2: Date Mapping with AT02_CUENTAS
        df = self._apply_date_mapping_tdc(df, context, source_data)

        # Step 3: Business rule - Tarjeta repetida (detect duplicates excluding Numero_Prestamo)
        try:
            df = self._validate_tdc_tarjeta_repetida(df, context)
        except Exception as e:
            self.logger.warning(f"TDC 'Tarjeta_repetida' validation skipped due to error: {e}")
        
        df = self._enforce_dot_decimal_strings(
            df,
            ('Valor_Inicial', 'Valor_Garantia', 'Valor_Garantía', 'Valor_Ponderado', 'Importe')
        )

        df = self._finalize_tdc_output(df)

        return df

    def _col_any(self, df: pd.DataFrame, candidates: list) -> Optional[str]:
        for c in candidates:
            if c in df.columns:
                return c
        return None

    def _validate_tdc_tarjeta_repetida(self, df: pd.DataFrame, context: TransformationContext) -> pd.DataFrame:
        """Detect repeated credit cards excluding Numero_Prestamo and export inconsistencies.

        Duplicate key (priority order based on availability):
          1) ('Identificacion_cliente','Identificacion_Cuenta','Tipo_Facilidad')
          2) ('Id_Documento','Tipo_Facilidad')

        Records sharing the same key are considered 'Tarjeta_repetida'.
        Export full-row subset to new INC_REPEATED_CARD_TDC_AT12_<PERIODO>.csv (legacy TARJETA_REPETIDA removed).
        """
        # Choose key according to available columns
        if all(c in df.columns for c in ['Identificacion_cliente', 'Identificacion_Cuenta', 'Tipo_Facilidad']):
            key_cols = ['Identificacion_cliente', 'Identificacion_Cuenta', 'Tipo_Facilidad']
        elif all(c in df.columns for c in ['Id_Documento', 'Tipo_Facilidad']):
            key_cols = ['Id_Documento', 'Tipo_Facilidad']
        else:
            # No sufficient columns to detect duplicates
            return df

        # Normalize key components to avoid false positives/negatives (strip non-digits/leading zeros)
        norm_parts = []
        for col in key_cols:
            try:
                norm_parts.append(self._normalize_join_key(df[col]))
            except Exception:
                norm_parts.append(df[col].astype(str))
        from pandas import Series as _Series
        try:
            key_series = (_Series(norm_parts[0]).astype(str))
            for part in norm_parts[1:]:
                key_series = key_series.str.cat(_Series(part).astype(str), sep='|')
        except Exception:
            # Fallback to non-normalized join if anything fails
            key_series = df[key_cols].astype(str).agg('|'.join, axis=1)
        dup_mask = key_series.duplicated(keep=False)
        if dup_mask.any():
            try:
                self.logger.info(f"TDC Tarjeta_repetida: detected {int(dup_mask.sum())} duplicate record(s) by key {key_cols}")
            except Exception:
                pass
            try:
                for idx in df[dup_mask].index:
                    self._add_incidence(
                        incidence_type=IncidenceType.BUSINESS_RULE_VIOLATION,
                        severity=IncidenceSeverity.MEDIUM,
                        rule_id='TARJETA_REPETIDA',
                        description='Duplicate credit card detected (excluding Numero_Prestamo)',
                        data={'record_index': int(idx), 'key_columns': ','.join(key_cols)}
                    )
            except Exception:
                pass

            try:
                payload = [{
                    'key': key_series.loc[i]
                } for i in df[dup_mask].index]
                # Store only new rule
                self._store_incidences('INC_REPEATED_CARD', payload, context)
            except Exception:
                pass

            try:
                self._export_error_subset(df, dup_mask, 'TDC_AT12', 'INC_REPEATED_CARD', context, None)
            except Exception as e:
                self.logger.warning(f"Failed to export INC_REPEATED_CARD subset: {e}")

        return df
    
    def _process_sobregiro_data_gated(self, df: pd.DataFrame, context: TransformationContext, 
                                    result: TransformationResult, source_data: Dict[str, pd.DataFrame],
                                    has_at02: bool = False, has_at03: bool = False) -> pd.DataFrame:
        """Process Sobregiro specific data with dependency gating."""
        self.logger.info("Processing SOBREGIRO_AT12 data - Stage 2 (gated)")
        # Ensure whitespace cleaning runs first for SOBREGIRO
        try:
            df = self._apply_eeor_tabular_cleaning(df, context, subtype='SOBREGIRO_AT12')
        except Exception as e:
            self.logger.warning(f"EEOR_TABULAR cleaning skipped for SOBREGIRO due to error: {e}")
        
        # Step 0: Ensure Tipo_Facilidad from AT03 (only if AT03 available)
        has_at03_creditos = ('AT03_CREDITOS' in source_data) and (not source_data['AT03_CREDITOS'].empty)
        if has_at03_creditos:
            try:
                df = self._ensure_tipo_facilidad_from_at03(
                    df,
                    'SOBREGIRO_AT12',
                    context,
                    result,
                    source_data,
                    at03_key='AT03_CREDITOS',
                    require=False
                )
            except Exception as e:
                self.logger.warning(f"Skipping FACILIDAD_FROM_AT03 for SOBREGIRO due to error: {e}")
        else:
            self.logger.info("Skipping FACILIDAD_FROM_AT03 for SOBREGIRO: AT03_CREDITOS not available")

        # Light normalization (trim + monetarias)
        df = self._normalize_tdc_basic(df)

        # 2.2. SOBREGIRO_AT12 Processing
        # Apply the same JOIN and date mapping logic as TDC (only if AT02 available)
        # The Numero_Garantia field is not modified for SOBREGIRO
        if has_at02:
            df = self._apply_date_mapping_sobregiro(df, context, source_data)
        else:
            self.logger.info("Skipping date mapping for SOBREGIRO: AT02_CUENTAS not available")
        
        # Format money columns with dot decimal if normalized
        for col in ('Valor_Inicial', 'Valor_Garantia', 'Valor_Garantía', 'Valor_Ponderado', 'valor_ponderado', 'Importe'):
            num_col = col + '__num'
            if num_col in df.columns:
                # Keep SOBREGIRO schema exact: prefer 'valor_ponderado' (lowercase) over 'Valor_Ponderado'
                if col == 'valor_ponderado':
                    target = 'valor_ponderado'
                elif col in ('Valor_Garantia', 'Valor_Garantía'):
                    target = 'Valor_Garantia'
                else:
                    target = col
                df[target] = self._format_money_dot(df[num_col])
        if 'Importe__num' in df.columns:
            df['Importe'] = self._format_money_dot(df['Importe__num'])

        df = self._enforce_dot_decimal_strings(
            df,
            ('Valor_Inicial', 'Valor_Garantia', 'Valor_Garantía', 'Valor_Ponderado', 'valor_ponderado', 'Importe')
        )

        for col in ('Numero_Garantia', 'Numero_Cis_Garantia'):
            if col in df.columns:
                df[col] = df[col].map(lambda v: '' if pd.isna(v) else str(v).strip())
        if 'Pais_Emision' in df.columns:
            df['Pais_Emision'] = '591'

        return df
    
    def _process_sobregiro_data(self, df: pd.DataFrame, context: TransformationContext, 
                              result: TransformationResult, source_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Process Sobregiro specific data according to Stage 2 specifications (legacy method for compatibility)."""
        self.logger.info("Processing SOBREGIRO_AT12 data - Stage 2")
        # Ensure whitespace cleaning runs first for SOBREGIRO
        try:
            df = self._apply_eeor_tabular_cleaning(df, context, subtype='SOBREGIRO_AT12')
        except Exception as e:
            self.logger.warning(f"EEOR_TABULAR cleaning skipped for SOBREGIRO due to error: {e}")
        
        # Step 0: Ensure Tipo_Facilidad from AT03 (must run before any other step)
        try:
            df = self._ensure_tipo_facilidad_from_at03(
                df,
                'SOBREGIRO_AT12',
                context,
                result,
                source_data,
                at03_key='AT03_CREDITOS',
                require=False
            )
        except Exception as e:
            self.logger.warning(f"Skipping FACILIDAD_FROM_AT03 for SOBREGIRO due to error: {e}")

        # Light normalization (trim + monetarias)
        df = self._normalize_tdc_basic(df)

        # 2.2. SOBREGIRO_AT12 Processing
        # Apply the same JOIN and date mapping logic as TDC
        # The Numero_Garantia field is not modified for SOBREGIRO
        df = self._apply_date_mapping_sobregiro(df, context, source_data)
        
        # Format money columns with dot decimal if normalized
        for col in ('Valor_Inicial', 'Valor_Garantia', 'Valor_Garantía', 'Valor_Ponderado', 'valor_ponderado', 'Importe'):
            num_col = col + '__num'
            if num_col in df.columns:
                # Keep SOBREGIRO schema exact: prefer 'valor_ponderado' (lowercase) over 'Valor_Ponderado'
                if col == 'valor_ponderado':
                    target = 'valor_ponderado'
                elif col in ('Valor_Garantia', 'Valor_Garantía'):
                    target = 'Valor_Garantia'
                else:
                    target = col
                df[target] = self._format_money_dot(df[num_col])
        if 'Importe__num' in df.columns:
            df['Importe'] = self._format_money_dot(df['Importe__num'])

        df = self._enforce_dot_decimal_strings(
            df,
            ('Valor_Inicial', 'Valor_Garantia', 'Valor_Garantía', 'Valor_Ponderado', 'valor_ponderado', 'Importe')
        )

        for col in ('Numero_Garantia', 'Numero_Cis_Garantia'):
            if col in df.columns:
                df[col] = df[col].map(lambda v: '' if pd.isna(v) else str(v).strip())
        if 'Pais_Emision' in df.columns:
            df['Pais_Emision'] = '591'

        return df

    def _ensure_tipo_facilidad_from_at03(
        self,
        df: pd.DataFrame,
        subtype: str,
        context: TransformationContext,
        result: TransformationResult,
        source_data: Dict[str, pd.DataFrame],
        at03_key: Union[str, Sequence[str]] = 'AT03_CREDITOS',
        require: bool = False
    ) -> pd.DataFrame:
        """Set Tipo_Facilidad based on presence of loan in AT03 datasets.

        Rules:
        - Normalizes Numero_Prestamo and compares with each provided AT03 `num_cta` column.
        - When multiple datasets are supplied, a loan must exist in *all available* datasets to qualify as '01'.
          Otherwise it is set to '02'.
        - Export CSV with changed rows only: FACILIDAD_FROM_AT03_[SUBTYPE]_[YYYYMMDD].csv, preserving all columns and
          adding Tipo_Facilidad_ORIGINAL next to Tipo_Facilidad.
        """
        if df is None or df.empty:
            return df

        # Normalize key configuration
        if isinstance(at03_key, (list, tuple, set)):
            candidate_keys = list(at03_key)
        else:
            candidate_keys = [at03_key]

        available_sets: List[set] = []
        missing_sources: List[str] = []
        
        # Helper: pick best candidate column for account number in AT03
        def _pick_numcta_column(at03_df: pd.DataFrame) -> Optional[str]:
            try:
                from src.core.naming import HeaderNormalizer as _HN
                norm_map = {c: _HN.normalize_headers([c])[0].upper() for c in at03_df.columns}
            except Exception:
                norm_map = {c: str(c).upper() for c in at03_df.columns}
            candidates = {
                'NUM_CTA', 'NUMCTA', 'NUMERO_CUENTA', 'NUM_CUENTA', 'NUMERO_PRESTAMO', 'NUM_PRESTAMO',
                'NUMERO_PRESTAMO_TDC', 'NUM_PRESTAMO_TDC', 'NUM_PREST', 'NUMCTATDC', 'NUM_CTA_TDC'
            }
            for original, norm in norm_map.items():
                if norm in candidates:
                    return original
            # Fallback to exact 'num_cta' if present
            if 'num_cta' in at03_df.columns:
                return 'num_cta'
            return None

        for key in candidate_keys:
            if key not in source_data or source_data[key].empty:
                missing_sources.append(key)
                continue
            at03_df = source_data[key]
            numcol = _pick_numcta_column(at03_df)
            if not numcol or numcol not in at03_df.columns:
                self.logger.warning(f"FACILIDAD_FROM_AT03 skipped: account column not found in {key}")
                continue
            normalized = self._normalize_join_key(at03_df[numcol])
            cleaned = {str(val) for val in normalized.dropna() if str(val).strip() != ''}
            if not cleaned:
                message = f"FACILIDAD_FROM_AT03 skipped: no usable account values found in {key}"
                if require:
                    raise RuntimeError(message)
                self.logger.warning(message)
                continue
            available_sets.append(cleaned)

        if missing_sources:
            msg = ", ".join(missing_sources)
            if require:
                raise RuntimeError(f"{msg} not available; required for FACILIDAD_FROM_AT03 in {subtype}")
            try:
                self.logger.info(f"FACILIDAD_FROM_AT03 ({subtype}): skipping missing sources {msg}")
            except Exception:
                pass

        if not available_sets:
            return df

        # Resolve loan number column on DF (support accented variant for TDC)
        loan_col = None
        if 'Numero_Prestamo' in df.columns:
            loan_col = 'Numero_Prestamo'
        elif 'Número_Préstamo' in df.columns:
            loan_col = 'Número_Préstamo'
        else:
            self.logger.warning("FACILIDAD_FROM_AT03 skipped: Numero_Prestamo column not found")
            return df

        if 'Tipo_Facilidad' not in df.columns:
            self.logger.warning("FACILIDAD_FROM_AT03 skipped: Tipo_Facilidad column not found")
            return df

        # Normalize keys
        left_keys = self._normalize_join_key(df[loan_col])

        # Determine presence across all available datasets (intersection by default)
        import pandas as _pd
        present_mask = _pd.Series(True, index=df.index)
        any_mask = _pd.Series(False, index=df.index)
        for key_set in available_sets:
            isin = left_keys.isin(key_set)
            isin = isin.fillna(False)
            present_mask &= isin
            any_mask |= isin

        # Build proposed values Series aligned to df
        new_vals = _pd.Series(_pd.NA, index=df.index, dtype=object)
        # If intersection finds no matches but union finds some, fall back to union
        union_matches = int(any_mask.sum())
        if int(present_mask.sum()) == 0 and union_matches > 0:
            try:
                self.logger.warning(
                    f"FACILIDAD_FROM_AT03 ({subtype}): no intersection matches across sources; "
                    f"falling back to union criteria (matches={union_matches})"
                )
            except Exception:
                pass
            present_mask = any_mask
        new_vals.loc[present_mask] = '01'
        new_vals.loc[~present_mask] = '02'

        # Identify changes
        current_vals = df['Tipo_Facilidad'].astype(str)
        change_mask = current_vals != new_vals.astype(str)
        changed_count = int(change_mask.sum())
        if changed_count > 0:
            try:
                self.logger.info(f"FACILIDAD_FROM_AT03 ({subtype}): updating Tipo_Facilidad for {changed_count} record(s)")
            except Exception:
                pass
            # Preserve original column for export
            original_map = {'Tipo_Facilidad': df['Tipo_Facilidad'].copy()}
            # Apply changes
            df = df.copy()
            df.loc[change_mask, 'Tipo_Facilidad'] = new_vals.loc[change_mask]
            # Export changed subset with original value alongside
            try:
                self._export_error_subset(df, change_mask, subtype, 'FACILIDAD_FROM_AT03', context, result, original_columns=original_map)
            except Exception as e:
                self.logger.warning(f"Failed to export FACILIDAD_FROM_AT03 subset: {e}")
            # Store concise incidences
            try:
                payload = [{
                    'Index': int(i),
                    'Numero_Prestamo': str(df.at[i, loan_col]) if loan_col in df.columns else '',
                    'Tipo_Facilidad_New': str(df.at[i, 'Tipo_Facilidad']),
                    'Action': 'Tipo_Facilidad set from AT03 presence'
                } for i in df[change_mask].index]
                if payload:
                    self._store_incidences('FACILIDAD_FROM_AT03', payload, context)
            except Exception:
                pass

        return df
    
    def _process_valores_data(self, df: pd.DataFrame, context: TransformationContext, 
                            result: TransformationResult, source_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Process VALORES_AT12 applying ETL rules for garantías 0507."""
        self.logger.info("Processing VALORES_AT12 data - Stage 2 (ETL 0507)")

        # Step 0: Remove blank rows before any normalization
        df = self._drop_blank_records(df, 'VALORES_AT12')

        # Step 1: Basic normalization (trim + money)
        df = self._normalize_tdc_basic(df)

        # Step 2: Keys normalization (Numero_Prestamo / Id_Documento)
        df = self._normalize_tdc_keys(df)

        # Step 2b: Resolve Tipo_Facilidad from AT03 datasets (same rule as TDC/SOBREGIRO)
        try:
            df = self._ensure_tipo_facilidad_from_at03(
                df,
                'VALORES_AT12',
                context,
                result,
                source_data,
                at03_key=['AT03_CREDITOS', 'AT03_TDC'],
                require=False
            )
        except Exception as exc:
            self.logger.warning(f"Skipping FACILIDAD_FROM_AT03 for VALORES due to error: {exc}")

        # Step 3: Generate Numero_Garantia (persistent, padded) for 0507
        df = self._generate_numero_garantia_valores(df, context)

        # Step 4: Enrichment & derived fields for 0507
        df = self._enrich_valores_0507(df)

        # Step 5: Importe = Valor_Garantia; format monetary columns with dot decimal
        # Only overwrite where numeric parse succeeded; otherwise preserve original value
        for col in ('Valor_Inicial', 'Valor_Garantia', 'Valor_Garantía', 'Valor_Ponderado'):
            num_col = col + '__num'
            if num_col in df.columns:
                target_name = 'Valor_Garantia' if col in ('Valor_Garantia', 'Valor_Garantía') else col
                try:
                    mask_ok = df[num_col].notna()
                    if mask_ok.any():
                        df.loc[mask_ok, target_name] = self._format_money_dot(df.loc[mask_ok, num_col])
                    # Fallback: for rows without numeric value, keep original but force dot decimal
                    if (~mask_ok).any() and target_name in df.columns:
                        df.loc[~mask_ok, target_name] = (
                            df.loc[~mask_ok, target_name].astype(str).str.replace(',', '.', regex=False)
                        )
                except Exception:
                    # As a last resort, simple comma→dot
                    try:
                        df[target_name] = df[target_name].astype(str).str.replace(',', '.', regex=False)
                    except Exception:
                        pass

        valor_num_series = None
        if 'Valor_Garantia__num' in df.columns:
            valor_num_series = df['Valor_Garantia__num']
        elif 'Valor_Garantía__num' in df.columns:
            valor_num_series = df['Valor_Garantía__num']

        if valor_num_series is not None:
            try:
                mask_ok = valor_num_series.notna()
                if mask_ok.any():
                    df.loc[mask_ok, 'Importe'] = self._format_money_dot(valor_num_series[mask_ok])
                # Fallback for rows without numeric parse: copy Valor_Garantia textual (comma→dot)
                if (~mask_ok).any():
                    df.loc[~mask_ok, 'Importe'] = (
                        df.loc[~mask_ok, 'Valor_Garantia'].astype(str).str.replace(',', '.', regex=False)
                    )
                if 'Importe__num' in df.columns:
                    df.loc[mask_ok, 'Importe__num'] = valor_num_series[mask_ok]
            except Exception:
                # Fallback to simple textual replacement
                try:
                    df['Importe'] = df.get('Valor_Garantia', df.get('Valor_Garantía', '')).astype(str).str.replace(',', '.', regex=False)
                except Exception:
                    pass
        elif 'Importe__num' in df.columns:
            df['Importe'] = self._format_money_dot(df['Importe__num'])

        if 'Importe__num' in df.columns and valor_num_series is not None:
            mismatch_mask = ~(df['Importe__num'].fillna(pd.NA).eq(valor_num_series.fillna(pd.NA)))
            mismatch_mask &= ~(df['Importe__num'].isna() & valor_num_series.isna())
            if mismatch_mask.any():
                sample = df.loc[mismatch_mask, ['Numero_Prestamo', 'Id_Documento']].head(5).to_dict('records')
                raise RuntimeError(
                    "VALORES_AT12: Importe must equal Valor_Garantia for all rows. "
                    f"Mismatch detected (examples: {sample})."
                )

        # Step 6: Shape final output columns (transformado)
        expected_cols = [
            'Fecha', 'Codigo_Banco', 'Numero_Prestamo', 'Numero_Ruc_Garantia', 'Id_Fideicomiso',
            'Nombre_Fiduciaria', 'Origen_Garantia', 'Tipo_Garantia', 'Tipo_Facilidad', 'Id_Documento',
            'Nombre_Organismo', 'Valor_Inicial', 'Valor_Garantia', 'Valor_Ponderado', 'Tipo_Instrumento',
            'Calificacion_Emisor', 'Calificacion_Emisision', 'Pais_Emision', 'Fecha_Ultima_Actualizacion',
            'Fecha_Vencimiento', 'Tipo_Poliza', 'Codigo_Region', 'Clave_Pais', 'Clave_Empresa',
            'Clave_Tipo_Garantia', 'Clave_Subtipo_Garantia', 'Clave_Tipo_Pren_Hipo', 'Numero_Garantia',
            'Numero_Cis_Garantia', 'Numero_Cis_Prestamo', 'Numero_Ruc_Prestamo', 'Moneda', 'Importe',
            'Status_Garantia', 'Status_Prestamo', 'Codigo_Origen', 'Segmento'
        ]
        for col in expected_cols:
            if col not in df.columns:
                df[col] = ''
        # País_Emision constant for VALORES output
        df['Pais_Emision'] = '591'
        # Prefer non-accented Codigo_Banco if present via mapping
        if 'Código_Banco' in df.columns and 'Codigo_Banco' not in df.columns:
            df['Codigo_Banco'] = df['Código_Banco']
        # Ensure Valor_Garantia column (non-accent)
        if 'Valor_Garantia' not in df.columns and 'Valor_Garantía' in df.columns:
            df['Valor_Garantia'] = df['Valor_Garantía']

        df = df[expected_cols]
        try:
            df = df.sort_values(['Fecha', 'Codigo_Banco', 'Numero_Prestamo', 'Numero_Ruc_Garantia'], ascending=True)
        except Exception:
            pass

        df = self._enforce_dot_decimal_strings(
            df,
            ('Valor_Inicial', 'Valor_Garantia', 'Valor_Ponderado', 'Importe')
        )

        return df
    
    def _generate_numero_garantia_tdc(self, df: pd.DataFrame, context: TransformationContext) -> pd.DataFrame:
        """Generate/normalize Número_Garantía for TDC_AT12 using functional context rules.

        - Target column: 'Número_Garantía' (exact accent and casing).
        - Key for assignment: (Id_Documento, Tipo_Facilidad).
        - Sequential from 850500; reuse for repeated keys within the run.
        - Output formatting: always 10 digits (left‑padded with zeros) when numeric.
        - Preserve original non-empty values (normalize to 10 digits only if purely numeric).
        - No incidences emitted; logging only.
        """
        self.logger.info("Generating/normalizing Número_Garantía for TDC_AT12")

        required_cols = ['Id_Documento', 'Tipo_Facilidad']
        missing_required = [c for c in required_cols if c not in df.columns]
        if missing_required:
            self.logger.warning(f"Skipping Número_Garantía generation; missing columns: {missing_required}")
            # Ensure target exists to avoid KeyErrors downstream
            target = None
            if 'Número_Garantía' in df.columns:
                target = 'Número_Garantía'
            elif 'Numero_Garantia' in df.columns:
                target = 'Numero_Garantia'
            elif 'num_garantía' in df.columns:
                target = 'num_garantía'
            if target is not None and target not in df.columns:
                df = df.copy(); df[target] = None
            return df

        df = df.copy()
        # Determine target column to update (prefer exact schema name)
        if 'Número_Garantía' in df.columns:
            target_col = 'Número_Garantía'
        elif 'Numero_Garantia' in df.columns:
            target_col = 'Numero_Garantia'
        elif 'num_garantía' in df.columns:
            target_col = 'num_garantía'
        else:
            # Create it explicitly if mapping hasn't run yet
            target_col = 'Número_Garantía'
            df[target_col] = None
        # Sort ascending by Id_Documento as per new TDC context
        df = df.sort_values('Id_Documento', ascending=True).reset_index(drop=True)

        unique_keys = {}
        next_number = 850500
        incidences = []
        try:
            orig_series = pd.Series(index=df.index, dtype=object)
        except Exception:
            orig_series = None

        for idx, row in df.iterrows():
            # Use tuple key to avoid collisions from string concatenation
            unique_key = (str(row.get('Id_Documento', '')), str(row.get('Tipo_Facilidad', '')))
            # Assign by key using sequential registry (overwrite any existing value); no padding
            if unique_key not in unique_keys:
                unique_keys[unique_key] = next_number
                next_number += 1
            assigned_num = unique_keys[unique_key]
            assigned = str(assigned_num)
            if assigned.isdigit():
                assigned = assigned.zfill(10)
            df.at[idx, target_col] = assigned
            incidences.append({
                'Index': idx,
                'Id_Documento': row.get('Id_Documento', ''),
                'Numero_Prestamo': row.get('Numero_Prestamo', ''),
                'Tipo_Facilidad': row.get('Tipo_Facilidad', ''),
                'Numero_Garantia_Assigned': df.at[idx, target_col]
            })

        # Log only (no incidences for Numero_Garantia generation)
        self.logger.info(f"Generated {len(unique_keys)} unique guarantee numbers for TDC_AT12")

        # Update tracker for VALORES rule: last assigned numeric value (if any)
        try:
            # Prefer values we assigned in this run
            assigned_vals = []
            for rec in incidences:
                try:
                    v = int(str(rec.get('Numero_Garantia_Assigned', '')).strip())
                    assigned_vals.append(v)
                except Exception:
                    continue
            if assigned_vals:
                last_val = max(assigned_vals)
            else:
                # Fallback: parse from target column
                ser = df['Número_Garantía'].astype(str).str.strip()
                nums = ser[ser.str.fullmatch(r"\d+")].astype(int)
                last_val = int(nums.max()) if not nums.empty else None
            if last_val is not None:
                self._last_tdc_num_garantia = max(self._last_tdc_num_garantia or 0, last_val)
        except Exception:
            pass
        
        # Error condition: repeated Numero_Prestamo for same (Id_Documento, Tipo_Facilidad)
        try:
            if 'Numero_Prestamo' in df.columns:
                dup_mask = (
                    df['Numero_Prestamo'].astype(str).ne('') &
                    df.duplicated(subset=['Id_Documento', 'Tipo_Facilidad', 'Numero_Prestamo'], keep=False)
                )
                if dup_mask.any():
                    count = int(dup_mask.sum())
                    try:
                        self.logger.warning(f"TDC: {count} repeated Numero_Prestamo within (Id_Documento, Tipo_Facilidad) detected; handled in-process (no incidence export)")
                    except Exception:
                        pass
        except Exception:
            pass
        # If we used a legacy/mapped variant, harmonize to 'Número_Garantía'
        if target_col != 'Número_Garantía':
            try:
                df['Número_Garantía'] = df[target_col]
                # Drop old column variant if different
                if target_col in df.columns and target_col != 'Número_Garantía':
                    df.drop(columns=[target_col], inplace=True, errors='ignore')
            except Exception:
                pass
        return df

    def _finalize_tdc_output(self, df: pd.DataFrame) -> pd.DataFrame:
        """Finalize TDC output layout: trim fields, standardize headers, enforce schema order."""
        if df is None or df.empty:
            return df

        df = df.copy()

        rename_map = {}
        mapping_pairs = [
            ('Fecha_Ultima_Actualizacion', 'Fecha_Última_Actualización'),
            ('Valor_Garantia', 'Valor_Garantía'),
            ('Codigo_Region', 'Código_Región'),
            ('Numero_Garantia', 'Número_Garantía'),
            ('Numero_Cis_Garantia', 'Número_Cis_Garantía'),
            ('Descripcion de la Garantia', 'Descripción de la Garantía'),
            ('Descripción de la Garantia', 'Descripción de la Garantía'),
            ('Descripcion_de_la_Garantia', 'Descripción de la Garantía'),
            ('Pais_Emision', 'País_Emisión')
        ]
        for source, target in mapping_pairs:
            if source in df.columns and target not in df.columns:
                rename_map[source] = target
        if rename_map:
            df.rename(columns=rename_map, inplace=True)

        # Guarantee column presence with proper accents
        if 'País_Emisión' not in df.columns:
            df['País_Emisión'] = ''
        df['País_Emisión'] = '591'

        # Trim sensitive identifiers to remove stray spaces
        def _strip_or_blank(val: Any) -> str:
            if pd.isna(val):
                return ''
            return str(val).strip()

        for col in ('Número_Cis_Garantía', 'Numero_Cis_Garantia', 'Número_Garantía', 'Numero_Garantia', 'Número_Cis_Prestamo', 'Numero_Cis_Prestamo'):
            if col in df.columns:
                df[col] = df[col].map(_strip_or_blank)

        if 'Número_Garantía' in df.columns:
            df['Número_Garantía'] = df['Número_Garantía'].map(
                lambda val: '' if val == '' else (val.zfill(10) if val.isdigit() else val)
            )

        # Ensure description column exists even if original file omitted it
        if 'Descripción de la Garantía' not in df.columns:
            df['Descripción de la Garantía'] = ''

        # Harmonize Codigo_Banco accent if needed
        if 'Codigo_Banco' in df.columns and 'Código_Banco' not in df.columns:
            df['Código_Banco'] = df['Codigo_Banco']

        # Harmonize Valor_Garantia non-accented variant
        if 'Valor_Garantía' not in df.columns and 'Valor_Garantia' in df.columns:
            df['Valor_Garantía'] = df['Valor_Garantia']

        try:
            from src.core.header_mapping import HeaderMapper as _HM
            expected_cols = list(_HM.TDC_AT12_EXPECTED)
        except Exception:
            expected_cols = [
                'Fecha', 'Código_Banco', 'Número_Préstamo', 'Número_Ruc_Garantía', 'Id_Fideicomiso',
                'Nombre_Fiduciaria', 'Origen_Garantía', 'Tipo_Garantía', 'Tipo_Facilidad', 'Id_Documento',
                'Nombre_Organismo', 'Valor_Inicial', 'Valor_Garantía', 'Valor_Ponderado', 'Tipo_Instrumento',
                'Calificación_Emisor', 'Calificación_Emisión', 'País_Emisión', 'Fecha_Última_Actualización',
                'Fecha_Vencimiento', 'Tipo_Poliza', 'Código_Región', 'Número_Garantía', 'Número_Cis_Garantía',
                'Moneda', 'Importe', 'Descripción de la Garantía'
            ]

        # Add any missing expected column as blank strings
        for col in expected_cols:
            if col not in df.columns:
                df[col] = ''

        df = df[expected_cols]

        return df
    
    def _apply_date_mapping_tdc(self, df: pd.DataFrame, context: TransformationContext, 
                               source_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Apply date mapping for TDC using AT02 per new rule.

        Join:
          - TDC.Id_Documento ↔ AT02.identificacion_de_cuenta (supporting common variants)
        Map:
          - TDC.Fecha_Ultima_Actualizacion ← AT02.Fecha_inicio (variants supported)
          - TDC.Fecha_Vencimiento ← AT02.Fecha_Vencimiento
        """
        self.logger.info("Applying date mapping for TDC_AT12 (Id_Documento ↔ identificacion_de_cuenta)")

        if 'AT02_CUENTAS' not in source_data or source_data['AT02_CUENTAS'].empty:
            self.logger.warning("AT02_CUENTAS data not found or is empty. Skipping date mapping for TDC.")
            return df

        at02_df = source_data['AT02_CUENTAS']

        # Resolve keys and date columns with robust candidates
        tdc_key = self._col_any(df, ['Id_Documento', 'id_documento', 'ID_DOCUMENTO'])
        at02_key = self._col_any(at02_df, ['identificacion_de_cuenta', 'Identificacion_de_cuenta', 'Identificacion_Cuenta', 'identificacion_cuenta'])
        at02_start = self._col_any(at02_df, ['Fecha_inicio', 'Fecha_Inicio', 'fecha_inicio', 'Fecha_proceso'])
        at02_end = self._col_any(at02_df, ['Fecha_Vencimiento', 'fecha_vencimiento', 'Fecha_vencimiento'])

        missing = []
        if not tdc_key:
            missing.append('Id_Documento')
        if not at02_key:
            missing.append('identificacion_de_cuenta')
        if not at02_start:
            missing.append('Fecha_inicio')
        if not at02_end:
            missing.append('Fecha_Vencimiento')
        if missing:
            self.logger.error(f"Missing columns for TDC date mapping: {missing}")
            return df

        original_count = len(df)
        right = at02_df[[at02_key, at02_start, at02_end]].copy()
        right.columns = ['_key_at02', 'Fecha_inicio_at02', 'Fecha_Vencimiento_at02']

        # Trim whitespace and normalize empty-like tokens in AT02 date fields
        try:
            for col in ['Fecha_inicio_at02', 'Fecha_Vencimiento_at02']:
                if col in right.columns:
                    s = right[col].astype(str).str.strip()
                    s = s.replace({'': pd.NA, 'nan': pd.NA, 'None': pd.NA, 'NaT': pd.NA}, regex=False)
                    right[col] = s
        except Exception:
            pass

        # Normalize join keys on both sides to improve matches and avoid format issues
        left = df.copy()
        left['_key_tdc'] = left[tdc_key].astype(str)
        try:
            left['_join_key'] = self._normalize_join_key(left['_key_tdc'])
        except Exception:
            left['_join_key'] = left['_key_tdc']

        right['_key_at02'] = right['_key_at02'].astype(str)
        try:
            right['_join_key'] = self._normalize_join_key(right['_key_at02'])
        except Exception:
            right['_join_key'] = right['_key_at02']

        # Deduplicate AT02 per normalized key to avoid 1-to-many row explosions on merge
        # Prefer the most recent available dates when duplicates exist
        try:
            r = right.copy()
            # Parse dates for ordering if possible
            import pandas as _pd
            for col in ['Fecha_inicio_at02', 'Fecha_Vencimiento_at02']:
                if col in r.columns:
                    try:
                        r[col + '_dt'] = _pd.to_datetime(r[col], errors='coerce', dayfirst=True)
                    except Exception:
                        r[col + '_dt'] = _pd.NaT
            sort_cols = []
            if 'Fecha_inicio_at02_dt' in r.columns:
                sort_cols.append('Fecha_inicio_at02_dt')
            if 'Fecha_Vencimiento_at02_dt' in r.columns:
                sort_cols.append('Fecha_Vencimiento_at02_dt')
            if sort_cols:
                r = r.sort_values(sort_cols, ascending=False)
            r = r.drop_duplicates(subset=['_join_key'], keep='first')
            right_dedup = r[['_join_key', 'Fecha_inicio_at02', 'Fecha_Vencimiento_at02']].copy()
        except Exception:
            # Conservative fallback: keep first occurrence per key
            right_dedup = right.drop_duplicates(subset=['_join_key'], keep='first')[[
                '_join_key', 'Fecha_inicio_at02', 'Fecha_Vencimiento_at02']].copy()

        merged = left.merge(right_dedup, on='_join_key', how='left')

        incidences = []
        # Resolve target columns (handle accented and non-accented variants)
        tgt_last_update = None
        if 'Fecha_Ultima_Actualizacion' in merged.columns:
            tgt_last_update = 'Fecha_Ultima_Actualizacion'
        elif 'Fecha_Última_Actualización' in merged.columns:
            tgt_last_update = 'Fecha_Última_Actualización'
        tgt_venc = 'Fecha_Vencimiento' if 'Fecha_Vencimiento' in merged.columns else None

        def _has_val(v: object) -> bool:
            sv = str(v).strip()
            return sv not in ('', 'nan', 'None', 'NaT')

        for idx, row in merged.iterrows():
            updated = False
            if tgt_last_update and _has_val(row.get('Fecha_inicio_at02')):
                merged.at[idx, tgt_last_update] = str(row.get('Fecha_inicio_at02')).strip()
                updated = True
            if tgt_venc and _has_val(row.get('Fecha_Vencimiento_at02')):
                merged.at[idx, tgt_venc] = str(row.get('Fecha_Vencimiento_at02')).strip()
                updated = True
            if updated:
                incidences.append({
                    'Index': int(idx),
                    'Id_Documento': row.get(tdc_key, ''),
                    'Fecha_Ultima_Actualizacion_Updated': row.get('Fecha_inicio_at02', ''),
                    'Fecha_Vencimiento_Updated': row.get('Fecha_Vencimiento_at02', ''),
                    'Action': 'Date mapping applied from AT02_CUENTAS (Id_Documento↔identificacion_de_cuenta)'
                })

        # Drop helper columns
        merged.drop(columns=[c for c in merged.columns if c in ['_key_tdc', '_key_at02', '_join_key', 'Fecha_inicio_at02', 'Fecha_Vencimiento_at02']], inplace=True, errors='ignore')

        # Log only (no incidences for date mapping in TDC)
        self.logger.info(f"Applied date mapping to {len(incidences)} out of {original_count} TDC records")

        return merged
    
    def _apply_date_mapping_sobregiro(self, df: pd.DataFrame, context: TransformationContext, 
                                     source_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Apply date mapping for SOBREGIRO_AT12 using AT02_CUENTAS.

        Update Fecha_Ultima_Actualizacion from AT02.Fecha_proceso and Fecha_Vencimiento from
        AT02.Fecha_Vencimiento using a robust join. Prefer single-key join on Id_Documento ↔
        identificacion_de_cuenta (normalized) to mirror TDC logic. Fallback to dual-key join
        (Identificacion_cliente, Identificacion_Cuenta) if present.
        """
        self.logger.info("Applying date mapping for SOBREGIRO_AT12")

        if 'AT02_CUENTAS' not in source_data or source_data['AT02_CUENTAS'].empty:
            self.logger.warning("AT02_CUENTAS data not found or is empty. Skipping SOBREGIRO date mapping.")
            return df

        at02_df = source_data['AT02_CUENTAS']

        # Target columns present in SOBREGIRO
        base_date_cols = []
        if 'Fecha_Ultima_Actualizacion' in df.columns:
            base_date_cols.append('Fecha_Ultima_Actualizacion')
        if 'Fecha_Vencimiento' in df.columns:
            base_date_cols.append('Fecha_Vencimiento')

        # Resolve join mode
        join_mode = None
        if 'Id_Documento' in df.columns and 'Identificacion_Cuenta' in at02_df.columns:
            join_mode = 'single_key'
        elif all(c in df.columns for c in ['Identificacion_cliente', 'Identificacion_Cuenta']) and \
             all(c in at02_df.columns for c in ['Identificacion_cliente', 'Identificacion_Cuenta']):
            join_mode = 'dual_key'
        else:
            self.logger.error(
                "Missing columns for SOBREGIRO mapping. "
                f"DF keys available: {list(df.columns)}, AT02 keys available: {list(at02_df.columns)}"
            )
            return df

        # Resolve AT02 date columns robustly (accept common variants)
        at02_start = self._col_any(at02_df, ['Fecha_inicio', 'Fecha_Inicio', 'fecha_inicio', 'Fecha_proceso'])
        at02_end = self._col_any(at02_df, ['Fecha_Vencimiento', 'fecha_vencimiento', 'Fecha_vencimiento'])
        missing = []
        if not at02_start:
            missing.append('Fecha_inicio')
        if not at02_end:
            missing.append('Fecha_Vencimiento')
        if missing:
            self.logger.error(
                f"AT02 required date columns missing for SOBREGIRO mapping: {missing}"
            )
            return df

        right = at02_df[[at02_start, at02_end, 'Identificacion_Cuenta']].copy() if 'Identificacion_Cuenta' in at02_df.columns else at02_df.copy()
        right = right.rename(columns={
            at02_start: 'Fecha_Ultima_Actualizacion_at02',
            at02_end: 'Fecha_Vencimiento_at02'
        })

        # Trim whitespace and normalize empty-like tokens in AT02 date fields
        try:
            for col in ['Fecha_Ultima_Actualizacion_at02', 'Fecha_Vencimiento_at02']:
                if col in right.columns:
                    s = right[col].astype(str).str.strip()
                    s = s.replace({'': pd.NA, 'nan': pd.NA, 'None': pd.NA, 'NaT': pd.NA}, regex=False)
                    right[col] = s
        except Exception:
            pass

        import pandas as _pd
        if join_mode == 'single_key':
            left = df.reset_index(drop=True).copy()
            # Normalize keys (digits-only, strip leading zeros); keep '0' for empties
            left['_join_key'] = self._normalize_join_key(left['Id_Documento']) if 'Id_Documento' in left.columns else left.get('Id_Documento', '')
            right['_join_key'] = self._normalize_join_key(right['Identificacion_Cuenta']) if 'Identificacion_Cuenta' in right.columns else right.get('Identificacion_Cuenta', '')
            # Deduplicate right by most recent dates
            r = right[['_join_key', 'Fecha_Ultima_Actualizacion_at02', 'Fecha_Vencimiento_at02']].copy()
            for col in ['Fecha_Ultima_Actualizacion_at02', 'Fecha_Vencimiento_at02']:
                try:
                    r[col + '_dt'] = _pd.to_datetime(r[col], errors='coerce', dayfirst=True)
                except Exception:
                    r[col + '_dt'] = _pd.NaT
            sort_cols = [c for c in ['Fecha_Ultima_Actualizacion_at02_dt', 'Fecha_Vencimiento_at02_dt'] if c in r.columns]
            if sort_cols:
                r = r.sort_values(sort_cols, ascending=False)
            r = r.drop_duplicates(subset=['_join_key'], keep='first')
            right_dedup = r[['_join_key', 'Fecha_Ultima_Actualizacion_at02', 'Fecha_Vencimiento_at02']]
            merged = left.merge(right_dedup, on='_join_key', how='left')
        else:
            # dual_key
            keys = ['Identificacion_cliente', 'Identificacion_Cuenta']
            left_cols = keys + base_date_cols
            left = df.reset_index(drop=True)[left_cols].copy()
            rename_base = {c: f"{c}_base" for c in base_date_cols}
            if rename_base:
                left.rename(columns=rename_base, inplace=True)
            right_cols = keys + ['Fecha_Ultima_Actualizacion_at02', 'Fecha_Vencimiento_at02']
            right = right[right_cols].copy()
            merged = left.merge(right, on=keys, how='left')

        # Diagnostics (debug level to avoid noise in INFO)
        try:
            self.logger.debug(f"SOBREGIRO merged columns: {list(merged.columns)}")
        except Exception:
            pass

        # Apply mapped dates with fallback to base
        out = df.reset_index(drop=True).copy()
        orig_out = out.copy()

        if 'Fecha_Ultima_Actualizacion' in out.columns:
            base_col = 'Fecha_Ultima_Actualizacion'
            if 'Fecha_Ultima_Actualizacion_base' in merged.columns:
                base_col = 'Fecha_Ultima_Actualizacion_base'
            if 'Fecha_Ultima_Actualizacion_at02' in merged.columns:
                # Clean new values: treat empty-like tokens as NA for proper fallback
                nu = merged['Fecha_Ultima_Actualizacion_at02'].astype(str).str.strip().replace({'': pd.NA, 'nan': pd.NA, 'None': pd.NA, 'NaT': pd.NA}, regex=False)
                out['Fecha_Ultima_Actualizacion'] = nu.fillna(merged.get(base_col))
            else:
                out['Fecha_Ultima_Actualizacion'] = merged.get(base_col)

        if 'Fecha_Vencimiento' in out.columns:
            base_col = 'Fecha_Vencimiento'
            if 'Fecha_Vencimiento_base' in merged.columns:
                base_col = 'Fecha_Vencimiento_base'
            if 'Fecha_Vencimiento_at02' in merged.columns:
                nv = merged['Fecha_Vencimiento_at02'].astype(str).str.strip().replace({'': pd.NA, 'nan': pd.NA, 'None': pd.NA, 'NaT': pd.NA}, regex=False)
                out['Fecha_Vencimiento'] = nv.fillna(merged.get(base_col))
            else:
                out['Fecha_Vencimiento'] = merged.get(base_col)

        # Incidences (legacy storage for traceability)
        incidences = []
        if 'Fecha_Ultima_Actualizacion' in base_date_cols and 'Fecha_Ultima_Actualizacion_at02' in merged.columns:
            updated = merged['Fecha_Ultima_Actualizacion_at02'].notna().sum()
            incidences.append({'Field': 'Fecha_Ultima_Actualizacion', 'Updated_From_AT02': int(updated), 'Action': 'Date mapping applied from AT02_CUENTAS'})
        if 'Fecha_Vencimiento' in base_date_cols and 'Fecha_Vencimiento_at02' in merged.columns:
            updated = merged['Fecha_Vencimiento_at02'].notna().sum()
            incidences.append({'Field': 'Fecha_Vencimiento', 'Updated_From_AT02': int(updated), 'Action': 'Date mapping applied from AT02_CUENTAS'})
        if incidences:
            self._store_incidences('SOBREGIRO_DATE_MAPPING', incidences, context)

        # Export per-row changes with ORIGINAL columns as specified in context
        try:
            changed_mask = None
            changed_cols = []
            for col in ['Fecha_Ultima_Actualizacion', 'Fecha_Vencimiento']:
                if col in out.columns:
                    cm = out[col].astype(str) != orig_out[col].astype(str)
                    changed_mask = cm if changed_mask is None else (changed_mask | cm)
                    if cm.any():
                        changed_cols.append(col)
            if changed_mask is not None and changed_mask.any():
                export_df = out.loc[changed_mask].copy()
                # Insert ORIGINAL columns next to updated ones
                try:
                    cols = list(export_df.columns)
                    new_cols = []
                    for c in cols:
                        new_cols.append(c)
                        if c in changed_cols:
                            export_df[f"{c}_ORIGINAL"] = orig_out.loc[export_df.index, c]
                            new_cols.append(f"{c}_ORIGINAL")
                    export_df = export_df[new_cols]
                except Exception:
                    # Fallback: appended ORIGINAL columns
                    for c in changed_cols:
                        export_df[f"{c}_ORIGINAL"] = orig_out.loc[export_df.index, c]
                # Save to DATE_MAPPING_CHANGES_SOBREGIRO_[YYYYMMDD].csv
                out_path = context.paths.incidencias_dir / f"DATE_MAPPING_CHANGES_SOBREGIRO_{context.period}.csv"
                out_path.parent.mkdir(parents=True, exist_ok=True)
                export_df.to_csv(
                    out_path,
                    index=False,
                    encoding='utf-8',
                    sep=getattr(context.config, 'output_delimiter', '|'),
                    quoting=1,
                    date_format='%Y%m%d'
                )
                try:
                    self.logger.info(f"DATE_MAPPING_CHANGES_SOBREGIRO -> {out_path.name} ({len(export_df)} records)")
                except Exception:
                    pass
        except Exception as e:
            self.logger.warning(f"Failed to export DATE_MAPPING_CHANGES_SOBREGIRO: {e}")

        return out
    
    def _generate_numero_garantia_valores(self, df: pd.DataFrame, context: TransformationContext) -> pd.DataFrame:
        """Generate Numero_Garantia for VALORES_AT12 (0507).

        If TDC was processed in this run and a last TDC number exists, start from (last_tdc + 500)
        and assign sequentially for VALORES (0507). Otherwise, fall back to persistent sequence.
        Always pad to 10 digits.
        """
        from src.core.sequence import SequenceRegistry
        if df is None or df.empty:
            return df
        df = df.copy()
        if 'Numero_Garantia' not in df.columns:
            df['Numero_Garantia'] = None
        mask = df.get('Tipo_Garantia').astype(str) == '0507'
        incidences = []
        # Determine strategy: TDC-based or persistent
        start_from_tdc: Optional[int] = None
        try:
            if self._last_tdc_num_garantia is not None and self._last_tdc_num_garantia > 0:
                start_from_tdc = int(self._last_tdc_num_garantia) + 500
        except Exception:
            start_from_tdc = None

        if start_from_tdc is not None:
            seq = start_from_tdc
            for idx in df[mask].index:
                padded = str(seq).zfill(10)
                df.at[idx, 'Numero_Garantia'] = padded
                incidences.append({
                    'Index': int(idx),
                    'Numero_Garantia_Assigned': padded,
                    'Action': 'VALORES Numero_Garantia assigned from TDC last + 500'
                })
                seq += 1
        else:
            # Persistent fallback
            state_dir = context.paths.base_transforms_dir / 'state'
            state_file = state_dir / 'valores_numero_garantia.json'
            start_num = int(getattr(context.config, 'valores_sequence_start', 1))
            reg = SequenceRegistry(state_file, start_number=start_num)
            for idx in df[mask].index:
                key = f"{df.at[idx, 'Id_Documento']}|{df.at[idx, 'Tipo_Facilidad']}"
                num = reg.get_or_assign(key)
                padded = str(num).zfill(10)
                df.at[idx, 'Numero_Garantia'] = padded
                incidences.append({
                    'Index': int(idx),
                    'Numero_Garantia_Assigned': padded,
                    'Action': 'VALORES Numero_Garantia assigned (persistent)'
                })
        if incidences:
            self._store_incidences('VALORES_NUMERO_GARANTIA_GENERATION', incidences, context)
        return df

    def _enrich_valores_0507(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply constants and derived fields to VALORES 0507 and derive Segmento."""
        if df is None or df.empty:
            return df
        df = df.copy()
        mask = df.get('Tipo_Garantia').astype(str) == '0507'
        if not mask.any():
            return df
        # Constants
        df.loc[mask, 'Clave_Pais'] = '24'
        df.loc[mask, 'Clave_Empresa'] = '24'
        df.loc[mask, 'Clave_Tipo_Garantia'] = '3'
        df.loc[mask, 'Clave_Subtipo_Garantia'] = '61'
        df.loc[mask, 'Clave_Tipo_Pren_Hipo'] = 'NA'
        df.loc[mask, 'Tipo_Instrumento'] = 'NA'
        df.loc[mask, 'Tipo_Poliza'] = 'NA'
        df.loc[mask, 'Status_Garantia'] = '0'
        df.loc[mask, 'Status_Prestamo'] = '-1'
        df.loc[mask, 'Calificacion_Emisor'] = 'NA'
        df.loc[mask, 'Calificacion_Emisision'] = 'NA'
        # Derived
        if 'Numero_Cis_Garantia' in df.columns:
            df.loc[mask, 'Numero_Cis_Prestamo'] = df.loc[mask, 'Numero_Cis_Garantia']
        if 'Numero_Ruc_Garantia' in df.columns:
            df.loc[mask, 'Numero_Ruc_Prestamo'] = df.loc[mask, 'Numero_Ruc_Garantia']
        # Segmento rule
        df.loc[mask, 'Segmento'] = 'PRE'
        # Importe = Valor_Garantia__num (done in caller formatting)
        if 'Valor_Garantia__num' in df.columns:
            df.loc[mask, 'Importe__num'] = df.loc[mask, 'Valor_Garantia__num']
        elif 'Valor_Garantía__num' in df.columns:
            df.loc[mask, 'Importe__num'] = df.loc[mask, 'Valor_Garantía__num']
        return df
    
    def _phase3_filter_fuera_cierre(self, df: pd.DataFrame, context: TransformationContext, 
                                   result: TransformationResult, source_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Phase 3: Generate FUERA_CIERRE_AT12 Excel report according to Stage 3 specifications."""
        self.logger.info("Executing Phase 3: Generate FUERA_CIERRE_AT12 Report")
        
        # Step 1: Execute query and get AFECTACIONES_AT12 input
        if 'AFECTACIONES_AT12' not in source_data or source_data['AFECTACIONES_AT12'].empty:
            self.logger.warning("AFECTACIONES_AT12 data not found or is empty. Skipping FUERA_CIERRE report generation.")
            return df
        
        afectaciones_df = source_data['AFECTACIONES_AT12'].copy()
        
        # Step 2: Generate Excel report with three tabs
        excel_report_path = self._generate_fuera_cierre_excel_report(afectaciones_df, context)
        
        # Step 3: Filter main DataFrame based on FUERA_CIERRE data (if available)
        if 'FUERA_CIERRE_AT12' in source_data and not source_data['FUERA_CIERRE_AT12'].empty:
            df = self._apply_fuera_cierre_filtering(df, source_data['FUERA_CIERRE_AT12'], context)
        
        # Store report path in result
        if hasattr(result, 'artifacts'):
            if not hasattr(result, 'artifacts') or result.artifacts is None:
                result.artifacts = {}
            result.artifacts['FUERA_CIERRE_REPORT'] = excel_report_path
        
        return df
    
    def _generate_fuera_cierre_excel_report(self, afectaciones_df: pd.DataFrame, context: TransformationContext) -> str:
        """Generate FUERA_CIERRE_AT12 Excel report with three tabs according to Stage 3.1 specifications."""
        self.logger.info("Generating FUERA_CIERRE_AT12 Excel report")
        
        from datetime import datetime
        from dateutil.relativedelta import relativedelta
        import os
        
        # Create output directory if it doesn't exist
        # Use processed directory under transforms/AT12 to store reports
        output_dir = context.paths.procesados_dir / 'reports'
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate report filename
        report_filename = f"FUERA_CIERRE_AT12_{context.period}.xlsx"
        report_path = output_dir / report_filename
        
        # Calculate date threshold for DESEMBOLSO (last three months)
        current_date = datetime.now()
        # Inclusive last three full months window start
        three_months_ago = current_date - relativedelta(months=3)
        
        # Prepare data with special rules
        processed_df = afectaciones_df.copy()
        
        # Apply special rule: If at_tipo_operacion is '0301', populate Nombre_Organismo with '182'
        if 'at_tipo_operacion' in processed_df.columns:
            mask_0301 = processed_df['at_tipo_operacion'] == '0301'
            if 'Nombre_Organismo' not in processed_df.columns:
                processed_df['Nombre_Organismo'] = ''
            processed_df.loc[mask_0301, 'Nombre_Organismo'] = '182'
        
        # Distribute data into tabs according to specifications
        tabs_data = self._distribute_afectaciones_into_tabs(processed_df, three_months_ago)
        
        # Create Excel file with tabs
        incidences = []
        try:
            with pd.ExcelWriter(report_path, engine='openpyxl') as writer:
                for tab_name, tab_data in tabs_data.items():
                    tab_data.to_excel(writer, sheet_name=tab_name, index=False)
                    
                    incidences.append({
                        'Tab': tab_name,
                        'Records_Count': len(tab_data),
                        'Action': f'Created {tab_name} tab with {len(tab_data)} records'
                    })
            
            # Store incidences
            if incidences:
                self._store_incidences('FUERA_CIERRE_EXCEL_GENERATION', incidences, context)
            
            self.logger.info(f"Generated FUERA_CIERRE_AT12 Excel report: {report_path}")
            return str(report_path)
            
        except Exception as e:
            self.logger.error(f"Error generating FUERA_CIERRE_AT12 Excel report: {str(e)}")
            return ""
    
    def _distribute_afectaciones_into_tabs(self, df: pd.DataFrame, three_months_ago) -> Dict[str, pd.DataFrame]:
        """Distribute AFECTACIONES data into three tabs according to Stage 3.1 specifications."""
        tabs_data = {}
        
        # Convert date column if it exists and is string
        if 'at_fecha_inicial_prestamo' in df.columns:
            try:
                df['at_fecha_inicial_prestamo'] = pd.to_datetime(df['at_fecha_inicial_prestamo'], errors='coerce')
            except:
                self.logger.warning("Could not convert at_fecha_inicial_prestamo to datetime")
        
        # DESEMBOLSO Tab: Records where at_fecha_inicial_prestamo is within the last three months
        if 'at_fecha_inicial_prestamo' in df.columns:
            desembolso_mask = df['at_fecha_inicial_prestamo'] >= three_months_ago
            tabs_data['DESEMBOLSO'] = df[desembolso_mask].copy()
        else:
            tabs_data['DESEMBOLSO'] = pd.DataFrame()
        
        # PYME Tab: Records where Segmento is 'PYME' or 'BEC'
        if 'Segmento' in df.columns:
            pyme_mask = df['Segmento'].isin(['PYME', 'BEC'])
            tabs_data['PYME'] = df[pyme_mask].copy()
        else:
            tabs_data['PYME'] = pd.DataFrame()
        
        # CARTERA Tab: All remaining records
        used_indices = set()
        if not tabs_data['DESEMBOLSO'].empty:
            used_indices.update(tabs_data['DESEMBOLSO'].index)
        if not tabs_data['PYME'].empty:
            used_indices.update(tabs_data['PYME'].index)
        
        remaining_indices = df.index.difference(list(used_indices))
        tabs_data['CARTERA'] = df.loc[remaining_indices].copy()
        
        return tabs_data
    
    def _apply_fuera_cierre_filtering(self, df: pd.DataFrame, fuera_cierre_df: pd.DataFrame, 
                                     context: TransformationContext) -> pd.DataFrame:
        """Apply filtering based on FUERA_CIERRE data."""
        self.logger.info("Applying FUERA_CIERRE filtering")
        
        if 'Numero_Prestamo' not in df.columns or 'prestamo' not in fuera_cierre_df.columns:
            self.logger.error("Required columns for FUERA_CIERRE filtering are missing. Skipping.")
            return df
        
        prestamos_to_exclude = fuera_cierre_df['prestamo'].unique()
        
        # Identify excluded loans for incidence reporting
        excluded_df = df[df['Numero_Prestamo'].isin(prestamos_to_exclude)]
        incidences = []
        for _, row in excluded_df.iterrows():
            incidences.append({
                'Numero_Prestamo': row['Numero_Prestamo'],
                'Reason': 'Excluded due to FUERA_CIERRE',
                'Period': context.period
            })
        
        if incidences:
            self._store_incidences('FUERA_CIERRE_FILTERING', incidences, context)
        
        # Filter the DataFrame
        filtered_df = df[~df['Numero_Prestamo'].isin(prestamos_to_exclude)]
        self.logger.info(f"Filtered out {len(df) - len(filtered_df)} loans due to FUERA_CIERRE.")
        
        return filtered_df
    
    def _phase4_valor_minimo_avaluo(self, df: pd.DataFrame, context: TransformationContext, 
                                  result: TransformationResult, source_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Phase 4: Validates the minimum appraisal value for guarantees according to specifications."""
        self.logger.info("Executing Phase 4: VALOR_MINIMO_AVALUO_AT12 validation")

        if 'VALOR_MINIMO_AVALUO_AT12' not in source_data or source_data['VALOR_MINIMO_AVALUO_AT12'].empty:
            self.logger.warning("VALOR_MINIMO_AVALUO_AT12 data not found or is empty. Skipping Phase 4.")
            return df

        valor_minimo_df = source_data['VALOR_MINIMO_AVALUO_AT12'].copy()
        
        if 'AT03_CREDITOS' not in source_data or source_data['AT03_CREDITOS'].empty:
            self.logger.warning("AT03_CREDITOS data not found or is empty. Skipping Phase 4.")
            return df
            
        at03_df = source_data['AT03_CREDITOS']

        # Step 1: Generate the VALOR_MINIMO_AVALUO_AT12 input (already provided)
        self.logger.info(f"Processing {len(valor_minimo_df)} records from VALOR_MINIMO_AVALUO_AT12")
        
        # Step 2: Filter where cu_tipo contains any alphabetic characters (not purely numeric)
        if 'cu_tipo' in valor_minimo_df.columns:
            cu_tipo_str = valor_minimo_df['cu_tipo'].astype(str)
            mask_alpha = cu_tipo_str.str.contains(r"[A-Za-z]", regex=True, na=False)
            valor_minimo_df = valor_minimo_df[mask_alpha]
            self.logger.info(f"Filtered to {len(valor_minimo_df)} records where cu_tipo contains alphabetic characters")
        else:
            self.logger.warning("Column 'cu_tipo' not found in VALOR_MINIMO_AVALUO_AT12. Proceeding without filtering.")

        # Step 3: Perform JOIN with AT03_CREDITOS using at_num_de_prestamos = num_cta
        join_key_valor = 'at_num_de_prestamos'
        join_key_at03 = 'num_cta'
        
        if join_key_valor not in valor_minimo_df.columns:
            self.logger.error(f"Required column '{join_key_valor}' not found in VALOR_MINIMO_AVALUO_AT12.")
            return df
            
        if join_key_at03 not in at03_df.columns:
            self.logger.error(f"Required column '{join_key_at03}' not found in AT03_CREDITOS.")
            return df

        # Ensure both join keys have the same dtype to avoid object/int64 merge errors
        try:
            valor_minimo_df[join_key_valor] = valor_minimo_df[join_key_valor].astype(str)
            at03_df[join_key_at03] = at03_df[join_key_at03].astype(str)
        except Exception:
            pass
        merged_df = valor_minimo_df.merge(at03_df, left_on=join_key_valor, right_on=join_key_at03, how='inner')
        self.logger.info(f"Merged data resulted in {len(merged_df)} records")
        
        if merged_df.empty:
            self.logger.warning("No matching records found after JOIN. Skipping Phase 4 validation.")
            return df

        # Step 4: For each record, compare saldo (from AT03) with nuevo_at_valor_garantia (from VALOR_MINIMO)
        incidences = []
        updates_applied = 0
        
        def _to_num(x):
            try:
                s = str(x)
                if s == '' or s.lower() == 'nan' or s.lower() == 'none':
                    return 0.0
                # Normalize Spanish decimal: remove thousands '.', replace decimal ',' with '.'
                s = s.replace('.', '').replace(',', '.')
                return float(s)
            except Exception:
                try:
                    return float(x)
                except Exception:
                    return 0.0

        for idx, row in merged_df.iterrows():
            saldo = _to_num(row.get('saldo', 0))
            nuevo_valor_garantia = _to_num(row.get('nuevo_at_valor_garantia', 0))
            
            # Standardized incidence data
            incidence_data = {
                'id_cliente': row.get('id_cliente', ''),
                'numero_prestamo': row.get(join_key_valor, ''),
                'valor_garantia': nuevo_valor_garantia,
                'saldo_adeudado': saldo
            }
            
            if saldo > nuevo_valor_garantia:
                # Problem: Report and use original values
                self._add_incidence(
                    IncidenceType.VALIDATION_FAILURE,
                    IncidenceSeverity.HIGH,
                    'VALOR_MINIMO_AVALUO_AT12',
                    "El saldo del préstamo excede el valor de la garantía.",
                    incidence_data
                )

                # Legacy support for old incidence format
                incidences.append({
                    'at_num_de_prestamos': row.get(join_key_valor, ''),
                    'saldo_at03': saldo,
                    'nuevo_at_valor_garantia': nuevo_valor_garantia,
                    'comparison_result': 'PROBLEM',
                    'action': 'Using original values',
                    'reason': 'Saldo exceeds guarantee value',
                    'period': context.period
                })
            else:
                # Correct: Update main DataFrame
                mask = df['Numero_Prestamo'] == str(row.get(join_key_valor, ''))
                if mask.any():
                    if 'at_valor_garantia' in df.columns:
                        # Store back as string with comma decimal
                        df.loc[mask, 'at_valor_garantia'] = f"{nuevo_valor_garantia:.2f}".replace('.', ',')
                    if 'at_valor_pond_garantia' in df.columns:
                        vpond = _to_num(row.get('nuevo_at_valor_pond_garantia', 0))
                        df.loc[mask, 'at_valor_pond_garantia'] = f"{vpond:.2f}".replace('.', ',')
                    updates_applied += 1
                
                incidences.append({
                    'at_num_de_prestamos': row.get(join_key_valor, ''),
                    'saldo_at03': saldo,
                    'nuevo_at_valor_garantia': nuevo_valor_garantia,
                    'comparison_result': 'CORRECT',
                    'action': 'Using updated values (nuevo_at_valor_garantia, nuevo_at_valor_pond_garantia)',
                    'reason': 'Saldo is within guarantee value limits',
                    'period': context.period
                })

        # Store incidences
        if incidences:
            self._store_incidences('VALOR_MINIMO_AVALUO_VALIDATION', incidences, context)
            # Export only the PROBLEM rows from the original VALOR_MINIMO_AVALUO_AT12 file (full columns)
            try:
                problem_keys = [inc.get('at_num_de_prestamos') for inc in incidences if inc.get('comparison_result') == 'PROBLEM']
                if problem_keys:
                    mask_export = valor_minimo_df[join_key_valor].isin(problem_keys)
                    self._export_error_subset(valor_minimo_df, mask_export, 'VALOR_MINIMO_AVALUO_AT12', 'VALOR_MINIMO_AVALUO_VALIDATION_PROBLEM', context, None)
            except Exception as e:
                self.logger.warning(f"Failed exporting VALOR_MINIMO_AVALUO problems: {e}")
            
        problem_count = len([inc for inc in incidences if inc['comparison_result'] == 'PROBLEM'])
        correct_count = len([inc for inc in incidences if inc['comparison_result'] == 'CORRECT'])
        
        self.logger.info(f"Phase 4 completed: {problem_count} problems reported, {correct_count} records validated as correct, {updates_applied} updates applied")
        
        return df
    

    
    def _store_incidences(self, subtype: str, incidences: List[Dict], context: TransformationContext) -> None:
        """Store incidences for later reporting (legacy method for backward compatibility)."""
        if subtype not in self.incidences_data:
            self.incidences_data[subtype] = []
        
        self.incidences_data[subtype].extend(incidences)
        self.logger.info(f"Stored {len(incidences)} incidences for {subtype}")
    
    def _generate_outputs(self, context: TransformationContext, 
                         transformed_data: Dict[str, pd.DataFrame], 
                         result: TransformationResult) -> None:
        """Generate AT12 output files."""
        self.logger.info("Generating AT12 outputs")
        
        # Generate incidence files
        self._generate_incidence_files(context, result)
        
        # Generate processed Excel files
        self._generate_processed_files(context, transformed_data, result)
        
        # Generate consolidated TXT file
        self._generate_consolidated_file(context, transformed_data, result)
    
    def _generate_incidence_files(self, context: TransformationContext, result: TransformationResult) -> None:
        """Generate incidence CSV files.

        - Per-rule, full-row subsets (INC_*) ya se exportan durante cada regla.
        - EEOO_TABULAR solo para validaciones globales de base (whitelist).
        """
        # Allow global exports for FUERA_CIERRE_EXCEL_GENERATION and any EEOR_TABULAR* variant
        for rule_key, incidences in self.incidences_data.items():
            if not incidences:
                continue
            is_allowed = (rule_key == 'FUERA_CIERRE_EXCEL_GENERATION') or rule_key.startswith('EEOR_TABULAR')
            if not is_allowed:
                continue
            try:
                incidences_df = pd.DataFrame(incidences)
                # Simplified naming without INC_ prefix: [RULE]_[YYYYMMDD].csv
                incidence_filename = f"{rule_key}_{context.period}.csv"
                incidence_path = context.paths.incidencias_dir / incidence_filename
                if self._save_dataframe_as_csv(incidences_df, incidence_path):
                    result.incidence_files.append(incidence_path)
                    # Log concise: RULE -> filename (row count)
                    try:
                        self.logger.info(f"{rule_key} -> {incidence_path.name} ({len(incidences_df)} records)")
                    except Exception:
                        self.logger.info(f"{rule_key} -> {incidence_path} ({len(incidences_df)} records)")
            except Exception as e:
                self.logger.warning(f"Failed to export global incidence {rule_key}: {e}")
    
    def _generate_processed_files(self, context: TransformationContext, 
                                transformed_data: Dict[str, pd.DataFrame], 
                                result: TransformationResult) -> None:
        """Generate processed Excel files."""
        # Helper to get expected headers from schema file
        def _get_expected_headers(subtype: str) -> list:
            try:
                import json, os
                from pathlib import Path as _Path
                schemas_dir = getattr(context.config, 'schemas_dir', None)
                base_dir = getattr(context.config, 'base_dir', os.getcwd())
                root = _Path(schemas_dir) if schemas_dir else _Path(base_dir) / 'schemas'
                schema_file = root / 'AT12' / 'schema_headers.json'
                if schema_file.exists():
                    data = json.loads(schema_file.read_text(encoding='utf-8'))
                    if isinstance(data, dict) and subtype in data:
                        return list(data[subtype].keys())
            except Exception:
                pass
            return []

        for subtype, df in transformed_data.items():
            if df.empty:
                continue

            df_processed = df.copy()

            processed_filename = f"AT12_{subtype}_{context.period}.xlsx"
            processed_path = context.paths.get_procesado_path(processed_filename)

            try:
                drop_cols = [c for c in df_processed.columns if str(c).endswith('__num')]
                if drop_cols:
                    df_processed = df_processed.drop(columns=drop_cols)
            except Exception:
                pass

            try:
                expected = _get_expected_headers(subtype)
                if expected:
                    from src.core.header_mapping import HeaderMapper as _HM
                    df_processed = _HM.standardize_dataframe_to_schema(df_processed, subtype, expected)
            except Exception:
                pass

            try:
                df_processed = self._zero_out_valor_ponderado(df_processed)
            except Exception:
                pass

            try:
                df_processed = self._enforce_dot_decimal(df_processed)
            except Exception:
                pass

            try:
                df_processed = self._sanitize_output_whitespace(df_processed, subtype=subtype)
            except Exception as exc:
                result.errors.append(str(exc))
                raise

            if self._save_dataframe_as_excel(df_processed, processed_path, sheet_name=subtype):
                result.processed_files.append(processed_path)
                self.logger.info(f"Generated processed file: {processed_path}")
    
    def _generate_consolidated_file(self, context: TransformationContext, 
                                  transformed_data: Dict[str, pd.DataFrame], 
                                  result: TransformationResult) -> None:
        """Generate consolidated TXT files according to Stage 5 specifications.
        
        Creates separate TXT files for each DataFrame with specific delimiters:
        - BASE_AT12: pipe (|) delimiter
        - TDC_AT12, SOBREGIRO_AT12, VALORES_AT12: space ( ) delimiter
        All files are headerless and saved to consolidated/ directory.
        """
        try:
            consolidated_files = []
            
            # Define delimiter mapping according to Stage 5 specifications
            delimiter_mapping = {
                'AT12_BASE': '|',  # Pipe delimiter for BASE
                'BASE_AT12': '|',  # Alternative naming
                'AT12_TDC': ' ',   # Space delimiter for TDC
                'TDC_AT12': ' ',   # Alternative naming
                'AT12_SOBREGIRO': ' ',  # Space delimiter for SOBREGIRO
                'SOBREGIRO_AT12': ' ',  # Alternative naming
                'AT12_VALORES': ' ',    # Space delimiter for VALORES
                'VALORES_AT12': ' '     # Alternative naming
            }
            
            for subtype, df in transformed_data.items():
                if not df.empty:
                    # Get appropriate delimiter for this subtype
                    delimiter = delimiter_mapping.get(subtype, '|')  # Default to pipe
                    
                    # Prepare a copy for TXT output
                    out_df = df.copy()
                    # Drop internal numeric helper columns
                    try:
                        drop_cols = [c for c in out_df.columns if str(c).endswith('__num')]
                        if drop_cols:
                            out_df.drop(columns=drop_cols, inplace=True)
                    except Exception:
                        pass
                    # Standardize columns to schema order/names for TXT
                    try:
                        expected = self._get_expected_headers(context, subtype)
                        if expected:
                            from src.core.header_mapping import HeaderMapper as _HM
                            out_df = _HM.standardize_dataframe_to_schema(out_df, subtype, expected)
                            # Verify column count and order against schema
                            cols = list(out_df.columns)
                            if len(cols) != len(expected) or cols != expected:
                                msg = (
                                    f"Schema mismatch for {subtype}: expected {len(expected)} columns, got {len(cols)}; "
                                    f"order_ok={cols == expected}"
                                )
                                self.logger.error(msg)
                                # Record error and skip writing this TXT to avoid malformed output
                                if hasattr(result, 'errors'):
                                    result.errors.append(msg)
                                continue
                    except Exception:
                        pass
                    # Zero out Valor_Ponderado across all TXT outputs
                    try:
                        out_df = self._zero_out_valor_ponderado(out_df)
                    except Exception:
                        pass
                    # Ensure dot decimal in money fields for all TXT outputs
                    try:
                        out_df = self._enforce_dot_decimal(out_df)
                    except Exception:
                        pass

                    try:
                        out_df = self._sanitize_output_whitespace(out_df, subtype=subtype)
                    except Exception as e:
                        if hasattr(result, 'errors'):
                            result.errors.append(str(e))
                        raise

                    # Generate filename for this subtype
                    subtype_filename = self._filename_parser.generate_output_filename(
                        atom=subtype,
                        year=int(context.year),
                        month=int(context.month),
                        run_id=context.run_id,
                        extension="txt"
                    )
                    
                    consolidated_path = context.paths.get_consolidated_path(subtype_filename)
                    consolidated_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    # Write DataFrame to TXT file without header
                    with open(consolidated_path, 'w', encoding='utf-8') as f:
                        for _, row in out_df.iterrows():
                            # Convert all values to string and join with delimiter
                            record = delimiter.join(str(row[col]) for col in out_df.columns)
                            f.write(record + '\n')
                    
                    consolidated_files.append(consolidated_path)
                    self.logger.info(f"Generated {subtype} file: {consolidated_path} ({len(df)} records, delimiter='{delimiter}')")
            
            # Store all generated files in result
            if consolidated_files:
                if not hasattr(result, 'consolidated_files'):
                    result.consolidated_files = []
                result.consolidated_files.extend(consolidated_files)
                
                # Keep backward compatibility with single file attribute
                result.consolidated_file = consolidated_files[0] if consolidated_files else None
                
                self.logger.info(f"Generated {len(consolidated_files)} consolidated TXT files according to Stage 5 specifications")
            
        except Exception as e:
            error_msg = f"Failed to generate consolidated files: {str(e)}"
            result.errors.append(error_msg)
            self.logger.error(error_msg)
    
    # Stage 1 Correction Methods
    def _apply_eeor_tabular_cleaning(self, df: pd.DataFrame, context: TransformationContext, subtype: str = "") -> pd.DataFrame:
        """1.1. EEOR TABULAR: Whitespace Errors - Remove unnecessary spaces from text fields."""
        text_columns = df.select_dtypes(include=['object']).columns
        incidences = []
        # Preserve original state for export (only text columns)
        try:
            original_df = df[text_columns].copy()
        except Exception:
            original_df = df.copy()
        changed_cols: set = set()
        # Track modified rows across any text column
        try:
            overall_mask = pd.Series(False, index=df.index)
        except Exception:
            overall_mask = None
        
        for col in text_columns:
            if col in df.columns:
                # Store original values for comparison
                original_values = df[col].astype(str)
                # Remove leading and trailing spaces, replace multiple spaces with single space
                cleaned_values = original_values.str.strip().str.replace(r'\s+', ' ', regex=True)
                
                # Count modifications
                modified_mask = original_values != cleaned_values
                modified_count = modified_mask.sum()
                
                if modified_count > 0:
                    # Apply the cleaning
                    df[col] = cleaned_values
                    changed_cols.add(col)
                    if overall_mask is not None:
                        overall_mask = overall_mask | modified_mask
                    
                    # Record incidences for modified records using standardized format
                    for idx in df[modified_mask].index:
                        self._add_incidence(
                            incidence_type=IncidenceType.DATA_QUALITY,
                            severity=IncidenceSeverity.LOW,
                            rule_id='EEOR_TABULAR_WHITESPACE_CLEANING',
                            description=f'Whitespace cleaned in column {col}',
                            data={
                                'record_index': int(idx),
                                'column_name': col,
                                'original_value': str(original_values.iloc[idx]),
                                'corrected_value': str(cleaned_values.iloc[idx])
                            }
                        )
                        
                        # Also keep legacy format for backward compatibility
                        incidences.append({
                            'Index': idx,
                            'Column': col,
                            'Original_Value': original_values.iloc[idx],
                            'Cleaned_Value': cleaned_values.iloc[idx],
                            'Rule': 'EEOR_TABULAR_WHITESPACE_CLEANING'
                        })
        
        if incidences:
            # Store by rule name including subtype to avoid overwrites across subtypes
            rule_key = 'EEOR_TABULAR'
            if subtype:
                rule_key = f"EEOR_TABULAR_{subtype}"
            self._store_incidences(rule_key, incidences, context)
            self.logger.info(f"Applied EEOR TABULAR cleaning to {len(incidences)} records across {len(text_columns)} text columns")
            # Export full-row subset for rows modified by EEOR cleaning
            try:
                if overall_mask is not None and overall_mask.any():
                    # Build mapping of original values for changed columns only
                    original_columns = {c: original_df[c] for c in changed_cols if c in original_df.columns}
                    # Export per-subtype to avoid mixing across types
                    target_subtype = subtype or 'BASE_AT12'
                    self._export_error_subset(df, overall_mask, target_subtype, 'EEOR_TABULAR', context, None, original_columns=original_columns)
            except Exception:
                pass
        else:
            try:
                self.logger.info("EEOR_TABULAR: no changes detected")
            except Exception:
                pass
        
        return df
    
    def _apply_error_0301_correction(self, df: pd.DataFrame, context: TransformationContext, subtype: str = "", result: Optional[TransformationResult] = None) -> pd.DataFrame:
        """ERROR_0301: Cascading Id_Documento rules for Tipo_Garantia == '0301'.

        Implements RULE_0301_01 through RULE_0301_04 as a cascade. Only rows with
        Tipo_Garantia exactly '0301' are considered. Two exports are produced after
        processing: modified documents and incident documents.
        """
        if 'Tipo_Garantia' not in df.columns or 'Id_Documento' not in df.columns:
            try:
                self.logger.info("Skipping ERROR_0301: missing required columns 'Tipo_Garantia' or 'Id_Documento'")
            except Exception:
                pass
            return df

        tg_norm = self._normalize_tipo_garantia_series(df['Tipo_Garantia'])
        mask_0301 = tg_norm == '0301'

        # Track original Id_Documento for change detection
        try:
            orig_id_series = df['Id_Documento'].astype(str).copy()
        except Exception:
            orig_id_series = df['Id_Documento'].copy()

        # Collections for exports
        modified_rows: List[Dict[str, Any]] = []
        incident_rows: List[Dict[str, Any]] = []

        # Counters for logging/diagnostics
        candidates_count = int(mask_0301.sum()) if hasattr(mask_0301, 'sum') else 0
        c_mod = 0
        c_inc = 0
        c_ex_r1_len15_valid = 0
        c_ex_r2_701 = 0
        c_ex_r3_41_42 = 0
        c_ex_r4_len10_valid = 0
        c_no_action = 0

        # Helper to append a modified record snapshot (ensure error + transform columns)
        def _append_modified(idx: int, original: str, corrected: str, rule_label: str, tipo_error: str, transformacion: str):
            row = df.loc[idx].to_dict()
            row['Id_Documento_ORIGINAL'] = original
            row['Id_Documento'] = corrected
            row['Regla'] = rule_label
            row['tipo de error'] = tipo_error
            row['transformacion'] = transformacion
            modified_rows.append(row)

        # Helper to append an incident snapshot (Spanish error type)
        def _append_incident(idx: int, id_value: str, tipo_error: str, descripcion: Optional[str] = None):
            row = df.loc[idx].to_dict()
            row['Id_Documento'] = id_value
            row['Id_Documento_ORIGINAL'] = id_value
            row['tipo de error'] = tipo_error
            row['transformacion'] = 'Sin cambio'
            if descripcion:
                row['descripcion'] = descripcion
            incident_rows.append(row)

        # Helpers for right-based (derecha→izquierda) positional logic
        def _slice_from_right(s: str, start_pos_from_right: int, end_pos_from_right_inclusive: int) -> str:
            """Return substring using 1-based positions counted from the right.
            Example: positions 13-15 from right => _slice_from_right(s, 15, 13).
            The returned substring preserves the original left-to-right character order.
            """
            n = len(s)
            # Convert right-positions to left-based indices
            # start_pos_from_right is the farther one (larger number)
            left_start = max(0, n - start_pos_from_right)
            left_end_exclusive = max(0, min(n, n - (end_pos_from_right_inclusive - 1)))
            return s[left_start:left_end_exclusive]

        # Process cascade per matching row
        for idx in df[mask_0301].index.tolist():
            current = str(df.at[idx, 'Id_Documento']) if pd.notna(df.at[idx, 'Id_Documento']) else ''
            original = str(orig_id_series.iloc[idx]) if idx in orig_id_series.index else current

            # RULE_0301_01: Positions 13-15 and length handling
            clen = len(current)
            applied = False
            if clen >= 15:
                # Right-based: positions 13-15 from the right
                sub_13_15 = _slice_from_right(current, 15, 13)
                if sub_13_15 in {'100', '110', '120', '130', '810'}:
                    if clen == 15:
                        # Valid, exclude from further ERROR_0301 processing
                        applied = True
                        c_ex_r1_len15_valid += 1
                    elif clen > 15:
                        # Truncate to 15 keeping rightmost 15 (derecha→izquierda)
                        corrected = current[-15:]
                        df.at[idx, 'Id_Documento'] = corrected
                        _append_modified(
                            idx,
                            original,
                            corrected,
                            'Truncation by R1',
                            'Longitud mayor a 15 con posiciones 13-15 válidas',
                            'Truncado (der→izq) a 15'
                        )
                        applied = True
                        c_mod += 1
            if applied:
                continue  # Move to next document (stop cascade for this row)

            # RULE_0301_02: Exclusion by '701' sequence at positions 11-9 or 10-8 (from right)
            matched_701 = False
            if len(current) >= 11:
                if _slice_from_right(current, 11, 9) == '701':
                    matched_701 = True
            if not matched_701 and len(current) >= 10:
                if _slice_from_right(current, 10, 8) == '701':
                    matched_701 = True
                    if len(current) == 10:
                        # Report valid 701 at 10-8 with exact length 10 (follow-up record)
                        _append_incident(
                            idx,
                            current,
                            'Secuencia 701 en posiciones 10-8 con longitud 10',
                            'Válido por 701; esperado 11 dígitos en casos 701'
                        )
            if matched_701:
                c_ex_r2_701 += 1
                # Exclude from further ERROR_0301 processing
                continue

            # RULE_0301_03: Exclusion by positions 9-10 ('41' or '42') when length >= 10 (right-based)
            if len(current) >= 10:
                sub_9_10 = _slice_from_right(current, 10, 9)
                if sub_9_10 in {'41', '42'}:
                    c_ex_r3_41_42 += 1
                    continue

            # RULE_0301_04: Remaining docs with '01' in positions 9-10
            if len(current) >= 10:
                sub_9_10 = _slice_from_right(current, 10, 9)
            else:
                sub_9_10 = ''

            if sub_9_10 == '01':
                # Truncate for any length > 10 (R1 already consumed valid 15-length cases)
                if len(current) > 10:
                    # Truncate to 10 keeping rightmost 10
                    corrected = current[-10:]
                    df.at[idx, 'Id_Documento'] = corrected
                    _append_modified(
                        idx,
                        original,
                        corrected,
                        'Truncation by R4A',
                        'Longitud mayor a 10 con "01" en posiciones 9-10',
                        'Truncado (der→izq) a 10'
                    )
                    c_mod += 1
                elif len(current) < 10:
                    # Incident: do not modify
                    _append_incident(
                        idx,
                        current,
                        'Longitud menor a 10 con "01" en posiciones 9-10',
                        'Length less than 10 while expecting "01" at positions 9-10'
                    )
                    c_inc += 1
                else:
                    # Exactly 10 and '01' in positions 9-10: valid; exclude
                    c_ex_r4_len10_valid += 1
            # Else: no action for other cases (remain unchanged)
            else:
                c_no_action += 1

        # Exports for ERROR_0301
        try:
            period = context.period
            base_dir = context.paths.incidencias_dir
            base_dir.mkdir(parents=True, exist_ok=True)

            # Modified export
            if modified_rows:
                mod_df = pd.DataFrame(modified_rows)
                # Reorder columns so Id_Documento_ORIGINAL is adjacent to Id_Documento
                if 'Id_Documento' in mod_df.columns and 'Id_Documento_ORIGINAL' in mod_df.columns:
                    cols = list(mod_df.columns)
                    # Remove ORIGINAL and reinsert after Id_Documento
                    cols = [c for c in cols if c != 'Id_Documento_ORIGINAL']
                    try:
                        idx_pos = cols.index('Id_Documento')
                        cols = cols[:idx_pos+1] + ['Id_Documento_ORIGINAL'] + cols[idx_pos+1:]
                        mod_df = mod_df[cols]
                    except ValueError:
                        pass
                mod_filename = f"ERROR_0301_MODIFIED_{period}.csv"
                mod_path = base_dir / mod_filename
                mod_df.to_csv(
                    mod_path,
                    index=False,
                    encoding='utf-8',
                    sep=getattr(context.config, 'output_delimiter', '|'),
                    quoting=1,
                    date_format='%Y%m%d'
                )
                if result is not None and hasattr(result, 'incidence_files'):
                    result.incidence_files.append(mod_path)
                self.logger.info(f"ERROR_0301 -> {mod_path.name} ({len(mod_df)} records)")

            # Incidents export
            if incident_rows:
                inc_df = pd.DataFrame(incident_rows)
                # Reorder columns so Id_Documento_ORIGINAL is adjacent to Id_Documento
                if 'Id_Documento' in inc_df.columns and 'Id_Documento_ORIGINAL' in inc_df.columns:
                    cols = list(inc_df.columns)
                    cols = [c for c in cols if c != 'Id_Documento_ORIGINAL']
                    try:
                        idx_pos = cols.index('Id_Documento')
                        cols = cols[:idx_pos+1] + ['Id_Documento_ORIGINAL'] + cols[idx_pos+1:]
                        inc_df = inc_df[cols]
                    except ValueError:
                        pass
                inc_filename = f"ERROR_0301_INCIDENTES_{period}.csv"
                inc_path = base_dir / inc_filename
                inc_df.to_csv(
                    inc_path,
                    index=False,
                    encoding='utf-8',
                    sep=getattr(context.config, 'output_delimiter', '|'),
                    quoting=1,
                    date_format='%Y%m%d'
                )
                if result is not None and hasattr(result, 'incidence_files'):
                    result.incidence_files.append(inc_path)
                self.logger.info(f"ERROR_0301 -> {inc_path.name} ({len(inc_df)} records)")
        except Exception as e:
            self.logger.warning(f"ERROR_0301 exports failed: {e}")

        # Always log a summary so it's visible in transformation logs
        try:
            self.logger.info(
                "ERROR_0301 summary: candidates=%s, modified=%s, incidents=%s, "
                "excluded(R1_len15)=%s, excluded(R2_701)=%s, excluded(R3_41_42)=%s, "
                "excluded(R4_len10_01)=%s, unchanged=%s" % (
                    candidates_count, c_mod, c_inc, c_ex_r1_len15_valid, c_ex_r2_701,
                    c_ex_r3_41_42, c_ex_r4_len10_valid, c_no_action
                )
            )
        except Exception:
            pass

        return df
    
    def _apply_coma_finca_empresa_correction(self, df: pd.DataFrame, context: TransformationContext) -> pd.DataFrame:
        """1.3. COMA EN FINCA EMPRESA: Remove commas from Id_Documento."""
        if 'Id_Documento' not in df.columns:
            try:
                self.logger.info("Skipping COMA_EN_FINCA_EMPRESA: missing 'Id_Documento'")
            except Exception:
                pass
            return df
        
        # Find records with commas in Id_Documento
        mask_comma = df['Id_Documento'].astype(str).str.contains(',', na=False)
        incidences = []
        try:
            orig_series = pd.Series(index=df.index, dtype=object)
        except Exception:
            orig_series = None
        
        if mask_comma.sum() > 0:
            # Store original values for incidence reporting
            for idx in df[mask_comma].index:
                original_id = df.loc[idx, 'Id_Documento']
                corrected_id = str(original_id).replace(',', '')
                df.loc[idx, 'Id_Documento'] = corrected_id
                if orig_series is not None:
                    orig_series.loc[idx] = original_id
                
                incidences.append({
                    'Index': idx,
                    'Original_Id_Documento': original_id,
                    'Corrected_Id_Documento': corrected_id,
                    'Rule': 'COMA_EN_FINCA_EMPRESA'
                })
            
            self._store_incidences('COMA_FINCA_EMPRESA', incidences, context)
            self.logger.info(f"Removed commas from {len(incidences)} Id_Documento records")
            # Export subset of rows with this issue keeping original columns
            try:
                original_columns = {'Id_Documento': orig_series} if orig_series is not None else None
                self._export_error_subset(df, mask_comma, 'BASE_AT12', 'COMA_EN_FINCA_EMPRESA', context, None, original_columns=original_columns)
            except Exception:
                pass
        else:
            try:
                self.logger.info("COMA_EN_FINCA_EMPRESA: no records with comma in Id_Documento")
            except Exception:
                pass
        
        return df
    
    def _apply_fecha_cancelacion_correction(self, df: pd.DataFrame, context: TransformationContext) -> pd.DataFrame:
        """1.4. Fecha Cancelación Errada: Correct erroneous expiration dates."""
        if 'Fecha_Vencimiento' not in df.columns:
            try:
                self.logger.info("Skipping FECHA_CANCELACION_ERRADA: missing 'Fecha_Vencimiento'")
            except Exception:
                pass
            return df
        
        incidences = []
        
        # Convert to string and extract year
        fecha_str = df['Fecha_Vencimiento'].astype(str)
        
        # Find records with invalid years (> 2100 or < 1985)
        # Track original values for export
        try:
            orig_series = pd.Series(index=df.index, dtype=object)
        except Exception:
            orig_series = None

        for idx in df.index:
            fecha_val = str(df.loc[idx, 'Fecha_Vencimiento'])
            if len(fecha_val) >= 4:
                try:
                    year = int(fecha_val[:4])
                    if year > 2100 or year < 1985:
                        original_fecha = df.loc[idx, 'Fecha_Vencimiento']
                        df.loc[idx, 'Fecha_Vencimiento'] = '21001231'
                        if orig_series is not None:
                            orig_series.loc[idx] = original_fecha
                        
                        incidences.append({
                            'Index': idx,
                            'Original_Fecha_Vencimiento': original_fecha,
                            'Corrected_Fecha_Vencimiento': '21001231',
                            'Rule': 'FECHA_CANCELACION_ERRADA'
                        })
                except ValueError:
                    # Invalid date format, also correct
                    original_fecha = df.loc[idx, 'Fecha_Vencimiento']
                    df.loc[idx, 'Fecha_Vencimiento'] = '21001231'
                    if orig_series is not None:
                        orig_series.loc[idx] = original_fecha
                    
                    incidences.append({
                        'Index': idx,
                        'Original_Fecha_Vencimiento': original_fecha,
                        'Corrected_Fecha_Vencimiento': '21001231',
                        'Rule': 'FECHA_CANCELACION_ERRADA_FORMAT'
                    })
        
        if incidences:
            self._store_incidences('FECHA_CANCELACION_ERRADA', incidences, context)
            self.logger.info(f"Corrected {len(incidences)} erroneous Fecha_Vencimiento records")
            # Export subset rows that had this error
            try:
                idxs = [rec.get('Index') for rec in incidences if 'Index' in rec]
                if idxs:
                    mask = df.index.isin(idxs)
                    original_columns = {'Fecha_Vencimiento': orig_series} if orig_series is not None else None
                    self._export_error_subset(df, mask, 'BASE_AT12', 'FECHA_CANCELACION_ERRADA', context, None, original_columns=original_columns)
            except Exception:
                pass
        else:
            try:
                self.logger.info("FECHA_CANCELACION_ERRADA: no invalid Fecha_Vencimiento records")
            except Exception:
                pass
        
        return df
    
    def _apply_fecha_avaluo_correction(self, df: pd.DataFrame, context: TransformationContext, source_data: Dict[str, pd.DataFrame], subtype: str = "") -> pd.DataFrame:
        """1.5. Fecha Avalúo Errada: Correct inconsistent appraisal update dates.

        Scope restriction: applies only to Tipo_Garantia in {'0207','0208','0209'}.
        """
        if 'Fecha_Ultima_Actualizacion' not in df.columns or 'Numero_Prestamo' not in df.columns:
            return df
        
        if 'AT03_CREDITOS' not in source_data or source_data['AT03_CREDITOS'].empty:
            self.logger.warning("AT03_CREDITOS data not available for fecha avalúo correction")
            try:
                self.logger.info("Skipping FECHA_AVALUO_ERRADA: AT03_CREDITOS not available")
            except Exception:
                pass
            return df
        
        at03_df = source_data['AT03_CREDITOS']
        incidences = []
        candidates = 0
        corrected = 0
        no_match = 0
        
        # Get last day of previous month for comparison
        from datetime import datetime, timedelta
        import calendar
        
        try:
            # Assuming context has year and month
            current_year = int(context.year)
            current_month = int(context.month)
            # Cutoff is the last day of the processing month (not previous month)
            last_day_curr_month = calendar.monthrange(current_year, current_month)[1]
            cutoff_date = f"{current_year}{current_month:02d}{last_day_curr_month:02d}"
            
        except (AttributeError, ValueError):
            # Fallback if context doesn't have proper date info
            cutoff_date = "20991231"  # Lenient default cutoff at far-future end of year
        
        try:
            orig_series_avaluo = pd.Series(index=df.index, dtype=object)
        except Exception:
            orig_series_avaluo = None

        # Build normalized key map for AT03 for robust join
        try:
            at03_df = at03_df.copy()
            at03_df['_norm_key'] = self._normalize_join_key(at03_df['num_cta'])
            at03_map = at03_df.dropna(subset=['_norm_key'])
            at03_map = at03_map.drop_duplicates('_norm_key', keep='first').set_index('_norm_key')
        except Exception:
            at03_map = None

        base_norm_keys = self._normalize_join_key(df['Numero_Prestamo'])

        # Restrict to Tipo_Garantia 0207/0208/0209 only
        if 'Tipo_Garantia' not in df.columns:
            return df
        tg_norm = self._normalize_tipo_garantia_series(df['Tipo_Garantia'])
        allowed_tg = {'0207', '0208', '0209'}
        target_index = df.index[tg_norm.isin(allowed_tg)]

        for idx in target_index:
            fecha_val = str(df.loc[idx, 'Fecha_Ultima_Actualizacion'])
            numero_prestamo = df.loc[idx, 'Numero_Prestamo']
            needs_correction = False
            reason = ""
            
            # Check conditions for correction
            if len(fecha_val) == 8 and fecha_val.isdigit():
                if fecha_val > cutoff_date:
                    needs_correction = True
                    reason = "Date > last day of previous month"
                elif fecha_val[:4].isdigit() and int(fecha_val[:4]) < 1985:
                    needs_correction = True
                    reason = "Year < 1985"
            else:
                needs_correction = True
                reason = "Invalid YYYYMMDD format"
            
            if needs_correction:
                candidates += 1
                # JOIN with AT03_CREDITOS using normalized key; fallback to exact match
                new_fecha = None
                matched = False
                if at03_map is not None:
                    key_norm = base_norm_keys.iloc[idx]
                    if isinstance(key_norm, str) and key_norm in at03_map.index:
                        try:
                            new_fecha = at03_map.at[key_norm, 'fec_ini_prestamo']
                            matched = True
                        except Exception:
                            matched = False
                if not matched:
                    at03_match = at03_df[at03_df['num_cta'] == numero_prestamo]
                    if not at03_match.empty and 'fec_ini_prestamo' in at03_match.columns:
                        new_fecha = at03_match.iloc[0]['fec_ini_prestamo']
                        matched = True

                if matched:
                    original_fecha = df.loc[idx, 'Fecha_Ultima_Actualizacion']
                    df.loc[idx, 'Fecha_Ultima_Actualizacion'] = new_fecha
                    if orig_series_avaluo is not None:
                        orig_series_avaluo.loc[idx] = original_fecha
                    
                    incidences.append({
                        'Index': idx,
                        'Numero_Prestamo': numero_prestamo,
                        'Original_Fecha_Ultima_Actualizacion': original_fecha,
                        'Corrected_Fecha_Ultima_Actualizacion': new_fecha,
                        'Reason': reason,
                        'Rule': 'FECHA_AVALUO_ERRADA'
                    })
                    corrected += 1
                else:
                    no_match += 1
        
        if incidences:
            self._store_incidences('FECHA_AVALUO_ERRADA', incidences, context)
            self.logger.info(f"Corrected {len(incidences)} Fecha_Ultima_Actualizacion records (candidates={candidates}, corrected={corrected}, no_match={no_match})")
            # Export subset rows that had this issue
            try:
                idxs = [rec.get('Index') for rec in incidences if 'Index' in rec]
                if idxs:
                    mask_export = df.index.isin(idxs)
                    original_columns = {'Fecha_Ultima_Actualizacion': orig_series_avaluo} if orig_series_avaluo is not None else None
                    self._export_error_subset(df, mask_export, subtype or 'BASE_AT12', 'FECHA_AVALUO_ERRADA', context, None, original_columns=original_columns)
            except Exception:
                pass
        
        return df

    def _apply_id_documento_padding(self, df: pd.DataFrame, context: TransformationContext, subtype: str = "") -> pd.DataFrame:
        """1.X. ID_DOCUMENTO_PADDING: Relleno a 10 dígitos solo para BASE.

        Regla:
        - Si `Id_Documento` es dígitos puros y su longitud es menor a 10, aplicar zfill(10).
        - Si longitud >= 10, no cambiar.
        - Si contiene caracteres no numéricos (p.ej., '/'), no cambiar.

        Esta regla debe correr al final de la cascada de Stage 1 para BASE.
        """
        if df is None or df.empty:
            return df
        # Solo para BASE
        if subtype and str(subtype).upper() not in {"BASE_AT12", "BASE", "AT12_BASE"}:
            return df
        if 'Id_Documento' not in df.columns:
            return df

        df = df.copy()
        try:
            original_series = df['Id_Documento'].astype(str)
        except Exception:
            original_series = df['Id_Documento']

        token = original_series.astype(str).str.strip()
        mask_digit = token.str.fullmatch(r'\d+')
        mask_short = token.str.len() < 10
        mask = mask_digit & mask_short

        if mask.any():
            # Apply padding
            df.loc[mask, 'Id_Documento'] = token.loc[mask].str.zfill(10)
            # Export incidences with original values adjacent
            try:
                original_columns = {'Id_Documento': original_series}
                self._export_error_subset(
                    df,
                    mask,
                    'BASE_AT12',
                    'ID_DOCUMENTO_PADDING',
                    context,
                    None,
                    original_columns=original_columns
                )
            except Exception:
                pass
            try:
                self.logger.info(f"ID_DOCUMENTO_PADDING: applied to {int(mask.sum())} record(s)")
            except Exception:
                pass
        else:
            try:
                self.logger.info("ID_DOCUMENTO_PADDING: no candidates found")
            except Exception:
                pass

        return df

    def _exclude_sobregiros_from_base(self, df: pd.DataFrame, context: TransformationContext, source_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """3.1. Excluir sobregiros del BASE por cruce con AT03 y Tipo_Facilidad='02'.

        Regla:
        - JOIN por `Numero_Prestamo` (normalizado dígitos-solo sin ceros a la izquierda) vs `AT03_CREDITOS.num_cta` (normalizado).
        - Eliminar filas de BASE donde `Tipo_Facilidad` == '02' y el préstamo está presente en AT03.
        - Exportar subset eliminado a `EXCLUDE_SOBREGIROS_BASE_<YYYYMMDD>.csv`.
        """
        if df is None or df.empty:
            return df
        if 'Numero_Prestamo' not in df.columns:
            try:
                self.logger.info("EXCLUDE_SOBREGIROS_FROM_BASE: Numero_Prestamo not found; skipping")
            except Exception:
                pass
            return df
        if 'Tipo_Facilidad' not in df.columns:
            try:
                self.logger.info("EXCLUDE_SOBREGIROS_FROM_BASE: Tipo_Facilidad not found in BASE; skipping")
            except Exception:
                pass
            return df
        if 'AT03_CREDITOS' not in source_data or source_data['AT03_CREDITOS'].empty:
            try:
                self.logger.info("EXCLUDE_SOBREGIROS_FROM_BASE: AT03_CREDITOS not available; skipping")
            except Exception:
                pass
            return df

        at03 = source_data['AT03_CREDITOS']
        if 'num_cta' not in at03.columns:
            try:
                self.logger.info("EXCLUDE_SOBREGIROS_FROM_BASE: num_cta missing in AT03_CREDITOS; skipping")
            except Exception:
                pass
            return df

        left_keys = self._normalize_join_key(df['Numero_Prestamo'])
        right_keys = self._normalize_join_key(at03['num_cta'])
        present_set = set(right_keys.dropna().astype(str))
        present_mask = left_keys.astype(str).isin(present_set)
        tipo_mask = df['Tipo_Facilidad'].astype(str).str.strip().eq('02')
        remove_mask = present_mask & tipo_mask

        count = int(remove_mask.sum()) if hasattr(remove_mask, 'sum') else 0
        if count:
            try:
                self.logger.info(f"EXCLUDE_SOBREGIROS_FROM_BASE: removing {count} record(s) from BASE")
            except Exception:
                pass
            # Export removed subset before filtering
            try:
                self._export_error_subset(df, remove_mask, 'BASE_AT12', 'EXCLUDE_SOBREGIROS_BASE', context, None, original_columns=None)
            except Exception:
                pass
            df = df[~remove_mask].copy()
        else:
            try:
                self.logger.info("EXCLUDE_SOBREGIROS_FROM_BASE: no candidates to remove")
            except Exception:
                pass
        return df

    def _apply_codigo_fiduciaria_update(self, df: pd.DataFrame, context: TransformationContext, subtype: str = "") -> pd.DataFrame:
        """3.3. Reemplazar código de fiduciaria obsoleto 508 -> 528 (solo BASE).

        - Columnas objetivo: 'Nombre_fiduciaria' si existe.
        - Exporta incidencias con valor original adyacente.
        """
        if df is None or df.empty:
            return df
        if subtype and str(subtype).upper() not in {"BASE_AT12", "BASE", "AT12_BASE"}:
            return df

        target_cols = [col for col in ('Nombre_fiduciaria', 'Nombre_Fiduciaria') if col in df.columns]
        if not target_cols:
            return df

        df = df.copy()
        combined_mask = pd.Series(False, index=df.index)
        original_columns: Dict[str, pd.Series] = {}

        for col in target_cols:
            series = df[col].astype(str).str.strip()
            mask = series.eq('508')
            if mask.any():
                try:
                    original_columns[col] = df[col].copy()
                except Exception:
                    pass
                df.loc[mask, col] = '528'
                combined_mask = combined_mask | mask

        if combined_mask.any():
            try:
                export_originals = original_columns if original_columns else None
                self._export_error_subset(df, combined_mask, 'BASE_AT12', 'FIDUCIARIA_CODE_UPDATE', context, None, original_columns=export_originals)
            except Exception:
                pass
            try:
                self.logger.info(f"FIDUCIARIA_CODE_UPDATE: updated {int(combined_mask.sum())} record(s)")
            except Exception:
                pass
        return df

    def _apply_contrato_privado_na(self, df: pd.DataFrame, context: TransformationContext, subtype: str = "") -> pd.DataFrame:
        """3.5. Setear Nombre_Organismo='NA' para registros 'Contrato Privado' (solo BASE).

        Busca 'Contrato Privado' (case-insensitive) en columnas candidatas y asigna 'NA'.
        Exporta incidencias con valor original de Nombre_Organismo.
        """
        if df is None or df.empty:
            return df
        if subtype and str(subtype).upper() not in {"BASE_AT12", "BASE", "AT12_BASE"}:
            return df

        # Consider common header variants for Id_Documento and Garantía descriptions
        candidates = [
            'Id_Documento', 'id_Documento', 'id_documento', 'ID_DOCUMENTO',
            'Tipo_Poliza',
            'Descripción de la Garantía', 'Descripcion de la Garantia',
            'Descripcion_de_la_Garantia', 'Descripción_de_la_Garantía'
        ]
        available = [c for c in candidates if c in df.columns]
        if not available:
            return df
        # Ensure target column exists
        if 'Nombre_Organismo' not in df.columns:
            df['Nombre_Organismo'] = ''
        contains_cp = None
        for c in available:
            # Normalize unicode and whitespace to catch NBSP and spacing variants
            series = df[c].astype(str)
            try:
                series = series.str.normalize('NFKC')
            except Exception:
                pass
            series = (
                series
                .str.replace('\u00A0', ' ', regex=False)  # NBSP to space
                .str.replace(r"\s+", " ", regex=True)   # collapse whitespace
                .str.strip()
            )
            # Flexible match: allow variable whitespace between words
            col_mask = series.str.contains(r'contrato\s*privado', case=False, na=False, regex=True)
            contains_cp = col_mask if contains_cp is None else (contains_cp | col_mask)
        if contains_cp is not None and contains_cp.any():
            try:
                original_columns = {'Nombre_Organismo': df['Nombre_Organismo'].copy()}
            except Exception:
                original_columns = None
            df.loc[contains_cp, 'Nombre_Organismo'] = 'NA'
            try:
                self._export_error_subset(df, contains_cp, 'BASE_AT12', 'CONTRATO_PRIVADO_NA', context, None, original_columns=original_columns)
            except Exception:
                pass
            try:
                self.logger.info(f"CONTRATO_PRIVADO_NA: applied to {int(contains_cp.sum())} record(s)")
            except Exception:
                pass
        return df
    
    def _apply_inmuebles_sin_poliza_correction(self, df: pd.DataFrame, context: TransformationContext, source_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """1.6. Inmuebles sin Póliza (Tipo_Poliza) alineado a especificación.

        - Caso 0207: JOIN con POLIZA_HIPOTECAS_AT12 por Numero_Prestamo = numcred; si `seguro_incendio` tiene valor,
          setear `Tipo_Poliza` a '01' o '02'.
        - Caso 0208: si `Tipo_Poliza` vacío, setear a '01'.
        """
        # Requisitos mínimos de columnas en la base
        if 'Tipo_Garantia' not in df.columns or 'Tipo_Poliza' not in df.columns or 'Numero_Prestamo' not in df.columns:
            try:
                self.logger.info("Skipping INMUEBLES_SIN_TIPO_POLIZA: missing required base columns")
            except Exception:
                pass
            return df

        df = df.copy()
        incidences = []
        try:
            orig_tipo_poliza = pd.Series(index=df.index, dtype=object)
        except Exception:
            orig_tipo_poliza = None

        # Normalizar vacío de Tipo_Poliza
        # Considera como vacío: NA/N/A (string), blancos y NaN
        _tp = df['Tipo_Poliza'].astype(str).str.strip()
        is_empty_tipo_poliza = (
            df['Tipo_Poliza'].isna()
            | (_tp == '')
            | (_tp.str.upper().isin(['NA', 'N/A']))
        )

        # Caso 0208: constante '01' cuando está vacío
        mask_0208 = (df['Tipo_Garantia'].astype(str) == '0208') & is_empty_tipo_poliza
        if mask_0208.any():
            for idx in df[mask_0208].index:
                original = df.loc[idx, 'Tipo_Poliza']
                df.loc[idx, 'Tipo_Poliza'] = '01'
                if orig_tipo_poliza is not None:
                    orig_tipo_poliza.loc[idx] = original
                incidences.append({
                    'Index': int(idx),
                    'Tipo_Garantia': '0208',
                    'Original_Tipo_Poliza': original,
                    'Corrected_Tipo_Poliza': '01',
                    'Rule': 'INMUEBLES_SIN_TIPO_POLIZA_0208_CONST'
                })
            try:
                total_cand_0208 = int(((df['Tipo_Garantia'].astype(str) == '0208') & is_empty_tipo_poliza).sum())
                self.logger.info(f"INMUEBLES_SIN_TIPO_POLIZA: 0208 candidates={total_cand_0208}, assigned={int(mask_0208.sum())}")
            except Exception:
                pass

        # Caso 0207: JOIN con POLIZA_HIPOTECAS_AT12 si disponible
        mask_0207 = (df['Tipo_Garantia'].astype(str) == '0207') & is_empty_tipo_poliza
        if mask_0207.any():
            if 'POLIZA_HIPOTECAS_AT12' not in source_data or source_data['POLIZA_HIPOTECAS_AT12'].empty:
                self.logger.warning("POLIZA_HIPOTECAS_AT12 data not available for 0207 Tipo_Poliza correction")
                try:
                    self.logger.info("Skipping INMUEBLES_SIN_TIPO_POLIZA_0207: POLIZA_HIPOTECAS_AT12 not available")
                except Exception:
                    pass
            else:
                hip_df = source_data['POLIZA_HIPOTECAS_AT12']
                # Columnas esperadas: numcred, seguro_incendio
                if 'numcred' in hip_df.columns and 'seguro_incendio' in hip_df.columns:
                    # Preparar mapa numcred(normalizado) -> seguro_incendio
                    hip_df = hip_df.copy()
                    hip_df['_norm_key'] = self._normalize_join_key(hip_df['numcred'])
                    map_seguro = hip_df.set_index('_norm_key')['seguro_incendio']
                    base_norm = self._normalize_join_key(df['Numero_Prestamo'])
                    _assigned_0207 = 0
                    _no_match_0207 = 0
                    _invalid_0207 = 0
                    for idx in df[mask_0207].index:
                        key = base_norm.loc[idx]
                        if pd.isna(key):
                            continue
                        val = map_seguro.get(key, None)
                        if pd.notna(val) and str(val).strip() != '':
                            # Solo valores '01' o '02' per especificación; si otro, ignorar
                            normalized = str(val).strip()
                            if normalized in {'01', '02'}:
                                original = df.loc[idx, 'Tipo_Poliza']
                                df.loc[idx, 'Tipo_Poliza'] = normalized
                                if orig_tipo_poliza is not None:
                                    orig_tipo_poliza.loc[idx] = original
                                incidences.append({
                                    'Index': int(idx),
                                    'Tipo_Garantia': '0207',
                                    'Numero_Prestamo': str(key),
                                    'Original_Tipo_Poliza': original,
                                    'Corrected_Tipo_Poliza': normalized,
                                    'Rule': 'INMUEBLES_SIN_TIPO_POLIZA_0207_JOIN_HIPOTECAS'
                                })
                                _assigned_0207 += 1
                            else:
                                _invalid_0207 += 1
                        else:
                            _no_match_0207 += 1
                    try:
                        total_cand_0207 = int(mask_0207.sum())
                        self.logger.info(
                            f"INMUEBLES_SIN_TIPO_POLIZA: 0207 candidates={total_cand_0207}, assigned={_assigned_0207}, no_match={_no_match_0207}, invalid_seguro={_invalid_0207}"
                        )
                    except Exception:
                        pass

        if incidences:
            self._store_incidences('INMUEBLES_SIN_TIPO_POLIZA', incidences, context)
            try:
                mask_export = mask_0208 | mask_0207
                original_columns = {'Tipo_Poliza': orig_tipo_poliza} if orig_tipo_poliza is not None else None
                self._export_error_subset(df, mask_export, 'BASE_AT12', 'INMUEBLES_SIN_TIPO_POLIZA', context, None, original_columns=original_columns)
            except Exception:
                pass
        else:
            try:
                self.logger.info("INMUEBLES_SIN_TIPO_POLIZA: no assignments (0208/0207)")
            except Exception:
                pass
        return df
    
    def _apply_inmuebles_sin_finca_correction(self, df: pd.DataFrame, context: TransformationContext) -> pd.DataFrame:
        """1.7. Inmuebles sin Finca: Normaliza Id_Documento a '99999/99999' según especificación.

        Aplica cuando Tipo_Garantia in {'0207','0208','0209'} y `Id_Documento` está vacío o en
        {"0/0", "1/0", "1/1", "1", "9999/1", "0/1", "0"}.
        """
        if 'Tipo_Garantia' not in df.columns or 'Id_Documento' not in df.columns:
            try:
                self.logger.info("Skipping INMUEBLES_SIN_FINCA: missing 'Tipo_Garantia' or 'Id_Documento'")
            except Exception:
                pass
            return df

        df = df.copy()
        invalid_values = {"0/0", "1/0", "1/1", "1", "9999/1", "0/1", "0"}

        tg_norm = self._normalize_tipo_garantia_series(df['Tipo_Garantia'])
        idoc = df['Id_Documento'].astype(str)
        is_invalid = self._is_empty_like(idoc) | (idoc.str.strip().isin(invalid_values))
        mask = tg_norm.isin({'0207', '0208', '0209'}) & is_invalid

        incidences = []
        try:
            orig_idoc = pd.Series(index=df.index, dtype=object)
        except Exception:
            orig_idoc = None
        if mask.any():
            for idx in df[mask].index:
                original = df.loc[idx, 'Id_Documento']
                df.loc[idx, 'Id_Documento'] = '99999/99999'
                if orig_idoc is not None:
                    orig_idoc.loc[idx] = original
                incidences.append({
                    'Index': int(idx),
                    'Tipo_Garantia': df.loc[idx, 'Tipo_Garantia'],
                    'Original_Id_Documento': original,
                    'Corrected_Id_Documento': '99999/99999',
                    'Rule': 'INMUEBLES_SIN_FINCA_NORMALIZACION'
                })

            self._store_incidences('INMUEBLES_SIN_FINCA', incidences, context)
            try:
                original_columns = {'Id_Documento': orig_idoc} if orig_idoc is not None else None
                self._export_error_subset(df, mask, 'BASE_AT12', 'INMUEBLES_SIN_FINCA', context, None, original_columns=original_columns)
            except Exception:
                pass
        else:
            try:
                self.logger.info("INMUEBLES_SIN_FINCA: no candidates in 0207/0208/0209 or all valid")
            except Exception:
                pass
        return df
    
    def _apply_poliza_auto_comercial_correction(self, df: pd.DataFrame, context: TransformationContext) -> pd.DataFrame:
        """1.8. Póliza Auto Comercial: Asignar Nombre_Organismo='700' cuando
        Tipo_Garantia in {'0101','0102','0103','0106','0108'} y Nombre_Organismo vacío.

        Nota: Comparación no destructiva; los valores de Tipo_Garantia no se alteran.
        """
        if 'Tipo_Garantia' not in df.columns or 'Nombre_Organismo' not in df.columns:
            try:
                self.logger.info("Skipping AUTO_COMERCIAL_ORG_CODE: missing 'Tipo_Garantia' or 'Nombre_Organismo'")
            except Exception:
                pass
            return df

        df = df.copy()
        tg_norm = self._normalize_tipo_garantia_series(df['Tipo_Garantia'])
        nom = df['Nombre_Organismo']
        is_empty_nom = self._is_empty_like(nom)
        valid_tg = {'0101', '0102', '0103', '0106', '0108'}
        mask = tg_norm.isin(valid_tg) & is_empty_nom

        incidences = []
        try:
            orig_nom = pd.Series(index=df.index, dtype=object)
        except Exception:
            orig_nom = None
        if mask.any():
            for idx in df[mask].index:
                original_nom = df.loc[idx, 'Nombre_Organismo']
                df.loc[idx, 'Nombre_Organismo'] = '700'
                if orig_nom is not None:
                    orig_nom.loc[idx] = original_nom
                incidences.append({
                    'Index': int(idx),
                    'Tipo_Garantia': '0106',
                    'Original_Nombre_Organismo': original_nom,
                    'Corrected_Nombre_Organismo': '700',
                    'Rule': 'AUTO_COMERCIAL_ORG_CODE'
                })
            self._store_incidences('AUTO_COMERCIAL_ORG_CODE', incidences, context)
            try:
                original_columns = {'Nombre_Organismo': orig_nom} if orig_nom is not None else None
                self._export_error_subset(df, mask, 'BASE_AT12', 'AUTO_COMERCIAL_ORG_CODE', context, None, original_columns=original_columns)
            except Exception:
                pass
        else:
            try:
                self.logger.info("AUTO_COMERCIAL_ORG_CODE: no empty Nombre_Organismo for 0106")
            except Exception:
                pass
        return df
    
    def _apply_error_poliza_auto_correction(self, df: pd.DataFrame, context: TransformationContext, source_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """1.9. Auto Policy Error (Rule 9).

        - Scope: Tipo_Garantia in {'0101','0103'}.
        - Join: Numero_Prestamo (BASE_AT12) = numcred (GARANTIA_AUTOS_AT12) with normalized keys (digits-only, no leading zeros).
        - Updates on successful match:
            * Id_Documento ← num_poliza (only when the base value is empty).
            * Importe and Valor_Garantia ← policy amount (`monto_asegurado` preferred).
            * Fecha_Última_Actualización ← policy Fecha_inicio.
            * Fecha_Vencimiento ← policy Fecha_Vencimiento.
        - Default handling: if no policy is found and Id_Documento was empty, assign '01'.
        - Additional normalization: when Tipo_Poliza resolves to 'NA', coerce to '01'.
        """
        if 'Tipo_Garantia' not in df.columns or 'Id_Documento' not in df.columns or 'Numero_Prestamo' not in df.columns:
            try:
                self.logger.info("Skipping AUTO_NUM_POLIZA_FROM_GARANTIA_AUTOS: missing required base columns")
            except Exception:
                pass
            return df

        df = df.copy()
        tg_norm = self._normalize_tipo_garantia_series(df['Tipo_Garantia'])
        scope_mask = tg_norm.isin({'0101', '0103'})
        if not scope_mask.any():
            return df

        try:
            id_doc_series = df['Id_Documento'].astype(str)
            id_doc_trim = id_doc_series.str.strip()
        except Exception:
            id_doc_series = df['Id_Documento']
            id_doc_trim = id_doc_series

        empty_id_mask = df['Id_Documento'].isna() | id_doc_trim.eq('')

        incidences: List[Dict[str, Any]] = []
        try:
            orig_idoc = pd.Series(index=df.index, dtype=object)
        except Exception:
            orig_idoc = None
        updated_indices: List[int] = []

        autos_df = None
        if 'GARANTIA_AUTOS_AT12' in source_data and not getattr(source_data['GARANTIA_AUTOS_AT12'], 'empty', True):
            autos_df = source_data['GARANTIA_AUTOS_AT12']
        else:
            try:
                from pathlib import Path as _Path
                data_raw = _Path(getattr(context.config, 'data_raw_dir', 'data/raw'))
                run = context.run_id
                candidates = sorted(list(data_raw.glob(f"GARANTIA_AUTOS_AT12_*__run-{run}.csv")))
                if not candidates:
                    candidates = sorted(list(data_raw.glob("GARANTIA_AUTOS_AT12_*__run-*.csv")))
                if not candidates:
                    source_dir = _Path(getattr(context.config, 'source_dir', 'source'))
                    src_cand = sorted(list(source_dir.glob("GARANTIA_AUTOS_AT12_*.*")))
                    if src_cand:
                        candidates = [src_cand[-1]]
                if candidates:
                    cand = candidates[-1]
                    try:
                        autos_df = self._file_reader.read_file(cand)
                    except Exception:
                        import pandas as _pd
                        autos_df = _pd.read_csv(cand, dtype=str, keep_default_na=False)
                    self.logger.info(f"AUTO_POLICY fallback loaded {cand.name} with {len(autos_df)} records")
            except Exception as e:
                self.logger.warning(f"AUTO_POLICY fallback load failed: {e}")

        if autos_df is None or getattr(autos_df, 'empty', True):
            self.logger.warning("GARANTIA_AUTOS_AT12 data not available for auto policy enrichment")
            try:
                self.logger.info("Skipping AUTO_NUM_POLIZA_FROM_GARANTIA_AUTOS: GARANTIA_AUTOS_AT12 not available")
            except Exception:
                pass
            return df

        if 'numcred' not in autos_df.columns or 'num_poliza' not in autos_df.columns:
            return df

        autos_df = autos_df.copy()
        autos_df['_norm_key'] = self._normalize_join_key(autos_df['numcred'])
        map_poliza = autos_df.set_index('_norm_key')['num_poliza']

        amount_candidates = [
            'monto_asegurado', 'Monto_Asegurado', 'MONTO_ASEGURADO',
            'importe', 'Importe', 'valor_poliza', 'Valor_Poliza',
            'monto', 'Monto', 'Valor_Garantia', 'Valor_Garantía'
        ]
        amount_col = next((c for c in amount_candidates if c in autos_df.columns), None)
        map_amount = autos_df.set_index('_norm_key')[amount_col] if amount_col else None

        start_col = next((c for c in ['fec_ini_cob', 'fec_ini_co', 'FEC_INI_COB', 'FEC_INI_CO', 'Fecha_inicio', 'fecha_inicio', 'Fecha_Inicio'] if c in autos_df.columns), None)
        end_col = next((c for c in ['fec_fin_cobe', 'fec_fin_co', 'FEC_FIN_COBE', 'FEC_FIN_CO', 'Fecha_Vencimiento', 'fecha_vencimiento', 'Fecha_vencimiento'] if c in autos_df.columns), None)
        map_start = autos_df.set_index('_norm_key')[start_col] if start_col else None
        map_end = autos_df.set_index('_norm_key')[end_col] if end_col else None

        exclusion_col = next((c for c in ['monto_asegurado', 'Monto_Asegurado', 'MONTO_ASEGURADO'] if c in autos_df.columns), None)
        map_excl = autos_df.set_index('_norm_key')[exclusion_col] if exclusion_col else None
        excl_tokens = {'NUEVO DESEMBOLSO', 'PERDIDA TOTAL', 'FALLECIDO'}

        base_norm = self._normalize_join_key(df['Numero_Prestamo'])

        try:
            orig_importe = pd.Series(index=df.index, dtype=object)
            orig_val_gar = pd.Series(index=df.index, dtype=object)
            orig_last_upd = pd.Series(index=df.index, dtype=object)
            orig_venc = pd.Series(index=df.index, dtype=object)
            orig_tipo_poliza = pd.Series(index=df.index, dtype=object)
        except Exception:
            orig_importe = orig_val_gar = orig_last_upd = orig_venc = orig_tipo_poliza = None

        diag_rows: List[Dict[str, Any]] = []
        matched = 0
        applied_count = 0

        for idx in df[scope_mask].index:
            key = base_norm.loc[idx]
            if pd.isna(key):
                continue

            original_id = df.loc[idx, 'Id_Documento']
            has_existing_id = not empty_id_mask.loc[idx]
            tipo_poliza_before = df.loc[idx, 'Tipo_Poliza'] if 'Tipo_Poliza' in df.columns else None

            updates: Dict[str, Any] = {}
            reason = 'NOT_FOUND'
            applied = False

            val = map_poliza.get(key, None) if map_poliza is not None else None
            sval = str(val).strip() if (val is not None and pd.notna(val)) else ''
            if pd.notna(val):
                matched += 1

            excl_case = False
            try:
                if map_excl is not None:
                    exv = map_excl.get(key, None)
                    if exv is not None:
                        exu = str(exv).strip().upper()
                        if exu in excl_tokens:
                            excl_case = True
            except Exception:
                pass

            if sval:
                reason = 'APPLIED'
                if not has_existing_id:
                    if orig_idoc is not None:
                        orig_idoc.loc[idx] = original_id
                    df.loc[idx, 'Id_Documento'] = sval
                    updates['Id_Documento'] = sval
                else:
                    reason = 'APPLIED_EXISTING_ID'
                applied = True
                applied_count += 1

                if (not excl_case) and (map_amount is not None):
                    aval = map_amount.get(key, None)
                    aval_str = str(aval).strip() if pd.notna(aval) else ''
                    if aval_str != '':
                        if 'Importe' in df.columns:
                            if orig_importe is not None:
                                orig_importe.loc[idx] = df.loc[idx, 'Importe']
                            df.loc[idx, 'Importe'] = aval_str
                            updates['Importe'] = aval_str
                        for tgt in ['Valor_Garantía', 'Valor_Garantia']:
                            if tgt in df.columns:
                                if orig_val_gar is not None:
                                    orig_val_gar.loc[idx] = df.loc[idx, tgt]
                                df.loc[idx, tgt] = aval_str
                                updates[tgt] = aval_str

                if map_start is not None:
                    dval = map_start.get(key, None)
                    dval_str = str(dval).strip() if pd.notna(dval) else ''
                    if dval_str != '':
                        for tgt in ['Fecha_Última_Actualización', 'Fecha_Ultima_Actualizacion']:
                            if tgt in df.columns:
                                if orig_last_upd is not None:
                                    orig_last_upd.loc[idx] = df.loc[idx, tgt]
                                df.loc[idx, tgt] = dval_str
                                updates[tgt] = dval_str

                if map_end is not None and 'Fecha_Vencimiento' in df.columns:
                    dval2 = map_end.get(key, None)
                    dval2_str = str(dval2).strip() if pd.notna(dval2) else ''
                    if dval2_str != '':
                        if orig_venc is not None:
                            orig_venc.loc[idx] = df.loc[idx, 'Fecha_Vencimiento']
                        df.loc[idx, 'Fecha_Vencimiento'] = dval2_str
                        updates['Fecha_Vencimiento'] = dval2_str
            else:
                if not has_existing_id:
                    reason = 'DEFAULT_NO_POLICY'
                    if orig_idoc is not None:
                        orig_idoc.loc[idx] = original_id
                    df.loc[idx, 'Id_Documento'] = '01'
                    updates['Id_Documento'] = '01'
                    applied = True
                else:
                    reason = 'POLICY_MISSING_EXISTING_ID'

            if 'Tipo_Poliza' in df.columns:
                current_tp = str(df.loc[idx, 'Tipo_Poliza']).strip()
                if current_tp.upper() == 'NA':
                    if orig_tipo_poliza is not None and tipo_poliza_before is not None:
                        orig_tipo_poliza.loc[idx] = tipo_poliza_before
                    df.loc[idx, 'Tipo_Poliza'] = '01'
                    updates['Tipo_Poliza'] = '01'
                    applied = True

            if updates:
                updated_indices.append(idx)
                incid = {
                    'Index': int(idx),
                    'Tipo_Garantia': str(df.loc[idx, 'Tipo_Garantia']),
                    'Numero_Prestamo': str(df.loc[idx, 'Numero_Prestamo']),
                    'Numero_Prestamo_JOIN_KEY': str(key),
                    'Original_Id_Documento': original_id,
                    'Corrected_Id_Documento': df.loc[idx, 'Id_Documento'],
                    'Rule': 'AUTO_NUM_POLIZA_FROM_GARANTIA_AUTOS'
                }
                incid.update({f'Updated_{k}': v for k, v in updates.items()})
                incidences.append(incid)

            diag_rows.append({
                'Numero_Prestamo': str(df.loc[idx, 'Numero_Prestamo']),
                'Numero_Prestamo_JOIN_KEY': str(key),
                'num_poliza': sval if sval else '01',
                'applied': applied,
                'reason': reason
            })

        try:
            self.logger.info(
                f"AUTO_POLICY: candidates={int(scope_mask.sum())}, autos_rows={len(autos_df)}, matched={matched}, applied={applied_count}"
            )
        except Exception:
            pass

        if incidences:
            self._store_incidences('AUTO_NUM_POLIZA_FROM_GARANTIA_AUTOS', incidences, context)
            try:
                if updated_indices:
                    export_mask = df.index.isin(updated_indices)
                    original_columns = {}
                    if orig_idoc is not None and not orig_idoc.dropna().empty:
                        original_columns['Id_Documento'] = orig_idoc
                    if 'Importe' in df.columns and orig_importe is not None:
                        original_columns['Importe'] = orig_importe
                    for tgt in ['Valor_Garantía', 'Valor_Garantia']:
                        if tgt in df.columns and orig_val_gar is not None:
                            original_columns[tgt] = orig_val_gar
                    for tgt in ['Fecha_Última_Actualización', 'Fecha_Ultima_Actualizacion']:
                        if tgt in df.columns and orig_last_upd is not None:
                            original_columns[tgt] = orig_last_upd
                    if 'Fecha_Vencimiento' in df.columns and orig_venc is not None:
                        original_columns['Fecha_Vencimiento'] = orig_venc
                    if 'Tipo_Poliza' in df.columns and orig_tipo_poliza is not None:
                        original_columns['Tipo_Poliza'] = orig_tipo_poliza
                    original_columns = original_columns or None
                    self._export_error_subset(df, export_mask, 'BASE_AT12', 'AUTO_NUM_POLIZA_FROM_GARANTIA_AUTOS', context, None, original_columns=original_columns)
            except Exception:
                pass
            try:
                if diag_rows:
                    diag_df = pd.DataFrame(diag_rows)
                    diag_out = diag_df[diag_df['applied'] == False].copy()
                    if not diag_out.empty:
                        diag_path = context.paths.incidencias_dir / f"AUTO_POLICY_JOIN_DIAG_{context.period}.csv"
                        diag_out.to_csv(diag_path, index=False, encoding='utf-8', sep=getattr(context.config, 'output_delimiter', '|'), quoting=1)
                        self.logger.info(f"AUTO_POLICY_JOIN_DIAG -> {diag_path.name} ({len(diag_out)} records)")
            except Exception:
                pass
        else:
            try:
                self.logger.info("AUTO_NUM_POLIZA_FROM_GARANTIA_AUTOS: no completions or no candidates (0101/0103 scope)")
            except Exception:
                pass
        return df
    def _apply_inmueble_sin_avaluadora_correction(self, df: pd.DataFrame, context: TransformationContext) -> pd.DataFrame:
        """1.10. Inmueble sin Avaluadora: Asignar Nombre_Organismo='774' cuando Tipo_Garantia in (0207,0208,0209) y Nombre_Organismo vacío."""
        if 'Tipo_Garantia' not in df.columns or 'Nombre_Organismo' not in df.columns:
            try:
                self.logger.info("Skipping INMUEBLE_SIN_AVALUADORA_ORG_CODE: missing 'Tipo_Garantia' or 'Nombre_Organismo'")
            except Exception:
                pass
            return df

        df = df.copy()
        tg_norm = self._normalize_tipo_garantia_series(df['Tipo_Garantia'])
        nom = df['Nombre_Organismo']
        is_empty_nom = self._is_empty_like(nom)
        mask = tg_norm.isin({'0207', '0208', '0209'}) & is_empty_nom

        incidences = []
        try:
            orig_nom = pd.Series(index=df.index, dtype=object)
        except Exception:
            orig_nom = None
        if mask.any():
            for idx in df[mask].index:
                original_nom = df.loc[idx, 'Nombre_Organismo']
                df.loc[idx, 'Nombre_Organismo'] = '774'
                if orig_nom is not None:
                    orig_nom.loc[idx] = original_nom
                incidences.append({
                    'Index': int(idx),
                    'Tipo_Garantia': str(df.loc[idx, 'Tipo_Garantia']),
                    'Original_Nombre_Organismo': original_nom,
                    'Corrected_Nombre_Organismo': '774',
                    'Rule': 'INMUEBLE_SIN_AVALUADORA_ORG_CODE'
                })
            self._store_incidences('INMUEBLE_SIN_AVALUADORA_ORG_CODE', incidences, context)
            try:
                original_columns = {'Nombre_Organismo': orig_nom} if orig_nom is not None else None
                self._export_error_subset(df, mask, 'BASE_AT12', 'INMUEBLE_SIN_AVALUADORA_ORG_CODE', context, None, original_columns=original_columns)
            except Exception:
                pass
        else:
            try:
                self.logger.info("INMUEBLE_SIN_AVALUADORA_ORG_CODE: no empty Nombre_Organismo for 0207/0208/0209")
            except Exception:
                pass
        return df

    def _apply_fiduciaria_extranjera_standardization(self, df: pd.DataFrame, context: TransformationContext, subtype: str = "") -> pd.DataFrame:
        """1.11. Estandarización de Fiduciaria Extranjera (FDE) en AT12_BASE.

        Condición: `Nombre_Fiduciaria` contiene 'FDE'.
        Acciones: `Origen_Garantia`/`Origen` = 'E'; `Codigo_Region`/`Cod_region` = '320'.
        Exporta incidencias a FDE_NOMBRE_FIDUCIARIO_<YYYYMMDD>.csv con columnas originales adyacentes.
        """
        if df is None or df.empty:
            return df
        # Solo para BASE
        if subtype and str(subtype).upper() not in {"BASE_AT12", "BASE", "AT12_BASE"}:
            return df

        cols = set(df.columns)
        if 'Nombre_Fiduciaria' not in cols:
            return df

        df = df.copy()
        selector = df['Nombre_Fiduciaria'].astype(str)
        mask = selector.str.contains('FDE', case=False, na=False)
        if not mask.any():
            try:
                self.logger.info("FDE_NOMBRE_FIDUCIARIO: no candidates found")
            except Exception:
                pass
            return df

        # Determine target columns
        origen_target = 'Origen_Garantia' if 'Origen_Garantia' in cols else ('Origen_Garantia' if 'Origen_Garantia' in cols else None)
        region_target = 'Codigo_Region' if 'Codigo_Region' in cols else ('Cod_region' if 'Cod_region' in cols else None)
        # Keep originals for export
        orig_map = {}
        if origen_target:
            try:
                orig_map[origen_target] = df[origen_target].copy()
            except Exception:
                pass
        if region_target:
            try:
                orig_map[region_target] = df[region_target].copy()
            except Exception:
                pass

        # Apply updates
        if origen_target:
            df.loc[mask, origen_target] = 'E'
        if region_target:
            df.loc[mask, region_target] = '320'

        # Export affected rows with ORIGINAL columns adjacent
        try:
            out_df = df.loc[mask].copy()
            if out_df.empty:
                return df
            # Insert _ORIGINAL columns next to targets
            if orig_map:
                cols_list = list(out_df.columns)
                new_order = []
                for c in cols_list:
                    new_order.append(c)
                    if c in orig_map:
                        oc = f"{c}_ORIGINAL"
                        try:
                            out_df[oc] = orig_map[c].reindex(out_df.index)
                        except Exception:
                            out_df[oc] = orig_map[c]
                        new_order.append(oc)
                try:
                    out_df = out_df[new_order]
                except Exception:
                    pass
            # File path (custom name, no subtype in filename)
            fname = f"FDE_NOMBRE_FIDUCIARIO_{context.period}.csv"
            fpath = context.paths.incidencias_dir / fname
            fpath.parent.mkdir(parents=True, exist_ok=True)
            out_df.to_csv(
                fpath,
                index=False,
                encoding='utf-8',
                sep=getattr(context.config, 'output_delimiter', '|'),
                quoting=1,
                date_format='%Y%m%d'
            )
            try:
                self.logger.info(f"FDE_NOMBRE_FIDUCIARIO -> {fpath.name} ({len(out_df)} records)")
            except Exception:
                pass
        except Exception as e:
            self.logger.warning(f"Failed exporting FDE_NOMBRE_FIDUCIARIO: {e}")
        return df
