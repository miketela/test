from typing import Dict, List, Optional, Any
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime

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

    # -------------------- TDC helpers (normalization/enrichment) --------------------
    def _normalize_tdc_basic(self, df: pd.DataFrame) -> pd.DataFrame:
        """Basic normalization for TDC: trim text and normalize monetary fields.

        - Trim whitespace for object columns.
        - Monetary fields: remove thousands '.', convert ',' to '.' for internal numeric use.
        """
        if df is None or df.empty:
            return df
        df = df.copy()
        # Trim object columns
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].astype(str).str.strip()
        # Monetary fields
        money_cols = ['Valor_Inicial', 'Valor_Garantía', 'Valor_Garantia', 'Valor_Ponderado', 'Importe']
        for col in money_cols:
            if col in df.columns:
                s = df[col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                df[col + '__num'] = pd.to_numeric(s, errors='coerce')
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
        df.loc[mask, 'Clave_Tipo_Pren_Hipo'] = '0'
        df.loc[mask, 'Tipo_Instrumento'] = 'NA'
        df.loc[mask, 'Tipo_Poliza'] = 'NA'
        df.loc[mask, 'Status_Garantia'] = '0'
        df.loc[mask, 'Status_Prestamo'] = '-1'
        # Derived
        if 'Numero_Cis_Garantia' in df.columns:
            df.loc[mask, 'Numero_Cis_Prestamo'] = df.loc[mask, 'Numero_Cis_Garantia']
        if 'Numero_Ruc_Garantia' in df.columns:
            df.loc[mask, 'Numero_Ruc_Prestamo'] = df.loc[mask, 'Numero_Ruc_Garantia']
        # Segmento rule: 02 -> PREMIRA, else PRE
        try:
            seg_mask = mask & (df.get('Tipo_Facilidad').astype(str) == '02')
            df.loc[seg_mask, 'Segmento'] = 'PREMIRA'
            df.loc[mask & ~seg_mask, 'Segmento'] = 'PRE'
        except Exception:
            pass
        # Importe = Valor_Garantia (use numeric internal if present)
        if 'Valor_Garantia__num' in df.columns:
            df.loc[mask, 'Importe__num'] = df.loc[mask, 'Valor_Garantia__num']
        elif 'Valor_Garantía__num' in df.columns:
            df.loc[mask, 'Importe__num'] = df.loc[mask, 'Valor_Garantía__num']
        return df

    def _format_money_comma(self, s: pd.Series) -> pd.Series:
        """Format numeric series with comma decimal as string (no thousand sep)."""
        return s.map(lambda x: ('' if pd.isna(x) else f"{float(x):.2f}".replace('.', ',')))

    def _export_error_subset(self, df: pd.DataFrame, mask: pd.Series, subtype: str, rule_name: str,
                              context: TransformationContext, result: Optional[TransformationResult]) -> None:
        """Export a CSV containing only rows that match the error mask, preserving all original columns."""
        try:
            out_df = df.loc[mask].copy()
            if out_df.empty:
                return
            # Build filename focusing on rule name and subtype
            filename = f"INC_{rule_name}_{subtype}_{context.period}.csv"
            out_path = context.paths.incidencias_dir / filename
            out_path.parent.mkdir(parents=True, exist_ok=True)
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
            self.logger.info(f"Exported error subset for {rule_name} ({subtype}): {out_path}")
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
        """Apply AT12-specific transformations using a five-stage pipeline.
        
        Args:
            df: Input DataFrame
            context: Transformation context
            result: Result object to update
            source_data: Dictionary of source DataFrames
            
        Returns:
            Transformed DataFrame
        """
        self.logger.info("Starting AT12 transformation pipeline")
        
        # Initialize IncidenceReporter
        self.incidence_reporter = IncidenceReporter(
            config=self.config,
            run_id=context.run_id,
            period=context.period
        )
        
        # Stage 1: Initial Data Cleansing and Formatting
        df = self._stage1_initial_cleansing(df, context, result, source_data, subtype)
        
        # Stage 2: Data Enrichment and Generation from Auxiliary Sources
        df = self._stage2_enrichment(df, context, result, source_data, subtype)
        
        # Stage 3: Business Logic Application and Reporting
        df = self._stage3_business_logic(df, context, result, source_data)
        
        # Stage 4: Data Validation and Quality Assurance (only when applicable)
        try:
            if 'Numero_Prestamo' in df.columns or 'at_num_de_prestamos' in df.columns:
                df = self._stage4_validation(df, context, result, source_data)
            else:
                self.logger.debug("Skipping Stage 4: required identifier column not present")
        except Exception as e:
            self.logger.warning(f"Stage 4 skipped due to error: {e}")
        
        self.logger.info("AT12 transformation pipeline completed")
        return df
    
    def _stage1_initial_cleansing(self, df: pd.DataFrame, context: TransformationContext, result: TransformationResult, source_data: Dict[str, pd.DataFrame], subtype: str = "") -> pd.DataFrame:
        """Stage 1: Initial Data Cleansing and Formatting"""
        return self._phase1_error_correction(df, context, result, source_data)

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
    
    def _phase1_error_correction(self, df: pd.DataFrame, context: TransformationContext, 
                               result: TransformationResult, source_data: Dict[str, pd.DataFrame], subtype: str = "") -> pd.DataFrame:
        """Phase 1: Apply error correction rules to the data according to Stage 1 specifications."""
        self.logger.info("Executing Phase 1: Initial Data Cleansing and Formatting")
        
        # Apply all error correction rules in sequence
        df = self._apply_eeor_tabular_cleaning(df, context)
        df = self._apply_error_0301_correction(df, context, subtype=subtype, result=result)
        df = self._apply_coma_finca_empresa_correction(df, context)
        df = self._apply_fecha_cancelacion_correction(df, context)
        
        # This method requires source_data for AT03_CREDITOS lookup
        if source_data:
            df = self._apply_fecha_avaluo_correction(df, context, source_data)
        
        df = self._apply_inmuebles_sin_poliza_correction(df, context)
        df = self._apply_inmuebles_sin_finca_correction(df, context)
        df = self._apply_poliza_auto_comercial_correction(df, context)
        df = self._apply_error_poliza_auto_correction(df, context)
        df = self._apply_inmueble_sin_avaluadora_correction(df, context)
        
        self.logger.info("Completed Phase 1: Error correction")
        return df
    
    def _phase2_input_processing(self, df: pd.DataFrame, context: TransformationContext, 
                               result: TransformationResult, source_data: Dict[str, pd.DataFrame], subtype_hint: str = "") -> pd.DataFrame:
        """Phase 2: Process input data based on subtype."""
        self.logger.info("Executing Phase 2: Input Processing")
        
        # Determine subtype from hint or DataFrame characteristics
        subtype = subtype_hint or self._determine_subtype(df, context)
        
        if subtype == 'TDC_AT12':
            return self._process_tdc_data(df, context, result, source_data)
        elif subtype == 'SOBREGIRO_AT12':
            return self._process_sobregiro_data(df, context, result, source_data)
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
    
    def _process_tdc_data(self, df: pd.DataFrame, context: TransformationContext, 
                        result: TransformationResult, source_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Process TDC (Tarjeta de Crédito) specific data (minimal: number + date mapping + duplicates)."""
        self.logger.info("Processing TDC_AT12 data - Stage 2")

        # Step 1: Generate Número_Garantía (legacy, in-memory)
        df = self._generate_numero_garantia_tdc(df, context)

        # Step 2: Date Mapping with AT02_CUENTAS
        df = self._apply_date_mapping_tdc(df, context, source_data)

        # Step 3: Business rule - Tarjeta repetida (detect duplicates excluding Numero_Prestamo)
        try:
            df = self._validate_tdc_tarjeta_repetida(df, context)
        except Exception as e:
            self.logger.warning(f"TDC 'Tarjeta_repetida' validation skipped due to error: {e}")
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
        Export full-row subset to INC_TARJETA_REPETIDA_TDC_AT12_<PERIODO>.csv and record incidences.
        """
        # Choose key according to available columns
        if all(c in df.columns for c in ['Identificacion_cliente', 'Identificacion_Cuenta', 'Tipo_Facilidad']):
            key_cols = ['Identificacion_cliente', 'Identificacion_Cuenta', 'Tipo_Facilidad']
        elif all(c in df.columns for c in ['Id_Documento', 'Tipo_Facilidad']):
            key_cols = ['Id_Documento', 'Tipo_Facilidad']
        else:
            # No sufficient columns to detect duplicates
            return df

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
                self._store_incidences('TARJETA_REPETIDA', [{
                    'key': key_series.loc[i]
                } for i in df[dup_mask].index], context)
            except Exception:
                pass

            try:
                self._export_error_subset(df, dup_mask, 'TDC_AT12', 'TARJETA_REPETIDA', context, None)
            except Exception as e:
                self.logger.warning(f"Failed to export TARJETA_REPETIDA subset: {e}")

        return df
    
    def _process_sobregiro_data(self, df: pd.DataFrame, context: TransformationContext, 
                              result: TransformationResult, source_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Process Sobregiro specific data according to Stage 2 specifications."""
        self.logger.info("Processing SOBREGIRO_AT12 data - Stage 2")
        
        # Light normalization (trim + monetarias)
        df = self._normalize_tdc_basic(df)

        # 2.2. SOBREGIRO_AT12 Processing
        # Apply the same JOIN and date mapping logic as TDC
        # The Numero_Garantia field is not modified for SOBREGIRO
        df = self._apply_date_mapping_sobregiro(df, context, source_data)
        
        # Format money columns with comma decimal if normalized
        for col in ('Valor_Inicial', 'Valor_Garantia', 'Valor_Garantía', 'Valor_Ponderado', 'valor_ponderado', 'Importe'):
            num_col = col + '__num'
            if num_col in df.columns:
                target = 'Valor_Ponderado' if col == 'valor_ponderado' else ('Valor_Garantia' if col in ('Valor_Garantia', 'Valor_Garantía') else col)
                df[target] = self._format_money_comma(df[num_col])
        if 'Importe__num' in df.columns:
            df['Importe'] = self._format_money_comma(df['Importe__num'])
        
        return df
    
    def _process_valores_data(self, df: pd.DataFrame, context: TransformationContext, 
                            result: TransformationResult, source_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Process VALORES_AT12 applying ETL rules for garantías 0507."""
        self.logger.info("Processing VALORES_AT12 data - Stage 2 (ETL 0507)")

        # Step 0: Basic normalization (trim + money)
        df = self._normalize_tdc_basic(df)

        # Step 1: Keys normalization (Numero_Prestamo / Id_Documento)
        df = self._normalize_tdc_keys(df)

        # Step 2: Generate Numero_Garantia (persistent, padded) for 0507
        df = self._generate_numero_garantia_valores(df, context)

        # Step 3: Enrichment & derived fields for 0507
        df = self._enrich_valores_0507(df)

        # Step 4: Importe = Valor_Garantia; format monetarias con coma
        for col in ('Valor_Inicial', 'Valor_Garantia', 'Valor_Garantía', 'Valor_Ponderado', 'Importe'):
            num_col = col + '__num'
            if num_col in df.columns:
                target_name = 'Valor_Garantia' if col in ('Valor_Garantia', 'Valor_Garantía') else col
                df[target_name] = self._format_money_comma(df[num_col])
        if 'Importe__num' in df.columns:
            df['Importe'] = self._format_money_comma(df['Importe__num'])

        # Step 5: Shape final output columns (transformado)
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
        return df
    
    def _generate_numero_garantia_tdc(self, df: pd.DataFrame, context: TransformationContext) -> pd.DataFrame:
        """Generate unique guarantee codes for TDC_AT12 according to original Stage 2.1 spec.

        Non-persistent, in-memory assignment starting at 855500. Key uses Id_Documento+Tipo_Facilidad.
        """
        self.logger.info("Generating Número_Garantía for TDC_AT12")

        required_cols = ['Id_Documento', 'Tipo_Facilidad']
        missing_required = [c for c in required_cols if c not in df.columns]
        if missing_required:
            self.logger.warning(f"Skipping Número_Garantía generation; missing columns: {missing_required}")
            if 'Numero_Garantia' not in df.columns:
                df = df.copy(); df['Numero_Garantia'] = None
            return df

        df = df.copy()
        df['Numero_Garantia'] = None
        df = df.sort_values('Id_Documento', ascending=False).reset_index(drop=True)

        unique_keys = {}
        next_number = 855500
        incidences = []

        for idx, row in df.iterrows():
            unique_key = f"{row.get('Id_Documento', '')}{row.get('Tipo_Facilidad', '')}"
            if unique_key not in unique_keys:
                unique_keys[unique_key] = next_number
                df.at[idx, 'Numero_Garantia'] = next_number
                self._add_incidence(
                    incidence_type=IncidenceType.DATA_QUALITY,
                    severity=IncidenceSeverity.MEDIUM,
                    rule_id='TDC_NUMERO_GARANTIA_NEW_ASSIGNMENT',
                    description=f'New guarantee number {next_number} assigned',
                    data={'record_index': int(idx), 'column_name': 'Numero_Garantia', 'corrected_value': str(next_number)}
                )
                incidences.append({
                    'Index': idx,
                    'Id_Documento': row.get('Id_Documento', ''),
                    'Numero_Prestamo': row.get('Numero_Prestamo', ''),
                    'Tipo_Facilidad': row.get('Tipo_Facilidad', ''),
                    'Numero_Garantia_Assigned': next_number,
                    'Action': 'New guarantee number assigned'
                })
                next_number += 1
            else:
                df.at[idx, 'Numero_Garantia'] = unique_keys[unique_key]
                incidences.append({
                    'Index': idx,
                    'Id_Documento': row.get('Id_Documento', ''),
                    'Numero_Prestamo': row.get('Numero_Prestamo', ''),
                    'Tipo_Facilidad': row.get('Tipo_Facilidad', ''),
                    'Numero_Garantia_Assigned': unique_keys[unique_key],
                    'Action': 'Existing guarantee number reused'
                })

        if incidences:
            self._store_incidences('TDC_NUMERO_GARANTIA_GENERATION', incidences, context)
        self.logger.info(f"Generated {len(unique_keys)} unique guarantee numbers for TDC_AT12")
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

        left = df.copy()
        left['_key_tdc'] = left[tdc_key].astype(str)
        right['_key_at02'] = right['_key_at02'].astype(str)

        merged = left.merge(right, left_on='_key_tdc', right_on='_key_at02', how='left')

        incidences = []
        for idx, row in merged.iterrows():
            updated = False
            if pd.notna(row.get('Fecha_inicio_at02')) and 'Fecha_Ultima_Actualizacion' in merged.columns:
                merged.at[idx, 'Fecha_Ultima_Actualizacion'] = row['Fecha_inicio_at02']
                updated = True
            if pd.notna(row.get('Fecha_Vencimiento_at02')) and 'Fecha_Vencimiento' in merged.columns:
                merged.at[idx, 'Fecha_Vencimiento'] = row['Fecha_Vencimiento_at02']
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
        merged.drop(columns=[c for c in merged.columns if c in ['_key_tdc', '_key_at02', 'Fecha_inicio_at02', 'Fecha_Vencimiento_at02']], inplace=True, errors='ignore')

        if incidences:
            self._store_incidences('TDC_DATE_MAPPING', incidences, context)
        self.logger.info(f"Applied date mapping to {len(incidences)} out of {original_count} TDC records")

        return merged
    
    def _apply_date_mapping_sobregiro(self, df: pd.DataFrame, context: TransformationContext,
                                     source_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Apply date mapping for SOBREGIRO_AT12 using AT02_CUENTAS.

        Maps Fecha_Apertura from AT02.Fecha_proceso and Fecha_Cancelacion from AT02.Fecha_Vencimiento
        when available. Falls back to original values without relying on pandas suffixes.
        """
        self.logger.info("Applying date mapping for SOBREGIRO_AT12")

        if 'AT02_CUENTAS' not in source_data or source_data['AT02_CUENTAS'].empty:
            self.logger.warning("AT02_CUENTAS data not found or is empty. Skipping SOBREGIRO date mapping.")
            return df

        at02_df = source_data['AT02_CUENTAS']

        # Required keys and columns
        keys = ['Identificacion_cliente', 'Identificacion_Cuenta']
        base_date_cols = []
        # Prefer schema-aligned columns for SOBREGIRO
        if 'Fecha_Ultima_Actualizacion' in df.columns:
            base_date_cols.append('Fecha_Ultima_Actualizacion')
        if 'Fecha_Vencimiento' in df.columns:
            base_date_cols.append('Fecha_Vencimiento')

        missing_keys_df = [k for k in keys if k not in df.columns]
        missing_keys_at02 = [k for k in keys if k not in at02_df.columns]
        missing_at02_dates = [c for c in ['Fecha_proceso', 'Fecha_Vencimiento'] if c not in at02_df.columns]

        if missing_keys_df or missing_keys_at02 or (not base_date_cols and missing_at02_dates):
            self.logger.error(
                f"Missing columns for SOBREGIRO mapping. DF keys missing: {missing_keys_df}, "
                f"AT02 keys missing: {missing_keys_at02}, AT02 date cols missing: {missing_at02_dates}"
            )
            return df

        # Prepare left (base) frame with explicit base suffix for safe fallback
        left_cols = keys + base_date_cols
        left = df.reset_index(drop=True)[left_cols].copy()
        rename_base = {c: f"{c}_base" for c in base_date_cols}
        if rename_base:
            left.rename(columns=rename_base, inplace=True)

        # Prepare right (AT02) frame with target names
        right = at02_df[keys + [c for c in ['Fecha_proceso', 'Fecha_Vencimiento'] if c in at02_df.columns]].copy()
        right.rename(columns={
            'Fecha_proceso': 'Fecha_Ultima_Actualizacion_at02',
            'Fecha_Vencimiento': 'Fecha_Vencimiento_at02'
        }, inplace=True)

        # Merge
        merged = left.merge(right, on=keys, how='left')

        # Diagnostics (debug level to avoid noise in INFO)
        try:
            self.logger.debug(f"SOBREGIRO merged columns: {list(merged.columns)}")
        except Exception:
            pass

        # Apply mapped dates with fallback to base
        out = df.reset_index(drop=True).copy()

        if 'Fecha_Ultima_Actualizacion' in base_date_cols:
            base_col = 'Fecha_Ultima_Actualizacion_base' if 'Fecha_Ultima_Actualizacion_base' in merged.columns else 'Fecha_Ultima_Actualizacion'
            if 'Fecha_Ultima_Actualizacion_at02' in merged.columns:
                out['Fecha_Ultima_Actualizacion'] = merged['Fecha_Ultima_Actualizacion_at02'].fillna(merged.get(base_col))
            else:
                out['Fecha_Ultima_Actualizacion'] = merged.get(base_col)

        if 'Fecha_Vencimiento' in base_date_cols:
            base_col = 'Fecha_Vencimiento_base' if 'Fecha_Vencimiento_base' in merged.columns else 'Fecha_Vencimiento'
            if 'Fecha_Vencimiento_at02' in merged.columns:
                out['Fecha_Vencimiento'] = merged['Fecha_Vencimiento_at02'].fillna(merged.get(base_col))
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

        return out
    
    def _generate_numero_garantia_valores(self, df: pd.DataFrame, context: TransformationContext) -> pd.DataFrame:
        """Generate persistent Numero_Garantia for VALORES_AT12 (0507), padded to 10."""
        from src.core.sequence import SequenceRegistry
        if df is None or df.empty:
            return df
        df = df.copy()
        if 'Numero_Garantia' not in df.columns:
            df['Numero_Garantia'] = None
        mask = df.get('Tipo_Garantia').astype(str) == '0507'
        state_dir = context.paths.base_transforms_dir / 'state'
        state_file = state_dir / 'valores_numero_garantia.json'
        start_num = int(getattr(context.config, 'valores_sequence_start', 1))
        reg = SequenceRegistry(state_file, start_number=start_num)
        incidences = []
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
        df.loc[mask, 'Clave_Tipo_Pren_Hipo'] = '0'
        df.loc[mask, 'Tipo_Instrumento'] = 'NA'
        df.loc[mask, 'Tipo_Poliza'] = 'NA'
        df.loc[mask, 'Status_Garantia'] = '0'
        df.loc[mask, 'Status_Prestamo'] = '-1'
        # Derived
        if 'Numero_Cis_Garantia' in df.columns:
            df.loc[mask, 'Numero_Cis_Prestamo'] = df.loc[mask, 'Numero_Cis_Garantia']
        if 'Numero_Ruc_Garantia' in df.columns:
            df.loc[mask, 'Numero_Ruc_Prestamo'] = df.loc[mask, 'Numero_Ruc_Garantia']
        # Segmento rule
        try:
            seg_mask = mask & (df.get('Tipo_Facilidad').astype(str) == '02')
            df.loc[seg_mask, 'Segmento'] = 'PREMIRA'
            df.loc[mask & ~seg_mask, 'Segmento'] = 'PRE'
        except Exception:
            pass
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
        
        for idx, row in merged_df.iterrows():
            saldo = row.get('saldo', 0)
            nuevo_valor_garantia = row.get('nuevo_at_valor_garantia', 0)
            
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
                mask = df['Numero_Prestamo'] == row.get(join_key_valor, '')
                if mask.any():
                    if 'at_valor_garantia' in df.columns:
                        df.loc[mask, 'at_valor_garantia'] = nuevo_valor_garantia
                    if 'at_valor_pond_garantia' in df.columns:
                        df.loc[mask, 'at_valor_pond_garantia'] = row.get('nuevo_at_valor_pond_garantia', 0)
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
        
        # Generate processed CSV files
        self._generate_processed_files(context, transformed_data, result)
        
        # Generate consolidated TXT file
        self._generate_consolidated_file(context, transformed_data, result)
    
    def _generate_incidence_files(self, context: TransformationContext, result: TransformationResult) -> None:
        """Generate incidence CSV files.

        - Per-rule, full-row subsets (INC_*) ya se exportan durante cada regla.
        - EEOO_TABULAR solo para validaciones globales de base (whitelist).
        """
        allowed_global = {'EEOR_TABULAR', 'FUERA_CIERRE_EXCEL_GENERATION'}

        for subtype, incidences in self.incidences_data.items():
            if not incidences or subtype not in allowed_global:
                continue
            try:
                incidences_df = pd.DataFrame(incidences)
                incidence_filename = f"EEOO_TABULAR_{subtype}_AT12_{context.period}.csv"
                incidence_path = context.paths.get_incidencia_path(incidence_filename)
                if self._save_dataframe_as_csv(incidences_df, incidence_path):
                    result.incidence_files.append(incidence_path)
                    self.logger.info(f"Generated global incidence file: {incidence_path}")
            except Exception as e:
                self.logger.warning(f"Failed to export global incidence {subtype}: {e}")
    
    def _generate_processed_files(self, context: TransformationContext, 
                                transformed_data: Dict[str, pd.DataFrame], 
                                result: TransformationResult) -> None:
        """Generate processed CSV files."""
        for subtype, df in transformed_data.items():
            if not df.empty:
                # Generate processed filename
                processed_filename = f"AT12_{subtype}_{context.period}.csv"
                processed_path = context.paths.get_procesado_path(processed_filename)
                
                # Save processed file
                if self._save_dataframe_as_csv(df, processed_path):
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
                        for _, row in df.iterrows():
                            # Convert all values to string and join with delimiter
                            record = delimiter.join(str(row[col]) for col in df.columns)
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
    def _apply_eeor_tabular_cleaning(self, df: pd.DataFrame, context: TransformationContext) -> pd.DataFrame:
        """1.1. EEOR TABULAR: Whitespace Errors - Remove unnecessary spaces from text fields."""
        text_columns = df.select_dtypes(include=['object']).columns
        incidences = []
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
            self._store_incidences('EEOR_TABULAR', incidences, context)
            self.logger.info(f"Applied EEOR TABULAR cleaning to {len(incidences)} records across {len(text_columns)} text columns")
            # Export full-row subset for rows modified by EEOR cleaning
            try:
                if overall_mask is not None and overall_mask.any():
                    self._export_error_subset(df, overall_mask, 'BASE_AT12', 'EEOR_TABULAR', context, None)
            except Exception:
                pass
        
        return df
    
    def _apply_error_0301_correction(self, df: pd.DataFrame, context: TransformationContext, subtype: str = "", result: Optional[TransformationResult] = None) -> pd.DataFrame:
        """1.2. Error 0301: Id_Documento Logic for Mortgage Guarantees (1-based indexing)."""
        if 'Tipo_Garantia' not in df.columns or 'Id_Documento' not in df.columns:
            return df

        mask_0301 = df['Tipo_Garantia'] == '0301'
        incidences = []

        for idx in df[mask_0301].index:
            id_documento = str(df.loc[idx, 'Id_Documento']) if pd.notna(df.loc[idx, 'Id_Documento']) else ''
            original_id = id_documento

            # Sub-Rule 1: positions 9-10 equal '01' or '41' or '42' → ensure length exactly 10
            cond_len_ge_10 = len(id_documento) >= 10
            pos_9_10 = id_documento[8:10] if cond_len_ge_10 else ''
            if pos_9_10 in {'01', '41', '42'}:
                if len(id_documento) > 10:
                    # Extract the first 10 characters
                    df.loc[idx, 'Id_Documento'] = id_documento[:10]
                elif len(id_documento) < 10:
                    # Flag manual review; do not modify value
                    self._add_incidence(
                        incidence_type=IncidenceType.VALIDATION_FAILURE,
                        severity=IncidenceSeverity.HIGH,
                        rule_id='Error_0301_Manual_Review_Required',
                        description='Length < 10 for Sub-Rule 1 (positions 9-10 in {01,41,42})',
                        data={
                            'record_index': int(idx),
                            'column_name': 'Id_Documento',
                            'original_value': original_id
                        }
                    )
                    incidences.append({
                        'Index': idx,
                        'Original_Id_Documento': original_id,
                        'Rule': 'Error_0301_Manual_Review_Required',
                        'Reason': 'Length < 10 for Sub-Rule 1'
                    })

            # Sub-Rule 2: Type 701 detection by position depending on length
            elif len(id_documento) in (10, 11):
                if len(id_documento) == 10:
                    seq = id_documento[7:10]  # positions 8-10
                else:
                    seq = id_documento[8:11]  # positions 9-11

                if seq == '701':
                    # Valid; include 10-char case in follow-up report
                    if len(id_documento) == 10:
                        incidences.append({
                            'Index': idx,
                            'Id_Documento': id_documento,
                            'Rule': 'Error_0301_Follow_Up_Report',
                            'Reason': 'Length 10 with sequence 701 at positions 8-10'
                        })
                    # No modification required

            # Sub-Rule 3: Length 15 handling and validation of positions 13-15
            else:
                if len(id_documento) > 15:
                    # Truncate to first 15 and report
                    df.loc[idx, 'Id_Documento'] = id_documento[:15]
                    incidences.append({
                        'Index': idx,
                        'Original_Id_Documento': original_id,
                        'Corrected_Id_Documento': id_documento[:15],
                        'Rule': 'Error_0301_Truncate_To_15'
                    })
                elif 0 < len(id_documento) < 15:
                    # Report deviation; do not modify
                    incidences.append({
                        'Index': idx,
                        'Original_Id_Documento': original_id,
                        'Rule': 'Error_0301_Length_Less_Than_15'
                    })
                elif len(id_documento) == 15:
                    # Validate positions 13-15 (no modification regardless of match)
                    pos_13_15 = id_documento[12:15]
                    _ = pos_13_15 in {'100', '110', '120', '123', '810'}
                    # No action required per spec; keep for possible future use

            # Log changes (any modification)
            if df.loc[idx, 'Id_Documento'] != original_id:
                incidences.append({
                    'Index': idx,
                    'Original_Id_Documento': original_id,
                    'Corrected_Id_Documento': df.loc[idx, 'Id_Documento'],
                    'Rule': 'Error_0301_Correction'
                })

        if incidences:
            self._store_incidences('ERROR_0301', incidences, context)
            self.logger.info(f"Applied Error 0301 correction/incidences to {len(incidences)} records")

        # Export a CSV with original columns for rows that had the 0301 condition
        try:
            if mask_0301.any():
                self._export_error_subset(df, mask_0301, subtype or 'BASE_AT12', 'ERROR_0301', context, result)
        except Exception:
            pass

        return df
    
    def _apply_coma_finca_empresa_correction(self, df: pd.DataFrame, context: TransformationContext) -> pd.DataFrame:
        """1.3. COMA EN FINCA EMPRESA: Remove commas from Id_Documento."""
        if 'Id_Documento' not in df.columns:
            return df
        
        # Find records with commas in Id_Documento
        mask_comma = df['Id_Documento'].astype(str).str.contains(',', na=False)
        incidences = []
        
        if mask_comma.sum() > 0:
            # Store original values for incidence reporting
            for idx in df[mask_comma].index:
                original_id = df.loc[idx, 'Id_Documento']
                corrected_id = str(original_id).replace(',', '')
                df.loc[idx, 'Id_Documento'] = corrected_id
                
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
                self._export_error_subset(df, mask_comma, 'BASE_AT12', 'COMA_EN_FINCA_EMPRESA', context, None)
            except Exception:
                pass
        
        return df
    
    def _apply_fecha_cancelacion_correction(self, df: pd.DataFrame, context: TransformationContext) -> pd.DataFrame:
        """1.4. Fecha Cancelación Errada: Correct erroneous expiration dates."""
        if 'Fecha_Vencimiento' not in df.columns:
            return df
        
        incidences = []
        
        # Convert to string and extract year
        fecha_str = df['Fecha_Vencimiento'].astype(str)
        
        # Find records with invalid years (> 2100 or < 1985)
        for idx in df.index:
            fecha_val = str(df.loc[idx, 'Fecha_Vencimiento'])
            if len(fecha_val) >= 4:
                try:
                    year = int(fecha_val[:4])
                    if year > 2100 or year < 1985:
                        original_fecha = df.loc[idx, 'Fecha_Vencimiento']
                        df.loc[idx, 'Fecha_Vencimiento'] = '21001231'
                        
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
                    self._export_error_subset(df, mask, 'BASE_AT12', 'FECHA_CANCELACION_ERRADA', context, None)
            except Exception:
                pass
        
        return df
    
    def _apply_fecha_avaluo_correction(self, df: pd.DataFrame, context: TransformationContext, source_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """1.5. Fecha Avalúo Errada: Correct inconsistent appraisal update dates."""
        if 'Fecha_Ultima_Actualizacion' not in df.columns or 'Numero_Prestamo' not in df.columns:
            return df
        
        if 'AT03_CREDITOS' not in source_data or source_data['AT03_CREDITOS'].empty:
            self.logger.warning("AT03_CREDITOS data not available for fecha avalúo correction")
            return df
        
        at03_df = source_data['AT03_CREDITOS']
        incidences = []
        
        # Get last day of previous month for comparison
        from datetime import datetime, timedelta
        import calendar
        
        try:
            # Assuming context has year and month
            current_year = int(context.year)
            current_month = int(context.month)
            
            if current_month == 1:
                prev_month = 12
                prev_year = current_year - 1
            else:
                prev_month = current_month - 1
                prev_year = current_year
            
            last_day_prev_month = calendar.monthrange(prev_year, prev_month)[1]
            cutoff_date = f"{prev_year}{prev_month:02d}{last_day_prev_month:02d}"
            
        except (AttributeError, ValueError):
            # Fallback if context doesn't have proper date info
            cutoff_date = "20231231"  # Default cutoff
        
        for idx in df.index:
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
                # JOIN with AT03_CREDITOS to get fec_ini_prestamo
                at03_match = at03_df[at03_df['num_cta'] == numero_prestamo]
                if not at03_match.empty and 'fec_ini_prestamo' in at03_match.columns:
                    original_fecha = df.loc[idx, 'Fecha_Ultima_Actualizacion']
                    new_fecha = at03_match.iloc[0]['fec_ini_prestamo']
                    df.loc[idx, 'Fecha_Ultima_Actualizacion'] = new_fecha
                    
                    incidences.append({
                        'Index': idx,
                        'Numero_Prestamo': numero_prestamo,
                        'Original_Fecha_Ultima_Actualizacion': original_fecha,
                        'Corrected_Fecha_Ultima_Actualizacion': new_fecha,
                        'Reason': reason,
                        'Rule': 'FECHA_AVALUO_ERRADA'
                    })
        
        if incidences:
            self._store_incidences('FECHA_AVALUO_ERRADA', incidences, context)
            self.logger.info(f"Corrected {len(incidences)} Fecha_Ultima_Actualizacion records")
            # Export subset rows that had this issue
            try:
                idxs = [rec.get('Index') for rec in incidences if 'Index' in rec]
                if idxs:
                    mask_export = df.index.isin(idxs)
                    self._export_error_subset(df, mask_export, 'BASE_AT12', 'FECHA_AVALUO_ERRADA', context, None)
            except Exception:
                pass
        
        return df
    
    def _apply_inmuebles_sin_poliza_correction(self, df: pd.DataFrame, context: TransformationContext) -> pd.DataFrame:
        """1.6. Inmuebles sin Póliza: Correct missing policy numbers for real estate."""
        if 'Tipo_Garantia' not in df.columns or 'Numero_Poliza' not in df.columns:
            return df
        
        # Find real estate guarantees without policy number
        mask_inmueble = df['Tipo_Garantia'].isin(['INMUEBLE', 'INMUEBLES'])
        mask_sin_poliza = (df['Numero_Poliza'].isna()) | (df['Numero_Poliza'].astype(str).str.strip() == '')
        mask_correction = mask_inmueble & mask_sin_poliza
        
        incidences = []
        
        if mask_correction.sum() > 0:
            for idx in df[mask_correction].index:
                original_poliza = df.loc[idx, 'Numero_Poliza']
                df.loc[idx, 'Numero_Poliza'] = '0'
                
                incidences.append({
                    'Index': idx,
                    'Tipo_Garantia': df.loc[idx, 'Tipo_Garantia'],
                    'Original_Numero_Poliza': original_poliza,
                    'Corrected_Numero_Poliza': '0',
                    'Rule': 'INMUEBLES_SIN_POLIZA'
                })
            
            self._store_incidences('INMUEBLES_SIN_POLIZA', incidences, context)
            self.logger.info(f"Corrected {len(incidences)} real estate records without policy number")
            # Export subset rows
            try:
                self._export_error_subset(df, mask_correction, 'BASE_AT12', 'INMUEBLES_SIN_POLIZA', context, None)
            except Exception:
                pass
        
        return df
    
    def _apply_inmuebles_sin_finca_correction(self, df: pd.DataFrame, context: TransformationContext) -> pd.DataFrame:
        """1.7. Inmuebles sin Finca: Correct missing property registration for real estate."""
        if 'Tipo_Garantia' not in df.columns or 'Id_Documento' not in df.columns:
            return df
        
        # Find real estate guarantees without property ID
        mask_inmueble = df['Tipo_Garantia'].isin(['INMUEBLE', 'INMUEBLES'])
        mask_sin_finca = (df['Id_Documento'].isna()) | (df['Id_Documento'].astype(str).str.strip() == '')
        mask_correction = mask_inmueble & mask_sin_finca
        
        incidences = []
        
        if mask_correction.sum() > 0:
            for idx in df[mask_correction].index:
                original_id = df.loc[idx, 'Id_Documento']
                df.loc[idx, 'Id_Documento'] = '0'
                
                incidences.append({
                    'Index': idx,
                    'Tipo_Garantia': df.loc[idx, 'Tipo_Garantia'],
                    'Original_Id_Documento': original_id,
                    'Corrected_Id_Documento': '0',
                    'Rule': 'INMUEBLES_SIN_FINCA'
                })
            
            self._store_incidences('INMUEBLES_SIN_FINCA', incidences, context)
            self.logger.info(f"Corrected {len(incidences)} real estate records without property ID")
            try:
                self._export_error_subset(df, mask_correction, 'BASE_AT12', 'INMUEBLES_SIN_FINCA', context, None)
            except Exception:
                pass
        
        return df
    
    def _apply_poliza_auto_comercial_correction(self, df: pd.DataFrame, context: TransformationContext) -> pd.DataFrame:
        """1.8. Póliza Auto Comercial: Correct commercial auto policy types."""
        if 'Tipo_Poliza' not in df.columns or 'Codigo_Ramo' not in df.columns:
            return df
        
        # Find commercial auto policies that need correction
        mask_auto_comercial = (df['Tipo_Poliza'].astype(str).str.upper() == 'AUTO COMERCIAL')
        incidences = []
        
        if mask_auto_comercial.sum() > 0:
            for idx in df[mask_auto_comercial].index:
                original_tipo = df.loc[idx, 'Tipo_Poliza']
                original_codigo = df.loc[idx, 'Codigo_Ramo']
                
                df.loc[idx, 'Tipo_Poliza'] = 'Auto'
                df.loc[idx, 'Codigo_Ramo'] = '3'
                
                incidences.append({
                    'Index': idx,
                    'Original_Tipo_Poliza': original_tipo,
                    'Corrected_Tipo_Poliza': 'Auto',
                    'Original_Codigo_Ramo': original_codigo,
                    'Corrected_Codigo_Ramo': '3',
                    'Rule': 'POLIZA_AUTO_COMERCIAL'
                })
            
            self._store_incidences('POLIZA_AUTO_COMERCIAL', incidences, context)
            self.logger.info(f"Corrected {len(incidences)} commercial auto policy records")
            try:
                self._export_error_subset(df, mask_auto_comercial, 'BASE_AT12', 'POLIZA_AUTO_COMERCIAL', context, None)
            except Exception:
                pass
        
        return df
    
    def _apply_error_poliza_auto_correction(self, df: pd.DataFrame, context: TransformationContext) -> pd.DataFrame:
        """1.9. Error Póliza Auto: Correct auto policy codes."""
        if 'Tipo_Poliza' not in df.columns or 'Codigo_Ramo' not in df.columns:
            return df
        
        # Find auto policies with incorrect codes
        mask_auto = (df['Tipo_Poliza'].astype(str).str.upper() == 'AUTO')
        mask_wrong_code = ~(df['Codigo_Ramo'].astype(str).isin(['3', '4']))
        mask_correction = mask_auto & mask_wrong_code
        
        incidences = []
        
        if mask_correction.sum() > 0:
            for idx in df[mask_correction].index:
                original_codigo = df.loc[idx, 'Codigo_Ramo']
                df.loc[idx, 'Codigo_Ramo'] = '3'  # Default to code 3
                
                incidences.append({
                    'Index': idx,
                    'Tipo_Poliza': df.loc[idx, 'Tipo_Poliza'],
                    'Original_Codigo_Ramo': original_codigo,
                    'Corrected_Codigo_Ramo': '3',
                    'Rule': 'ERROR_POLIZA_AUTO'
                })
            
            self._store_incidences('ERROR_POLIZA_AUTO', incidences, context)
            self.logger.info(f"Corrected {len(incidences)} auto policy code records")
            try:
                self._export_error_subset(df, mask_correction, 'BASE_AT12', 'ERROR_POLIZA_AUTO', context, None)
            except Exception:
                pass
        
        return df
    
    def _apply_inmueble_sin_avaluadora_correction(self, df: pd.DataFrame, context: TransformationContext) -> pd.DataFrame:
        """1.10. Inmueble sin Avaluadora: Correct real estate without appraisal company."""
        if 'Tipo_Poliza' not in df.columns or 'Codigo_Ramo' not in df.columns:
            return df
        
        # Find real estate policies without appraisal company
        mask_inmueble_sin_avaluadora = (df['Tipo_Poliza'].astype(str).str.upper() == 'INMUEBLE SIN AVALUADORA')
        incidences = []
        
        if mask_inmueble_sin_avaluadora.sum() > 0:
            for idx in df[mask_inmueble_sin_avaluadora].index:
                original_tipo = df.loc[idx, 'Tipo_Poliza']
                original_codigo = df.loc[idx, 'Codigo_Ramo']
                
                df.loc[idx, 'Tipo_Poliza'] = 'Inmueble'
                df.loc[idx, 'Codigo_Ramo'] = '5'
                
                incidences.append({
                    'Index': idx,
                    'Original_Tipo_Poliza': original_tipo,
                    'Corrected_Tipo_Poliza': 'Inmueble',
                    'Original_Codigo_Ramo': original_codigo,
                    'Corrected_Codigo_Ramo': '5',
                    'Rule': 'INMUEBLE_SIN_AVALUADORA'
                })
            
            self._store_incidences('INMUEBLE_SIN_AVALUADORA', incidences, context)
            self.logger.info(f"Corrected {len(incidences)} real estate without appraisal company records")
            try:
                self._export_error_subset(df, mask_inmueble_sin_avaluadora, 'BASE_AT12', 'INMUEBLE_SIN_AVALUADORA', context, None)
            except Exception:
                pass
        
        return df
