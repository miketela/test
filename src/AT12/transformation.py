"""AT12-specific transformation engine.

This module implements the business rules and transformations specific to AT12
regulatory atoms, using pandas for all data processing operations.

Transformation Phases:
1. Error Correction: Fix common data quality issues
2. Input Processing: Handle specific subtypes (TDC_AT12, SOBREGIRO_AT12, VALORES_AT12)
3. Filtering: Apply FUERA_CIERRE_AT12 filtering
4. Validation: Apply VALOR_MINIMO_AVALUO_AT12 validation
"""

from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
import logging

from ..core.transformation import TransformationEngine, TransformationContext, TransformationResult
from ..core.config import Config
from ..core.naming import FilenameParser
from ..core.header_mapping import HeaderMapper


class AT12TransformationEngine(TransformationEngine):
    """AT12-specific transformation engine with pandas-based processing.
    
    Implements all AT12 business rules and transformations while maintaining
    separation of concerns and testability.
    """
    
    # AT12 specific constants
    EXPECTED_SUBTYPES = [
        'BASE_AT12', 'TDC_AT12', 'SOBREGIRO_AT12', 'VALORES_AT12', 
        'FUERA_CIERRE_AT12', 'VALOR_MINIMO_AVALUO_AT12',
        'AT02_CUENTAS', 'AT03_CREDITOS', 'POLIZA_HIPOTECAS_AT12', 'GARANTIA_AUTOS_AT12'
    ]
    REQUIRED_SUBTYPES = ['BASE_AT12']  # BASE is always required
    
    # Business rule thresholds
    MIN_AVALUO_THRESHOLD = 1000000  # Minimum avaluo value
    
    def __init__(self, config: Config):
        """Initialize AT12 transformation engine.
        
        Args:
            config: Configuration instance
        """
        super().__init__(config)
        self.header_mapper = HeaderMapper()
        self.incidences_data = {}  # Store incidences by subtype
        
        # Override the filename parser with AT12-specific expected subtypes
        self._filename_parser = FilenameParser(self.EXPECTED_SUBTYPES)
    
    def _apply_transformations(self, context: TransformationContext, 
                             source_data: Dict[str, pd.DataFrame], 
                             result: TransformationResult) -> Dict[str, pd.DataFrame]:
        """Apply AT12-specific transformations in a multi-phase process."""
        self.logger.info("Starting AT12 transformations")
        self.incidences_data = {}

        # Phase 1: Error Correction
        corrected_data = self._phase1_error_correction(context, source_data, result)

        # Phase 2: Input Processing (specific subtypes)
        processed_data = self._phase2_input_processing(context, corrected_data, result)

        # --- Consolidation Step ---
        subtypes_to_consolidate = ['BASE_AT12', 'TDC_AT12', 'SOBREGIRO_AT12', 'VALORES_AT12']
        dfs_to_consolidate = [processed_data[st] for st in subtypes_to_consolidate if st in processed_data and not processed_data[st].empty]
        
        if not dfs_to_consolidate:
            result.warnings.append("No data available for consolidation.")
            self.logger.warning("No data to consolidate. Transformation will stop before consolidation.")
            return processed_data

        consolidated_df = pd.concat(dfs_to_consolidate, ignore_index=True, sort=False)
        self.logger.info(f"Consolidated {len(consolidated_df)} records from {len(dfs_to_consolidate)} subtypes.")

        # Phase 3: Filtering (FUERA_CIERRE)
        consolidated_df = self._phase3_filter_fuera_cierre(consolidated_df, context, result, source_data)

        # Phase 4: Validation (VALOR_MINIMO_AVALUO)
        consolidated_df = self._phase4_valor_minimo_avaluo(consolidated_df, context, result, source_data)

        # Store the final consolidated data
        final_data = processed_data.copy()
        final_data['CONSOLIDATED_AT12'] = consolidated_df

        self.logger.info("AT12 transformations completed")
        return final_data
    
    def _phase1_error_correction(self, context: TransformationContext, 
                               source_data: Dict[str, pd.DataFrame], 
                               result: TransformationResult) -> Dict[str, pd.DataFrame]:
        """Phase 1: Apply error correction to all data.
        
        Args:
            context: Transformation context
            source_data: Source data by subtype
            result: Result object to update
            
        Returns:
            Corrected data by subtype
        """
        self.logger.info("Phase 1: Error correction")
        corrected_data = {}
        
        for subtype, df in source_data.items():
            self.logger.info(f"Correcting errors in {subtype} ({len(df)} records)")
            
            # Create a copy to avoid modifying original data
            corrected_df = df.copy()
            
            # Apply header mapping/normalization
            corrected_df = self._normalize_headers(corrected_df, subtype)
            
            # Apply data type corrections
            corrected_df = self._correct_data_types(corrected_df, subtype)
            
            # Apply business rule corrections
            corrected_df = self._apply_business_corrections(corrected_df, subtype, context, source_data)
            
            # Track corrections as metrics
            original_count = len(df)
            corrected_count = len(corrected_df)
            
            if original_count != corrected_count:
                result.warnings.append(
                    f"{subtype}: Record count changed from {original_count} to {corrected_count} after corrections"
                )
            
            corrected_data[subtype] = corrected_df
            self.logger.info(f"Completed error correction for {subtype}")
        
        return corrected_data
    
    def _phase2_input_processing(self, context: TransformationContext, 
                               source_data: Dict[str, pd.DataFrame], 
                               result: TransformationResult) -> Dict[str, pd.DataFrame]:
        """Phase 2: Process specific input subtypes by enriching them."""
        self.logger.info("Executing Phase 2: Input Processing")
        processed_data = source_data.copy()

        if 'TDC_AT12' in processed_data:
            processed_data['TDC_AT12'] = self._process_tdc_data(processed_data['TDC_AT12'], context, result, source_data)
        
        if 'SOBREGIRO_AT12' in processed_data:
            processed_data['SOBREGIRO_AT12'] = self._process_sobregiro_data(processed_data['SOBREGIRO_AT12'], context, result, source_data)

        if 'VALORES_AT12' in processed_data:
            processed_data['VALORES_AT12'] = self._process_valores_data(processed_data['VALORES_AT12'], context, result, source_data)
        
        return processed_data
    
    def _normalize_headers(self, df: pd.DataFrame, subtype: str) -> pd.DataFrame:
        """Normalize column headers using HeaderMapper.
        
        Args:
            df: DataFrame to normalize
            subtype: Data subtype
            
        Returns:
            DataFrame with normalized headers
        """
        try:
            # Get expected headers for this subtype
            mapping = self.header_mapper.get_mapping_for_subtype(f"AT12_{subtype}")
            
            if isinstance(mapping, dict):
                # Apply header mapping
                df_normalized = df.rename(columns=mapping)
            else:
                # Use header normalizer for cleaning
                normalized_headers = [self.header_mapper.normalizer.normalize_header(col) for col in df.columns]
                df_normalized = df.copy()
                df_normalized.columns = normalized_headers
            
            return df_normalized
            
        except Exception as e:
            self.logger.warning(f"Header normalization failed for {subtype}: {str(e)}")
            return df
    
    def _correct_data_types(self, df: pd.DataFrame, subtype: str) -> pd.DataFrame:
        """Apply data type corrections.
        
        Args:
            df: DataFrame to correct
            subtype: Data subtype
            
        Returns:
            DataFrame with corrected data types
        """
        corrected_df = df.copy()
        
        # Common date column patterns
        date_columns = [col for col in corrected_df.columns if 'fecha' in col.lower() or 'date' in col.lower()]
        
        for col in date_columns:
            try:
                # Convert to datetime, then to YYYYMMDD string format
                corrected_df[col] = pd.to_datetime(corrected_df[col], errors='coerce')
                corrected_df[col] = corrected_df[col].dt.strftime('%Y%m%d')
            except Exception as e:
                self.logger.warning(f"Date conversion failed for column {col} in {subtype}: {str(e)}")
        
        # Common numeric column patterns
        numeric_columns = [col for col in corrected_df.columns if any(keyword in col.lower() 
                          for keyword in ['valor', 'monto', 'saldo', 'avaluo', 'amount'])]
        
        for col in numeric_columns:
            try:
                # Convert to numeric, replacing errors with NaN
                corrected_df[col] = pd.to_numeric(corrected_df[col], errors='coerce')
            except Exception as e:
                self.logger.warning(f"Numeric conversion failed for column {col} in {subtype}: {str(e)}")
        
        return corrected_df
    
    def _apply_business_corrections(self, df: pd.DataFrame, subtype: str, context: TransformationContext, source_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Apply AT12-specific business rule corrections for Phase 1.
        
        Args:
            df: DataFrame to correct
            subtype: Data subtype
            context: Transformation context
            source_data: Dictionary of all source dataframes for lookups
            
        Returns:
            DataFrame with business corrections applied
        """
        if subtype != 'BASE_AT12':
            return df

        corrected_df = df.copy()

        # 1.1. EEOR TABULAR: Whitespace Errors
        for col in corrected_df.select_dtypes(include=['object']).columns:
            if corrected_df[col].dtype == 'object':
                corrected_df[col] = corrected_df[col].str.strip()
                corrected_df[col] = corrected_df[col].str.replace(r'\s+', ' ', regex=True)

        # 1.2. Error 0301: Id_Documento Logic for Mortgage Guarantees
        if 'Tipo_Garantia' in corrected_df.columns and 'Id_Documento' in corrected_df.columns:
            mask_0301 = corrected_df['Tipo_Garantia'] == '0301'
            doc_col = corrected_df.loc[mask_0301, 'Id_Documento'].astype(str)

            # Sub-rule 1
            mask_sub1 = doc_col.str.slice(8, 10).isin(['01', '41', '42']) & (doc_col.str.len() > 10)
            corrected_df.loc[mask_0301 & mask_sub1, 'Id_Documento'] = doc_col[mask_sub1].str.slice(-10)

            # Sub-rule 3
            mask_sub3 = doc_col.str.slice(0, 3).isin(['100', '110', '120', '123', '810'])
            corrected_df.loc[mask_0301 & mask_sub3 & (doc_col.str.len() > 11), 'Id_Documento'] = doc_col[mask_sub3 & (doc_col.str.len() > 11)].str.slice(0, 11)
            corrected_df.loc[mask_0301 & mask_sub3 & (doc_col.str.len() < 11), 'Id_Documento'] = doc_col[mask_sub3 & (doc_col.str.len() < 11)].str.pad(width=11, side='right', fillchar='0')

        # 1.3. COMA EN FINCA EMPRESA
        if 'Id_Documento' in corrected_df.columns:
            corrected_df['Id_Documento'] = corrected_df['Id_Documento'].str.replace(',', '', regex=False)

        # 1.4. Fecha Cancelación Errada
        if 'Fecha_Vencimiento' in corrected_df.columns:
            corrected_df['Fecha_Vencimiento'] = pd.to_datetime(corrected_df['Fecha_Vencimiento'], errors='coerce')
            mask = (corrected_df['Fecha_Vencimiento'].dt.year > 2100) | (corrected_df['Fecha_Vencimiento'].dt.year < 1985)
            corrected_df.loc[mask, 'Fecha_Vencimiento'] = pd.to_datetime('2100-12-01')

        # 1.5. Fecha Avalúo Errada
        if 'Fecha_Ultima_Actualizacion' in corrected_df.columns and 'AT03_CREDITOS' in source_data:
            at03_df = source_data['AT03_CREDITOS']
            if not at03_df.empty and 'Numero_Prestamo' in corrected_df.columns and 'num_cta' in at03_df.columns and 'fec_ini_prestamo' in at03_df.columns:
                corrected_df['Fecha_Ultima_Actualizacion'] = pd.to_datetime(corrected_df['Fecha_Ultima_Actualizacion'], errors='coerce')
                last_day_prev_month = pd.to_datetime(context.period, format='%Y%m') - pd.DateOffset(days=1)
                mask = (corrected_df['Fecha_Ultima_Actualizacion'] > last_day_prev_month) | \
                       (corrected_df['Fecha_Ultima_Actualizacion'].dt.year < 1985) | \
                       (corrected_df['Fecha_Ultima_Actualizacion'].isna())
                
                merged_df = corrected_df.merge(at03_df[['num_cta', 'fec_ini_prestamo']], left_on='Numero_Prestamo', right_on='num_cta', how='left')
                corrected_df.loc[mask, 'Fecha_Ultima_Actualizacion'] = pd.to_datetime(merged_df['fec_ini_prestamo'], errors='coerce')

        # 1.6. Inmuebles sin Póliza
        if 'POLIZA_HIPOTECAS_AT12' in source_data and 'Tipo_Garantia' in corrected_df.columns and 'Tipo_Poliza' in corrected_df.columns:
            poliza_df = source_data['POLIZA_HIPOTECAS_AT12']
            if not poliza_df.empty and 'Numero_Prestamo' in corrected_df.columns and 'numcred' in poliza_df.columns and 'seguro_incendio' in poliza_df.columns:
                mask_0207 = (corrected_df['Tipo_Garantia'] == '0207') & corrected_df['Tipo_Poliza'].isna()
                merged_df = corrected_df[mask_0207].merge(poliza_df, left_on='Numero_Prestamo', right_on='numcred', how='left')
                if not merged_df.empty:
                     corrected_df.loc[merged_df.index, 'Tipo_Poliza'] = merged_df['seguro_incendio']

            mask_0208 = (corrected_df['Tipo_Garantia'] == '0208') & corrected_df['Tipo_Poliza'].isna()
            corrected_df.loc[mask_0208, 'Tipo_Poliza'] = '01'

        # 1.7. Inmuebles sin Finca
        if 'Tipo_Garantia' in corrected_df.columns and 'Id_Documento' in corrected_df.columns:
            invalid_values = ["0/0", "1/0", "1/1", "1", "9999/1", "0/1", "0"]
            mask = corrected_df['Tipo_Garantia'].isin(['0207', '0208', '0209']) & \
                   (corrected_df['Id_Documento'].isin(invalid_values) | corrected_df['Id_Documento'].isna())
            corrected_df.loc[mask, 'Id_Documento'] = '9999/9999'

        # 1.8. Póliza Auto Comercial
        if 'Tipo_Garantia' in corrected_df.columns and 'Nombre_Organismo' in corrected_df.columns:
            mask = (corrected_df['Tipo_Garantia'] == '0106') & (corrected_df['Nombre_Organismo'].isna())
            corrected_df.loc[mask, 'Nombre_Organismo'] = '700'

        # 1.9. Error en Póliza de Auto
        if 'GARANTIA_AUTOS_AT12' in source_data and 'Tipo_Garantia' in corrected_df.columns and 'Id_Documento' in corrected_df.columns:
            garantia_autos_df = source_data['GARANTIA_AUTOS_AT12']
            if not garantia_autos_df.empty and 'Numero_Prestamo' in corrected_df.columns and 'numcred' in garantia_autos_df.columns and 'num_poliza' in garantia_autos_df.columns:
                mask = (corrected_df['Tipo_Garantia'] == '0101') & corrected_df['Id_Documento'].isna()
                merged_df = corrected_df[mask].merge(garantia_autos_df, left_on='Numero_Prestamo', right_on='numcred', how='left')
                if not merged_df.empty:
                    corrected_df.loc[merged_df.index, 'Id_Documento'] = merged_df['num_poliza']

        # 1.10. Inmueble sin Avaluadora
        if 'Tipo_Garantia' in corrected_df.columns and 'Nombre_Organismo' in corrected_df.columns:
            mask = corrected_df['Tipo_Garantia'].isin(['0207', '0208', '0209']) & (corrected_df['Nombre_Organismo'].isna())
            corrected_df.loc[mask, 'Nombre_Organismo'] = '774'

        return corrected_df
    
    def _process_tdc_data(self, df: pd.DataFrame, context: TransformationContext, result: TransformationResult, source_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Process TDC_AT12 specific transformations.
        
        Args:
            df: TDC DataFrame
            context: Transformation context
            result: Result object to update
            source_data: Dictionary of all source dataframes for lookups
            
        Returns:
            Processed TDC DataFrame
        """
        self.logger.info("Processing TDC_AT12 data")
        processed_df = df.copy()

        # 2.1. Número_Garantía Generation
        if 'Número_Garantía' in processed_df.columns:
            mask_empty_garantia = processed_df['Número_Garantía'].isna()
            if mask_empty_garantia.any():
                df_to_update = processed_df[mask_empty_garantia].copy()
                df_to_update['unique_key'] = df_to_update['Id_Documento'].astype(str) + df_to_update['Numero_Prestamo'].astype(str) + df_to_update['Tipo_Facilidad'].astype(str)

                unique_keys_to_number = df_to_update['unique_key'].unique()
                
                start_id = 855500
                new_guarantee_ids = range(start_id, start_id + len(unique_keys_to_number))
                key_to_id_map = dict(zip(unique_keys_to_number, new_guarantee_ids))
                
                new_garantias = df_to_update['unique_key'].map(key_to_id_map)
                processed_df.loc[mask_empty_garantia, 'Número_Garantía'] = new_garantias

        # 2.1. Date Mapping
        if 'AT02_CUENTAS' in source_data:
            at02_df = source_data['AT02_CUENTAS']
            if 'Identificacion_cliente' in processed_df.columns and 'Identificacion_Cuenta' in processed_df.columns and \
               'Identificacion_cliente' in at02_df.columns and 'Identificacion_Cuenta' in at02_df.columns:
                merged_df = processed_df.merge(at02_df, on=['Identificacion_cliente', 'Identificacion_Cuenta'], how='left')
                if 'Fecha_Apertura' in processed_df.columns and 'Fecha_Apertura_cuenta' in merged_df.columns:
                    processed_df['Fecha_Apertura'] = merged_df['Fecha_Apertura_cuenta']

        return processed_df

    def _process_sobregiro_data(self, df: pd.DataFrame, context: TransformationContext, result: TransformationResult, source_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Process SOBREGIRO_AT12 specific transformations."""
        self.logger.info("Processing SOBREGIRO_AT12 data")
        processed_df = df.copy()

        # 2.2. Merge with BASE_AT12
        if 'BASE_AT12' in source_data:
            base_at12_df = source_data['BASE_AT12']
            if 'Numero_Prestamo' in processed_df.columns and 'Numero_Prestamo' in base_at12_df.columns:
                merged_df = processed_df.merge(base_at12_df, on='Numero_Prestamo', how='inner', suffixes=('', '_base'))
                
                # Mapping fields
                processed_df['Fecha_Apertura'] = merged_df['Fecha_Apertura_base']
                processed_df['Fecha_Vencimiento'] = merged_df['Fecha_Vencimiento_base']
                processed_df['Valor_Inicial'] = merged_df['Valor_Inicial_base']
                processed_df['Saldo_Actual'] = merged_df['Saldo_Actual_base']
                processed_df['Tipo_Garantia'] = '0103'
                processed_df['Id_Documento'] = merged_df['Numero_Prestamo']
                processed_df['Numero_Garantia'] = 'SOB' + merged_df['Numero_Prestamo'].astype(str)

        return processed_df

    def _process_valores_data(self, df: pd.DataFrame, context: TransformationContext, result: TransformationResult, source_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Process VALORES_AT12 specific transformations."""
        self.logger.info("Processing VALORES_AT12 data")
        processed_df = df.copy()

        # 2.3. Merge with BASE_AT12
        if 'BASE_AT12' in source_data:
            base_at12_df = source_data['BASE_AT12']
            if 'Numero_Prestamo' in processed_df.columns and 'Numero_Prestamo' in base_at12_df.columns:
                merged_df = processed_df.merge(base_at12_df, on='Numero_Prestamo', how='inner', suffixes=('', '_base'))

                # Mapping fields
                processed_df['Fecha_Apertura'] = merged_df['Fecha_Apertura_base']
                processed_df['Fecha_Vencimiento'] = merged_df['Fecha_Vencimiento_base']
                processed_df['Valor_Inicial'] = merged_df['Valor_Inicial_base']
                processed_df['Saldo_Actual'] = merged_df['Saldo_Actual_base']
                processed_df['Tipo_Garantia'] = '0102'
                processed_df['Id_Documento'] = merged_df['Numero_Prestamo']
                processed_df['Numero_Garantia'] = 'VAL' + merged_df['Numero_Prestamo'].astype(str)

        return processed_df

    def _phase3_filter_fuera_cierre(self, df: pd.DataFrame, context: TransformationContext, result: TransformationResult, source_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Phase 3: Filters loans that are out of the closing cycle.
        This phase identifies and filters out loans that are not part of the current reporting period.
        """
        self.logger.info("Executing Phase 3: Filtering FUERA_CIERRE loans")
        
        if 'FUERA_CIERRE_AT12' not in source_data or source_data['FUERA_CIERRE_AT12'].empty:
            self.logger.warning("FUERA_CIERRE_AT12 data not found or is empty. Skipping Phase 3.")
            return df

        fuera_cierre_df = source_data['FUERA_CIERRE_AT12']
        
        if 'Numero_Prestamo' not in df.columns or 'prestamo' not in fuera_cierre_df.columns:
            self.logger.error("Required columns for Phase 3 are missing. Skipping.")
            return df

        # Identify loans to be excluded
        prestamos_to_exclude = fuera_cierre_df['prestamo'].unique()
        
        # Filter the DataFrame
        initial_rows = len(df)
        filtered_df = df[~df['Numero_Prestamo'].isin(prestamos_to_exclude)]
        final_rows = len(filtered_df)
        
        self.logger.info(f"Phase 3 completed. Filtered out {initial_rows - final_rows} loans.")
        
        return filtered_df

    def _phase4_valor_minimo_avaluo(self, df: pd.DataFrame, context: TransformationContext, result: TransformationResult, source_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Phase 4: Validates the minimum appraisal value for guarantees.
        This phase ensures that the guarantee value meets the required threshold based on the loan balance.
        """
        self.logger.info("Executing Phase 4: Validating VALOR_MINIMO_AVALUO")

        if 'VALOR_MINIMO_AVALUO_AT12' not in source_data or source_data['VALOR_MINIMO_AVALUO_AT12'].empty:
            self.logger.warning("VALOR_MINIMO_AVALUO_AT12 data not found or is empty. Skipping Phase 4.")
            return df

        valor_minimo_df = source_data['VALOR_MINIMO_AVALUO_AT12']
        
        if 'AT03_CREDITOS' not in source_data or source_data['AT03_CREDITOS'].empty:
            self.logger.warning("AT03_CREDITOS data not found or is empty. Skipping Phase 4.")
            return df
            
        at03_df = source_data['AT03_CREDITOS']

        # 1. Filter VALOR_MINIMO_AVALUO_AT12
        valor_minimo_df = valor_minimo_df[valor_minimo_df['nuevo_at_val_garantia'] > valor_minimo_df['nuevo_at_saldo_corte']]

        # 2. Join with AT03_CREDITOS
        if 'num_prestamo' in valor_minimo_df.columns and 'num_cta' in at03_df.columns:
            merged_df = valor_minimo_df.merge(at03_df, left_on='num_prestamo', right_on='num_cta', how='inner')
        else:
            self.logger.error("Required columns for merging in Phase 4 are missing.")
            return df

        # 3. Conditional Logic
        merged_df['saldo'] = merged_df.apply(
            lambda row: row['nuevo_at_saldo_corte'] if row['nuevo_at_saldo_corte'] > row['Saldo_Inicial'] else row['Saldo_Inicial'],
            axis=1
        )

        # 4. Final Comparison
        incident_mask = merged_df['saldo'] > merged_df['nuevo_at_val_garantia']
        incident_df = merged_df[incident_mask]

        if not incident_df.empty:
            # Here you would typically report the incidents
            self.logger.warning(f"Phase 4 found {len(incident_df)} incidents related to minimum appraisal value.")

        # The main DataFrame `df` is not modified in this phase, as it's a validation step.
        # Incidents are reported, but the records are not removed from the main flow.
        return df
    
    def _store_incidences(self, subtype: str, incidences: List[Dict], context: TransformationContext) -> None:
        """Store incidences for later reporting.
        
        Args:
            subtype: Data subtype
            incidences: List of incidence records
            context: Transformation context
        """
        if subtype not in self.incidences_data:
            self.incidences_data[subtype] = []
        
        self.incidences_data[subtype].extend(incidences)
        self.logger.info(f"Stored {len(incidences)} incidences for {subtype}")
    
    def _generate_outputs(self, context: TransformationContext, 
                         transformed_data: Dict[str, pd.DataFrame], 
                         result: TransformationResult) -> None:
        """Generate AT12 output files.
        
        Args:
            context: Transformation context
            transformed_data: Transformed data by subtype
            result: Result object to update
        """
        self.logger.info("Generating AT12 outputs")
        
        # Generate incidence files
        self._generate_incidence_files(context, result)
        
        # Generate processed CSV files
        self._generate_processed_files(context, transformed_data, result)
        
        # Generate consolidated TXT file
        self._generate_consolidated_file(context, transformed_data, result)
    
    def _generate_incidence_files(self, context: TransformationContext, result: TransformationResult) -> None:
        """Generate incidence CSV files.
        
        Args:
            context: Transformation context
            result: Result object to update
        """
        for subtype, incidences in self.incidences_data.items():
            if incidences:
                # Convert incidences to DataFrame
                incidences_df = pd.DataFrame(incidences)
                
                # Generate incidence filename
                incidence_filename = f"EEOO_TABULAR_{subtype}_AT12_{context.period}.csv"
                incidence_path = context.paths.get_incidencia_path(incidence_filename)
                
                # Save incidence file
                if self._save_dataframe_as_csv(incidences_df, incidence_path):
                    result.incidence_files.append(incidence_path)
                    self.logger.info(f"Generated incidence file: {incidence_path}")
    
    def _generate_processed_files(self, context: TransformationContext, 
                                transformed_data: Dict[str, pd.DataFrame], 
                                result: TransformationResult) -> None:
        """Generate processed CSV files.
        
        Args:
            context: Transformation context
            transformed_data: Transformed data by subtype
            result: Result object to update
        """
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
        """Generate consolidated TXT file.
        
        Args:
            context: Transformation context
            transformed_data: Transformed data by subtype
            result: Result object to update
        """
        try:
            # Generate consolidated filename using FilenameParser
            consolidated_filename = self._filename_parser.generate_output_filename(
                atom="AT12",
                year=int(context.year),
                month=int(context.month),
                run_id=context.run_id,
                extension="txt"
            )
            
            consolidated_path = context.paths.get_consolidated_path(consolidated_filename)
            
            # Combine all transformed data for consolidated output
            consolidated_records = []
            
            for subtype, df in transformed_data.items():
                if not df.empty:
                    # Convert DataFrame to consolidated format
                    # This is a placeholder - actual format depends on AT12 specifications
                    for _, row in df.iterrows():
                        record = f"{subtype}|{context.period}|" + "|".join(str(row[col]) for col in df.columns)
                        consolidated_records.append(record)
            
            # Write consolidated file
            if consolidated_records:
                consolidated_path.parent.mkdir(parents=True, exist_ok=True)
                
                with open(consolidated_path, 'w', encoding='utf-8') as f:
                    f.write("\n".join(consolidated_records))
                
                result.consolidated_file = consolidated_path
                self.logger.info(f"Generated consolidated file: {consolidated_path} ({len(consolidated_records)} records)")
            
        except Exception as e:
            error_msg = f"Failed to generate consolidated file: {str(e)}"
            result.errors.append(error_msg)
            self.logger.error(error_msg)