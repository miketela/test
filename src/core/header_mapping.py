#!/usr/bin/env python3
"""
Header mapping utilities for SBP Atoms Pipeline.
Handles specific header mappings for different file types.
"""

from typing import Dict, List, Optional, Union
from .naming import HeaderNormalizer


class HeaderMapper:
    """Maps headers from input format to expected schema format."""
    
    # Mapping for AT02_CUENTAS from input format to normalized schema format
    AT02_CUENTAS_MAPPING = [
        'Fecha', 'Cod_banco', 'Cod_Subsidiaria', 'Tipo_Deposito', 'Tipo_Cliente',
        'Tasa', 'Origen', 'Cod_region', 'Fecha_Inicio', 'Fecha_Vencimiento',
        'Monto', 'Monto_Pignorado', 'Numero_renovacion', 'Fecha_Renovacion',
        'Intereses_por_Pagar', 'Periodicidad_pago_intereses', 'Identificacion_cliente',
        'Identificacion_Cuenta', 'Actividad', 'Tamano_Empresa', 'Genero',
        'Beneficiario_declarado', 'Estatus_actividad_movimiento', 'Identificacion_cliente_2',
        'Tipo_Producto', 'Subproducto', 'Fecha_proceso', 'Moneda', 'Importe',
        'Importe_por_pagar', 'Segmento'
    ]
    
    # Expected headers for TDC_AT12 as defined by user-provided headers.
    # Preserve spaces and accents exactly as provided for validation.
    TDC_AT12_EXPECTED = [
        'Fecha',
        'Código_Banco',
        'Número_Préstamo',
        'Número_Ruc_Garantía',
        'Id_Fideicomiso',
        'Nombre_Fiduciaria',
        'Origen_Garantía',
        'Tipo_Garantía',
        'Tipo_Facilidad',
        'Id_Documento',
        'Nombre_Organismo',
        'Valor_Inicial',
        'Valor_Garantía',
        'Valor_Ponderado',
        'Tipo_Instrumento',
        'Calificación_Emisor',
        'Calificación_Emisión',
        'País_Emisión',
        'Fecha_Última_Actualización',
        'Fecha_Vencimiento',
        'Tipo_Poliza',
        'Código_Región',
        'num_garantía',
        'Número_Cis_Garantía',
        'Moneda',
        'Importe',
        'Descripción de la Garantía'
    ]

    # Build a normalization helper (uppercase normalized form) for consistent lookups
    @staticmethod
    def _norm_key(name: str) -> str:
        return HeaderNormalizer.normalize_headers([name])[0].upper()

    # Base auto-map from normalized expected -> expected (with accents)
    TDC_AT12_BASE_MAP: Dict[str, str] = {
        HeaderNormalizer.normalize_headers([x])[0].upper(): x for x in TDC_AT12_EXPECTED
    }

    # Additional synonyms and common abbreviations found in TDC inputs
    TDC_AT12_SYNONYMS: Dict[str, str] = {
        # Abbreviations
        'COD_BANCO': 'Código_Banco',
        'CODIGO_BANCO': 'Código_Banco',
        'NUM_PRESTAMO': 'Número_Préstamo',
        'NUMERO_PRESTAMO': 'Número_Préstamo',
        'NUM_RUC_GARANTIA': 'Número_Ruc_Garantía',
        'NUMERO_RUC_GARANTIA': 'Número_Ruc_Garantía',
        'ORIGEN_GARANTIA': 'Origen_Garantía',
        'TIPO_GARANTIA': 'Tipo_Garantía',
        'VALOR_GARANTIA': 'Valor_Garantía',
        'CALIFICACION_EMISOR': 'Calificación_Emisor',
        'CALIFICACION_EMISION': 'Calificación_Emisión',
        'PAIS_EMISION': 'País_Emisión',
        'FECHA_ULTIMA_ACTUALIZACION': 'Fecha_Última_Actualización',
        'COD_REGION': 'Código_Región',
        'NUM_GARANTIA': 'num_garantía',
        'NUMERO_GARANTIA': 'num_garantía',
        'NUM_CIS_GARANTIA': 'Número_Cis_Garantía',
        'NUMERO_CIS_GARANTIA': 'Número_Cis_Garantía',
        'DESCRIPCION_DE_LA_GARANTIA': 'Descripción de la Garantía',
        # Direct pass-throughs in uppercase to be safe
        'ID_FIDEICOMISO': 'Id_Fideicomiso',
        'NOMBRE_FIDUCIARIA': 'Nombre_Fiduciaria',
        'TIPO_FACILIDAD': 'Tipo_Facilidad',
        'ID_DOCUMENTO': 'Id_Documento',
        'NOMBRE_ORGANISMO': 'Nombre_Organismo',
        'VALOR_INICIAL': 'Valor_Inicial',
        'VALOR_PONDERADO': 'Valor_Ponderado',
        'TIPO_INSTRUMENTO': 'Tipo_Instrumento',
        'FECHA_VENCIMIENTO': 'Fecha_Vencimiento',
        'TIPO_POLIZA': 'Tipo_Poliza',
        'MONEDA': 'Moneda',
        'IMPORTE': 'Importe',
        'ACMON': 'ACMON',
        'ACIMP2': 'ACIMP2',
        'ACNGA': 'ACNGA',
        'ACCIS': 'ACCIS',
        'LIMITE': 'LIMITE',
        'SALDO': 'SALDO',
    }

    # Merge base map and synonyms into final mapping dict for TDC
    TDC_AT12_MAPPING: Dict[str, str] = {**TDC_AT12_BASE_MAP, **TDC_AT12_SYNONYMS}
    
    @staticmethod
    def get_mapping_for_subtype(subtype: str) -> Union[List[str], Dict[str, str]]:
        """Get header mapping for a specific subtype.
        
        Args:
            subtype: The subtype name (e.g., 'AT02_CUENTAS')
        
        Returns:
            List of headers for direct replacement (AT02_CUENTAS) or 
            Dictionary mapping input headers to normalized headers
        """
        if subtype == "AT02_CUENTAS":
            return HeaderMapper.AT02_CUENTAS_MAPPING.copy()
        if subtype == "TDC_AT12":
            return HeaderMapper.TDC_AT12_MAPPING.copy()
        return {}
    
    @staticmethod
    def map_headers(headers: List[str], subtype: str) -> List[str]:
        """Map headers from input format to schema format.
        
        Args:
            headers: Original headers from the file
            subtype: The subtype name
        
        Returns:
            Mapped headers according to the subtype mapping
        """
        mapping = HeaderMapper.get_mapping_for_subtype(subtype)
        
        if subtype == 'AT02_CUENTAS' and isinstance(mapping, list):
            # For AT02_CUENTAS, directly replace with schema headers
            return mapping[:len(headers)]
        
        if isinstance(mapping, dict) and mapping:
            # For dict mappings (e.g., TDC_AT12), map by normalized key → expected (accented) header
            mapped: List[str] = []
            for h in headers:
                key = HeaderMapper._norm_key(h)
                mapped.append(mapping.get(key, HeaderNormalizer.normalize_headers([h])[0]))
            return mapped
        
        # Fallback: normalized headers (no accents), uppercase for stability
        return [HeaderNormalizer.normalize_headers([header])[0].upper() for header in headers]
    
    @staticmethod
    def validate_mapped_headers(original_headers: List[str], subtype: str, 
                              expected_headers: List[str]) -> Dict[str, any]:
        """Validate headers after mapping.
        
        Args:
            original_headers: Original headers from file
            subtype: The subtype name
            expected_headers: Expected headers from schema
        
        Returns:
            Validation result dictionary
        """
        # Map the headers
        mapped_headers = HeaderMapper.map_headers(original_headers, subtype)
        
        # Validate against expected headers
        return HeaderNormalizer.validate_headers_against_schema(
            mapped_headers, expected_headers, order_strict=False
        )
    
    @staticmethod
    def get_mapping_report(headers: List[str], subtype: str) -> Dict[str, any]:
        """
        Generate a mapping report for headers.
        
        Args:
            headers: List of input headers
            subtype: The subtype to generate report for
            
        Returns:
            Dictionary with mapping statistics and detailed mappings
        """
        mapping = HeaderMapper.get_mapping_for_subtype(subtype)
        mappings_list = []
        
        if subtype == 'AT02_CUENTAS' and isinstance(mapping, list):
            # For AT02_CUENTAS, all mappings are direct replacements
            for i, original_header in enumerate(headers):
                if i < len(mapping):
                    mappings_list.append({
                        'original': original_header,
                        'mapped': mapping[i],
                        'method': 'direct'
                    })
            
            return {
                'total_headers': len(headers),
                'direct_mappings': len(headers),
                'normalized_mappings': 0,
                'mappings': mappings_list
            }
        
        if isinstance(mapping, dict) and mapping:
            # Report dict-based mappings (e.g., TDC_AT12)
            for header in headers:
                key = HeaderMapper._norm_key(header)
                if key in mapping:
                    mappings_list.append({
                        'original': header,
                        'mapped': mapping[key],
                        'method': 'dict'
                    })
                else:
                    normalized = HeaderNormalizer.normalize_headers([header])[0]
                    mappings_list.append({
                        'original': header,
                        'mapped': normalized,
                        'method': 'normalized'
                    })
            return {
                'total_headers': len(headers),
                'direct_mappings': 0,
                'normalized_mappings': len([m for m in mappings_list if m['method'] == 'normalized']),
                'mappings': mappings_list
            }
        
        # For other subtypes, all are normalized
        for header in headers:
            normalized = HeaderNormalizer.normalize_headers([header])[0].upper()
            mappings_list.append({
                'original': header,
                'mapped': normalized,
                'method': 'normalized'
            })
        
        return {
            'total_headers': len(headers),
            'direct_mappings': 0,
            'normalized_mappings': len(headers),
            'mappings': mappings_list
        }
