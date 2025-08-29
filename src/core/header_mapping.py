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
        
        # For other subtypes, normalize headers
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