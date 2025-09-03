import pandas as pd
from unittest.mock import MagicMock

from src.core.header_mapping import HeaderMapper
import json
from pathlib import Path


def test_standardize_dataframe_to_schema_sobregiro(tmp_path):
    # Load expected headers for SOBREGIRO_AT12 from schema
    schema_path = Path('schemas/AT12/schema_headers.json')
    with open(schema_path, 'r', encoding='utf-8') as f:
        schema = json.load(f)
    expected = list(schema['SOBREGIRO_AT12'].keys())

    # Messy input headers: casing/accents variations and an extra column
    df_in = pd.DataFrame({
        'fecha': ['20250831'],
        'CODIGO_BANCO': ['24'],
        'Numero_Prestamo': ['0001234567'],
        'Valor_Ponderado': ['1,23'],  # different case than schema's 'valor_ponderado'
        'Id_Documento': ['ABC'],
        'EXTRA_COL': ['X']
    })

    out = HeaderMapper.standardize_dataframe_to_schema(df_in, 'SOBREGIRO_AT12', expected)

    # Columns exactly match expected (order preserved)
    assert list(out.columns) == expected
    # Values that can be mapped are preserved
    assert out.loc[0, 'Fecha'] == '20250831'
    assert out.loc[0, 'Codigo_Banco'] == '24'
    assert out.loc[0, 'Numero_Prestamo'] == '0001234567'
    assert out.loc[0, 'Id_Documento'] == 'ABC'
    # valor_ponderado should map (normalized/fuzzy) to Valor_Ponderado
    assert out.loc[0, 'valor_ponderado'] == '1,23'
    # Extra column dropped
    assert 'EXTRA_COL' not in out.columns
