from src.core.header_mapping import HeaderMapper


def test_tdc_fuzzy_mapping_handles_misencoded_headers():
    original = [
        'Fecha ',
        'C�digo_Banco',
        'N�mero_Pr�stamo',
        'N�mero_Ruc_Garant�a',
        'Id_Fideicomiso',
        'Nombre_Fiduciaria',
        'Origen_Garant�a',
        'Tipo_Garant�a ',
        'Tipo_Facilidad',
        'Id_Documento',
    ]

    mapped = HeaderMapper.map_headers(original, 'TDC_AT12')
    assert mapped[0] == 'Fecha'
    assert 'Código_Banco' in mapped
    assert 'Número_Préstamo' in mapped
    assert 'Número_Ruc_Garantía' in mapped
    assert 'Origen_Garantía' in mapped
    assert 'Tipo_Garantía' in mapped

