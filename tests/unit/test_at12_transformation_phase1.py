import pandas as pd
import pytest
from unittest.mock import MagicMock

from src.AT12.transformation import AT12TransformationEngine
from src.core.transformation import TransformationContext
from src.core.paths import AT12Paths
from src.core.incidence_reporter import IncidenceReporter

@pytest.fixture
def engine_and_context(tmp_path):
    # Paths
    base_dir = tmp_path / "transforms"
    incidencias_dir = tmp_path / "incidencias"
    procesados_dir = tmp_path / "procesados"
    paths = AT12Paths(
        base_transforms_dir=base_dir,
        incidencias_dir=incidencias_dir,
        procesados_dir=procesados_dir
    )
    paths.ensure_directories()

    # Minimal config mock
    config = MagicMock()
    config.get.return_value = {}

    # Engine
    engine = AT12TransformationEngine(config=config)

    # Context
    context = TransformationContext(
        run_id="test_run",
        period="20240131",
        config=config,
        paths=paths,
        source_files=[],
        logger=MagicMock()
    )

    # Attach incidence reporter for rules that use it
    engine.incidence_reporter = IncidenceReporter(
        config=config,
        run_id=context.run_id,
        period=context.period
    )

    return engine, context

def test_rule_16_inmuebles_sin_poliza(engine_and_context):
    engine, context = engine_and_context
    # Base with 0207 and 0208 without Tipo_Poliza
    df = pd.DataFrame({
        'Tipo_Garantia': ['0207', '0208', '0207'],
        'Numero_Prestamo': ['P001', 'P002', 'P003'],
        'Tipo_Poliza': ['', '', '']
    })
    # POLIZA_HIPOTECAS_AT12 with mapping for P001 and P003
    hip = pd.DataFrame({
        'numcred': ['P001', 'P003'],
        'seguro_incendio': ['02', '01']
    })
    source_data = {'POLIZA_HIPOTECAS_AT12': hip}
    out = engine._apply_inmuebles_sin_poliza_correction(df, context, source_data)
    # 0208 should be '01'
    assert out.loc[1, 'Tipo_Poliza'] == '01'
    # 0207 from join
    assert out.loc[0, 'Tipo_Poliza'] == '02'
    assert out.loc[2, 'Tipo_Poliza'] == '01'

def test_rule_17_inmuebles_sin_finca(engine_and_context):
    engine, context = engine_and_context
    df = pd.DataFrame({
        'Tipo_Garantia': ['0207', '0208', '0209', '9999'],
        'Id_Documento': ['', '0/0', '1', '']
    })
    out = engine._apply_inmuebles_sin_finca_correction(df, context)
    # For 0207/0208/0209, invalid/empty become '99999/99999'
    assert (out.loc[0, 'Id_Documento'] == '99999/99999')
    assert (out.loc[1, 'Id_Documento'] == '99999/99999')
    assert (out.loc[2, 'Id_Documento'] == '99999/99999')
    # 9999 (no aplica) remains
    assert (out.loc[3, 'Id_Documento'] == '')

def test_rule_18_auto_comercial_org_code(engine_and_context):
    engine, context = engine_and_context
    df = pd.DataFrame({
        'Tipo_Garantia': ['0106', '0106', '0207'],
        'Nombre_Organismo': ['', '  ', 'X']
    })
    out = engine._apply_poliza_auto_comercial_correction(df, context)
    assert out.loc[0, 'Nombre_Organismo'] == '700'
    assert out.loc[1, 'Nombre_Organismo'] == '700'
    # Not applicable to 0207
    assert out.loc[2, 'Nombre_Organismo'] == 'X'

def test_rule_19_auto_num_poliza_from_join(engine_and_context):
    engine, context = engine_and_context
    df = pd.DataFrame({
        'Tipo_Garantia': ['0101', '0103', '0106'],
        'Numero_Prestamo': ['A001', 'A002', 'A003'],
        'Id_Documento': ['', ' ', '']
    })
    autos = pd.DataFrame({
        'numcred': ['A001', 'A002'],
        # A001 alphanumeric policy -> now should update; A002 numeric -> should update
        'num_poliza': ['POL-1', '12345']
    })
    source_data = {'GARANTIA_AUTOS_AT12': autos}
    out = engine._apply_error_poliza_auto_correction(df, context, source_data)
    # A001 (0101): alphanumeric policy -> update per new rule
    assert out.loc[0, 'Id_Documento'] == 'POL-1'
    # A002 (0103): numeric policy -> update
    assert out.loc[1, 'Id_Documento'] == '12345'
    # Not applicable to 0106
    assert out.loc[2, 'Id_Documento'] == ''

def test_rule_110_inmueble_sin_avaluadora_org_code(engine_and_context):
    engine, context = engine_and_context
    df = pd.DataFrame({
        'Tipo_Garantia': ['0207', '0208', '0209', '0106'],
        'Nombre_Organismo': ['', None, ' ', '']
    })
    out = engine._apply_inmueble_sin_avaluadora_correction(df, context)
    assert out.loc[0, 'Nombre_Organismo'] == '774'
    assert out.loc[1, 'Nombre_Organismo'] == '774'
    assert out.loc[2, 'Nombre_Organismo'] == '774'
    # Not applicable to 0106
    assert out.loc[3, 'Nombre_Organismo'] == ''
