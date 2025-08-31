import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from src.AT12.transformation import AT12TransformationEngine
from src.core.transformation import TransformationContext, TransformationResult
from src.core.incidence_reporter import IncidenceType, IncidenceSeverity
from src.core.paths import AT12Paths
from src.core.naming import FilenameParser
from src.core.incidence_reporter import IncidenceReporter


@pytest.fixture
def mock_paths(tmp_path):
    """Create mock AT12Paths for testing."""
    base_dir = tmp_path / "transforms"
    incidencias_dir = tmp_path / "incidencias"
    procesados_dir = tmp_path / "procesados"
    paths = AT12Paths(
        base_transforms_dir=base_dir,
        incidencias_dir=incidencias_dir,
        procesados_dir=procesados_dir
    )
    paths.ensure_directories()
    return paths

@pytest.fixture
def sample_config():
    """Sample configuration for testing."""
    return {
        "csv_params": {
            "encoding": "utf-8",
            "delimiter": ","
        },
        "business_rules": {
            "valor_minimo_avaluo": 1000.0,
            "tdc_limite_credito_minimo": 500.0
        }
    }

@pytest.fixture
def mock_logger():
    """Mock logger for testing."""
    return Mock()

@pytest.fixture
def engine(mock_paths, sample_config, mock_logger):
    """Create AT12TransformationEngine instance for testing."""
    config = MagicMock()
    config.get.return_value = sample_config
    
    engine = AT12TransformationEngine(
        config=config
    )
    engine.logger = mock_logger
    engine.paths = mock_paths
    engine.period = "202401"
    return engine


def test_phase4_valor_minimo_avaluo_incidence(engine, mock_logger):
    """
    Test that _phase4_valor_minimo_avaluo correctly reports an incidence
    when saldo > nuevo_at_valor_garantia.
    """
    # Arrange
    df = pd.DataFrame({
        'Numero_Prestamo': ['P001'],
        'at_valor_garantia': [1000],
        'at_valor_pond_garantia': [800]
    })
    
    valor_minimo_df = pd.DataFrame({
        'cu_tipo': ['letras'],
        'at_num_de_prestamos': ['P001'],
        'nuevo_at_valor_garantia': [1500],
        'nuevo_at_valor_pond_garantia': [1200]
    })
    
    at03_df = pd.DataFrame({
        'num_cta': ['P001'],
        'id_cliente': ['C001'],
        'saldo': [2000]  # saldo > nuevo_at_valor_garantia
    })
    
    source_data = {
        'VALOR_MINIMO_AVALUO_AT12': valor_minimo_df,
        'AT03_CREDITOS': at03_df
    }
    
    context = TransformationContext(
        run_id="test_run",
        period="202401",
        config=engine.config,
        paths=engine.paths,
        source_files=[],
        logger=mock_logger
    )
    result = TransformationResult(success=True, processed_files=[], incidence_files=[], consolidated_file=None, metrics={}, errors=[], warnings=[])
    
    # Initialize IncidenceReporter
    engine.incidence_reporter = IncidenceReporter(
        config=engine.config,
        run_id=context.run_id,
        period=context.period
    )

    # Act
    processed_df = engine._phase4_valor_minimo_avaluo(df, context, result, source_data)

    # Assert
    incidences = engine.incidence_reporter.get_all_incidences()
    assert len(incidences) == 1
    incidence = incidences[0]
    assert incidence.incidence_type == IncidenceType.VALIDATION_FAILURE
    assert incidence.severity == IncidenceSeverity.HIGH
    assert "El saldo del préstamo excede el valor de la garantía." in incidence.description
    assert incidence.rule_name == 'VALOR_MINIMO_AVALUO_AT12'
    assert incidence.metadata['numero_prestamo'] == 'P001'
    assert incidence.metadata['saldo_adeudado'] == 2000
    assert incidence.metadata['valor_garantia'] == 1500