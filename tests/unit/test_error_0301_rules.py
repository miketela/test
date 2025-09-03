import pandas as pd
from unittest.mock import MagicMock

from src.AT12.transformation import AT12TransformationEngine
from src.core.transformation import TransformationContext, TransformationResult
from src.core.paths import AT12Paths


def make_engine_and_context(tmp_path):
    base_dir = tmp_path / "transforms"
    incidencias_dir = tmp_path / "incidencias"
    procesados_dir = tmp_path / "procesados"
    paths = AT12Paths(
        base_transforms_dir=base_dir,
        incidencias_dir=incidencias_dir,
        procesados_dir=procesados_dir,
    )
    paths.ensure_directories()

    config = MagicMock()
    config.get.return_value = {}
    # Ensure delimiter used in exports is a real string
    setattr(config, 'output_delimiter', '|')

    engine = AT12TransformationEngine(config=config)
    context = TransformationContext(
        run_id="test_run",
        period="20240131",
        config=config,
        paths=paths,
        source_files=[],
        logger=MagicMock(),
    )
    return engine, context


def test_error_0301_cascade_dataset(tmp_path):
    engine, context = make_engine_and_context(tmp_path)

    # Build dataset with representative cases
    df = pd.DataFrame([
        # Non-0301 (ignored)
        {"Tipo_Garantia": "0207", "Id_Documento": "SHOULD_IGNORE"},
        # 15-char with positions 13-15 from right = '110' -> valid, unchanged
        {"Tipo_Garantia": "0301", "Id_Documento": "110220000142223"},
        # >15 with positions 13-15 from right = '120' -> truncate to rightmost 15
        {"Tipo_Garantia": "0301", "Id_Documento": "0120000000000000"},
        # '701' at positions 11-9 from right (len=11) -> valid (exclude, no export)
        {"Tipo_Garantia": "0301", "Id_Documento": "70100000000"},
        # '701' at positions 10-8 from right (len=10) -> valid but export as follow-up
        {"Tipo_Garantia": "0301", "Id_Documento": "7010000000"},
        # Positions 9-10 from right == '41' with len=10 -> valid
        {"Tipo_Garantia": "0301", "Id_Documento": "4100000000"},
        # Positions 9-10 from right == '01' and len>10 !=15 -> truncate to rightmost 10
        {"Tipo_Garantia": "0301", "Id_Documento": "990100000000"},
    ])

    # Prepare result container
    result = TransformationResult(
        success=False,
        processed_files=[],
        incidence_files=[],
        consolidated_file=None,
        metrics={},
        errors=[],
        warnings=[],
    )

    out = engine._apply_error_0301_correction(df.copy(), context, subtype="BASE_AT12", result=result)

    # Validate in-memory corrections
    # >15 with 120 (from right) truncated keeping rightmost 15
    assert (out['Id_Documento'] == "120000000000000").any()
    # With '01' at 9-10 from right and len>10 -> last 10 = '0100000000'
    assert (out['Id_Documento'] == "0100000000").any()
    # 15-char user example unchanged (R1 handles validation, no truncation)
    assert (out['Id_Documento'] == "110220000142223").any()

    # Validate modified export
    mod_path = context.paths.incidencias_dir / f"ERROR_0301_MODIFIED_{context.period}.csv"
    assert mod_path.exists(), "Modified export not generated"
    mod_df = pd.read_csv(mod_path, sep="|", dtype=str)

    # Ensure Id_Documento_ORIGINAL is adjacent to Id_Documento
    cols = list(mod_df.columns)
    assert "Id_Documento" in cols and "Id_Documento_ORIGINAL" in cols
    id_pos = cols.index("Id_Documento")
    assert cols[id_pos + 1] == "Id_Documento_ORIGINAL"

    # Ensure the two expected modified rows are present
    # Row with >15 (truncate to rightmost 15)
    assert (
        (mod_df["Id_Documento_ORIGINAL"].astype(str) == "0120000000000000").any()
        and (mod_df["Id_Documento"].astype(str) == "120000000000000").any()
    )
    # Row with >10 and '01' at 9-10 (from right) (truncate to rightmost 10)
    assert (
        (mod_df["Id_Documento_ORIGINAL"].astype(str) == "990100000000").any()
        and (mod_df["Id_Documento"].astype(str) == "0100000000").any()
    )

    # Validate columns tipo de error and transformacion exist in modified export
    assert "tipo de error" in mod_df.columns
    assert "transformacion" in mod_df.columns

    # Validate incidents export for 701 at 10-8 with len=10
    inc_path = context.paths.incidencias_dir / f"ERROR_0301_INCIDENTES_{context.period}.csv"
    assert inc_path.exists(), "Incidents export not generated"
    inc_df = pd.read_csv(inc_path, sep='|', dtype=str)
    assert (
        (inc_df['Id_Documento'] == '7010000000').any()
        and (inc_df['tipo de error'] == 'Secuencia 701 en posiciones 10-8 con longitud 10').any()
        and (inc_df['transformacion'] == 'Sin cambio').any()
    )
