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
    df = pd.DataFrame(
        [
            # Non-0301 (ignored)
            {"Tipo_Garantia": "0207", "Id_Documento": "SHOULD_IGNORE"},
            # 15-char with positions 13-15 valid (110): valid, unchanged
            {"Tipo_Garantia": "0301", "Id_Documento": "123456789012110"},
            # >15 with positions 13-15 valid (120): truncate to 15
            {"Tipo_Garantia": "0301", "Id_Documento": "10000000000012099"},
            # Contains '701': valid, unchanged
            {"Tipo_Garantia": "0301", "Id_Documento": "123701456"},
            # Positions 9-10 == '41' (len=10): valid, unchanged
            {"Tipo_Garantia": "0301", "Id_Documento": "0000000041"},
            # Positions 9-10 == '01' and len > 10 but != 15: truncate to 10
            {"Tipo_Garantia": "0301", "Id_Documento": "999999990123"},
            # Edge reported by users: 15-char with '01' at 9-10, should not be truncated by R4
            {"Tipo_Garantia": "0301", "Id_Documento": "110220000142223"},
        ]
    )

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
    # >15 with 120 at 13-15 truncated to 15
    assert out.loc[2, "Id_Documento"] == "100000000000120"
    # 12-char with '01' at 9-10 truncated to 10
    assert out.loc[5, "Id_Documento"] == "9999999901"
    # 15-char with valid 110 unchanged
    assert out.loc[1, "Id_Documento"] == "123456789012110"
    # 15-char user example unchanged (no R4 truncation)
    assert out.loc[6, "Id_Documento"] == "110220000142223"

    # Validate modified export
    mod_path = context.paths.incidencias_dir / f"ERROR_0301_MODIFIED_{context.period}.csv"
    assert mod_path.exists(), "Modified export not generated"
    mod_df = pd.read_csv(mod_path, sep="|")

    # Ensure Id_Documento_ORIGINAL is adjacent to Id_Documento
    cols = list(mod_df.columns)
    assert "Id_Documento" in cols and "Id_Documento_ORIGINAL" in cols
    id_pos = cols.index("Id_Documento")
    assert cols[id_pos + 1] == "Id_Documento_ORIGINAL"

    # Ensure the two expected modified rows are present
    # Row with >15 (truncate to 15)
    assert (
        (mod_df["Id_Documento_ORIGINAL"].astype(str) == "10000000000012099").any()
        and (mod_df["Id_Documento"].astype(str) == "100000000000120").any()
    )
    # Row with >10 and '01' at 9-10 (truncate to 10)
    assert (
        (mod_df["Id_Documento_ORIGINAL"].astype(str) == "999999990123").any()
        and (mod_df["Id_Documento"].astype(str) == "9999999901").any()
    )

    # Validate columns tipo de error and transformacion exist in modified export
    assert "tipo de error" in mod_df.columns
    assert "transformacion" in mod_df.columns
