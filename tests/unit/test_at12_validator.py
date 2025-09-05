import json
from pathlib import Path
import pandas as pd

from src.AT12.validators import AT12Validator
from src.core.config import Config


def _mk_config(tmp_path: Path) -> Config:
    cfg = Config()
    cfg.base_dir = str(tmp_path)
    cfg.data_processed_dir = str(tmp_path / "data" / "processed")
    cfg.metrics_dir = str(tmp_path / "metrics")
    cfg.logs_dir = str(tmp_path / "logs")
    cfg.schemas_dir = str(tmp_path / "schemas")
    cfg.source_dir = str(tmp_path / "source")
    cfg.__post_init__()
    return cfg


def test_date_not_after_period_end_flags_future_dates(tmp_path):
    cfg = _mk_config(tmp_path)
    # Create processed file resembling pipeline naming
    proc_dir = Path(cfg.data_processed_dir) / "transforms" / "AT12" / "procesados"
    proc_dir.mkdir(parents=True, exist_ok=True)
    fp = proc_dir / "AT12_TDC_AT12_20240101.csv"

    df = pd.DataFrame({
        "Fecha_Inicio": ["20240131", "20240201"],
        "Monto": ["100,00", "200,00"],
    })
    df.to_csv(fp, index=False, encoding="utf-8")

    v = AT12Validator(cfg, year=2024, month=1, run_id="202401")
    res = v.validate_dates_not_after_period_end([fp])
    assert res.name == "DATE_NOT_AFTER_PERIOD_END"
    assert res.status == "FAIL"
    assert "subtypes" in res.details
    # Write summary to ensure JSON path builds
    out = v.write_summary([res])
    assert out.exists()
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["atom"] == "AT12"


def test_csv_alignment_mismatch_detects_bad_rows(tmp_path):
    cfg = _mk_config(tmp_path)
    raw_dir = Path(cfg.base_dir) / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    fp = raw_dir / "BROKEN_AT12_20240131.csv"
    fp.write_text("A,B,C\n1,2,3\n4,5\n7,8,9\n", encoding="utf-8")

    v = AT12Validator(cfg, year=2024, month=1, run_id="202401")
    res = v.validate_csv_alignment([fp])
    assert res.name == "CSV_WIDTH_MATCH"
    assert res.details["rows_flagged"] >= 1
    # Status should be WARN or FAIL depending on file counting; both acceptable
    assert res.status in {"WARN", "FAIL"}

