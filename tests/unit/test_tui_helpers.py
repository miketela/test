import builtins
from pathlib import Path

import types


def test_extract_and_infer(monkeypatch, tmp_path):
    from scripts import tui

    assert tui.extract_date_from_name("BASE_AT12_20250701.csv") == (2025, 7)
    assert tui.infer_subtype("TDC_AT12_20250701__run-202507.csv") == "TDC_AT12"


def test_find_latest_run_in_raw(monkeypatch, tmp_path):
    from scripts import tui

    raw = tmp_path / "data" / "raw"
    raw.mkdir(parents=True)
    # Create two runs
    (raw / "BASE_AT12_20240101__run-202401.csv").write_text("x")
    (raw / "BASE_AT12_20250701__run-202507.csv").write_text("x")

    monkeypatch.setattr(tui, "RAW_DIR", raw)
    latest = tui.find_latest_run_in_raw()
    assert latest == (2025, 7)


def test_collect_and_clean_outputs(monkeypatch, tmp_path):
    from scripts import tui

    # Create fake project structure under temp root
    project_root = tmp_path
    base = project_root / "data" / "processed" / "transforms" / "AT12"
    (base / "incidencias").mkdir(parents=True)
    (base / "procesados").mkdir(parents=True)
    (base / "consolidated").mkdir(parents=True)
    (base / "state").mkdir(parents=True)
    (project_root / "metrics").mkdir(parents=True)

    # Create some files to delete
    for p in [
        base / "incidencias" / "a.csv",
        base / "procesados" / "b.xlsx",
        base / "consolidated" / "c.TXT",
        base / "state" / "seq.json",
        project_root / "metrics" / "m.json",
    ]:
        p.write_text("test")

    # Point TUI PROJECT_ROOT to our temp project
    monkeypatch.setattr(tui, "PROJECT_ROOT", project_root)

    files = tui._collect_output_files()
    # Expect to collect 5 files
    assert len(files) == 5

    # Auto-confirm deletion
    monkeypatch.setattr(tui, "prompt_confirm", lambda *a, **k: True)
    tui.action_clean()

    # All files should be gone
    for p in files:
        assert not p.exists()
