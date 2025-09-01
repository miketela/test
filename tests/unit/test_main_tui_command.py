import sys


def test_main_tui_invokes_loader(monkeypatch):
    import main as app

    called = {"ok": False}

    def fake_tui_main():
        called["ok"] = True

    monkeypatch.setattr(app, "_load_tui_main", lambda: fake_tui_main)
    monkeypatch.setattr(sys, "argv", ["main.py", "tui"])  # simulate CLI
    rc = app.main()
    assert rc == 0
    assert called["ok"] is True

