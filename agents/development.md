# Agente de Desarrollo

## Propósito
- Mantener calidad de código, estilo y productividad del equipo.

## Flujo de Trabajo
- Entorno: `python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`.
- Lint/format/types antes de PR: `black .`, `flake8 src tests`, `mypy src`.
- Commits: Conventional Commits (`feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`).
- Mensajes ejemplo: `feat(AT12): implement etapa 1 de limpieza`.

## Estilo y Convenciones
- Python 3.8+, PEP8, indentación 4 espacios.
- Nombres: archivos/módulos `snake_case.py`; clases `PascalCase`; funciones/vars `snake_case`.
- Tipos en funciones públicas; docstrings breves con propósito y efectos.

## Seguridad y Datos
- No subir PII real. Usar `tests/fixtures/` y datos sintéticos.
- Artefactos pesados permanecen en `data/`, `metrics/`, `reports/` (gitignored donde aplique).
- Config por ENV/CLI (`config.py`): `SOURCE_DIR`, `RAW_DIR`, `REPORTS_DIR`, `METRICS_DIR`, `LOGS_DIR`, `STRICT_PERIOD`.

## Validación Local
- Pruebas: `pytest -v` (o `pytest -m integration`).
- Reportes: `python main.py report --metrics-file metrics/<json>`.
- Salidas esperadas por run: métricas, PDF, logs, espejos en `data/raw`.

## Checklist de PRs
- Incluye pruebas y ejemplos de uso (comandos).
- Describe impacto en config/env y artefactos.
- Actualiza `schemas/` y `context_files/` si cambia el contrato.
