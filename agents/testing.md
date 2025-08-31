# Agente de Testing

## Propósito
- Asegurar confiabilidad del flujo `explore` y `transform` con cobertura adecuada.

## Alcance y Entradas
- Entradas: `pytest.ini`, `tests/`, `context_files/technical_context.md` y `functional_context.md`.
- Tipos: unitarios (`tests/unit/`) e integración (`tests/integration/`).

## Estrategia
- Marcadores: `unit`, `integration`, `slow`, `requires_data`.
- Convenciones: archivos `test_*.py`, clases `Test*`, funciones `test_*`.
- Foco en invariantes: coherencia de período, duplicados, mapeo de headers, artefactos generados, códigos de salida.

## Cobertura y Calidad
- Cobertura sugerida: `pytest --cov=src --cov-report=term-missing`.
- Validar rutas y artefactos: `data/raw`, `metrics/*.json`, `reports/*.pdf`, `logs/`.
- Tests con datos sintéticos y fixtures en `tests/fixtures/` (evitar datos reales).

## Comandos Útiles
- Todo: `pytest -v`.
- Unitarios: `pytest -m unit`.
- Integración: `pytest -m integration`.
- Sin pruebas lentas: `pytest -m 'not slow'`.

## Checklist de PRs
- Nuevas reglas → pruebas unitarias y de integración.
- CI local: `black .`, `flake8 src tests`, `mypy src` antes de abrir PR.
