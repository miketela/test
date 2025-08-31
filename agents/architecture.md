# Agente de Arquitectura

## Propósito
- Salvaguardar capas, interfaces y artefactos del MVP v0.8.
- Mantener trazabilidad, auditabilidad y performance sin romper simplicidad.

## Alcance y Entradas
- Entradas: `context_files/architecture.md`, `technical_context.md`, `functional_context.md`.
- Capas: `main.py` (orquestador), `config.py` (config), `src/AT12/` (por átomo), `src/core/` (utilidades puras), `schemas/`.

## Responsabilidades Clave
- Definir y revisar interfaces públicas por átomo:
  - `discover_files(period)`, `explore(...)`, `transform(...)` en `src/AT12/processor.py`.
- Asegurar reglas de mapeo de headers:
  - AT02_CUENTAS: reemplazo directo por esquema.
  - Otros: normalización (acentos, mayúsculas, underscores).
- Configuración y precedencia: CLI > ENV > defaults (`config.py`).
- Observabilidad: logs estructurados, `metrics/*.json`, `reports/*.pdf`, versionado `__run-<id>`.
- Rendimiento: `CHUNK_ROWS`, paralelismo por subtipo, I/O secuencial con SHA256.

## Checklist de PRs
- Orquestación solo en `main.py`; lógica de negocio en módulos.
- Pureza de `src/core/` (sin dependencias cíclicas con átomos).
- Esquemas en `schemas/AT12/*.json` coherentes con validaciones.
- Artefactos generados en rutas esperadas (`data/raw`, `metrics`, `reports`, `logs`).

## Comandos Útiles
- Explorar: `python main.py explore --atoms AT12 --year 2025 --month 08`.
- Transformar: `python main.py transform --atoms AT12 --year 2025 --month 08`.
- Reporte: `python main.py report --metrics-file metrics/<file>.json`.
