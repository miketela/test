# Open Items & Pending Definitions (MVP v0.7)

## ✅ COMPLETADO - Flujo Explore
- ✅ Implementación completa del flujo `explore` para AT12
- ✅ Soporte dual para archivos CSV y XLSX
- ✅ Suite de pruebas completa (70 passed, 2 skipped)
- ✅ Generación de reportes PDF con estadísticas detalladas
- ✅ Versionado de archivos con run_id
- ✅ Métricas de calidad de datos en formato JSON

---

## 🔄 PENDIENTE - Flujo Transform (Prioridad Alta)

### 1) Transformation — Consolidated TXT Layout
- Define **delimiter** (proposal: `,`) and quoting rules for the final TXT.
- Define **column order & names** for the monthly consolidated output.
- Agree on **normalizations** (trims, uppercase, null encoding).
- Decide on **header line** and/or metadata (e.g., run timestamp) in the TXT.

### 2) Join & Data Quality Rules
- **Join keys** across AT12 subtypes (e.g., `ID_CLIENTE`, `ID_CUENTA`, `FECHA_CORTE`).
- Default join type (proposal: `LEFT` from `BASE_AT12`).
- **Deduplication** criteria and **priority** when conflicts exist.
- **Derived fields** (aggregations, flags, buckets, etc.).
- **Pre-filters** (e.g., valid statuses, amounts > 0).

### 3) Transform Implementation
- Implementar método `transform()` en `AT12Processor`
- Definir estructura de salida consolidada
- Crear tests de integración para el flujo transform
- Documentar el proceso de transformación

---

## 📋 ITEMS MENORES (Prioridad Baja)

### 4) Header Schemas (opcional)
- Complete `schemas/AT12/schema_headers.json` with **columns per subtype**.
- Align whether **order** is strict (`order_strict: true`) for all subtypes.

### 5) Security & PII
- Currently **no masking** in PDFs and logs.
- Review with compliance whether masking (e.g., last 4 digits) is required before production.
- Define folder **permissions** (OS/ACLs) and backup handling.

### 6) Monitoring & Ops
- Do we need a **run SLA** metric (target runtime)?
- **Alerting** (email/Slack) on `exit code 1` or `2`?
- Rotation policy and cleanup beyond 24 months.

### 7) AT03 (separate track)
- Complete `schemas/AT03/expected_files.json` and `schema_headers.json` (currently `draft`).
- Design its **transformation** (similar yet different).
- Decide whether it shares part of the consolidated view with AT12 or remains separate.

### 8) Future UI
- Define the orchestrator's **internal API** so a frontend can plug in (inputs, state, artifact paths).
- Decide on a per-run **state.json** format for the UI.
