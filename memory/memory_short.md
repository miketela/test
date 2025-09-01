# Memoria a Corto Plazo: Estado Actual del Proyecto

## Estado del Proyecto: ✅ FASE 1 AT12 ALINEADA + INCIDENCIAS ESTANDAR

**Run ID Actual:** `202401__run-202401`  
**Última Actualización:** Reglas Fase 1 (1.6–1.10) alineadas con Transform Context; incidencias y joins implementados

## Decisiones Técnicas Clave (STM)

**✅ Interfaz AT12Processor:**
- Constructor: `AT12Processor(config.to_dict())` - Requiere diccionario, no objeto Config
- Método explore: `explore(year, month, run_id)` - Tres parámetros obligatorios
- Retorna: `ProcessingResult` dataclass con atributos específicos

**✅ Configuración de Logging:**
- `setup_logging(config.log_level)` - Requiere string, no objeto Config
- Nivel por defecto: INFO

**✅ Sistema de Header Mapping:**
- **HeaderMapper**: Clase principal para mapeo inteligente de headers
- **HeaderNormalizer**: Normalización avanzada con eliminación de acentos
- **AT02_CUENTAS**: Manejo especializado con mapeo directo y posicional
- **AT03**: Nuevo subtipo integrado con 71 campos de crédito para verificaciones AT12
- **Integración**: Completamente integrado con AT12ProcessorInterface

**✅ Esquemas AT12 Expandidos:**
- 8 tipos de archivos configurados en schema_headers.json (incluyendo AT03)
- expected_files.json actualizado con todos los subtipos
- Soporte para TDC, VALORES, SOBREGIRO, GARANTIA_AUTOS, POLIZA_HIPOTECAS, AFECTACIONES, VALOR_MINIMO_AVALUO
- AT03 integrado con 71 campos para verificaciones de crédito

**✅ Soporte Dual de Formatos:**
- CSV y XLSX completamente implementado
- `UniversalFileReader` detecta formato automáticamente
- Schemas actualizados para ambos formatos

## Artefactos Generados (Pointers)

**Métricas:** `/metrics/exploration_metrics_AT12_202401__run-202401.json`  
**Reportes PDF:** `/reports/exploration_metrics_AT12_202401__run-202401_report_with_stats.pdf`  
**Datos Procesados:** `/data/raw/BASE_AT12_20240131__run-202401.CSV`

**✅ Fase 1 Alineada (Reglas Clave):**
- 1.6 `INMUEBLES_SIN_TIPO_POLIZA`: 0208 → '01'; 0207 JOIN `POLIZA_HIPOTECAS_AT12` (`Numero_Prestamo` ↔ `numcred`) usando `seguro_incendio` ('01'/'02').
- 1.7 `INMUEBLES_SIN_FINCA`: `Id_Documento='99999/99999'` (valores inválidos definidos).
- 1.8 `AUTO_COMERCIAL_ORG_CODE`: `Nombre_Organismo='700'` si `Tipo_Garantia='0106'`.
- 1.9 `AUTO_NUM_POLIZA_FROM_GARANTIA_AUTOS`: JOIN `GARANTIA_AUTOS_AT12` (`Numero_Prestamo` ↔ `numcred`) → `Id_Documento=num_poliza`.
- 1.10 `INMUEBLE_SIN_AVALUADORA_ORG_CODE`: `Nombre_Organismo='774'` para 0207/0208/0209.

## Tests - Estado Final

Tests unitarios agregados para 1.6–1.10 (archivo `tests/unit/test_at12_transformation_phase1.py`). Ejecutar con `pytest -m unit`.

Formato de incidencias por regla: `<REGLA>_<YYYYMMDD>.csv` (simplificado, sin prefijo).

## Próxima Acción

**Pendiente:** Ajustes menores Stage 2/3 según nuevas especificaciones; ejecutar `pytest -m unit` y validar con datos reales.
