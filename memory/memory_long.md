# Memoria a Largo Plazo: Proyecto SBP Atoms Pipeline

## 1. Visión General del Proyecto

Este proyecto implementa un pipeline de procesamiento de datos para "átomos" regulatorios de la SBP (Superintendencia de Banca, Seguros y AFP) (MVP v0.8). El objetivo es automatizar la ingesta, validación, análisis y reporte de datos regulatorios, comenzando con el átomo AT12. El sistema está diseñado para ser robusto, escalable y fácil de mantener, con un sistema avanzado de mapeo de headers para procesamiento robusto de archivos.

## 2. Arquitectura y Componentes Clave

El sistema sigue una arquitectura modular que separa las responsabilidades en componentes bien definidos.

### 2.1. Estructura de Directorios

La estructura del proyecto está organizada para separar el código fuente, las pruebas, los esquemas y los artefactos generados.

```
sbp-atoms/
├── src/                # Código fuente de la aplicación
│   ├── core/           # Utilidades y componentes compartidos (config, log, fs, io, etc.)
│   └── AT12/           # Lógica de negocio específica para el procesador AT12
├── tests/              # Pruebas automatizadas
│   ├── unit/           # Pruebas unitarias para componentes aislados
│   └── integration/    # Pruebas de integración para flujos de trabajo completos
├── schemas/            # Esquemas de datos (JSON Schema) y reglas de validación
├── memory/             # Directorio para la memoria a corto y largo plazo
├── main.py             # Orquestador principal y punto de entrada (CLI)
└── ...                 # Otros directorios (data, logs, reports, etc.)
```

### 2.2. Componentes Principales

1.  **Orquestador (`main.py`):**
    *   Punto de entrada que maneja la interfaz de línea de comandos (CLI) usando `argparse`.
    *   Orquesta los dos flujos de trabajo principales: `explore` y `transform`.
    *   Inicializa la configuración y el logging para toda la aplicación.

2.  **Núcleo (`src/core/`):**
    *   **`config.py`**: Carga y gestiona la configuración desde un archivo `config.json` y permite la sobreescritura con variables de entorno.
    *   **`log.py`**: Configura un logger estandarizado para toda la aplicación.
    *   **`fs.py`**: Abstracciones para operaciones del sistema de archivos, como `find_files_by_pattern` y `copy_with_versioning`.
    *   **`io.py`**: Proporciona `StrictCSVReader` y `StrictCSVWriter` para manejar CSVs asegurando que las cabeceras y delimitadores son los esperados.
    *   **`metrics.py`**: `MetricsCalculator` que genera estadísticas detalladas sobre la calidad de los datos de un archivo.
    *   **`naming.py`**: `FilenameParser` que valida y extrae información de los nombres de archivo según las convenciones regulatorias (ej. `BASE_AT12_20240131.CSV`) y normalización de headers.
    *   **`header_mapping.py`**: Sistema avanzado de mapeo de headers con soporte para AT02_CUENTAS y normalización automática.
    *   **`reports.py`**: `PDFReportGenerator` que utiliza `reportlab` para crear reportes en PDF a partir de los resultados del análisis.
    *   **`time_utils.py`**: Funciones para resolver y formatear períodos de tiempo (ej. `202401`).

3.  **Procesador AT12 (`src/AT12/processor.py`):**
    *   Clase `AT12Processor` que encapsula toda la lógica de negocio para el átomo AT12.
    *   Su constructor (`__init__`) solo requiere un objeto de configuración.
    *   **Método `explore`**: Ejecuta un pipeline de 4 fases:
        1.  **Descubrimiento:** Utiliza `fs.find_files_by_pattern` para localizar archivos CSV en el directorio fuente.
        2.  **Validación:** Usa `naming.FilenameParser` para validar nombres de archivo y `io.StrictCSVReader` para verificar cabeceras y estructura.
        3.  **Versionado:** Copia los archivos validados a un directorio `data/raw` con un `run_id` único para trazabilidad.
        4.  **Análisis:** Emplea `metrics.MetricsCalculator` para generar un archivo JSON con métricas detalladas del proceso.
    *   **Método `transform`**: (Pendiente de implementación) Contendrá la lógica para transformar y consolidar los datos del átomo.

4.  **Generación de Reportes (`src/core/reports.py`):**
    *   La función `create_exploration_report` orquesta la creación del PDF.
    *   `PDFReportGenerator` es una clase que construye el documento sección por sección:
        *   **Página de Título:** Con el nombre del átomo, período y `run_id`.
        *   **Resumen Ejecutivo:** Métricas clave como total de archivos, registros y puntaje de calidad.
        *   **Análisis de Archivos:** Detalles de cada archivo procesado.
        *   **Calidad de Datos:** Gráficos (actualmente mockeados) sobre la calidad de los datos.
        *   **Análisis de Columnas:** Estadísticas por columna.
        *   **Apéndice Técnico:** Información sobre el entorno de ejecución.

## 3. Sistema de Header Mapping ✅

### Características Principales
- **Manejo Especializado de AT02_CUENTAS**: Reemplazo directo de headers con esquema predefinido
- **Normalización Estándar**: Eliminación de acentos, normalización de mayúsculas, limpieza de caracteres especiales
- **Reportes de Mapeo**: Trazabilidad detallada con transformaciones original→mapeado
- **Integración de Validación**: Headers validados contra esquemas esperados

### Implementación Técnica
- **Clase HeaderMapper**: Orquestador principal de mapeo
- **Clase HeaderNormalizer**: Utilidades de normalización de texto
- **AT02_CUENTAS_MAPPING**: 30 headers predefinidos para reemplazo directo
- **Estrategia de Fallback**: Normalización estándar para archivos no-AT02_CUENTAS

### Mejoras Implementadas
- **Fecha**: Enero 2025
- **Alcance**: Sistema completo de mapeo de headers con soporte AT02_CUENTAS
- **Cambios Clave**:
  - Creada clase `HeaderMapper` con manejo especializado AT02_CUENTAS
  - Implementado `HeaderNormalizer` con eliminación de acentos y limpieza de texto
  - Agregados reportes de mapeo comprensivos con trazas de auditoría
  - Actualizadas todas las pruebas para validar nueva funcionalidad de mapeo
  - Corregido error "mappings" en procesador AT12
  - **AT03 Schema Integration**: Agregado AT03 como subtipo AT12 para verificaciones (71 campos de crédito)

## 4. Flujos de Trabajo

### 3.1. Flujo `explore`

1.  **Ejecución:** `python main.py explore AT12 --year 2024 --month 1`
2.  **Orquestación:** `main.py` invoca a `AT12Processor.explore()`.
3.  **Procesamiento:** El procesador ejecuta las 4 fases (descubrimiento, validación, versionado, análisis).
4.  **Salida (Artefactos):**
    *   Un archivo de métricas en `metrics/exploration_metrics_AT12_..._run-....json`.
    *   Archivos versionados en `data/raw/`.
    *   Un reporte en `reports/exploration_metrics_..._report.pdf`.

### 3.2. Flujo `transform`

*   **Estado:** No implementado.
*   **Visión:** Este flujo tomará los datos crudos y validados del `explore`, aplicará reglas de negocio (joins, transformaciones, etc.) y generará un producto de datos consolidado.

## 4. Pruebas

El proyecto tiene una suite de pruebas robusta utilizando `pytest`.

*   **Pruebas Unitarias (`tests/unit/`):** Prueban cada componente de forma aislada. Se utilizan mocks extensivamente para simular dependencias (ej. sistema de archivos, configuraciones).
*   **Pruebas de Integración (`tests/integration/`):** Prueban el flujo de trabajo `explore` de extremo a extremo, asegurando que todos los componentes colaboren correctamente.
*   **Fixtures (`tests/conftest.py`, `tests/fixtures/`):** Proporcionan datos y configuraciones reutilizables para las pruebas, como `sample_config`, `temp_dir`, y `sample_metrics_data`.
*   **Estado Final de las Pruebas (✅ COMPLETADO):**
    *   **Suite Completa:** 70 passed, 2 skipped, 0 failed
    *   **Pruebas Unitarias:** Todas funcionando correctamente
    *   **Pruebas de Integración:** 10 passed - Flujo explore completamente validado
    *   **Cobertura:** Todos los componentes críticos cubiertos

## 5. Run Digest - Implementación Completada

### 5.1. Inputs y Configuración
*   **Período:** 2024-01 (year=2024, month=1)
*   **Run ID:** 202401__run-202401
*   **Formatos Soportados:** CSV y XLSX (dual format)
*   **Configuración:** Cargada desde config.json con soporte para variables de entorno

### 5.2. Decisiones Técnicas Durables
*   **Interfaz AT12Processor:** Constructor requiere diccionario de configuración
*   **Método explore:** Firma con tres parámetros (year, month, run_id)
*   **Logging:** setup_logging requiere string de nivel, no objeto Config
*   **Formato de Salida:** ProcessingResult dataclass para consistencia
*   **Detección de Formato:** UniversalFileReader para CSV/XLSX automático

### 5.3. Artefactos Finales
*   **Métricas:** exploration_metrics_AT12_202401__run-202401.json
*   **Reporte PDF:** exploration_metrics_AT12_202401__run-202401_report_with_stats.pdf
*   **Datos Versionados:** BASE_AT12_20240131__run-202401.CSV
*   **Resumen Excel:** exploration_metrics_AT12_202401__run-202401_summary.xlsx

### 5.4. Calidad y Validación
*   **Tests:** 100% de éxito en suite completa
*   **Integración:** Flujo explore completamente funcional
*   **Formatos:** CSV y XLSX validados y funcionando
*   **Reportes:** PDF con estadísticas detalladas generado exitosamente
*   **Sistema de Incidencias:** Completamente corregido y estandarizado (Enero 2025)

### 5.5. Esquemas AT12 - Actualización Reciente (2024)
*   **Expansión de Tipos:** 7 nuevos subtipos de archivos AT12 configurados
*   **TDC_AT12:** Garantías de tarjetas de crédito (33 campos)
*   **VALORES_AT12:** Garantías de valores/títulos (27 campos)
*   **SOBREGIRO_AT12:** Garantías de sobregiros (27 campos)
*   **GARANTIA_AUTOS_AT12:** Garantías de autos (10 campos)
*   **POLIZA_HIPOTECAS_AT12:** Pólizas hipotecarias (7 campos)
*   **AFECTACIONES_AT12:** Afectaciones de préstamos (9 campos)
*   **VALOR_MINIMO_AVALUO_AT12:** Valores mínimos de avalúo (16 campos)
*   **AT03:** Datos de crédito requeridos para procesos de verificación AT12 (71 campos incluyendo garantías, provisiones y cronogramas de pago)
*   **Configuración:** Todos los esquemas incluidos en schema_headers.json y expected_files.json

### 5.6. Sistema de Incidencias AT12 - Correcciones Críticas (Enero 2025)
*   **Problema Resuelto:** Conflicto de métodos `_add_incidence` duplicados causando tipos de incidencia incorrectos
*   **Estandarización:** Todas las validaciones AT12 ahora usan firma consistente: `incidence_type`, `severity`, `rule_id`, `description`, `data`
*   **Validaciones Funcionando:** Test `test_phase4_valor_minimo_avaluo_incidence` pasando correctamente
*   **Campos Correctos:** `VALIDATION_FAILURE`, `HIGH` severity, `rule_name` y `metadata` apropiados
*   **Integración IncidenceReporter:** Completamente integrado con sistema estándar de reporte

### 5.7. Próximos Pasos
*   **Transform Flow:** Pendiente de especificaciones funcionales
*   **Escalabilidad:** Arquitectura preparada para nuevos átomos (AT03, etc.)
*   **Mantenimiento:** Código documentado y completamente testeado
*   **Validación:** Probar procesamiento con los nuevos tipos de archivos AT12

### 5.8. Alineación Fase 1 AT12 con Especificación (Transform Context)
Se alinearon las reglas de la Fase 1 con `context_files/transform_context.md` y se estandarizaron los nombres de incidencias y joins:

- 1.6 Inmuebles sin Póliza (Tipo_Poliza)
  - `Tipo_Garantia='0208'` y `Tipo_Poliza` vacío → `Tipo_Poliza='01'`.
  - `Tipo_Garantia='0207'` y `Tipo_Poliza` vacío → JOIN `POLIZA_HIPOTECAS_AT12` (`Numero_Prestamo` ↔ `numcred`) y, si `seguro_incendio` ∈ {'01','02'}, asignar.
  - Incidencia: `INMUEBLES_SIN_TIPO_POLIZA` → `INC_INMUEBLES_SIN_TIPO_POLIZA_BASE_AT12_<PERIODO>.csv`.

- 1.7 Inmuebles sin Finca
  - `Tipo_Garantia` ∈ {'0207','0208','0209'} con `Id_Documento` vacío o en {"0/0","1/0","1/1","1","9999/1","0/1","0"} → `Id_Documento='99999/99999'`.
  - Incidencia: `INMUEBLES_SIN_FINCA` → `INC_INMUEBLES_SIN_FINCA_BASE_AT12_<PERIODO>.csv`.

- 1.8 Póliza Auto Comercial
  - `Tipo_Garantia='0106'` y `Nombre_Organismo` vacío → `Nombre_Organismo='700'`.
  - Incidencia: `AUTO_COMERCIAL_ORG_CODE` → `INC_AUTO_COMERCIAL_ORG_CODE_BASE_AT12_<PERIODO>.csv`.

- 1.9 Error en Póliza de Auto
  - `Tipo_Garantia='0101'` y `Id_Documento` vacío → JOIN `GARANTIA_AUTOS_AT12` (`Numero_Prestamo` ↔ `numcred`), `Id_Documento = num_poliza`.
  - Incidencia: `AUTO_NUM_POLIZA_FROM_GARANTIA_AUTOS` → `INC_AUTO_NUM_POLIZA_FROM_GARANTIA_AUTOS_BASE_AT12_<PERIODO>.csv`.

- 1.10 Inmueble sin Avaluadora
  - `Tipo_Garantia` ∈ {'0207','0208','0209'} y `Nombre_Organismo` vacío → `Nombre_Organismo='774'`.
  - Incidencia: `INMUEBLE_SIN_AVALUADORA_ORG_CODE` → `INC_INMUEBLE_SIN_AVALUADORA_ORG_CODE_BASE_AT12_<PERIODO>.csv`.

Notas:
- Todas las incidencias exportan subset de filas completas con las columnas originales.
- Los JOINs usan las claves exactas detalladas arriba; si el dataset auxiliar no está presente, se omite la corrección y se registra warning.
