# Plan de cierre Hito 4 y Hito 5

Proyecto: evaluacion de politicas de acceso a educacion superior en Colombia, 2018-2024  
Fecha de revision: 2026-05-25  
Repositorio base: version remota actualizada en `main` despues del pull.

## 1. Objetivo

Cerrar el proyecto final uniendo Hito 4 y Hito 5 en una sola entrega defendible. La meta no es rehacer el Hito 3, sino convertirlo en una entrega final con robustez, triangulacion, analitica/IA justificada, informe final, dashboard reproducible y paquete de replicacion.

Segun la rubrica:

- Hito 4 exige robustez completa, triangulacion y analitica/IA donde agregue valor.
- Hito 5 exige informe final, paquete de replicacion y presentacion ejecutiva neutral.

Como la fecha actual es 2026-05-25 y la rubrica marcaba el Hito 5 hasta mayo 15, el trabajo debe priorizar evidencia final y entregables concretos.

## 2. Estado actual del repo

El repo actualizado ya contiene una base fuerte para Hito 3:

- ETL versionado con `etl/pipeline.py`.
- Esquema estrella y documentacion en `docs/star_schema_design.md`.
- Analisis causal en `analysis/`: tendencias, ITS, DiD, event study y bootstrap.
- Dashboard Streamlit en `dashboard/app.py`.
- Notebook en `notebooks/hito3_analisis.ipynb`.
- Informe tecnico de Hito 3 en `Hito3_Informe_Tecnico.docx`.
- Airflow DAG para orquestacion.

La brecha principal para la entrega final esta en convertir esa base en evidencia final de Hito 4/5:

- resultados generados y congelados en `data/results/`;
- robustez sistematica como artefacto reproducible;
- triangulacion explicita con PND/SINERGIA e ICFES;
- un modelo de analitica/IA con validacion y baseline;
- matriz de trazabilidad oficial;
- paquete de replicacion para que otro equipo pueda ejecutar;
- informe final y presentacion ejecutiva neutral.

## 3. Diagnostico contra rubrica

| Criterio | Peso | Estado estimado | Evidencia actual | Brecha para cerrar |
|---|---:|---|---|---|
| A. Alcance, preguntas y teoria del cambio | 10 | Competente | `docs/metodologia_hito3.md`, README | Consolidar teoria del cambio, amenazas a validez y atribucion vs contribucion en informe final |
| B. Trazabilidad PND/SINERGIA | 15 | Basico/competente | Se menciona SINERGIA ID 91 y existe fuente PND | Crear matriz meta -> indicador -> fuente -> periodicidad -> limitaciones -> uso analitico |
| C. Ingenieria y gobernanza | 20 | Competente | ETL, Docker, Airflow, calidad, diccionario, linaje | Agregar data contracts, verificacion reproducible y reporte final de calidad |
| D. Metodologia macro/evaluacion | 20 | Competente | ITS, DiD, panel TWFE, event study, Chow, HAC | Crear robustez final, discutir endogeneidad, anticipacion y limites del control privado |
| E. Analitica/IA | 10 | Basico | Bootstrap y series, pero sin modulo final de IA | Agregar forecast/backtesting o clustering de trayectorias con validacion |
| F. Validacion e incertidumbre | 10 | Competente parcial | Bootstrap, escenarios, placebos | Automatizar matriz de sensibilidad y generar artefactos finales |
| G. Narrativa neutral | 5 | Competente | Metodologia separa hechos e inferencias | Llevarlo al informe final y presentacion con doble lectura critica/defensora |
| H. Productos finales | 10 | Basico/competente | Dashboard e informe Hito 3 | Crear informe final, presentacion, anexos y paquete de replicacion |

Objetivo tactico: apuntar a 85+ puntos, sin ningun criterio en nivel 1. Los criterios mas criticos son B, C, D, F y H.

## 4. Prioridades P0

### P0.1 Generar evidencia final

Acciones:

- Levantar PostgreSQL.
- Ejecutar el ETL si la base no esta poblada:
  - `uv run python -m etl.pipeline --skip-ingest --skip-upload`
  - o `docker compose run pipeline python -m etl.pipeline --skip-ingest --skip-upload`
- Ejecutar el analisis:
  - `uv run python analysis/runner.py`
- Verificar que se generen:
  - `data/results/tendencias_*.csv`
  - `data/results/its_*.json`
  - `data/results/its_datos_*.csv`
  - `data/results/did_agregado_*.json`
  - `data/results/did_panel_*.json`
  - `data/results/event_study_*.csv`
  - `data/results/bootstrap_*.json`
  - `data/results/resumen_ejecutivo_hito3.json`
  - `data/results/plots/*.html`
- Verificar reporte de calidad:
  - `data/processed/_quality_reports/quality_report.json`

Criterio de aceptacion:

- `data/results/` contiene resultados reales.
- El dashboard abre y carga los resultados sin pedir una corrida manual.

### P0.2 Matriz de trazabilidad oficial

Crear:

- `docs/matriz_trazabilidad_indicadores.md`

Contenido minimo:

| Promesa/meta | Indicador oficial | Fuente | Periodicidad | Tabla/campo repo | Uso analitico | Limitaciones |
|---|---|---|---|---|---|---|
| Acceso a educacion superior publica | Estudiantes nuevos en educacion superior publica, SINERGIA ID 91 | PND/SINERGIA | Segun fuente | PND + SNIES `primer_curso` | Resultado principal y validacion externa | Rezagos, definiciones y comparabilidad |
| Matricula educacion superior | Matriculados por sector IES | SNIES/MEN | Semestral | `facts.fact_estudiantes` | ITS y DiD | No mide permanencia ni calidad |
| Calidad/resultados academicos | Proxy ICFES disponible | ICFES | Anual/segun prueba | CSV ICFES | Triangulacion/contexto | No es efecto directo de educacion superior |
| Contexto macro | Desempleo u otra variable contextual | DANE si se incorpora | Mensual/trimestral | Nueva fuente o anexo | Sensibilidad/covariable | Diferente granularidad temporal |

Criterio de aceptacion:

- La matriz permite rastrear de cada meta al dato usado y del dato a la decision metodologica.

### P0.3 Robustez completa

Crear:

- `analysis/robustez.py`

Salidas:

- `data/results/robustez_sensibilidad.csv`
- `data/results/robustez_resumen.json`
- `data/results/plots/robustez_forest_plot.html`

Sensibilidades minimas:

- Punto de quiebre: `2021-S2`, `2022-S1`, `2022-S2`, `2023-S1`.
- Variable dependiente: niveles, logaritmo, primeras diferencias.
- Muestra: todas las IES, solo universidades, excluir IES pequenas si el panel lo permite.
- Estimador: ITS oficial, DiD agregado, DiD panel.

Cada corrida debe reportar:

- estimador;
- intervalo de confianza;
- p-value;
- N;
- signo;
- conclusion corta;
- si el resultado es estable o sensible al supuesto.

Criterio de aceptacion:

- El informe final contiene una tabla de robustez real, no solo una descripcion.

### P0.4 Triangulacion PND/SNIES/ICFES

Crear:

- `analysis/triangulacion.py`

Salidas:

- `data/results/triangulacion_pnd_snies.csv`
- `data/results/triangulacion_icfes_snies.csv`
- `data/results/triangulacion_resumen.json`
- `data/results/plots/triangulacion_pnd_snies.html`

Analisis minimos:

- Comparar indicador PND/SINERGIA ID 91 contra SNIES `primer_curso` en IES oficiales.
- Reportar correlacion o consistencia de tendencia, explicando diferencias de definicion.
- Usar ICFES como fuente de contexto/triangulacion, sin afirmar causalidad directa si no hay puente metodologico.
- Agregar embudo SNIES: inscritos -> admitidos -> matriculados -> primer curso -> graduados.

Criterio de aceptacion:

- La triangulacion debe decir si las fuentes son compatibles, divergentes o no comparables.

### P0.5 Informe final

Crear:

- `docs/informe_final_hito4_hito5.md`

Estructura sugerida:

1. Resumen ejecutivo.
2. Pregunta, alcance y teoria del cambio.
3. Atribucion vs contribucion.
4. Matriz de trazabilidad PND/SINERGIA.
5. Arquitectura de datos y gobernanza.
6. Calidad, diccionario, linaje y reproducibilidad.
7. Metodologia: ITS, DiD, panel, event study y bootstrap.
8. Robustez y sensibilidad.
9. Triangulacion PND/SNIES/ICFES.
10. Analitica/IA y validacion.
11. Resultados principales.
12. Lecturas alternativas: critica y defensora.
13. Limitaciones y amenazas a la validez.
14. Conclusiones neutrales.
15. Anexos tecnicos.

Criterio de aceptacion:

- Cada criterio A-H de la rubrica queda cubierto con una seccion o anexo.

### P0.6 Presentacion final

Crear:

- `docs/presentacion_final_hito4_hito5.md`

Duracion sugerida: 10 a 12 minutos.

Guion:

1. Problema y pregunta.
2. Alcance y teoria del cambio.
3. Trazabilidad oficial PND/SINERGIA.
4. Pipeline y esquema estrella.
5. Calidad, linaje y reproducibilidad.
6. ITS y DiD.
7. Resultados principales.
8. Robustez y sensibilidad.
9. Triangulacion.
10. Analitica/IA con validacion.
11. Dashboard y paquete de replicacion.
12. Conclusiones neutrales y limites.

Criterio de aceptacion:

- La presentacion debe verse como cierre final, no como repeticion del Hito 3.

## 5. Prioridades P1

### P1.1 Analitica/IA con valor claro

Opcion recomendada: forecast/backtesting de series.

Crear:

- `analysis/modelo_series.py`

Implementar:

- baseline naive: ultimo valor observado;
- tendencia OLS;
- ETS o SARIMAX simple con `statsmodels`, solo si la serie lo permite;
- backtesting con ventana pre-2022;
- metricas MAE, MAPE, RMSE.

Salidas:

- `data/results/modelo_series_resultados.json`
- `data/results/plots/modelo_series_backtesting.html`

Valor para la rubrica:

- Aporta a contrafactual y validacion.
- Compara contra baselines simples.
- Evita "IA por IA".

Opcion alternativa: clustering de trayectorias territoriales o por IES.

Crear:

- `analysis/heterogeneidad.py`

Features:

- crecimiento pre-2022;
- cambio post-2022;
- volatilidad;
- tamano promedio;
- sector;
- departamento o caracter IES.

Valor:

- Identifica heterogeneidad en patrones, sin afirmar causalidad.

### P1.2 Data contracts y pruebas minimas

Crear:

- `docs/data_contracts.md`
- `scripts/verify_reproducibility.py`

Contratos minimos:

- `facts.fact_estudiantes` tiene `tipo_evento`, `institucion_id`, `programa_id`, `tiempo_id`, `cantidad`.
- `dim_tiempo` cubre 2018-S1 a 2024-S2.
- `dim_institucion.sector_ies` contiene categorias normalizadas.
- `fact_estudiantes.cantidad` no tiene negativos.
- Las consultas de analisis retornan Oficial y Privada para `matriculados` y `primer_curso`.

Verificacion minima:

- Conexion a base.
- Existencia de schemas `raw`, `unified`, `facts`.
- Existencia de resultados finales.
- Dashboard puede leer archivos.

### P1.3 Dashboard final

Actualizar:

- `dashboard/app.py`

Cambios:

- Cambiar titulo de Hito 3 a entrega final Hito 4/5.
- Agregar tabs de Robustez, Triangulacion y Replicacion.
- Cargar `robustez_resumen.json`, `triangulacion_resumen.json` y `modelo_series_resultados.json`.
- Mostrar fecha de corrida y fuente de resultados.

### P1.4 Paquete de replicacion

Crear:

- `docs/replication_package.md`

Contenido:

- Requisitos: Python, uv, Docker, credenciales Drive, `.env`.
- Comandos desde cero.
- Comandos con datos ya descargados.
- Salidas esperadas.
- Como correr calidad.
- Como regenerar resultados.
- Como abrir dashboard.
- Problemas comunes.

### P1.5 Seguridad antes de entrega

Acciones:

- Revisar `.env`, `token.json`, dumps y logs.
- Crear `.env.example` con placeholders.
- Evitar credenciales reales en `config/globals.py`.
- Confirmar `.gitignore` para:
  - `.env`
  - `token.json`
  - `data/raw/`
  - `data/exports/`
  - logs sensibles.

## 6. Prioridades P2

- Exportar capturas del dashboard para anexos.
- Agregar `Makefile` o `justfile`.
- Crear `CHANGELOG.md` de Hito 4/5.
- Convertir informe final Markdown a DOCX/PDF.
- Preparar demo de 5 minutos sobre dashboard.
- Agregar anexo academico de amenazas a la validez.

## 7. Cronograma recomendado

### Dia 1: evidencia y trazabilidad

- Ejecutar ETL si hace falta.
- Ejecutar `analysis/runner.py`.
- Revisar `data/results/`.
- Crear matriz de trazabilidad.

Salida:

- Resultados reales generados.
- Matriz de indicadores en borrador.

### Dia 2: robustez y triangulacion

- Implementar `analysis/robustez.py`.
- Implementar `analysis/triangulacion.py`.
- Generar CSV, JSON y graficos.

Salida:

- Tabla de robustez.
- Resumen de triangulacion.

### Dia 3: analitica/IA y dashboard

- Implementar `analysis/modelo_series.py` o `analysis/heterogeneidad.py`.
- Validar contra baseline.
- Actualizar dashboard.

Salida:

- Artefacto de IA.
- Dashboard final.

### Dia 4: informe y presentacion

- Crear informe final.
- Crear presentacion final.
- Crear paquete de replicacion.

Salida:

- Documentos finales listos.

### Dia 5: verificacion y limpieza

- Ejecutar verificacion reproducible.
- Revisar secretos.
- Ensayar demo.
- Completar checklist final.

Salida:

- Entrega cerrada.

## 8. Checklist final por rubrica

### A. Alcance y teoria del cambio

- [ ] Pregunta final clara.
- [ ] Periodo y sector delimitados.
- [ ] Teoria del cambio explicada.
- [ ] Atribucion vs contribucion separadas.
- [ ] Amenazas a la validez documentadas.

### B. Trazabilidad oficial

- [ ] Matriz PND/SINERGIA completa.
- [ ] Indicador ID 91 vinculado a SNIES `primer_curso`.
- [ ] Fuentes, periodicidad y limitaciones.
- [ ] Comparabilidad interanual discutida.

### C. Ingenieria y gobernanza

- [ ] ETL corre.
- [ ] Calidad genera reporte.
- [ ] Diccionario generado.
- [ ] Linaje disponible.
- [ ] Data contracts documentados.
- [ ] Replicacion documentada.

### D. Metodologia

- [ ] ITS ejecutado.
- [ ] DiD agregado ejecutado.
- [ ] DiD panel/event study ejecutado.
- [ ] Placebos y robustez incluidos.
- [ ] Endogeneidad y anticipacion discutidas.

### E. Analitica/IA

- [ ] Modelo con proposito claro.
- [ ] Baseline definido.
- [ ] Metricas de validacion.
- [ ] Interpretacion limitada a evidencia.

### F. Validacion e incertidumbre

- [ ] Bootstrap ejecutado.
- [ ] Intervalos reportados.
- [ ] Sensibilidad a T0, muestra y forma funcional.
- [ ] Escenarios base/optimista/adverso.
- [ ] Fuentes de incertidumbre explicadas.

### G. Neutralidad

- [ ] Hechos, inferencias y limites separados.
- [ ] Lectura critica y defensora incluidas.
- [ ] Lenguaje no partidista.

### H. Productos finales

- [ ] Informe final.
- [ ] Presentacion ejecutiva.
- [ ] Dashboard final.
- [ ] Repo documentado.
- [ ] Anexos tecnicos.
- [ ] Paquete de replicacion.
- [ ] Resultados en `data/results/`.

## 9. Orden recomendado de implementacion

1. `analysis/robustez.py`
2. `analysis/triangulacion.py`
3. `analysis/modelo_series.py` o `analysis/heterogeneidad.py`
4. `analysis/runner_final.py`
5. `dashboard/app.py`
6. `scripts/verify_reproducibility.py`
7. `docs/matriz_trazabilidad_indicadores.md`
8. `docs/informe_final_hito4_hito5.md`
9. `docs/presentacion_final_hito4_hito5.md`
10. `docs/replication_package.md`

Salida ideal de `analysis/runner_final.py`:

```text
data/results/
  resumen_final_hito4_hito5.json
  robustez_sensibilidad.csv
  robustez_resumen.json
  triangulacion_pnd_snies.csv
  triangulacion_icfes_snies.csv
  triangulacion_resumen.json
  modelo_series_resultados.json
  plots/
    robustez_forest_plot.html
    triangulacion_pnd_snies.html
    modelo_series_backtesting.html
```

## 10. Riesgos y mitigaciones

| Riesgo | Impacto | Mitigacion |
|---|---|---|
| No hay datos suficientes para IA compleja | Alto en criterio E | Usar forecast/backtesting simple con baseline |
| PND/ICFES no comparan directamente con SNIES | Medio | Presentar como triangulacion/contexto, no causalidad |
| ETL falla por Drive/token | Alto | Documentar modo `--skip-ingest` y uso con datos ya descargados |
| Dashboard sin resultados | Alto | Generar resultados antes de demo |
| Secretos en repo | Alto | Limpiar `.env`, token y defaults sensibles |
| Resultados no significativos | Bajo | Reportar neutralmente; la nota premia rigor |

## 11. Definicion de entrega lista

La entrega esta lista cuando:

1. `uv run python analysis/runner.py` o `analysis/runner_final.py` genera resultados sin errores.
2. `data/results/` tiene CSV, JSON y graficos finales.
3. El dashboard abre y muestra resultados.
4. El informe final referencia artefactos reales.
5. La presentacion tiene guion ejecutivo.
6. El paquete de replicacion permite ejecutar el proyecto.
7. No hay secretos reales en archivos versionables.
8. Cada criterio A-H tiene evidencia ubicable en el repo.

## 12. Recomendacion tactica

No conviene abrir nuevos sectores ni cambiar la pregunta. El repo ya esta centrado en educacion superior y eso es defendible. La mejor estrategia es cerrar con rigor:

- robustez real;
- triangulacion honesta;
- analitica/IA pequena pero validada;
- informe final neutral;
- dashboard funcional;
- paquete de replicacion.

Eso cubre Hito 4 y Hito 5 sin dispersar el trabajo.
