# Rúbrica para el Trabajo de Curso
**SEMINARIO DE INGENIERÍA DE DATOS E INTELIGENCIA ARTIFICIAL — UAO**
**PROYECTO DE CURSO**

---

## 1. Rúbrica analítica ponderada (100 puntos)

### Niveles (4)

| Nivel | Descripción |
|-------|-------------|
| **1 — Insuficiente** | No cumple |
| **2 — Básico** | Cumple parcialmente; brechas relevantes |
| **3 — Competente** | Cumple; brechas menores |
| **4 — Sobresaliente** | Cumple con rigor alto; replicable; sólida trazabilidad |

### Tabla de criterios y ponderación

| Criterio | Peso | Evidencia mínima (qué debe existir) |
|----------|:----:|--------------------------------------|
| **A.** Alcance, preguntas y teoría del cambio | 10 | Documento de alcance, supuestos, rutas causales por sector, criterios de atribución vs contribución |
| **B.** Trazabilidad a metas/indicadores oficiales (PND–SINERGIA y sectoriales) | 15 | Matriz "promesa/metas → indicador → fuente → periodicidad → limitaciones" (PND/SINERGIA + DANE/SISPRO/etc.) |
| **C.** Ingeniería de datos y gobernanza (ETL, calidad, linaje, reproducibilidad) | 20 | Pipelines versionados, diccionario de datos, pruebas de calidad, linaje, control de cambios |
| **D.** Metodología macroeconómica y de evaluación (identificación, contrafactual, coherencia) | 20 | Estrategia(s) causal/macro justificadas, modelos implementados, supuestos explícitos |
| **E.** Modelos de analítica/IA (cuando agreguen valor) | 10 | Modelos (p.ej. nowcasting, NLP de documentos, series de tiempo) con validación y no "IA por IA" |
| **F.** Validación, sensibilidad e incertidumbre | 10 | Backtesting, análisis de sensibilidad, intervalos/incertidumbre, robustez |
| **G.** Interpretación y narrativa técnicamente neutral | 5 | Resultados interpretados sin sesgo partidista; distinción clara entre hallazgo, inferencia y opinión |
| **H.** Productos finales (informe, dashboard, repositorio, anexos) | 10 | Informe ejecutivo + técnico, tablero reproducible, repositorio, anexos de datos/código |
| **Total** | **100** | |

---

## 2. Descriptores por nivel (qué significa 1–4 en cada criterio)

### A. Alcance, preguntas y teoría del cambio (10)

- **1:** Objetivo ambiguo; mezcla logros con opinión; sin delimitación sectorial/temporal.
- **2:** Preguntas definidas, pero sin mapa causal (qué política → qué resultado) o sin manejo de shocks externos.
- **3:** Preguntas medibles, teoría del cambio por sector, supuestos y riesgos identificados.
- **4:** Además, define atribución vs contribución, mecanismos de transmisión macro, y plan explícito de "amenazas a la validez".

### B. Trazabilidad a metas/indicadores oficiales (15)

- **1:** Indicadores ad hoc sin justificación ni fuente verificable.
- **2:** Indicadores con fuente, pero sin relación clara con metas PND/seguimiento oficial.
- **3:** Matriz completa PND/SINERGIA ↔ indicadores; integra fuentes sectoriales (p.ej., DANE empleo; SISPRO afiliación/coberturas) con criterios de calidad.
- **4:** Además, evidencia "trazabilidad bidireccional": (i) desde metas → datos y (ii) desde datos → decisiones; documenta cambios metodológicos y comparabilidad interanual.

### C. Ingeniería de datos y gobernanza (20)

- **1:** ETL manual; sin control de versiones; sin diccionarios; datos no auditables.
- **2:** ETL básico automatizado, pero sin pruebas/linaje/gestión de faltantes.
- **3:** Pipelines reproducibles (scripts + configuración), pruebas de calidad, diccionario, linaje, trazas de ejecución.
- **4:** Además, CI/CD (aunque sea local), "data contracts", monitoreo de calidad, y empaquetado para replicación por terceros (otro equipo UAO).

### D. Metodología macroeconómica y de evaluación (20)

- **1:** Comparaciones "antes-después" sin controlar contexto.
- **2:** Modelos razonables, pero con identificación débil (sin contrafactual creíble o supuestos no defendidos).
- **3:** Selección metodológica adecuada al fenómeno: p.ej. DiD, control sintético, SVAR, modelos de equilibrio/insumo-producto, microsimulación vinculada a macro; justificación de supuestos y limitaciones.
- **4:** Además, triangulación de 2+ enfoques (macro + micro/sectorial), pruebas placebo/robustez, y discusión de endogeneidad/anticipación.

### E. Modelos de analítica/IA (10)

- **1:** IA sin propósito (modelos sin mejorar inferencia o medición).
- **2:** Modelos predictivos con métricas, pero sin explicar utilidad para la evaluación.
- **3:** IA aporta valor claro: nowcasting, imputación informada, detección de quiebres estructurales, NLP para mapear políticas ↔ instrumentos ↔ cronología (con evaluación).
- **4:** Además, interpretabilidad, auditoría de sesgos, y comparación contra baselines simples.

### F. Validación, sensibilidad e incertidumbre (10)

- **1:** Sin validación; resultados puntuales sin error.
- **2:** Validación parcial (una partición) o sensibilidad limitada.
- **3:** Backtesting, intervalos (bootstrap/Bayes), sensibilidad a supuestos clave (lag, especificación, covariables).
- **4:** Además, análisis de escenarios (optimista/base/adverso), cuantificación de incertidumbre por fuente (medición vs modelo vs especificación).

### G. Interpretación y narrativa neutral (5)

- **1:** Lenguaje valorativo o conclusiones políticas no sustentadas.
- **2:** Neutralidad declarada, pero inferencias exceden evidencia.
- **3:** Separación explícita: hechos medidos / inferencias / límites; lenguaje técnico sobrio.
- **4:** Además, "doble lectura": cómo un crítico y un defensor podrían cuestionar/respaldar el hallazgo, sin inclinar la balanza.

### H. Productos finales (10)

- **1:** Entregables incompletos; no reproducibles.
- **2:** Informe y gráficos, pero sin repositorio estructurado ni anexos.
- **3:** Informe ejecutivo + técnico, tablero, repositorio documentado, anexos (metadatos, supuestos, diccionario).
- **4:** Además, paquete de replicación (one-click), y resumen para tomadores de decisión + apéndice académico (estilo paper).

---

## 3. Rúbrica por hitos (gates) para 3 meses

> Úsela como "pasa/no pasa" semanal. Si un hito no pasa, el equipo no avanza al siguiente sin remediación.

### Hito 1 — Semana 1 (Feb 1–7)

- Alcance y preguntas cerradas **(A)** + matriz preliminar de indicadores **(B)**.
- Inventario de fuentes: SINERGIA/PND, DANE empleo, SISPRO/MinSalud salud, etc.

### Hito 2 — Semanas 2–3

- ETL mínimo viable **(C)**: ingestión + limpieza + diccionario + control de versiones.
- Prototipo de tablero de seguimiento **(H)**.

### Hito 3 — Semanas 4–6

- Metodología aprobada **(D)**: diseño de identificación + contrafactual(es).
- Primeros resultados por sector (educación/salud/vivienda/infra/empleo) con incertidumbre inicial **(F)**.

### Hito 4 — Semanas 7–9

- Robustez completa **(F)** + triangulación **(D)**.
- IA/analítica solo donde agregue valor **(E)**.

### Hito 5 — Semanas 10–11 (hasta May 15)

- Informe final **(H)** + paquete de replicación **(C/H)** + presentación ejecutiva neutral **(G)**.

---

## 4. Recomendación práctica de calificación (para decisión final)

| Resultado | Condición |
|-----------|-----------|
| **Aprobación alta** | ≥ 85/100 y ningún criterio en nivel 1 |
| **Aprobación condicionada** | 70–84/100, con plan de remediación en C o D (típicamente los más críticos) |
| **No aprobado** | < 70/100 o nivel 1 en C (reproducibilidad/datos) o D (metodología) |

---

## 5. Lista breve de fuentes "ancla" sugeridas (para la matriz de trazabilidad)

- **PND 2022–2026** — Documentos oficiales DNP.
- **SINERGIA PND** — Metas/indicadores y avance.
- **DANE Mercado laboral** — Empleo/desempleo, informalidad.
- **SISPRO / MinSalud** — Aseguramiento y otros indicadores.
- **ANI** — Avance de concesiones 4G/5G (cuando infraestructura sea parte del análisis).
