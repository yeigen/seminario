# Hito 3 — Metodología de Evaluación (Criterio D)
## Colombia / Educación Superior / Gobierno Nacional 2022–2026

**Equipo:** Juan Jacobo Delgado · Gabriel Martinez · Nicolas Cuaran · Juan Jose Orozco · Sebastian Belalcazar  
**Semanas:** 4–6  
**Estado:** Metodología aprobada + Primeros resultados con incertidumbre (Criterios D y F)

---

## 1. Pregunta de evaluación central

> **¿En qué medida las políticas de acceso a educación superior del Gobierno Petro (2022–2026) —en particular la gratuidad y los programas de matrícula— modificaron la matrícula y la primera matrícula en Instituciones de Educación Superior (IES) públicas, respecto a la tendencia previa y al comportamiento de las IES privadas como grupo de control?**

Esta pregunta operacionaliza las preguntas P1 y P6 del Hito 1 sobre acceso a educación superior, y se alinea con el indicador SINERGIA ID 91 ("Estudiantes nuevos en Educación Superior Pública").

---

## 2. Marco de identificación causal

### 2.1 Naturaleza del problema de identificación

La política educativa del Gobierno Petro opera a nivel nacional sin variación aleatoria en la asignación de tratamiento. Esto implica:

- **No existe un grupo de control "puro" en educación superior pública** (toda IES oficial se ve afectada).
- **La atribución completa es inviable** sin un experimento natural o discontinuidad geográfica clara.
- Se aplica el criterio de **contribución** (no atribución): estimar en qué dirección y magnitud la política está correlacionada con cambios observados, controlando tendencias preexistentes y covariables de contexto.

### 2.2 Estrategia de identificación adoptada

Se combinan **dos enfoques complementarios** para triangulación:

| Método | Pregunta que responde | Supuesto clave |
|---|---|---|
| **Análisis de Series de Tiempo Interrumpidas (ITS)** | ¿Cambió el nivel o la tendencia de matrícula en IES públicas al inicio del gobierno? | La tendencia pre-2022 habría continuado sin la política |
| **Diferencias en Diferencias (DiD) — sector público vs. privado** | ¿Cambió la matrícula en IES públicas más que en privadas tras 2022? | Tendencias paralelas pre-tratamiento entre sectores |

---

## 3. Método 1 — Series de Tiempo Interrumpidas (ITS)

### 3.1 Especificación

Sea $Y_t$ el total nacional de estudiantes matriculados en IES **oficiales** en el semestre $t$, con $t = 1, 2, \ldots, T$ indexado desde 2018-S1.

El modelo ITS de segmented regression es:

$$Y_t = \alpha_0 + \alpha_1 \cdot t + \alpha_2 \cdot D_t + \alpha_3 \cdot (t - T_0) \cdot D_t + \varepsilon_t$$

donde:

| Parámetro | Interpretación |
|---|---|
| $\alpha_0$ | Nivel inicial en $t=1$ (2018-S1) |
| $\alpha_1$ | Tendencia pre-intervención (por semestre) |
| $D_t = \mathbb{1}[t \geq T_0]$ | Indicador post-intervención ($T_0$ = 2022-S2) |
| $\alpha_2$ | Cambio inmediato de nivel en el punto de quiebre |
| $\alpha_3$ | Cambio en la pendiente (tendencia) post-intervención |

**Punto de intervención:** $T_0$ corresponde al segundo semestre de 2022 (inicio efectivo del gobierno Petro, agosto 2022). Se usan los semestres 2018-S1 a 2024-S2 (14 puntos temporales).

### 3.2 Supuestos y amenazas a la validez

| Supuesto | Evaluación | Prueba de robustez |
|---|---|---|
| La tendencia pre-2022 habría continuado sin cambio de gobierno | Plausible pero no verificable directamente | Prueba placebo: desplazar $T_0$ a 2021-S2 y 2020-S2 |
| No hay shocks simultáneos en $T_0$ | Parcialmente violado: efectos rezagados de pandemia aún presentes en 2022 | Incluir variable de contexto (tasa de desempleo DANE) |
| Errores $\varepsilon_t$ no autocorrelacionados | Posible autocorrelación en series semestrales | Errores Newey-West con lag=2; prueba Durbin-Watson |
| Las series son comparables año a año | Documentado en Hito 2 (cambios metodológicos SNIES) | Sensibilidad: excluir años de transición metodológica |

### 3.3 Contrafactual ITS

El contrafactual es la proyección de la tendencia lineal pre-2022 hacia el periodo 2022–2024. La diferencia entre los valores observados y el contrafactual proyectado es el **efecto estimado de la política** bajo este marco.

---

## 4. Método 2 — Diferencias en Diferencias (DiD)

### 4.1 Especificación

Se usa un panel de IES (unidad $i$, semestre $t$) con:

- **Grupo tratado:** IES Oficiales ($\text{OFICIAL}_i = 1$)
- **Grupo control:** IES Privadas ($\text{OFICIAL}_i = 0$)
- **Periodo pre:** 2018-S1 a 2022-S1
- **Periodo post:** 2022-S2 a 2024-S2

El modelo de regresión por OLS con efectos fijos de IES es:

$$\ln(Y_{it} + 1) = \mu_i + \gamma_t + \beta \cdot (\text{POST}_t \times \text{OFICIAL}_i) + \varepsilon_{it}$$

donde:

| Parámetro | Interpretación |
|---|---|
| $\mu_i$ | Efectos fijos de IES (controla heterogeneidad no observable constante) |
| $\gamma_t$ | Efectos fijos de tiempo (controla shocks comunes) |
| $\text{POST}_t = \mathbb{1}[t \geq T_0]$ | Indicador post-intervención |
| $\beta$ | **Estimador DiD**: diferencia en cambios de ln(matrícula) entre sectores |

La variable dependiente $\ln(Y_{it} + 1)$ usa logaritmo para suavizar la distribución sesgada de matrícula entre IES grandes y pequeñas. El estimador $\beta$ aproxima el cambio porcentual diferencial.

### 4.2 Versión TWFE simplificada (sin efectos fijos de IES)

Para el análisis agregado por sector (cuando no hay suficientes IES en el panel balanceado):

$$Y_{st} = \alpha + \beta_1 \cdot \text{POST}_t + \beta_2 \cdot \text{OFICIAL}_s + \beta_3 \cdot (\text{POST}_t \times \text{OFICIAL}_s) + \varepsilon_{st}$$

donde $Y_{st}$ es la matrícula total del sector $s$ (Oficial/Privado) en el semestre $t$. El estimador de interés es $\hat{\beta}_3$.

### 4.3 Supuesto de tendencias paralelas

El supuesto central del DiD es que, en ausencia de tratamiento, las IES oficiales habrían seguido la misma tendencia que las privadas. Se verifica mediante:

1. **Gráfico pre-tendencias:** Evolución de ln(matrícula) por sector, 2018–2022.
2. **Prueba formal:** Regresión de la interacción por año pre-tratamiento (event study); los coeficientes pre-2022 no deben diferir significativamente de cero.
3. **Prueba placebo sectorial:** DiD con sectores que NO reciben la política (p. ej., IES privadas élite vs. privadas masivas).

### 4.4 Limitaciones del DiD en este contexto

- Las IES privadas pueden reaccionar de forma estratégica a la política pública (ajustar matrículas y precios), lo que viola la exclusión del grupo control.
- Las IES oficiales no son homogéneas: universidades públicas grandes vs. instituciones técnicas y tecnológicas oficiales tienen dinámicas distintas.
- Los datos SNIES tienen periodicidad semestral; algunos valores de 2024 pueden estar pendientes de consolidación.

---

## 5. Variables y fuentes

### 5.1 Variable dependiente principal

| Variable | Definición | Fuente | Tabla en BD |
|---|---|---|---|
| `matriculados` | Total estudiantes matriculados (headcount) | SNIES | `facts.fact_estudiantes` WHERE `tipo_evento = 'matriculados'` |
| `primer_curso` | Estudiantes nuevos (primera matrícula) | SNIES | `facts.fact_estudiantes` WHERE `tipo_evento = 'primer_curso'` |
| `graduados` | Graduados en el periodo | SNIES | `facts.fact_estudiantes` WHERE `tipo_evento = 'graduados'` |

### 5.2 Variables de clasificación / panel

| Variable | Fuente | Tabla |
|---|---|---|
| `sector_ies` (Oficial / Privada) | SNIES | `facts.dim_institucion` |
| `caracter_ies` (Universidad, Inst. Tecnológica, etc.) | SNIES | `facts.dim_institucion` |
| `codigo_departamento`, `nombre_departamento` | DANE/SNIES | `facts.dim_geografia` |
| `ano`, `semestre` | SNIES | `facts.dim_tiempo` |

### 5.3 Indicador de tratamiento (política)

El tratamiento es el **inicio del gobierno Petro en agosto 2022**, concretamente:
- Decreto de gratuidad en educación superior pública (2022-2023)
- Programas "Generación E", expansión de apoyos a permanencia

**Variable de tratamiento:** $D_{it} = \mathbb{1}[\text{sector}_i = \text{Oficial}] \times \mathbb{1}[t \geq 2022\text{-S2}]$

---

## 6. Validación y análisis de incertidumbre (Criterio F)

### 6.1 Intervalos de confianza bootstrap

Para los estimadores ITS ($\hat{\alpha}_2$, $\hat{\alpha}_3$) y DiD ($\hat{\beta}_3$) se calculan intervalos de confianza mediante **bootstrap no paramétrico** con $B = 1{,}000$ re-muestras:

- Método: bootstrap por bloques temporales (block bootstrap con bloque = 2 semestres) para preservar la autocorrelación de la serie.
- Se reporta el IC 95% percentil.

### 6.2 Análisis de sensibilidad a supuestos clave

| Dimensión de sensibilidad | Variaciones analizadas |
|---|---|
| Punto de quiebre ($T_0$) | 2021-S2, 2022-S1, 2022-S2 (base), 2023-S1 |
| Muestra de IES en DiD | Todas las IES / Solo universidades / Excluir IES < 500 estudiantes |
| Forma funcional | Niveles / Logaritmo / Primeras diferencias |
| Grupo de control DiD | Solo privadas / Solo privadas con mismo departamento |

### 6.3 Pruebas de robustez

1. **Prueba placebo temporal:** Aplicar el modelo ITS/DiD con $T_0$ en 2020-S2 (inicio pandemia) — los coeficientes deben capturar el shock pandemia, no la política.
2. **Prueba placebo sectorial:** DiD entre subgrupos de privadas (élite vs. masivas) — el estimador debe ser cercano a cero.
3. **Test de Chow:** Prueba formal de quiebre estructural en $T_0$ para la serie ITS.

### 6.4 Cuantificación de fuentes de incertidumbre

Se distinguen tres fuentes:

| Fuente | Descripción | Cuantificación |
|---|---|---|
| **Incertidumbre de medición** | Rezagos de reporte SNIES, revisiones retroactivas | IC bootstrap sobre los datos observados |
| **Incertidumbre de modelo** | Especificación lineal vs. cuadrática, lag de efectos | Comparar AIC/BIC entre especificaciones alternativas |
| **Incertidumbre de identificación** | Validez del supuesto de tendencias paralelas | Gráfico y prueba formal de pre-tendencias |

---

## 7. Atribución vs. contribución

Siguiendo el criterio definido en el Hito 1:

| Tipo | Aplicación en este análisis |
|---|---|
| **Contribución** (por defecto) | Todos los resultados: el gobierno es uno de varios factores que influyen en la matrícula (recuperación post-pandemia, ciclo económico, demografía). Los estimadores miden asociación, no causalidad pura. |
| **Atribución parcial** (cuando viable) | En departamentos donde hay variación en la implementación del programa de matrícula gratuita se puede plantear un DiD departamental más riguroso (pendiente de datos de implementación territorial). |

---

## 8. Productos de este hito

| Entregable | Ruta en repositorio | Descripción |
|---|---|---|
| Este documento | `docs/metodologia_hito3.md` | Marco metodológico completo (Criterio D) |
| Scripts de análisis | `analysis/` | Módulos Python: queries, ITS, DiD, bootstrap, runner |
| Resultados generados | `data/results/` | CSVs + JSON con coeficientes, IC y tablas |
| Notebook de análisis | `notebooks/hito3_analisis.ipynb` | Ejecución documentada paso a paso |
| Dashboard actualizado | `dashboard/app.py` | Visualización interactiva de resultados (Criterio H) |

---

## 9. Declaración de neutralidad técnica (Criterio G)

Los resultados de este hito se presentan bajo los siguientes principios:

- **Hechos medidos:** estadísticas descriptivas y estimadores de modelos con sus incertidumbres.
- **Inferencias:** interpretaciones causales explícitamente condicionadas a supuestos declarados.
- **Límites:** no se extrapola más allá de lo que los datos y el diseño permiten afirmar.
- Los estimadores pueden ser positivos o negativos; ninguno se interpreta como validación ni invalidación de una posición política.
