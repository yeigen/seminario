# Matriz de trazabilidad de indicadores

Proyecto: evaluacion de politicas de acceso a educacion superior en Colombia, 2018-2024  
Uso: evidencia del criterio B de la rubrica y soporte para Hito 4/5.

## 1. Proposito

Esta matriz conecta las promesas o metas de politica publica con los indicadores oficiales, las fuentes usadas en el repositorio, las decisiones metodologicas y las limitaciones de comparabilidad. Su funcion es doble:

- Trazabilidad desde la meta oficial hacia el dato usado.
- Trazabilidad desde el dato disponible hacia la decision analitica.

## 2. Matriz principal

| Meta o promesa de politica | Indicador operativo | Fuente oficial | Periodicidad | Tabla/campo en el repo | Uso analitico | Limitaciones |
|---|---|---|---|---|---|---|
| Ampliar el acceso a educacion superior publica | Estudiantes nuevos en educacion superior publica, asociado a SINERGIA ID 91 | PND/SINERGIA y SNIES/MEN | Seguimiento oficial segun PND; SNIES semestral | PND: `raw/processed pnd/seguimiento_pnd`; SNIES: `facts.fact_estudiantes` con `tipo_evento = 'primer_curso'` y `dim_institucion.sector_ies = 'Oficial'` | Variable de resultado principal para primera matricula; validacion cruzada entre avance oficial y dato SNIES | Las definiciones exactas pueden diferir entre PND y SNIES; puede haber rezagos de reporte y revisiones administrativas |
| Medir cambio en matricula de IES publicas | Total de matriculados por sector IES | SNIES/MEN | Semestral | `facts.fact_estudiantes.cantidad`, `tipo_evento = 'matriculados'`, `dim_tiempo`, `dim_institucion.sector_ies` | Resultado principal para ITS, DiD agregado y dashboard | Mide conteo de matriculados, no permanencia, calidad ni bienestar; sensible a cambios de reporte institucional |
| Comparar sector oficial frente a control privado | Diferencial oficial vs privada antes/despues de 2022-S2 | SNIES/MEN | Semestral | `facts.fact_estudiantes`, `dim_institucion.sector_ies`, `dim_tiempo` | Identificacion DiD; grupo tratado oficial y control privado | El sector privado puede reaccionar a la politica; el supuesto de tendencias paralelas debe validarse y discutirse |
| Evaluar acceso inicial, no solo matricula total | Primera matricula / primer curso por sector | SNIES/MEN | Semestral | `facts.fact_estudiantes`, `tipo_evento = 'primer_curso'` | Resultado complementario para acceso; se compara con el indicador oficial de estudiantes nuevos | Puede no coincidir exactamente con la definicion administrativa de "estudiante nuevo" en SINERGIA |
| Evaluar trayectoria del embudo educativo | Inscritos, admitidos, matriculados, primer curso y graduados | SNIES/MEN | Semestral | `facts.fact_estudiantes`, columna `tipo_evento` | Triangulacion interna: permite ver si el cambio aparece en todo el embudo o solo en una etapa | Las etapas no necesariamente corresponden a la misma cohorte; no debe interpretarse como tasa longitudinal individual |
| Controlar capacidad institucional | Docentes por sector IES | SNIES/MEN | Semestral/anual segun reporte | `facts.fact_docentes`, `dim_institucion.sector_ies`, `dim_tiempo` | Proxy de capacidad instalada y contexto sectorial | No mide calidad docente, infraestructura ni gasto; posible heterogeneidad por tipo de dedicacion |
| Triangulacion sectorial externa | Indicadores ICFES disponibles en Saber 3-5-9 | ICFES | Anual o segun prueba | `raw/processed icfes/saber_359` si esta disponible | Contexto educativo y validacion de pipeline multi-fuente; no se usa como efecto causal directo de educacion superior | No mide educacion superior; su relacion con la politica universitaria es indirecta |
| Contexto macroeconomico | Desempleo, informalidad u otra variable contextual | DANE si se incorpora | Mensual/trimestral | Fuente externa pendiente o anexo | Sensibilidad a shocks macro y recuperacion post-pandemia | Diferente granularidad temporal; requiere agregacion a semestre y cuidado de rezagos |

## 3. Decisiones metodologicas derivadas

| Dato disponible | Decision tomada | Justificacion | Riesgo |
|---|---|---|---|
| SNIES tiene observaciones semestrales 2018-S1 a 2024-S2 | Usar 2022-S2 como punto de intervencion | El gobierno inicia en agosto de 2022; 2022-S2 es el primer semestre bajo la nueva administracion | Algunas politicas pueden tener efectos anticipados o rezagados |
| Existen IES oficiales y privadas | Usar DiD oficial vs privada | Permite comparar el cambio relativo post-2022 con un grupo no tratado directamente | El grupo privado no es un control perfecto |
| Hay pocas observaciones temporales agregadas | Complementar ITS con bootstrap, placebos y DiD | Reduce dependencia de una sola especificacion | La inferencia sigue siendo sensible al tamano de muestra |
| Existen varios tipos de evento SNIES | Reportar matriculados y primer curso como resultados centrales | Matriculados mide escala; primer curso mide acceso inicial | Ambas variables pueden responder a dinamicas distintas |
| PND/SINERGIA e ICFES pueden no ser comparables uno a uno | Usarlos como triangulacion, no como prueba causal directa | Evita sobreinterpretar fuentes con definiciones diferentes | La triangulacion puede ser cualitativa si faltan columnas homologables |

## 4. Evidencia esperada en la entrega final

- `data/results/resumen_final_hito4_hito5.json`: resumen integrado de hallazgos.
- `data/results/triangulacion_pnd_snies.csv`: comparacion PND/SINERGIA vs SNIES cuando la fuente PND este disponible.
- `data/results/triangulacion_embudo_snies.csv`: embudo por sector y periodo.
- `data/results/robustez_sensibilidad.csv`: tabla de sensibilidad de estimadores.
- `docs/informe_final_hito4_hito5.md`: interpretacion final con limitaciones.

## 5. Advertencia de interpretacion

La trazabilidad no convierte una asociacion estadistica en atribucion causal completa. En esta entrega, los resultados se interpretan como evidencia de contribucion condicionada a supuestos: continuidad de tendencias, validez parcial del grupo privado como control, estabilidad de medicion y ausencia de shocks simultaneos no controlados.
