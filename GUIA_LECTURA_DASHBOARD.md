# Guía de lectura del dashboard — qué significa cada número

Recorrido **panel por panel** del tablero, explicando cada símbolo, tabla y
gráfico con los valores reales que aparecen. Complementa a `GLOSARIO.md`
(siglas) y `CONCEPTOS_Y_FORMULAS.md` (fórmulas).

> **Idea base:** el evento que se estudia es el **cambio de gobierno = 2022-S2**.
> Todo se mira "antes vs después" de ese punto.

---

## 0. La barra lateral (filtros)

- **Tipo de evento:** cambia QUÉ se cuenta en todo el tablero:
  - *Matriculados* = total de estudiantes matriculados.
  - *Primera matrícula* = estudiantes que entran por primera vez (primer curso).
  - *Graduados* = estudiantes que se gradúan.
- **Sector IES (Oficial / Privada):** **solo afecta la pestaña «Temporal (ITS)»**,
  porque ese método analiza **un solo grupo a la vez**. Al cambiarlo, se recalcula
  el ITS para ese sector. Las pestañas General, SNIES y Sectorial (DiD) **siempre
  muestran ambos sectores**, no cambian con este filtro.

---

## 1. Pestaña «General»

### KPIs de arriba
- **Total / Oficial / Privada:** estudiantes en el **último periodo disponible**
  (2024-S2). Total = Oficial + Privada.
- **Participación oficial = 56.1%** → qué porcentaje del total es del sector
  oficial en ese último periodo:

  ```
  Oficial / (Oficial + Privada) = 1.437.957 / (1.437.957 + 1.123.750) = 56,1 %
  ```
  Es decir, **de cada 100 matriculados, ~56 están en IES públicas (oficiales)**.

### Gráfico «Evolución»
Dos líneas (Oficial y Privada) a lo largo del tiempo, con una línea vertical en
**2022-S2** marcando el cambio de gobierno.

### Gráfico «Composición por sector»
Área apilada al 100 %: muestra **cómo se reparte** el total entre Oficial y
Privada en cada periodo (la participación, en %).

---

## 2. Pestaña «SNIES» (detalle de la fuente)

- **Cambio pre/post 2022:** media de estudiantes **antes** vs **después** de
  2022-S2, con el % de cambio.
- **Variación porcentual anual:** crecimiento de cada periodo frente al **mismo
  semestre del año anterior**. Vacío en el primer año (no hay con qué comparar) y
  **2019 no existe en SNIES** (la serie salta 2018 → 2020).
- **Tabla por semestre y sector:** los datos crudos detrás de los gráficos.

---

## 3. Pestaña «Temporal (ITS)» — el filtro Oficial/Privada actúa aquí

ITS proyecta la tendencia previa a 2022-S2 y mide la brecha con lo observado.

### Los 4 KPIs — qué significan los de cada sector

**Oficial:**
| KPI | Valor | Lectura |
|---|---|---|
| Cambio inmediato | **−39.907** | salto en matrícula justo en 2022-S2 (α₂) |
| Cambio de tendencia / semestre | **+4.735** | cómo cambia el ritmo por semestre tras el evento (α₃) |
| Ajuste del modelo (R²) | **0.7496** | el modelo explica el **75 %** de la variación → buen ajuste |
| Significancia (nivel) | **No significativo** | el cambio inmediato (α₂) tiene p=0,20 > 0,05 |

**Privada:**
| KPI | Valor | Lectura |
|---|---|---|
| Cambio inmediato | **+54.080** | salto en 2022-S2 (α₂); p=0,056, **apenas por encima** de 0,05 |
| Cambio de tendencia / semestre | **+18.078** | (α₃); este sí es significativo (p=0,0004) |
| Ajuste del modelo (R²) | **0.3486** | explica solo el **35 %** → ajuste más pobre que el oficial |
| Significancia (nivel) | **No significativo** | el badge se refiere **solo al cambio inmediato (α₂)** |

> ⚠️ **Ojo con "Significancia (nivel)":** ese rótulo evalúa **únicamente el cambio
> inmediato (α₂)**. En Privada el *cambio de tendencia* (α₃) sí es significativo,
> pero el badge no lo refleja porque mide el nivel. R² alto (Oficial) ≠ efecto
> significativo: una cosa es **ajustar bien** y otra es que el **salto sea real**.

### Gráfico «Observado vs proyección sin política»
La línea sólida es lo real; la punteada es el contrafactual (la tendencia previa
proyectada). La diferencia entre ambas es el efecto.

### Pruebas con puntos de quiebre alternativos: **t = 4, t = 6**
`t` es el **índice del semestre** (1, 2, 3 … en orden). Como **2019 no existe**,
la numeración salta:

| t | Periodo | |
|---|---|---|
| 1 | 2018-S1 | |
| 2 | 2018-S2 | |
| 3 | 2020-S1 | |
| **4** | **2020-S2** | ← placebo |
| 5 | 2021-S1 | |
| **6** | **2021-S2** | ← placebo |
| 7 | 2022-S1 | |
| **8** | **2022-S2** | ← **evento real (T₀)** |
| 9–12 | 2023-S1 … 2024-S2 | |

Los **placebos** (t=4 y t=6) son **cortes falsos** ubicados *antes* del evento
real. Se vuelve a correr el ITS fingiendo que el cambio ocurrió ahí. Si en esas
fechas falsas también apareciera un efecto fuerte, el resultado real sería
sospechoso. Es una prueba de robustez.

---

## 4. Análisis de incertidumbre (Bootstrap, 1.000 simulaciones)

Se re-estima el modelo 1.000 veces remuestreando los datos.
- **Valor central:** el promedio de las 1.000 estimaciones.
- **Límite inferior / superior:** el rango donde cae el 95 % de ellas
  (percentiles 2,5 % y 97,5 %).

**Cambio inmediato en la tendencia (ITS α₂):**
```
Valor central: -2.956   Límite inf: -248.327   Límite sup: 132.303
```
**Efecto diferencial entre sectores (DiD β₃):**
```
Valor central: 111.806  Límite inf: -553.342   Límite sup: 824.183
```
**Cómo leerlo:** ambos rangos **incluyen el 0** y son **muy amplios** → mucha
incertidumbre. No se puede afirmar que el efecto sea distinto de cero. (El valor
central del bootstrap puede diferir del estimado puntual porque cada simulación
usa una remuestra distinta.)

---

## 5. Pestaña «Sectorial (DiD)»

- **Tabla 2×2 (medias por sector y periodo):** medias pre/post de Oficial y
  Privada y sus diferencias.
- **Efecto diferencial (β₃) = (cambio Oficial) − (cambio Privada).**
- **Event study:** el efecto semestre a semestre. Si los puntos **antes de 2022**
  están cerca de 0, ambos sectores venían parejos (supuesto de tendencias
  paralelas, necesario para que el DiD sea creíble).

---

## 6. Pestaña «Robustez»

### Los 4 KPIs
| KPI | Valor | Significado |
|---|---|---|
| Especificaciones probadas | **84** | combinaciones de (corte × forma × muestra) corridas |
| Resultados significativos | **35** | cuántas dieron un efecto distinto de 0 (p<0,05) |
| % significativas | **42 %** | 35 / 84 |
| Métodos evaluados | **4** | ITS-nivel, ITS-tendencia, DiD-agregado, DiD-panel |

**Idea:** repetir el mismo análisis bajo muchos supuestos y ver si el resultado
**aguanta**. Mientras más significativas y estables, más robusto.

### Tabla de detalle — fila por fila
Columnas: **Análisis** (qué estimador), **Indicador** (matriculados…),
**Corte** (qué fecha se probó como evento), **Efecto** (coeficiente), **Límite
inferior/superior** (IC 95 %), **Concluyente** (Sí/No = significativo).

**Por qué se repite el mismo «Corte» (ej. 2021-S2) varias veces:**
cada corte se prueba con **3 formas funcionales** (niveles, logaritmo,
diferencias) × **3 muestras** (todas / solo universidades / excluir IES < 500).
Por eso ves el mismo 2021-S2 en filas consecutivas: **son la misma fecha bajo
distintos supuestos**.

**Por qué algunas filas muestran «0» o «-0»:**
esas son la **forma logarítmica o en diferencias**, donde el coeficiente es muy
pequeño (ej. 0,027 o −0,0028) y al redondear a entero se ve como `0`. **No
significa efecto cero** — es otra **escala**:
- en *niveles* el efecto está en personas (ej. 32.507),
- en *log* es un cambio proporcional (≈ %),
- en *diferencias* es el cambio del cambio.
Por eso un IC como `[-3, 4]` o `[-0, 0]` simplemente refleja esa escala diminuta.

Ejemplo real de tu tabla:
```
Cambio inmediato (ITS) | 2022-S1 | -56.600 | [-109.902; -3.298] | Sí   ← niveles, significativo
Cambio inmediato (ITS) | 2022-S1 |      -0 | [-0; -0]           | Sí   ← log, mismo efecto en otra escala
```

---

## 7. Pestaña «Triangulación»

### KPIs
- **PND disponible: Sí** → la fuente del Plan Nacional de Desarrollo existe.
- **Filas PND ID 91: 1** → solo hay **un** registro del indicador 91 que empareja
  (la serie del PND es muy corta).
- **Filas ICFES: 30.466** → registros del insumo ICFES usado para el contexto.

### Tabla «PND/SINERGIA vs SNIES»
```
Año   SNIES primera matrícula (oficial)   PND — indicador
2020   460.168                            (vacío)
2021   490.746                            (vacío)
2022   422.085                            (vacío)
2023   489.877                            65.063
2024   557.410                            125.441
```
- **SNIES primera matrícula (oficial):** estudiantes de primer curso oficiales ese año.
- **PND — indicador:** valor reportado del indicador ID 91 del Plan.
- **Por qué solo 2023 y 2024 tienen PND:** SINERGIA **empezó a reportar ese
  indicador en 2023**; antes no hay dato. Por eso se comparan **tendencias**, no
  niveles (las escalas no son equivalentes).

### Tabla «Embudo SNIES» — columna por columna
Sigue el recorrido del estudiante:
```
Inscritos → Admitidos → Matriculados → Primera matrícula → Graduados
```
Las **3 últimas columnas son proporciones** (no conteos):
| Columna | Fórmula | Ejemplo (2020-S1 Oficial) |
|---|---|---|
| Tasa de admisión | Admitidos / Inscritos | 296.811 / 734.282 = **0,40** (40 %) |
| Matrícula / admitidos | Matriculados / Admitidos | 1.117.474 / 296.811 = **3,76** (>1 porque "matriculados" es el total acumulado y "admitidos" son solo los nuevos del semestre) |
| 1ra matrícula / matriculados | Primera matrícula / Matriculados | 240.743 / 1.117.474 = **0,22** (22 %) |

**Por qué hay celdas vacías:**
- **2018** no tiene «Primera matrícula» ni «Graduados» → esos registros en SNIES
  **empiezan en 2020**. Por eso esas columnas (y su proporción) están en blanco.
- **2023** no tiene «Admitidos» ni «Graduados» en la fuente → quedan vacías la
  «Tasa de admisión» y «Matrícula / admitidos», pero «1ra matrícula / matriculados»
  sí se calcula (ambos datos existen).

### Tabla «Proxy ICFES de contexto»
```
Año   Proxy ICFES
2018   267,49
2019   240,38
2020   268,61
2021   278,14
2022   242,32
```
Es un **indicador de contexto** derivado de los datos ICFES de cada año (en tu
corrida, el **puntaje promedio** del examen, rango ~240–280). Sirve para ver si
el **nivel académico** se movió en paralelo a la matrícula. **No entra en los
cálculos causales** (ITS/DiD); es solo referencia. El valor exacto depende de qué
variable ICFES se agregó.

---

## 8. ¿Por qué Amazonas tiene tan pocos datos? ¿Son reales?

**Sí, son los datos reales extraídos.** Amazonas tiene pocos registros porque es
un departamento **poco poblado**: muy pocos estudiantes presentan el examen.
Cifras reales de tu base (ICFES Saber 11, total de observaciones):

| Departamento | Observaciones |
|---|---|
| Guainía | 1.373 |
| Vaupés | 1.572 |
| Vichada | 2.212 |
| San Andrés | 2.394 |
| **Amazonas** | **3.292** |
| … | … |
| Valle | 200.201 |
| Antioquia | 297.962 |
| Bogotá | 340.196 |

Amazonas (3.292) frente a Bogotá (340.196) → ~**100 veces menos**. No es un error
de extracción: refleja la realidad demográfica. Por eso en rankings o cruces
territoriales esos departamentos aparecen con muestras chicas (y conviene
interpretarlos con cautela: pocos datos = más variabilidad).

---

## Resumen de "trampas" de lectura
- **R² alto ≠ efecto significativo.** Una mide ajuste; la otra, si el efecto es real.
- **"No significativo" no es "cero":** es "no hay evidencia suficiente".
- **"0 / -0" en robustez** = coeficiente en escala log/diferencias, no efecto nulo.
- **IC que cruza 0** → el efecto no se distingue del azar.
- **Celdas vacías** = la fuente no tiene ese dato ese año (no es un bug).
- **t** es el número de semestre en orden, **saltando 2019** (no existe en SNIES).
