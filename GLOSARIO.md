# Glosario de conceptos y siglas

Documento de referencia para interpretar el dashboard de educación superior en
Colombia. Reúne las siglas, los métodos estadísticos y las notas sobre los datos.

---

## Fuentes de datos

### SNIES — Sistema Nacional de Información de la Educación Superior
Sistema oficial del Ministerio de Educación de Colombia que consolida la
información de las instituciones de educación superior (IES): inscritos,
admitidos, matriculados, primera matrícula (primer curso) y graduados, por
sector (Oficial / Privada), año y semestre.

### ICFES — Instituto Colombiano para la Evaluación de la Educación
Entidad que administra las pruebas estandarizadas del país. En el dashboard se
usan dos:
- **Saber 11:** prueba de fin de educación media (grado 11).
- **Saber Pro:** prueba de fin de pregrado universitario.

Se reportan el **puntaje promedio** y las **observaciones** (número de
evaluados), a nivel nacional y por departamento.

### PND — Plan Nacional de Desarrollo
Documento de política pública que fija las metas del gobierno para su periodo.
Su seguimiento se hace vía **SINERGIA** (sistema de seguimiento a metas del
DNP — Departamento Nacional de Planeación). En el dashboard, el "indicador
ID 91" del PND se cruza con SNIES para triangular la información.

> **Nota:** el indicador PND puede aparecer vacío en algunos años cuando la
> fuente SINERGIA no publicó dato para ese periodo.

---

## ¿Por qué faltan años (2018–2020)?

Los datos **no son continuos** y conviene tenerlo presente al leer las series:

- **Matriculados:** disponible para **2018** y **2020–2024**. **2019 no está en
  la fuente SNIES** usada, por lo que la serie "salta" de 2018 a 2020.
- **Primera matrícula y graduados:** disponibles **desde 2020**. Por eso, en la
  tabla de embudo, las columnas de "Primera matrícula" y "Graduados" aparecen
  **vacías en 2018**.
- **Graduados:** además falta **2023** en algunos cortes.

**Consecuencia en los cálculos:** la *variación anual* compara cada periodo con
el mismo semestre del año anterior. Aparece **vacía en el primer año** (no hay
periodo previo) y, como falta 2019, la primera variación calculada (2020 vs
2018) en realidad cubre **dos años**, no uno.

---

## Métodos de análisis

### ITS — Interrupted Time Series (Series de Tiempo Interrumpidas)
Se ajusta la tendencia de los datos **antes** de un evento (el cambio de
gobierno en **2022-S2**) y se **proyecta** como si nada hubiera ocurrido. La
diferencia entre lo **observado** y esa **proyección contrafactual** es el
efecto atribuible al evento.

Dos parámetros clave:
- **Cambio inmediato (nivel):** el salto que ocurre justo después del evento.
- **Cambio de tendencia (pendiente):** cómo cambia el ritmo de crecimiento por
  semestre tras el evento.

Es un método de **un solo grupo**: no requiere grupo de comparación, pero asume
que la tendencia previa habría continuado igual.

### DiD — Difference-in-Differences (Diferencias en Diferencias)
Compara **dos grupos** (aquí **Oficial** vs **Privada**) antes y después del
evento. El sector privado funciona como **grupo de control**: descuenta todo lo
que habría pasado de igual forma en ambos (economía, demografía, etc.).

> **Efecto DiD = (cambio en Oficial) − (cambio en Privada).**

Así se aísla lo atribuible a la política que afecta sobre todo al sector
oficial. Supone **tendencias paralelas**: antes del evento ambos sectores se
movían de forma similar.

### Event study (estudio de eventos)
Versión del DiD que estima el efecto **semestre a semestre** en lugar de un solo
número. Sirve para **verificar las tendencias paralelas**: si los puntos
**previos** a 2022 están cerca de cero, los dos sectores venían comportándose
igual, lo que da validez al DiD.

### Prueba de Chow / cambio estructural
Prueba estadística que confirma si hubo un **quiebre** en la serie en el punto
del evento (si el comportamiento cambió de forma significativa antes vs después).

### Placebos (puntos de quiebre alternativos)
Se repite el análisis fijando el "evento" en fechas **falsas** (donde no hubo
política). Si ahí también apareciera un efecto fuerte, el resultado original
sería sospechoso. Sirve como prueba de robustez.

### Bootstrap (simulaciones)
Se re-estima el modelo cientos/miles de veces remuestreando los datos para medir
qué tan **estables** son los resultados y construir los intervalos de confianza.

---

## Indicadores estadísticos

### IC 95% — Intervalo de Confianza al 95%
Rango dentro del cual, con 95% de confianza, se encuentra el verdadero valor del
efecto. Se reporta con dos límites:
- **IC 95% inferior** (o "IC −95%", límite bajo): extremo inferior del rango.
- **IC 95% superior** (límite alto): extremo superior del rango.

**Regla práctica:** si el intervalo **no incluye el 0**, el efecto se considera
**estadísticamente significativo** (es improbable que sea casualidad). Si
**cruza el 0**, no se puede descartar que el efecto real sea nulo.

### Significancia (estadística)
Indica si un resultado es **distinto de cero** más allá del azar, normalmente al
nivel del 5% (p-valor < 0.05). En el dashboard se muestra como
**"Significativo" / "No significativo"**:
- **Significativo:** hay evidencia de que el efecto es real.
- **No significativo:** los datos no permiten distinguir el efecto de cero (no
  significa que sea cero, solo que no hay evidencia suficiente).

### R² (R cuadrado) — Bondad de ajuste
Mide qué porcentaje de la variación de los datos logra **explicar el modelo**,
en una escala de 0 a 1:
- **Cercano a 1:** el modelo describe muy bien los datos.
- **Cercano a 0:** el modelo explica poco.

Es una medida de *ajuste*, no de causalidad.

---

## La tabla "Embudo SNIES"

Muestra el recorrido del estudiante por etapas, por periodo y sector:

`Inscritos → Admitidos → Matriculados → Primera matrícula → Graduados`

Las tres últimas columnas son **proporciones** (no conteos):

| Columna | Significado | Lectura |
|---|---|---|
| **Tasa de admisión** | Admitidos / Inscritos | 0.37 = se admite al 37% de los inscritos |
| **Matrícula / admitidos** | Matriculados / Admitidos | puede ser **>1** porque "matriculados" es el total acumulado del semestre, mientras "admitidos" son solo los nuevos de ese semestre |
| **1ra matrícula / matriculados** | Primera matrícula / Matriculados | 0.21 = el 21% de los matriculados son de primer curso |

**Por qué se ven "0" y "1":** son proporciones decimales (0.21, 0.85). Si se
redondean a entero se ven como `0` o `1`; en el dashboard ahora se muestran con
**dos decimales** para que se lean correctamente.

**Por qué hay celdas vacías en 2018:** "Primera matrícula" y "Graduados" no
existen en SNIES antes de 2020, así que esas columnas (y la proporción que
depende de ellas) quedan en blanco para 2018.

---

## Nota sobre el rótulo "Privado"

En la fuente conviven dos etiquetas para el mismo sector privado: **"Privada"**
(el grueso de los datos) y **"Privado"** (unos pocos cientos de registros, un
artefacto de captura). En el dashboard se **consolidan dentro de "Privada"**
para no mostrar filas/tarjetas duplicadas.
