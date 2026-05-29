# Conceptos, fórmulas y ejemplos

Explicación de los métodos del proyecto **tal como están implementados** en
`analysis/` (ITS, DiD, R², robustez, triangulación), con sus fórmulas y un
ejemplo numérico para cada uno. Para las siglas y definiciones cortas ver
`GLOSARIO.md`.

- **Periodo de análisis:** 2018-S1 a 2024-S2.
- **Punto de quiebre (T₀):** 2022-S2 (cambio de gobierno).
- **Implementación:** regresiones con `statsmodels`; errores robustos.

---

## 1. ITS — Series de Tiempo Interrumpidas
**Archivo:** `analysis/its.py`

### Qué mide
Si una serie cambió **justo en el evento**. Se ajusta la tendencia *antes* de T₀
y se proyecta como si nada hubiera pasado (**contrafactual**); el **efecto** es la
diferencia entre lo observado y esa proyección.

### Fórmula (regresión segmentada)

```
Y_t = α₀ + α₁·t + α₂·D_t + α₃·(t − T₀)·D_t + ε_t
```

| Término | Significado |
|---|---|
| `t` | índice de semestre (1 = 2018-S1) |
| `D_t` | 1 si t ≥ T₀ (post-evento), 0 si no |
| `(t − T₀)·D_t` | tendencia que solo "cuenta" después del evento |
| **α₀** | nivel inicial |
| **α₁** | tendencia pre-evento (matriculados por semestre) |
| **α₂** | **cambio inmediato de nivel** (el salto en T₀) |
| **α₃** | **cambio de pendiente** post-evento |

### Cómo se estima
OLS con errores **HAC / Newey-West (maxlags = 2)** para corregir la
autocorrelación típica de las series temporales. Los IC 95% salen del modelo.
El **contrafactual** se obtiene prediciendo con `D = 0` y `t_post = 0`, y
`efecto = observado − contrafactual`.

### Pruebas de apoyo
- **Chow:** confirma si hubo quiebre estructural (ver §6).
- **Placebos:** se repite el ITS con cortes falsos (t0 = 4, t0 = 6); si ahí
  también aparece efecto, el resultado real pierde credibilidad.

### Ejemplo numérico (Oficial, matriculados — resultado real)
- α₁ = **+24.996** matriculados/semestre (crecía sostenidamente antes de 2022).
- α₂ = **−39.907**, IC95% [−101.414; 21.599], p = 0,20 → **no significativo**
  (el intervalo cruza 0).
- α₃ = **+4.735**/sem, p = 0,67 → **no significativo**.
- **Lectura:** no hay un salto claro atribuible al evento.

> **Mini-ejemplo de intuición:** si antes del evento la matrícula crecía 25.000
> por semestre y en 2024-S2 se esperaban (contrafactual) 1.400.000 pero se
> observaron 1.380.000, el "efecto estimado" de ese semestre sería −20.000.

---

## 2. DiD — Diferencias en Diferencias
**Archivo:** `analysis/did.py`

### Qué mide
Compara **dos grupos** (Oficial vs Privada, que actúa de **control**) antes y
después del evento. Resta lo que habría pasado de igual forma en ambos
(economía, demografía) y aísla lo atribuible a la política.

### Fórmula (DiD agregado)

```
Y_st = α + β₁·POST_t + β₂·OFICIAL_s + β₃·(POST_t × OFICIAL_s) + ε_st
```

- `POST` = 1 desde 2022-S2 · `OFICIAL` = 1 si el sector es oficial.
- **β₃** (coeficiente de la interacción) **es el efecto DiD**.
- Errores robustos **HC1**.

Equivale al cálculo manual de "doble diferencia":

```
β₃ ≈ (media_Oficial_post − media_Oficial_pre) − (media_Privada_post − media_Privada_pre)
```

### Variante fuerte: DiD panel TWFE
Usa cada IES individual con **efectos fijos de institución (μᵢ) y de tiempo (γₜ)**
sobre `ln(matriculados + 1)`:

```
ln(Y_it + 1) = μ_i + γ_t + β·(POST_t × OFICIAL_i) + ε_it
```

Como la variable es logarítmica, el efecto se lee en porcentaje:
`efecto ≈ (e^β − 1) × 100`.

### Event study (tendencias paralelas)
Estima β para **cada semestre** contra el periodo base (2022-S1). Si los
coeficientes **previos a 2022 son ≈ 0**, se valida el supuesto de **tendencias
paralelas** (clave para que el DiD sea creíble).

### Ejemplo numérico (matriculados — resultado real)
| | Pre-2022 | Post-2022 | Cambio |
|---|---:|---:|---:|
| Oficial | 1.210.332 | 1.329.870 | **+119.538** |
| Privada | 1.121.021 | 1.127.422 | **+6.401** |

```
β₃ = 119.538 − 6.401 = 113.137  (DiD manual)
```
El modelo da β₃ = **+116.330**, IC95% [−374.088; 606.749], p = 0,64 →
**no significativo** (el intervalo cruza 0). El sector oficial creció más, pero
no se puede descartar que sea ruido.

---

## 3. R² — Bondad de ajuste
**Dónde:** `modelo.rsquared` en `its.py` y `did.py`.

### Qué mide
Qué fracción de la variación de los datos **logra explicar el modelo** (0 a 1).

### Fórmula

```
R² = 1 − (RSS / TSS)
```

- **RSS** = Σ(yᵢ − ŷᵢ)²  → error que deja el modelo (residuos).
- **TSS** = Σ(yᵢ − ȳ)²   → variación total de los datos.

### Ejemplo numérico
Si TSS = 1.000.000 y el modelo deja RSS = 250.000:

```
R² = 1 − 250.000 / 1.000.000 = 0,75  →  explica el 75 %
```
Es justo el valor del ITS Oficial (R² = 0,75). El DiD agregado dio 0,34 (explica
el 34 %). **Ojo:** R² mide ajuste, **no** causalidad.

---

## 4. Significancia e IC 95%

### Intervalo de Confianza al 95% (IC 95%)
Rango donde, con 95% de confianza, está el verdadero efecto.

**Regla práctica:**
- Si el IC **no incluye 0** → efecto **significativo** (improbable que sea azar).
- Si el IC **cruza 0** → **no significativo** (no se distingue de cero).

### Significancia (p-valor)
En el código: `significativo = p_value < 0.05`. Un p < 0,05 indica que el
resultado es difícil de explicar solo por azar.

### Ejemplo
- β₃ = 116.330, IC95% [−374.088; **606.749**] → incluye 0 → **no significativo**.
- DiD panel β = 0,08, IC95% [0,03; 0,13] → no incluye 0 → **significativo**.

---

## 5. Robustez
**Archivo:** `analysis/robustez.py`

### Qué es
**No es una fórmula nueva.** Es repetir los **mismos estimadores** (ITS y DiD)
bajo muchas combinaciones de supuestos y ver si el resultado **aguanta**. Se
varían tres ejes:

| Eje | Valores probados |
|---|---|
| Punto de quiebre (T₀) | 2021-S2, 2022-S1, 2022-S2, 2023-S1 |
| Forma funcional | niveles · log · diferencias |
| Muestra | todas · solo universidades · excluir IES < 500 |

Cada combinación corre el modelo y guarda: coeficiente, IC, p-valor, **signo** y
si es **significativo**. Luego se cuenta, por estimador, cuántas corridas son
significativas y si el **signo se mantiene estable**.

### Ejemplo numérico (resultado real: 84 corridas)
| Método | Corridas | Significativas | Signo estable |
|---|---:|---:|:--:|
| DiD agregado (β₃) | 24 | 0 | No |
| **DiD panel TWFE** | 12 | **12** | **Sí (positivo)** |
| ITS nivel (α₂) | 24 | 13 | No |
| ITS tendencia (α₃) | 24 | 10 | No |

**Lectura:** los resultados son **sensibles a la especificación**. Solo el DiD
panel con controles es estable y significativo (12/12) → señal de un posible
efecto positivo, pero **no robusto** en los modelos agregados.

---

## 6. Prueba de Chow (quiebre estructural)
**Archivo:** `its.py` (`prueba_chow`)

### Qué mide
Si dividir la serie en T₀ (modelo pre + modelo post) explica **significativamente
mejor** que un solo modelo. Si sí, hay quiebre estructural.

### Fórmula

```
F = [ (RSS_único − (RSS_pre + RSS_post)) / k ]
    ────────────────────────────────────────────
       [ (RSS_pre + RSS_post) / (n − 2k) ]
```
con k = 2 parámetros (intercepto + tendencia). Un p < 0,05 indica quiebre.

### Ejemplo (Oficial, matriculados)
F = 0,23, p = 0,80 → **sin evidencia de quiebre estructural**.

---

## 7. Bootstrap (incertidumbre)
**Archivo:** `analysis/bootstrap.py`

### Qué es
Re-estima el modelo **1.000 veces** remuestreando los datos, para medir qué tan
**estables** son los resultados y construir intervalos de confianza empíricos
(percentiles 2,5 % y 97,5 % de las 1.000 estimaciones).

### Ejemplo numérico (real)
- α₂ ITS: IC95% bootstrap **[−248.327; 132.303]**.
- β₃ DiD: IC95% bootstrap **[−553.342; 824.183]**.

Ambos incluyen 0 y son amplios → mucha incertidumbre, coherente con la
no-significancia.

---

## 8. Triangulación
**Archivo:** `analysis/triangulacion.py`

### Qué es
Cruzar **fuentes independientes** para ver si las tendencias son **compatibles**.
No es un cálculo causal — es una verificación de consistencia.

Tres cruces:
1. **PND/SINERGIA vs SNIES:** indicador ID 91 del PND comparado con la primera
   matrícula oficial (se comparan **tendencias**, no niveles, porque el PND mide
   avance administrativo).
2. **ICFES como contexto:** puntajes y coberturas por territorio.
3. **Embudo SNIES:** proporciones de paso entre etapas.

### El embudo y sus proporciones

```
Inscritos → Admitidos → Matriculados → Primera matrícula → Graduados
```

| Proporción | Fórmula | Ejemplo |
|---|---|---|
| Tasa de admisión | Admitidos / Inscritos | 245.042 / 655.593 = **0,37** (37 %) |
| Matrícula / admitidos | Matriculados / Admitidos | puede ser >1 (matrícula acumulada vs admitidos nuevos) |
| 1ra matrícula / matriculados | Primera matrícula / Matriculados | 240.743 / 1.117.474 = **0,21** (21 %) |

> ⚠️ **Nota del propio análisis:** *"La triangulación compara compatibilidad de
> tendencias entre fuentes. No debe leerse como atribución causal independiente."*

---

## Resumen de una línea por concepto

| Concepto | En una frase |
|---|---|
| **ITS** | Observado vs proyección sin política (un solo grupo). |
| **DiD** | Oficial vs Privada (control), antes vs después. |
| **DiD panel TWFE** | DiD por IES con efectos fijos; efecto en %. |
| **Event study** | DiD semestre a semestre; valida tendencias paralelas. |
| **R²** | Qué tanto explica el modelo (0–1). |
| **IC 95% / significancia** | Si el rango no incluye 0, el efecto es real. |
| **Chow** | ¿Hubo quiebre estructural en T₀? |
| **Bootstrap** | 1.000 simulaciones para medir incertidumbre. |
| **Robustez** | Repetir todo bajo distintos supuestos. |
| **Triangulación** | ¿Las fuentes cuentan una historia compatible? |
