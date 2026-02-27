# EDA — Análisis de Columnas de `seminario.db`

## Resumen General

| Métrica | Valor |
|---|---|
| Base de datos | `/home/yeigen/Documents/seminario/seminario/data/seminario.db` |
| Total tablas | 13 |
| Columnas únicas (sin `id`) | 82 |
| Total filas (todas las tablas) | 3,565,757 |

---

## Detalle por Tabla

### `administrativos`

**Filas:** 20,657 | **Columnas:** 33

| # | Columna | Tipo | PK | Nulos (%) | Valores de ejemplo |
|---|---|---|---|---|---|
| 1 | `id` | INTEGER | ✅ | 0.0% | 1, 2, 3 |
| 2 | `codigo_de_la_institucion` | TEXT |  | 6.0% | 1101, 1102, 1103 |
| 3 | `ies_padre` | TEXT |  | 0.1% | 1101, 1105, 1110 |
| 4 | `institucion_de_educacion_superior_ies` | TEXT |  | 0.1% | UNIVERSIDAD NACIONAL DE COLOMBIA, UNIVERSIDAD PEDAGOGICA ... |
| 5 | `principal_o_seccional` | TEXT |  | 6.0% | PRINCIPAL, SECCIONAL, Principal |
| 6 | `id_sector_ies` | TEXT |  | 0.1% | 1, 2, 1.0 |
| 7 | `sector_ies` | TEXT |  | 0.1% | OFICIAL, PRIVADA, Oficial |
| 8 | `id_caracter_ies` | TEXT |  | 77.7% | 4, 3, 2 |
| 9 | `caracter_ies` | TEXT |  | 0.1% | Universidad, Institución Universitaria/Escuela Tecnológic... |
| 10 | `codigo_del_departamento_ies` | TEXT |  | 0.1% | 11, 5, 17 |
| 11 | `departamento_de_domicilio_de_la_ies` | TEXT |  | 0.1% | BOGOTA D.C, ANTIOQUIA, CALDAS |
| 12 | `codigo_del_municipio_ies` | TEXT |  | 0.1% | 11001, 5001, 17001 |
| 13 | `municipio_de_domicilio_de_la_ies` | TEXT |  | 0.1% | BOGOTA D.C., MEDELLIN, MANIZALES |
| 14 | `ano` | TEXT |  | 0.1% | 2018, 2019, 2020 |
| 15 | `semestre` | TEXT |  | 0.1% | 1, 2, 1.0 |
| 16 | `auxiliar` | TEXT |  | 77.7% | 260, 92, 14 |
| 17 | `tecnico` | TEXT |  | 77.7% | 971, 358, 130 |
| 18 | `profesional` | TEXT |  | 77.7% | 468, 81, 38 |
| 19 | `directivo` | TEXT |  | 77.7% | 147, 54, 25 |
| 20 | `total` | TEXT |  | 77.7% | 1846, 585, 207 |
| 21 | `id_caracter` | TEXT |  | 22.4% | 4, 3, 2 |
| 22 | `id_sexo` | TEXT |  | 22.4% | 1, 2 |
| 23 | `sexo_del_docente` | TEXT |  | 22.4% | Hombre, Mujer |
| 24 | `id_maximo_nivel_de_formacion_del_docente` | TEXT |  | 22.4% | 2, 3, 4 |
| 25 | `maximo_nivel_de_formacion_del_docente` | TEXT |  | 22.4% | Doctorado, Maestría, Especialización Universitaria |
| 26 | `id_tiempo_de_dedicacion` | TEXT |  | 22.4% | 1, 3, 2 |
| 27 | `tiempo_de_dedicacion_del_docente` | TEXT |  | 22.4% | Tiempo Completo o Exclusiva, Catedra, Medio Tiempo |
| 28 | `id_tipo_de_contrato` | TEXT |  | 22.4% | 1, 4, 3 |
| 29 | `tipo_de_contrato_del_docente` | TEXT |  | 22.4% | Término Indefinido, Ocasional, Horas (profesores de catedra) |
| 30 | `no_de_docentes` | TEXT |  | 22.4% | 640, 643, 1 |
| 31 | `coodigo_de_la_institucion` | TEXT |  | 94.0% | 1101, 1102, 1103 |
| 32 | `tipo_ies` | TEXT |  | 94.0% | Principal, Seccional |
| 33 | `ies_acreditada` | TEXT |  | 94.0% | Si, No |

### `administrativos_unified`

**Filas:** 20,252 | **Columnas:** 32

| # | Columna | Tipo | PK | Nulos (%) | Valores de ejemplo |
|---|---|---|---|---|---|
| 1 | `codigo_de_la_institucion` | TEXT |  | 6.2% | 1101, 1102, 1103 |
| 2 | `ies_padre` | TEXT |  | 0.0% | 1101, 1105, 1110 |
| 3 | `institucion_de_educacion_superior_ies` | TEXT |  | 0.0% | UNIVERSIDAD NACIONAL DE COLOMBIA, UNIVERSIDAD PEDAGOGICA ... |
| 4 | `principal_o_seccional` | TEXT |  | 6.2% | PRINCIPAL, SECCIONAL, Principal |
| 5 | `id_sector_ies` | TEXT |  | 0.0% | 1, 2, 1.0 |
| 6 | `sector_ies` | TEXT |  | 0.0% | OFICIAL, PRIVADA, Oficial |
| 7 | `id_caracter_ies` | TEXT |  | 79.2% | 4, 3, 2 |
| 8 | `caracter_ies` | TEXT |  | 0.0% | Universidad, Institución Universitaria/Escuela Tecnológic... |
| 9 | `codigo_del_departamento_ies` | TEXT |  | 0.0% | 11, 5, 17 |
| 10 | `departamento_de_domicilio_de_la_ies` | TEXT |  | 0.0% | BOGOTA D.C, ANTIOQUIA, CALDAS |
| 11 | `codigo_del_municipio_ies` | TEXT |  | 0.0% | 11001, 5001, 17001 |
| 12 | `municipio_de_domicilio_de_la_ies` | TEXT |  | 0.0% | BOGOTA D.C., MEDELLIN, MANIZALES |
| 13 | `ano` | REAL |  | 0.0% | 2018.0, 2019.0, 2020.0 |
| 14 | `semestre` | TEXT |  | 0.0% | 1, 2, 1.0 |
| 15 | `auxiliar` | TEXT |  | 79.2% | 260, 92, 14 |
| 16 | `tecnico` | TEXT |  | 79.2% | 971, 358, 130 |
| 17 | `profesional` | TEXT |  | 79.2% | 468, 81, 38 |
| 18 | `directivo` | TEXT |  | 79.2% | 147, 54, 25 |
| 19 | `total` | TEXT |  | 79.2% | 1846, 585, 207 |
| 20 | `id_caracter` | TEXT |  | 20.8% | 4, 3, 2 |
| 21 | `id_sexo` | TEXT |  | 20.8% | 1, 2 |
| 22 | `sexo_del_docente` | TEXT |  | 20.8% | Hombre, Mujer |
| 23 | `id_maximo_nivel_de_formacion_del_docente` | TEXT |  | 20.8% | 2, 3, 4 |
| 24 | `maximo_nivel_de_formacion_del_docente` | TEXT |  | 20.8% | Doctorado, Maestría, Especialización Universitaria |
| 25 | `id_tiempo_de_dedicacion` | TEXT |  | 20.8% | 1, 3, 2 |
| 26 | `tiempo_de_dedicacion_del_docente` | TEXT |  | 20.8% | Tiempo Completo o Exclusiva, Catedra, Medio Tiempo |
| 27 | `id_tipo_de_contrato` | TEXT |  | 20.8% | 1, 4, 3 |
| 28 | `tipo_de_contrato_del_docente` | TEXT |  | 20.8% | Término Indefinido, Ocasional, Horas (profesores de catedra) |
| 29 | `no_de_docentes` | TEXT |  | 20.8% | 640, 643, 1 |
| 30 | `coodigo_de_la_institucion` | TEXT |  | 93.8% | 1101, 1102, 1103 |
| 31 | `tipo_ies` | TEXT |  | 93.9% | Principal, Seccional |
| 32 | `ies_acreditada` | TEXT |  | 93.9% | Si, No |

### `admitidos`

**Filas:** 313,920 | **Columnas:** 58

| # | Columna | Tipo | PK | Nulos (%) | Valores de ejemplo |
|---|---|---|---|---|---|
| 1 | `id` | INTEGER | ✅ | 0.0% | 1, 2, 3 |
| 2 | `codigo_de_la_institucion` | TEXT |  | 0.0% | 1101, 1102, 1103 |
| 3 | `ies_padre` | TEXT |  | 0.0% | 1101, 1105, 1106 |
| 4 | `institucion_de_educacion_superior_ies` | TEXT |  | 0.0% | UNIVERSIDAD NACIONAL DE COLOMBIA, UNIVERSIDAD PEDAGOGICA ... |
| 5 | `principal_o_seccional` | TEXT |  | 22.9% | Principal, Seccional, PRINCIPAL |
| 6 | `id_sector_ies` | TEXT |  | 0.0% | 1, 2, 1.0 |
| 7 | `sector_ies` | TEXT |  | 0.0% | OFICIAL, PRIVADA, Oficial |
| 8 | `id_caracter_ies` | TEXT |  | 63.3% | 4, 3, 2 |
| 9 | `caracter_ies` | TEXT |  | 0.0% | Universidad, Institución Universitaria/Escuela Tecnológic... |
| 10 | `codigo_del_departamento_ies` | TEXT |  | 0.0% | 11, 5, 17 |
| 11 | `departamento_de_domicilio_de_la_ies` | TEXT |  | 0.0% | BOGOTA D.C, ANTIOQUIA, CALDAS |
| 12 | `codigo_del_municipio_ies` | TEXT |  | 14.8% | 11001, 5001, 17001 |
| 13 | `municipio_de_domicilio_de_la_ies` | TEXT |  | 0.0% | BOGOTA D.C., MEDELLIN, MANIZALES |
| 14 | `codigo_snies_del_programa` | TEXT |  | 5.3% | 1, 2, 3 |
| 15 | `programa_academico` | TEXT |  | 5.3% | INGENIERIA AGRONOMICA, MEDICINA VETERINARIA, ZOOTECNIA |
| 16 | `id_nivel_academico` | TEXT |  | 5.3% | 1, 2, 1.0 |
| 17 | `nivel_academico` | TEXT |  | 5.3% | PREGRADO, POSGRADO, Pregrado |
| 18 | `id_nivel_de_formacion` | TEXT |  | 5.3% | 6, 10, 1 |
| 19 | `nivel_de_formacion` | TEXT |  | 5.3% | Universitaria, Especialización Médico Quirúrgica, Especia... |
| 20 | `id_metodologia` | TEXT |  | 22.9% | 1, 2, 3 |
| 21 | `metodologia` | TEXT |  | 22.9% | Presencial, Distancia (tradicional), Distancia (virtual) |
| 22 | `id_area` | TEXT |  | 7.3% | 8, 1, 2 |
| 23 | `area_de_conocimiento` | TEXT |  | 7.3% | Ingeniería, arquitectura, urbanismo y afines, Agronomía, ... |
| 24 | `id_nucleo` | TEXT |  | 7.3% | 824, 13, 12 |
| 25 | `nucleo_basico_del_conocimiento_nbc` | TEXT |  | 7.3% | Ingeniería agronómica, pecuaria y afines, Medicina veteri... |
| 26 | `codigo_del_departamento_programa` | TEXT |  | 5.3% | 11, 5, 17 |
| 27 | `departamento_de_oferta_del_programa` | TEXT |  | 5.3% | BOGOTA D.C, ANTIOQUIA, CALDAS |
| 28 | `codigo_del_municipio_programa` | TEXT |  | 5.3% | 11001, 5001, 17001 |
| 29 | `municipio_de_oferta_del_programa` | TEXT |  | 5.3% | BOGOTA D.C., MEDELLIN, MANIZALES |
| 30 | `id_sexo` | TEXT |  | 0.0% | 1, 2, 0 |
| 31 | `sexo` | TEXT |  | 5.3% | Hombre, Mujer, NO INFORMA |
| 32 | `ano` | TEXT |  | 0.0% | 2018, 2019, 2020 |
| 33 | `semestre` | TEXT |  | 0.0% | 1, 2, 1.0 |
| 34 | `admisiones_2018` | TEXT |  | 86.2% | 103, 70, 45 |
| 35 | `id_caracter` | TEXT |  | 36.7% | 4, 1, 2 |
| 36 | `codigo_del_municipio` | TEXT |  | 85.3% | 11001, 52001, 76111 |
| 37 | `id_cine_campo_amplio` | TEXT |  | 19.1% | 4, 1, 7 |
| 38 | `desc_cine_campo_amplio` | TEXT |  | 19.1% | ADMINISTRACIÓN DE EMPRESAS Y DERECHO, EDUCACIÓN, INGENIER... |
| 39 | `id_cine_campo_especifico` | TEXT |  | 19.1% | 41, 42, 11 |
| 40 | `desc_cine_campo_especifico` | TEXT |  | 19.1% | EDUCACIÓN COMERCIAL Y ADMINISTRACIÓN, DERECHO, EDUCACIÓN |
| 41 | `id_cine_codigo_detallado` | TEXT |  | 36.7% | 413, 421, 113 |
| 42 | `desc_cine_codigo_detallado` | TEXT |  | 36.7% | GESTIÓN Y ADMINISTRACIÓN, DERECHO, FORMACIÓN PARA DOCENTE... |
| 43 | `admitidos` | TEXT |  | 19.1% | 13, 10, 1 |
| 44 | `tipo_ies` | TEXT |  | 77.1% | Principal, Seccional |
| 45 | `ies_acreditada` | TEXT |  | 77.1% | Si, No |
| 46 | `sexo_del_docente` | TEXT |  | 94.7% | Masculino, Femenino |
| 47 | `id_maximo_nivel_de_formacion_del_docente` | TEXT |  | 94.7% | 2.0, 3.0, 4.0 |
| 48 | `maximo_nivel_de_formacion_del_docente` | TEXT |  | 94.7% | Doctorado, Maestría, Especialización Universitaria |
| 49 | `id_tiempo_de_dedicacion` | TEXT |  | 94.7% | 1.0, 4.0, 2.0 |
| 50 | `tiempo_de_dedicacion_del_docente` | TEXT |  | 94.7% | Tiempo Completo o Exclusiva, Catedra, Medio Tiempo |
| 51 | `id_tipo_de_contrato` | TEXT |  | 94.7% | 1.0, 4.0, 3.0 |
| 52 | `tipo_de_contrato` | TEXT |  | 94.7% | Término Indefinido, Ocasional, Horas (profesores de catedra) |
| 53 | `docentes` | TEXT |  | 94.7% | 640.0, 658.0, 1.0 |
| 54 | `programa_acreditado` | TEXT |  | 82.5% | Si, No |
| 55 | `id_modalidad` | TEXT |  | 82.5% | 1.0, 2.0, 4.0 |
| 56 | `modalidad` | TEXT |  | 82.5% | Presencial, A distancia, Presencial-Virtual |
| 57 | `id_cine_campo_detallado` | TEXT |  | 82.5% | 811.0, 915.0, 732.0 |
| 58 | `desc_cine_campo_detallado` | TEXT |  | 82.5% | Producción agrícola y ganadera, Fisioterapia, Fonoaudiolo... |

### `admitidos_unified`

**Filas:** 313,917 | **Columnas:** 57

| # | Columna | Tipo | PK | Nulos (%) | Valores de ejemplo |
|---|---|---|---|---|---|
| 1 | `codigo_de_la_institucion` | TEXT |  | 0.0% | 1101, 1102, 1103 |
| 2 | `ies_padre` | TEXT |  | 0.0% | 1101, 1105, 1106 |
| 3 | `institucion_de_educacion_superior_ies` | TEXT |  | 0.0% | UNIVERSIDAD NACIONAL DE COLOMBIA, UNIVERSIDAD PEDAGOGICA ... |
| 4 | `principal_o_seccional` | TEXT |  | 22.9% | Principal, Seccional, PRINCIPAL |
| 5 | `id_sector_ies` | TEXT |  | 0.0% | 1, 2, 1.0 |
| 6 | `sector_ies` | TEXT |  | 0.0% | OFICIAL, PRIVADA, Oficial |
| 7 | `id_caracter_ies` | TEXT |  | 63.3% | 4, 3, 2 |
| 8 | `caracter_ies` | TEXT |  | 0.0% | Universidad, Institución Universitaria/Escuela Tecnológic... |
| 9 | `codigo_del_departamento_ies` | TEXT |  | 0.0% | 11, 5, 17 |
| 10 | `departamento_de_domicilio_de_la_ies` | TEXT |  | 0.0% | BOGOTA D.C, ANTIOQUIA, CALDAS |
| 11 | `codigo_del_municipio_ies` | TEXT |  | 14.8% | 11001, 5001, 17001 |
| 12 | `municipio_de_domicilio_de_la_ies` | TEXT |  | 0.0% | BOGOTA D.C., MEDELLIN, MANIZALES |
| 13 | `codigo_snies_del_programa` | TEXT |  | 5.3% | 1, 2, 3 |
| 14 | `programa_academico` | TEXT |  | 5.3% | INGENIERIA AGRONOMICA, MEDICINA VETERINARIA, ZOOTECNIA |
| 15 | `id_nivel_academico` | TEXT |  | 5.3% | 1, 2, 1.0 |
| 16 | `nivel_academico` | TEXT |  | 5.3% | PREGRADO, POSGRADO, Pregrado |
| 17 | `id_nivel_de_formacion` | TEXT |  | 5.3% | 6, 10, 1 |
| 18 | `nivel_de_formacion` | TEXT |  | 5.3% | Universitaria, Especialización Médico Quirúrgica, Especia... |
| 19 | `id_metodologia` | TEXT |  | 22.9% | 1, 2, 3 |
| 20 | `metodologia` | TEXT |  | 22.9% | Presencial, Distancia (tradicional), Distancia (virtual) |
| 21 | `id_area` | TEXT |  | 7.3% | 8, 1, 2 |
| 22 | `area_de_conocimiento` | TEXT |  | 7.3% | Ingeniería, arquitectura, urbanismo y afines, Agronomía, ... |
| 23 | `id_nucleo` | TEXT |  | 7.3% | 824, 13, 12 |
| 24 | `nucleo_basico_del_conocimiento_nbc` | TEXT |  | 7.3% | Ingeniería agronómica, pecuaria y afines, Medicina veteri... |
| 25 | `codigo_del_departamento_programa` | TEXT |  | 5.3% | 11, 5, 17 |
| 26 | `departamento_de_oferta_del_programa` | TEXT |  | 5.3% | BOGOTA D.C, ANTIOQUIA, CALDAS |
| 27 | `codigo_del_municipio_programa` | TEXT |  | 5.3% | 11001, 5001, 17001 |
| 28 | `municipio_de_oferta_del_programa` | TEXT |  | 5.3% | BOGOTA D.C., MEDELLIN, MANIZALES |
| 29 | `id_sexo` | TEXT |  | 0.0% | 1, 2, 0 |
| 30 | `sexo` | TEXT |  | 5.3% | Hombre, Mujer, NO INFORMA |
| 31 | `ano` | REAL |  | 0.0% | 2018.0, 2019.0, 2020.0 |
| 32 | `semestre` | TEXT |  | 0.0% | 1, 2, 1.0 |
| 33 | `admisiones_2018` | TEXT |  | 86.2% | 103, 70, 45 |
| 34 | `id_caracter` | TEXT |  | 36.7% | 4, 1, 2 |
| 35 | `codigo_del_municipio` | TEXT |  | 85.3% | 11001, 52001, 76111 |
| 36 | `id_cine_campo_amplio` | TEXT |  | 19.1% | 4, 1, 7 |
| 37 | `desc_cine_campo_amplio` | TEXT |  | 19.1% | ADMINISTRACIÓN DE EMPRESAS Y DERECHO, EDUCACIÓN, INGENIER... |
| 38 | `id_cine_campo_especifico` | TEXT |  | 19.1% | 41, 42, 11 |
| 39 | `desc_cine_campo_especifico` | TEXT |  | 19.1% | EDUCACIÓN COMERCIAL Y ADMINISTRACIÓN, DERECHO, EDUCACIÓN |
| 40 | `id_cine_codigo_detallado` | TEXT |  | 36.7% | 413, 421, 113 |
| 41 | `desc_cine_codigo_detallado` | TEXT |  | 36.7% | GESTIÓN Y ADMINISTRACIÓN, DERECHO, FORMACIÓN PARA DOCENTE... |
| 42 | `admitidos` | TEXT |  | 19.1% | 13, 10, 1 |
| 43 | `tipo_ies` | TEXT |  | 77.1% | Principal, Seccional |
| 44 | `ies_acreditada` | TEXT |  | 77.1% | Si, No |
| 45 | `sexo_del_docente` | TEXT |  | 94.7% | Masculino, Femenino |
| 46 | `id_maximo_nivel_de_formacion_del_docente` | TEXT |  | 94.7% | 2.0, 3.0, 4.0 |
| 47 | `maximo_nivel_de_formacion_del_docente` | TEXT |  | 94.7% | Doctorado, Maestría, Especialización Universitaria |
| 48 | `id_tiempo_de_dedicacion` | TEXT |  | 94.7% | 1.0, 4.0, 2.0 |
| 49 | `tiempo_de_dedicacion_del_docente` | TEXT |  | 94.7% | Tiempo Completo o Exclusiva, Catedra, Medio Tiempo |
| 50 | `id_tipo_de_contrato` | TEXT |  | 94.7% | 1.0, 4.0, 3.0 |
| 51 | `tipo_de_contrato` | TEXT |  | 94.7% | Término Indefinido, Ocasional, Horas (profesores de catedra) |
| 52 | `docentes` | TEXT |  | 94.7% | 640.0, 658.0, 1.0 |
| 53 | `programa_acreditado` | TEXT |  | 82.5% | Si, No |
| 54 | `id_modalidad` | TEXT |  | 82.5% | 1.0, 2.0, 4.0 |
| 55 | `modalidad` | TEXT |  | 82.5% | Presencial, A distancia, Presencial-Virtual |
| 56 | `id_cine_campo_detallado` | TEXT |  | 82.5% | 811.0, 915.0, 732.0 |
| 57 | `desc_cine_campo_detallado` | TEXT |  | 82.5% | Producción agrícola y ganadera, Fisioterapia, Fonoaudiolo... |

### `docentes`

**Filas:** 133,453 | **Columnas:** 56

| # | Columna | Tipo | PK | Nulos (%) | Valores de ejemplo |
|---|---|---|---|---|---|
| 1 | `id` | INTEGER | ✅ | 0.0% | 1, 2, 3 |
| 2 | `codigo_de_la_institucion` | TEXT |  | 0.0% | 1101, 1102, 1103 |
| 3 | `ies_padre` | TEXT |  | 0.0% | 1101, 1105, 1106 |
| 4 | `institucion_de_educacion_superior_ies` | TEXT |  | 0.0% | UNIVERSIDAD NACIONAL DE COLOMBIA, UNIVERSIDAD PEDAGOGICA ... |
| 5 | `principal_o_seccional` | TEXT |  | 51.6% | PRINCIPAL, SECCIONAL, Principal |
| 6 | `id_sector_ies` | TEXT |  | 0.0% | 1, 2, 1.0 |
| 7 | `sector_ies` | TEXT |  | 0.0% | OFICIAL, PRIVADA, Oficial |
| 8 | `id_caracter` | TEXT |  | 51.6% | 4, 3, 2 |
| 9 | `caracter_ies` | TEXT |  | 0.0% | Universidad, Institución Universitaria/Escuela Tecnológic... |
| 10 | `codigo_del_departamento_ies` | TEXT |  | 0.0% | 11, 5, 17 |
| 11 | `departamento_de_domicilio_de_la_ies` | TEXT |  | 0.0% | BOGOTA D.C, ANTIOQUIA, CALDAS |
| 12 | `codigo_del_municipio_ies` | TEXT |  | 0.0% | 11001, 5001, 17001 |
| 13 | `municipio_de_domicilio_de_la_ies` | TEXT |  | 0.0% | BOGOTA D.C., MEDELLIN, MANIZALES |
| 14 | `id_sexo` | TEXT |  | 0.0% | 1, 2, 0 |
| 15 | `sexo_del_docente` | TEXT |  | 39.2% | Hombre, Mujer, HOMBRE |
| 16 | `id_maximo_nivel_de_formacion_del_docente` | TEXT |  | 39.2% | 2, 3, 4 |
| 17 | `maximo_nivel_de_formacion_del_docente` | TEXT |  | 39.2% | Doctorado, Maestría, Especialización Universitaria |
| 18 | `id_tiempo_de_dedicacion` | TEXT |  | 39.2% | 1, 4, 2 |
| 19 | `tiempo_de_dedicacion_del_docente` | TEXT |  | 39.2% | Tiempo Completo o Exclusiva, Catedra, Medio Tiempo |
| 20 | `id_tipo_de_contrato` | TEXT |  | 39.2% | 1, 4, 3 |
| 21 | `tipo_de_contrato_del_docente` | TEXT |  | 51.6% | Término Indefinido, Ocasional, Horas (profesores de catedra) |
| 22 | `ano` | TEXT |  | 0.0% | 2018, 2019, <U+FEFF>2019 |
| 23 | `semestre` | TEXT |  | 0.0% | 1, 2, 1.0 |
| 24 | `no_de_docentes` | TEXT |  | 51.6% | 603, 614, 62 |
| 25 | `tiempo_de_dedicacion_del_docente_1` | TEXT |  | 87.9% | CATEDRA, TIEMPO COMPLETO O EXCLUSIVA, MEDIO TIEMPO |
| 26 | `nivel_cine` | TEXT |  | 87.7% | 8.0, 7.0, 6.0 |
| 27 | `tipo_ies` | TEXT |  | 48.4% | Principal, Seccional |
| 28 | `id_caracter_ies` | TEXT |  | 48.4% | 4.0, 3.0, 2.0 |
| 29 | `ies_acreditada` | TEXT |  | 48.4% | Si, No |
| 30 | `codigo_snies_del_programa` | TEXT |  | 60.8% | 1.0, 2.0, 3.0 |
| 31 | `programa_academico` | TEXT |  | 60.8% | INGENIERIA AGRONOMICA, MEDICINA VETERINARIA, ZOOTECNIA |
| 32 | `programa_acreditado` | TEXT |  | 60.8% | Si, No |
| 33 | `id_nivel_academico` | TEXT |  | 60.8% | 1.0, 2.0 |
| 34 | `nivel_academico` | TEXT |  | 60.8% | Pregrado, Posgrado |
| 35 | `id_nivel_de_formacion` | TEXT |  | 60.8% | 6.0, 10.0, 1.0 |
| 36 | `nivel_de_formacion` | TEXT |  | 60.8% | Universitario, Especialización médico quirúrgica, Especia... |
| 37 | `id_modalidad` | TEXT |  | 60.8% | 1.0, 4.0, 2.0 |
| 38 | `modalidad` | TEXT |  | 60.8% | Presencial, Presencial-Virtual, A distancia |
| 39 | `id_area` | TEXT |  | 60.8% | 8.0, 1.0, 2.0 |
| 40 | `area_de_conocimiento` | TEXT |  | 60.8% | Ingeniería, arquitectura, urbanismo y afines, Agronomía, ... |
| 41 | `id_nucleo` | TEXT |  | 60.8% | 824.0, 13.0, 12.0 |
| 42 | `nucleo_basico_del_conocimiento_nbc` | TEXT |  | 60.8% | Ingeniería agronómica, pecuaria y afines, Medicina veteri... |
| 43 | `id_cine_campo_amplio` | TEXT |  | 60.8% | 8.0, 2.0, 9.0 |
| 44 | `desc_cine_campo_amplio` | TEXT |  | 60.8% | Agropecuario, Silvicultura, Pesca y Veterinaria, Arte y H... |
| 45 | `id_cine_campo_especifico` | TEXT |  | 60.8% | 81.0, 84.0, 21.0 |
| 46 | `desc_cine_campo_especifico` | TEXT |  | 60.8% | Agropecuario, Veterinaria, Artes |
| 47 | `id_cine_campo_detallado` | TEXT |  | 60.8% | 811.0, 841.0, 219.0 |
| 48 | `desc_cine_campo_detallado` | TEXT |  | 60.8% | Producción agrícola y ganadera, Veterinaria, Artes no cla... |
| 49 | `codigo_del_departamento_programa` | TEXT |  | 60.8% | 11.0, 5.0, 17.0 |
| 50 | `departamento_de_oferta_del_programa` | TEXT |  | 60.8% | Bogotá, D.C., Antioquia, Caldas |
| 51 | `codigo_del_municipio_programa` | TEXT |  | 60.8% | 11001.0, 5001.0, 17001.0 |
| 52 | `municipio_de_oferta_del_programa` | TEXT |  | 60.8% | Bogotá, D.C., Medellín, Manizales |
| 53 | `sexo` | TEXT |  | 60.8% | Masculino, Femenino, No binario |
| 54 | `admitidos` | TEXT |  | 60.8% | 67.0, 65.0, 36.0 |
| 55 | `tipo_de_contrato` | TEXT |  | 87.6% | Término Indefinido, Ocasional, Horas (profesores de catedra) |
| 56 | `docentes` | TEXT |  | 87.6% | 67.0, 64.0, 73.0 |

### `docentes_unified`

**Filas:** 133,450 | **Columnas:** 55

| # | Columna | Tipo | PK | Nulos (%) | Valores de ejemplo |
|---|---|---|---|---|---|
| 1 | `codigo_de_la_institucion` | TEXT |  | 0.0% | 1101, 1102, 1103 |
| 2 | `ies_padre` | TEXT |  | 0.0% | 1101, 1105, 1106 |
| 3 | `institucion_de_educacion_superior_ies` | TEXT |  | 0.0% | UNIVERSIDAD NACIONAL DE COLOMBIA, UNIVERSIDAD PEDAGOGICA ... |
| 4 | `principal_o_seccional` | TEXT |  | 51.6% | PRINCIPAL, SECCIONAL, Principal |
| 5 | `id_sector_ies` | TEXT |  | 0.0% | 1, 2, 1.0 |
| 6 | `sector_ies` | TEXT |  | 0.0% | OFICIAL, PRIVADA, Oficial |
| 7 | `id_caracter` | TEXT |  | 51.6% | 4, 3, 2 |
| 8 | `caracter_ies` | TEXT |  | 0.0% | Universidad, Institución Universitaria/Escuela Tecnológic... |
| 9 | `codigo_del_departamento_ies` | TEXT |  | 0.0% | 11, 5, 17 |
| 10 | `departamento_de_domicilio_de_la_ies` | TEXT |  | 0.0% | BOGOTA D.C, ANTIOQUIA, CALDAS |
| 11 | `codigo_del_municipio_ies` | TEXT |  | 0.0% | 11001, 5001, 17001 |
| 12 | `municipio_de_domicilio_de_la_ies` | TEXT |  | 0.0% | BOGOTA D.C., MEDELLIN, MANIZALES |
| 13 | `id_sexo` | TEXT |  | 0.0% | 1, 2, 0 |
| 14 | `sexo_del_docente` | TEXT |  | 39.2% | Hombre, Mujer, HOMBRE |
| 15 | `id_maximo_nivel_de_formacion_del_docente` | TEXT |  | 39.2% | 2, 3, 4 |
| 16 | `maximo_nivel_de_formacion_del_docente` | TEXT |  | 39.2% | Doctorado, Maestría, Especialización Universitaria |
| 17 | `id_tiempo_de_dedicacion` | TEXT |  | 39.2% | 1, 4, 2 |
| 18 | `tiempo_de_dedicacion_del_docente` | TEXT |  | 39.2% | Tiempo Completo o Exclusiva, Catedra, Medio Tiempo |
| 19 | `id_tipo_de_contrato` | TEXT |  | 39.2% | 1, 4, 3 |
| 20 | `tipo_de_contrato_del_docente` | TEXT |  | 51.6% | Término Indefinido, Ocasional, Horas (profesores de catedra) |
| 21 | `ano` | REAL |  | 0.0% | 2018.0, 2019.0, 2020.0 |
| 22 | `semestre` | TEXT |  | 0.0% | 1, 2, 1.0 |
| 23 | `no_de_docentes` | TEXT |  | 51.6% | 603, 614, 62 |
| 24 | `tiempo_de_dedicacion_del_docente_1` | TEXT |  | 87.9% | CATEDRA, TIEMPO COMPLETO O EXCLUSIVA, MEDIO TIEMPO |
| 25 | `nivel_cine` | TEXT |  | 87.7% | 8.0, 7.0, 6.0 |
| 26 | `tipo_ies` | TEXT |  | 48.4% | Principal, Seccional |
| 27 | `id_caracter_ies` | TEXT |  | 48.4% | 4.0, 3.0, 2.0 |
| 28 | `ies_acreditada` | TEXT |  | 48.4% | Si, No |
| 29 | `codigo_snies_del_programa` | TEXT |  | 60.8% | 1.0, 2.0, 3.0 |
| 30 | `programa_academico` | TEXT |  | 60.8% | INGENIERIA AGRONOMICA, MEDICINA VETERINARIA, ZOOTECNIA |
| 31 | `programa_acreditado` | TEXT |  | 60.8% | Si, No |
| 32 | `id_nivel_academico` | TEXT |  | 60.8% | 1.0, 2.0 |
| 33 | `nivel_academico` | TEXT |  | 60.8% | Pregrado, Posgrado |
| 34 | `id_nivel_de_formacion` | TEXT |  | 60.8% | 6.0, 10.0, 1.0 |
| 35 | `nivel_de_formacion` | TEXT |  | 60.8% | Universitario, Especialización médico quirúrgica, Especia... |
| 36 | `id_modalidad` | TEXT |  | 60.8% | 1.0, 4.0, 2.0 |
| 37 | `modalidad` | TEXT |  | 60.8% | Presencial, Presencial-Virtual, A distancia |
| 38 | `id_area` | TEXT |  | 60.8% | 8.0, 1.0, 2.0 |
| 39 | `area_de_conocimiento` | TEXT |  | 60.8% | Ingeniería, arquitectura, urbanismo y afines, Agronomía, ... |
| 40 | `id_nucleo` | TEXT |  | 60.8% | 824.0, 13.0, 12.0 |
| 41 | `nucleo_basico_del_conocimiento_nbc` | TEXT |  | 60.8% | Ingeniería agronómica, pecuaria y afines, Medicina veteri... |
| 42 | `id_cine_campo_amplio` | TEXT |  | 60.8% | 8.0, 2.0, 9.0 |
| 43 | `desc_cine_campo_amplio` | TEXT |  | 60.8% | Agropecuario, Silvicultura, Pesca y Veterinaria, Arte y H... |
| 44 | `id_cine_campo_especifico` | TEXT |  | 60.8% | 81.0, 84.0, 21.0 |
| 45 | `desc_cine_campo_especifico` | TEXT |  | 60.8% | Agropecuario, Veterinaria, Artes |
| 46 | `id_cine_campo_detallado` | TEXT |  | 60.8% | 811.0, 841.0, 219.0 |
| 47 | `desc_cine_campo_detallado` | TEXT |  | 60.8% | Producción agrícola y ganadera, Veterinaria, Artes no cla... |
| 48 | `codigo_del_departamento_programa` | TEXT |  | 60.8% | 11.0, 5.0, 17.0 |
| 49 | `departamento_de_oferta_del_programa` | TEXT |  | 60.8% | Bogotá, D.C., Antioquia, Caldas |
| 50 | `codigo_del_municipio_programa` | TEXT |  | 60.8% | 11001.0, 5001.0, 17001.0 |
| 51 | `municipio_de_oferta_del_programa` | TEXT |  | 60.8% | Bogotá, D.C., Medellín, Manizales |
| 52 | `sexo` | TEXT |  | 60.8% | Masculino, Femenino, No binario |
| 53 | `admitidos` | TEXT |  | 60.8% | 67.0, 65.0, 36.0 |
| 54 | `tipo_de_contrato` | TEXT |  | 87.6% | Término Indefinido, Ocasional, Horas (profesores de catedra) |
| 55 | `docentes` | TEXT |  | 87.6% | 67.0, 64.0, 73.0 |

### `graduados`

**Filas:** 307,601 | **Columnas:** 53

| # | Columna | Tipo | PK | Nulos (%) | Valores de ejemplo |
|---|---|---|---|---|---|
| 1 | `id` | INTEGER | ✅ | 0.0% | 1, 2, 3 |
| 2 | `codigo_de_la_institucion` | TEXT |  | 0.0% | 1101, 1102, 1103 |
| 3 | `ies_padre` | TEXT |  | 0.0% | 1101, 1105, 1106 |
| 4 | `institucion_de_educacion_superior_ies` | TEXT |  | 0.0% | UNIVERSIDAD NACIONAL DE COLOMBIA, UNIVERSIDAD PEDAGOGICA ... |
| 5 | `principal_o_seccional` | TEXT |  | 30.0% | Principal, Seccional, PRINCIPAL |
| 6 | `id_sector_ies` | TEXT |  | 0.0% | 1, 2, 1.0 |
| 7 | `sector_ies` | TEXT |  | 0.0% | OFICIAL, PRIVADA, Oficial |
| 8 | `id_caracter` | TEXT |  | 30.0% | 4, 3, 2 |
| 9 | `caracter_ies` | TEXT |  | 0.0% | Universidad, Institución Universitaria/Escuela Tecnológic... |
| 10 | `codigo_del_departamento_ies` | TEXT |  | 0.0% | 11, 5, 17 |
| 11 | `departamento_de_domicilio_de_la_ies` | TEXT |  | 0.0% | BOGOTA D.C, ANTIOQUIA, CALDAS |
| 12 | `codigo_del_municipio` | TEXT |  | 73.0% | 11001, 5001, 17001 |
| 13 | `municipio_de_domicilio_de_la_ies` | TEXT |  | 0.0% | BOGOTA D.C., MEDELLIN, MANIZALES |
| 14 | `codigo_snies_del_programa` | TEXT |  | 0.0% | 1, 2, 3 |
| 15 | `programa_academico` | TEXT |  | 14.8% | INGENIERIA AGRONOMICA, MEDICINA VETERINARIA, ZOOTECNIA |
| 16 | `id_nivel_academico` | TEXT |  | 14.8% | 1, 2, 1.0 |
| 17 | `nivel_academico` | TEXT |  | 14.8% | PREGRADO, POSGRADO, Pregrado |
| 18 | `id_nivel_de_formacion` | TEXT |  | 14.8% | 6, 10, 1 |
| 19 | `nivel_de_formacion` | TEXT |  | 14.8% | Universitaria, Especialización Médico Quirúrgica, Especia... |
| 20 | `id_metodologia` | TEXT |  | 30.0% | 1, 2, 3 |
| 21 | `metodologia` | TEXT |  | 30.0% | Presencial, Distancia (tradicional), Distancia (virtual) |
| 22 | `id_area` | TEXT |  | 29.4% | 8, 1, 2 |
| 23 | `area_de_conocimiento` | TEXT |  | 14.8% | Ingeniería, arquitectura, urbanismo y afines, Agronomía, ... |
| 24 | `id_nucleo` | TEXT |  | 14.8% | 824, 13, 12 |
| 25 | `nucleo_basico_del_conocimiento_nbc` | TEXT |  | 14.8% | Ingeniería agronómica, pecuaria y afines, Medicina veteri... |
| 26 | `codigo_del_departamento_programa` | TEXT |  | 14.8% | 11, 5, 17 |
| 27 | `departamento_de_oferta_del_programa` | TEXT |  | 14.8% | BOGOTA D.C, ANTIOQUIA, CALDAS |
| 28 | `codigo_del_municipio_programa` | TEXT |  | 29.6% | 11001, 5001, 17001 |
| 29 | `municipio_de_oferta_del_programa` | TEXT |  | 14.8% | BOGOTA D.C., MEDELLIN, MANIZALES |
| 30 | `id_sexo` | TEXT |  | 14.8% | 1, 2, 1.0 |
| 31 | `sexo` | TEXT |  | 14.8% | Hombre, Mujer, MUJER |
| 32 | `ano` | TEXT |  | 14.8% | 2018, 2019, 2020 |
| 33 | `semestre` | TEXT |  | 14.8% | 1, 2, 1.0 |
| 34 | `graduados` | TEXT |  | 14.8% | 16, 35, 17 |
| 35 | `id_cine_campo_amplio` | TEXT |  | 56.5% | 3, 9, 5 |
| 36 | `desc_cine_campo_amplio` | TEXT |  | 56.5% | CIENCIAS SOCIALES, PERIODISMO E INFORMACIÓN, SALUD Y BIEN... |
| 37 | `id_cine_campo_especifico` | TEXT |  | 28.0% | 31, 91, 32 |
| 38 | `desc_cine_campo_especifico` | TEXT |  | 28.0% | CIENCIAS SOCIALES Y DEL COMPORTAMIENTO, SALUD, PERIODISMO... |
| 39 | `id_cine_codigo_detallado` | TEXT |  | 43.1% | 311, 912, 321 |
| 40 | `desc_cine_codigo_detallado` | TEXT |  | 43.1% | ECONOMÍA, MEDICINA, PERIODISMO, COMUNICACIÓN Y REPORTAJES |
| 41 | `codigo_del_municipio_ies` | TEXT |  | 27.0% | 11001, 5001, 17001 |
| 42 | `id_cine_campo_amplio_desc` | TEXT |  | 71.5% | 8, 2, 9 |
| 43 | `cine_campo_amplio` | TEXT |  | 71.5% | Agropecuario, Silvicultura, Pesca y Veterinaria, Arte y H... |
| 44 | `ies_acreditada` | TEXT |  | 40.7% | S, N, Si |
| 45 | `programa_acreditado` | TEXT |  | 55.5% | S, N, Si |
| 46 | `id_area_de_conocimiento` | TEXT |  | 85.4% | 8, 1, 2 |
| 47 | `cdigo_del_municipio_programa` | TEXT |  | 85.2% | 11001, 5001, 17001 |
| 48 | `tipo_ies` | TEXT |  | 70.0% | Principal, Seccional |
| 49 | `id_caracter_ies` | TEXT |  | 70.0% | 4.0, 3.0, 2.0 |
| 50 | `id_modalidad` | TEXT |  | 84.9% | 1.0, 2.0, 4.0 |
| 51 | `modalidad` | TEXT |  | 84.9% | Presencial, A distancia, Presencial-Virtual |
| 52 | `id_cine_campo_detallado` | TEXT |  | 84.9% | 811.0, 915.0, 732.0 |
| 53 | `desc_cine_campo_detallado` | TEXT |  | 84.9% | Producción agrícola y ganadera, Fisioterapia, Fonoaudiolo... |

### `graduados_unified`

**Filas:** 274,073 | **Columnas:** 52

| # | Columna | Tipo | PK | Nulos (%) | Valores de ejemplo |
|---|---|---|---|---|---|
| 1 | `codigo_de_la_institucion` | TEXT |  | 0.0% | 1101, 1102, 1103 |
| 2 | `ies_padre` | TEXT |  | 0.0% | 1101, 1105, 1106 |
| 3 | `institucion_de_educacion_superior_ies` | TEXT |  | 0.0% | UNIVERSIDAD NACIONAL DE COLOMBIA, UNIVERSIDAD PEDAGOGICA ... |
| 4 | `principal_o_seccional` | TEXT |  | 21.4% | Principal, Seccional, PRINCIPAL |
| 5 | `id_sector_ies` | TEXT |  | 0.0% | 1, 2, 1.0 |
| 6 | `sector_ies` | TEXT |  | 0.0% | OFICIAL, PRIVADA, Oficial |
| 7 | `id_caracter` | TEXT |  | 21.4% | 4, 3, 2 |
| 8 | `caracter_ies` | TEXT |  | 0.0% | Universidad, Institución Universitaria/Escuela Tecnológic... |
| 9 | `codigo_del_departamento_ies` | TEXT |  | 0.0% | 11, 5, 17 |
| 10 | `departamento_de_domicilio_de_la_ies` | TEXT |  | 0.0% | BOGOTA D.C, ANTIOQUIA, CALDAS |
| 11 | `codigo_del_municipio` | TEXT |  | 69.7% | 11001, 5001, 17001 |
| 12 | `municipio_de_domicilio_de_la_ies` | TEXT |  | 0.0% | BOGOTA D.C., MEDELLIN, MANIZALES |
| 13 | `codigo_snies_del_programa` | TEXT |  | 0.0% | 1, 2, 3 |
| 14 | `programa_academico` | TEXT |  | 4.4% | INGENIERIA AGRONOMICA, MEDICINA VETERINARIA, ZOOTECNIA |
| 15 | `id_nivel_academico` | TEXT |  | 4.4% | 1, 2, 1.0 |
| 16 | `nivel_academico` | TEXT |  | 4.4% | PREGRADO, POSGRADO, Pregrado |
| 17 | `id_nivel_de_formacion` | TEXT |  | 4.4% | 6, 10, 1 |
| 18 | `nivel_de_formacion` | TEXT |  | 4.4% | Universitaria, Especialización Médico Quirúrgica, Especia... |
| 19 | `id_metodologia` | TEXT |  | 21.4% | 1, 2, 3 |
| 20 | `metodologia` | TEXT |  | 21.4% | Presencial, Distancia (tradicional), Distancia (virtual) |
| 21 | `id_area` | TEXT |  | 20.8% | 8, 1, 2 |
| 22 | `area_de_conocimiento` | TEXT |  | 4.4% | Ingeniería, arquitectura, urbanismo y afines, Agronomía, ... |
| 23 | `id_nucleo` | TEXT |  | 4.4% | 824, 13, 12 |
| 24 | `nucleo_basico_del_conocimiento_nbc` | TEXT |  | 4.4% | Ingeniería agronómica, pecuaria y afines, Medicina veteri... |
| 25 | `codigo_del_departamento_programa` | TEXT |  | 4.4% | 11, 5, 17 |
| 26 | `departamento_de_oferta_del_programa` | TEXT |  | 4.4% | BOGOTA D.C, ANTIOQUIA, CALDAS |
| 27 | `codigo_del_municipio_programa` | TEXT |  | 21.0% | 11001, 5001, 17001 |
| 28 | `municipio_de_oferta_del_programa` | TEXT |  | 4.4% | BOGOTA D.C., MEDELLIN, MANIZALES |
| 29 | `id_sexo` | TEXT |  | 4.4% | 1, 2, 1.0 |
| 30 | `sexo` | TEXT |  | 4.4% | Hombre, Mujer, MUJER |
| 31 | `ano` | REAL |  | 4.4% | 2018.0, 2019.0, 2020.0 |
| 32 | `semestre` | TEXT |  | 4.4% | 1, 2, 1.0 |
| 33 | `graduados` | TEXT |  | 4.4% | 16, 35, 17 |
| 34 | `id_cine_campo_amplio` | TEXT |  | 51.2% | 3, 9, 5 |
| 35 | `desc_cine_campo_amplio` | TEXT |  | 51.2% | CIENCIAS SOCIALES, PERIODISMO E INFORMACIÓN, SALUD Y BIEN... |
| 36 | `id_cine_campo_especifico` | TEXT |  | 19.2% | 31, 91, 32 |
| 37 | `desc_cine_campo_especifico` | TEXT |  | 19.2% | CIENCIAS SOCIALES Y DEL COMPORTAMIENTO, SALUD, PERIODISMO... |
| 38 | `id_cine_codigo_detallado` | TEXT |  | 36.2% | 311, 912, 321 |
| 39 | `desc_cine_codigo_detallado` | TEXT |  | 36.2% | ECONOMÍA, MEDICINA, PERIODISMO, COMUNICACIÓN Y REPORTAJES |
| 40 | `codigo_del_municipio_ies` | TEXT |  | 30.3% | 11001, 5001, 17001 |
| 41 | `id_cine_campo_amplio_desc` | TEXT |  | 68.0% | 8, 2, 9 |
| 42 | `cine_campo_amplio` | TEXT |  | 68.0% | Agropecuario, Silvicultura, Pesca y Veterinaria, Arte y H... |
| 43 | `ies_acreditada` | TEXT |  | 45.7% | S, N, Si |
| 44 | `programa_acreditado` | TEXT |  | 50.1% | S, N, Si |
| 45 | `id_area_de_conocimiento` | TEXT |  | 83.6% | 8, 1, 2 |
| 46 | `cdigo_del_municipio_programa` | TEXT |  | 83.4% | 11001, 5001, 17001 |
| 47 | `tipo_ies` | TEXT |  | 78.6% | Principal, Seccional |
| 48 | `id_caracter_ies` | TEXT |  | 78.6% | 4.0, 3.0, 2.0 |
| 49 | `id_modalidad` | TEXT |  | 83.0% | 1.0, 2.0, 4.0 |
| 50 | `modalidad` | TEXT |  | 83.0% | Presencial, A distancia, Presencial-Virtual |
| 51 | `id_cine_campo_detallado` | TEXT |  | 83.0% | 811.0, 915.0, 732.0 |
| 52 | `desc_cine_campo_detallado` | TEXT |  | 83.0% | Producción agrícola y ganadera, Fisioterapia, Fonoaudiolo... |

### `inscritos`

**Filas:** 369,553 | **Columnas:** 52

| # | Columna | Tipo | PK | Nulos (%) | Valores de ejemplo |
|---|---|---|---|---|---|
| 1 | `id` | INTEGER | ✅ | 0.0% | 1, 2, 3 |
| 2 | `codigo_de_la_institucion` | TEXT |  | 0.0% | 1101, 1102, 1103 |
| 3 | `ies_padre` | TEXT |  | 0.0% | 1101, 1105, 1106 |
| 4 | `institucion_de_educacion_superior_ies` | TEXT |  | 0.0% | UNIVERSIDAD NACIONAL DE COLOMBIA, UNIVERSIDAD PEDAGOGICA ... |
| 5 | `principal_o_seccional` | TEXT |  | 31.2% | Principal, Seccional |
| 6 | `id_sector_ies` | TEXT |  | 28.7% | 1, 2, 1.0 |
| 7 | `sector_ies` | TEXT |  | 28.7% | OFICIAL, PRIVADA, Oficial |
| 8 | `id_caracter` | TEXT |  | 31.2% | 4, 3, 2 |
| 9 | `caracter_ies` | TEXT |  | 0.0% | Universidad, Institución Universitaria/Escuela Tecnológic... |
| 10 | `codigo_del_departamento_ies` | TEXT |  | 0.0% | 11, 5, 17 |
| 11 | `departamento_de_domicilio_de_la_ies` | TEXT |  | 0.0% | BOGOTA D.C, ANTIOQUIA, CALDAS |
| 12 | `codigo_del_municipio_ies` | TEXT |  | 13.6% | 11001, 5001, 17001 |
| 13 | `municipio_de_domicilio_de_la_ies` | TEXT |  | 0.0% | BOGOTA D.C., MEDELLIN, MANIZALES |
| 14 | `codigo_snies_del_programa` | TEXT |  | 0.0% | 0, 1, 10 |
| 15 | `programa_academico` | TEXT |  | 0.0% | SIN PROGRAMA ESPECIFICO, INGENIERIA AGRONOMICA, NUTRICION... |
| 16 | `id_nivel_academico` | TEXT |  | 0.0% | 0, 1, 2 |
| 17 | `nivel_academico` | TEXT |  | 0.0% | NO APLICA, PREGRADO, POSGRADO |
| 18 | `id_nivel_de_formacion` | TEXT |  | 0.0% | 0, 6, 2 |
| 19 | `nivel_de_formacion` | TEXT |  | 0.0% | NO APLICA, Universitaria, Maestría |
| 20 | `id_metodologia` | TEXT |  | 31.2% | 0, 1, 2 |
| 21 | `metodologia` | TEXT |  | 31.2% | NO APLICA, Presencial, Distancia (tradicional) |
| 22 | `id_area` | TEXT |  | 0.0% | 0, 8, 4 |
| 23 | `area_de_conocimiento` | TEXT |  | 0.0% | NO APLICA, Ingeniería, arquitectura, urbanismo y afines, ... |
| 24 | `id_nucleo` | TEXT |  | 0.0% | 0, 824, 446 |
| 25 | `nucleo_basico_del_conocimiento_nbc` | TEXT |  | 0.0% | NO APLICA, Ingeniería agronómica, pecuaria y afines, Nutr... |
| 26 | `codigo_del_departamento_programa` | TEXT |  | 0.0% | 2, 11, 5 |
| 27 | `departamento_de_oferta_del_programa` | TEXT |  | 0.2% | BOGOTA D.C, ANTIOQUIA, CALDAS |
| 28 | `codigo_del_municipio_programa` | TEXT |  | 0.0% | 0, 11001, 5001 |
| 29 | `municipio_de_oferta_del_programa` | TEXT |  | 0.0% | NO APLICA, BOGOTA D.C., MEDELLIN |
| 30 | `id_sexo` | TEXT |  | 0.0% | 1, 2, 1.0 |
| 31 | `sexo` | TEXT |  | 0.0% | Hombre, Mujer, Masculino |
| 32 | `ano` | TEXT |  | 0.0% | 2018, 2019, 2020 |
| 33 | `semestre` | TEXT |  | 0.0% | 1, 2, 1.0 |
| 34 | `inscripciones_2018` | TEXT |  | 87.9% | 19770, 13085, 23959 |
| 35 | `codigo_del_municipio` | TEXT |  | 86.4% | 11001, 5001, 17001 |
| 36 | `id_cine_campo_amplio` | TEXT |  | 12.1% | 8, 9, 7 |
| 37 | `desc_cine_campo_amplio` | TEXT |  | 12.1% | Agropecuario, Silvicultura, Pesca y Veterinaria, Salud y ... |
| 38 | `id_cine_campo_especifico` | TEXT |  | 12.1% | 81, 91, 73 |
| 39 | `desc_cine_campo_especifico` | TEXT |  | 12.1% | Agropecuario, Salud, Arquitectura y construcción |
| 40 | `id_cine_codigo_detallado` | TEXT |  | 43.3% | 811, 915, 732 |
| 41 | `desc_cine_codigo_detallado` | TEXT |  | 43.3% | Producción agrícola y ganadera, Fisioterapia, Fonoaudiolo... |
| 42 | `inscritos` | TEXT |  | 12.1% | 2, 79, 65 |
| 43 | `id_sector` | TEXT |  | 71.3% | 1, 2 |
| 44 | `ies_sector_ies` | TEXT |  | 71.3% | OFICIAL, PRIVADA |
| 45 | `tipo_ies` | TEXT |  | 68.8% | Principal, Seccional |
| 46 | `id_caracter_ies` | TEXT |  | 68.8% | 4.0, 3.0, 2.0 |
| 47 | `ies_acreditada` | TEXT |  | 68.8% | Si, No |
| 48 | `programa_acreditado` | TEXT |  | 68.8% | No, Si |
| 49 | `id_modalidad` | TEXT |  | 68.8% | 0.0, 1.0, 4.0 |
| 50 | `modalidad` | TEXT |  | 68.8% | Sin información, Presencial, Presencial-Virtual |
| 51 | `id_cine_campo_detallado` | TEXT |  | 68.8% | 9900.0, 811.0, 841.0 |
| 52 | `desc_cine_campo_detallado` | TEXT |  | 68.8% | Sin información, Producción agrícola y ganadera, Veterinaria |

### `inscritos_unified`

**Filas:** 369,549 | **Columnas:** 51

| # | Columna | Tipo | PK | Nulos (%) | Valores de ejemplo |
|---|---|---|---|---|---|
| 1 | `codigo_de_la_institucion` | TEXT |  | 0.0% | 1101, 1102, 1103 |
| 2 | `ies_padre` | TEXT |  | 0.0% | 1101, 1105, 1106 |
| 3 | `institucion_de_educacion_superior_ies` | TEXT |  | 0.0% | UNIVERSIDAD NACIONAL DE COLOMBIA, UNIVERSIDAD PEDAGOGICA ... |
| 4 | `principal_o_seccional` | TEXT |  | 31.2% | Principal, Seccional |
| 5 | `id_sector_ies` | TEXT |  | 28.7% | 1, 2, 1.0 |
| 6 | `sector_ies` | TEXT |  | 28.7% | OFICIAL, PRIVADA, Oficial |
| 7 | `id_caracter` | TEXT |  | 31.2% | 4, 3, 2 |
| 8 | `caracter_ies` | TEXT |  | 0.0% | Universidad, Institución Universitaria/Escuela Tecnológic... |
| 9 | `codigo_del_departamento_ies` | TEXT |  | 0.0% | 11, 5, 17 |
| 10 | `departamento_de_domicilio_de_la_ies` | TEXT |  | 0.0% | BOGOTA D.C, ANTIOQUIA, CALDAS |
| 11 | `codigo_del_municipio_ies` | TEXT |  | 13.6% | 11001, 5001, 17001 |
| 12 | `municipio_de_domicilio_de_la_ies` | TEXT |  | 0.0% | BOGOTA D.C., MEDELLIN, MANIZALES |
| 13 | `codigo_snies_del_programa` | TEXT |  | 0.0% | 0, 1, 10 |
| 14 | `programa_academico` | TEXT |  | 0.0% | SIN PROGRAMA ESPECIFICO, INGENIERIA AGRONOMICA, NUTRICION... |
| 15 | `id_nivel_academico` | TEXT |  | 0.0% | 0, 1, 2 |
| 16 | `nivel_academico` | TEXT |  | 0.0% | NO APLICA, PREGRADO, POSGRADO |
| 17 | `id_nivel_de_formacion` | TEXT |  | 0.0% | 0, 6, 2 |
| 18 | `nivel_de_formacion` | TEXT |  | 0.0% | NO APLICA, Universitaria, Maestría |
| 19 | `id_metodologia` | TEXT |  | 31.2% | 0, 1, 2 |
| 20 | `metodologia` | TEXT |  | 31.2% | NO APLICA, Presencial, Distancia (tradicional) |
| 21 | `id_area` | TEXT |  | 0.0% | 0, 8, 4 |
| 22 | `area_de_conocimiento` | TEXT |  | 0.0% | NO APLICA, Ingeniería, arquitectura, urbanismo y afines, ... |
| 23 | `id_nucleo` | TEXT |  | 0.0% | 0, 824, 446 |
| 24 | `nucleo_basico_del_conocimiento_nbc` | TEXT |  | 0.0% | NO APLICA, Ingeniería agronómica, pecuaria y afines, Nutr... |
| 25 | `codigo_del_departamento_programa` | TEXT |  | 0.0% | 2, 11, 5 |
| 26 | `departamento_de_oferta_del_programa` | TEXT |  | 0.2% | BOGOTA D.C, ANTIOQUIA, CALDAS |
| 27 | `codigo_del_municipio_programa` | TEXT |  | 0.0% | 0, 11001, 5001 |
| 28 | `municipio_de_oferta_del_programa` | TEXT |  | 0.0% | NO APLICA, BOGOTA D.C., MEDELLIN |
| 29 | `id_sexo` | TEXT |  | 0.0% | 1, 2, 1.0 |
| 30 | `sexo` | TEXT |  | 0.0% | Hombre, Mujer, Masculino |
| 31 | `ano` | REAL |  | 0.0% | 2018.0, 2019.0, 2020.0 |
| 32 | `semestre` | TEXT |  | 0.0% | 1, 2, 1.0 |
| 33 | `inscripciones_2018` | TEXT |  | 87.9% | 19770, 13085, 23959 |
| 34 | `codigo_del_municipio` | TEXT |  | 86.4% | 11001, 5001, 17001 |
| 35 | `id_cine_campo_amplio` | TEXT |  | 12.1% | 8, 9, 7 |
| 36 | `desc_cine_campo_amplio` | TEXT |  | 12.1% | Agropecuario, Silvicultura, Pesca y Veterinaria, Salud y ... |
| 37 | `id_cine_campo_especifico` | TEXT |  | 12.1% | 81, 91, 73 |
| 38 | `desc_cine_campo_especifico` | TEXT |  | 12.1% | Agropecuario, Salud, Arquitectura y construcción |
| 39 | `id_cine_codigo_detallado` | TEXT |  | 43.3% | 811, 915, 732 |
| 40 | `desc_cine_codigo_detallado` | TEXT |  | 43.3% | Producción agrícola y ganadera, Fisioterapia, Fonoaudiolo... |
| 41 | `inscritos` | TEXT |  | 12.1% | 2, 79, 65 |
| 42 | `id_sector` | TEXT |  | 71.3% | 1, 2 |
| 43 | `ies_sector_ies` | TEXT |  | 71.3% | OFICIAL, PRIVADA |
| 44 | `tipo_ies` | TEXT |  | 68.8% | Principal, Seccional |
| 45 | `id_caracter_ies` | TEXT |  | 68.8% | 4.0, 3.0, 2.0 |
| 46 | `ies_acreditada` | TEXT |  | 68.8% | Si, No |
| 47 | `programa_acreditado` | TEXT |  | 68.8% | No, Si |
| 48 | `id_modalidad` | TEXT |  | 68.8% | 0.0, 1.0, 4.0 |
| 49 | `modalidad` | TEXT |  | 68.8% | Sin información, Presencial, Presencial-Virtual |
| 50 | `id_cine_campo_detallado` | TEXT |  | 68.8% | 9900.0, 811.0, 841.0 |
| 51 | `desc_cine_campo_detallado` | TEXT |  | 68.8% | Sin información, Producción agrícola y ganadera, Veterinaria |

### `matriculados`

**Filas:** 491,126 | **Columnas:** 51

| # | Columna | Tipo | PK | Nulos (%) | Valores de ejemplo |
|---|---|---|---|---|---|
| 1 | `id` | INTEGER | ✅ | 0.0% | 1, 2, 3 |
| 2 | `codigo_de_la_institucion` | TEXT |  | 0.0% | 1101, 1102, 1103 |
| 3 | `ies_padre` | TEXT |  | 0.0% | 1101, 1105, 1106 |
| 4 | `institucion_de_educacion_superior_ies` | TEXT |  | 0.0% | UNIVERSIDAD NACIONAL DE COLOMBIA, UNIVERSIDAD PEDAGOGICA ... |
| 5 | `principal_o_seccional` | TEXT |  | 30.1% | Principal, Seccional, PRINCIPAL |
| 6 | `id_sector_ies` | TEXT |  | 0.0% | 1, 2, 1.0 |
| 7 | `sector_ies` | TEXT |  | 0.0% | OFICIAL, PRIVADA, Oficial |
| 8 | `id_caracter` | TEXT |  | 30.1% | 4, 3, 2 |
| 9 | `caracter_ies` | TEXT |  | 0.0% | Universidad, Institución Universitaria/Escuela Tecnológic... |
| 10 | `codigo_del_departamento_ies` | TEXT |  | 0.0% | 11, 5, 17 |
| 11 | `departamento_de_domicilio_de_la_ies` | TEXT |  | 0.0% | BOGOTA D.C, ANTIOQUIA, CALDAS |
| 12 | `codigo_del_municipio_ies` | TEXT |  | 14.3% | 11001, 5001, 17001 |
| 13 | `municipio_de_domicilio_de_la_ies` | TEXT |  | 0.0% | BOGOTA D.C., MEDELLIN, MANIZALES |
| 14 | `codigo_snies_del_programa` | TEXT |  | 0.0% | 1, 2, 3 |
| 15 | `programa_academico` | TEXT |  | 0.0% | INGENIERIA AGRONOMICA, MEDICINA VETERINARIA, ZOOTECNIA |
| 16 | `id_nivel_academico` | TEXT |  | 0.0% | 1, 2, 1.0 |
| 17 | `nivel_academico` | TEXT |  | 0.0% | PREGRADO, POSGRADO, Pregrado |
| 18 | `id_nivel_de_formacion` | TEXT |  | 0.0% | 6, 10, 1 |
| 19 | `nivel_de_formacion` | TEXT |  | 0.0% | Universitaria, Especialización Médico Quirúrgica, Especia... |
| 20 | `id_metodologia` | TEXT |  | 30.1% | 1, 2, 3 |
| 21 | `metodologia` | TEXT |  | 30.1% | Presencial, Distancia (tradicional), Distancia (virtual) |
| 22 | `id_area` | TEXT |  | 42.8% | 8, 1, 2 |
| 23 | `area_de_conocimiento` | TEXT |  | 0.0% | Ingeniería, arquitectura, urbanismo y afines, Agronomía, ... |
| 24 | `id_nucleo` | TEXT |  | 0.0% | 824, 13, 12 |
| 25 | `nucleo_basico_del_conocimiento_nbc` | TEXT |  | 0.0% | Ingeniería agronómica, pecuaria y afines, Medicina veteri... |
| 26 | `codigo_del_departamento_programa` | TEXT |  | 0.0% | 11, 5, 17 |
| 27 | `departamento_de_oferta_del_programa` | TEXT |  | 0.0% | BOGOTÁ D.C, ANTIOQUIA, CALDAS |
| 28 | `codigo_del_municipio_programa` | TEXT |  | 0.0% | 11001, 5001, 17001 |
| 29 | `municipio_de_oferta_del_programa` | TEXT |  | 0.0% | BOGOTÁ, D.C., MEDELLÍN, MANIZALES |
| 30 | `id_sexo` | TEXT |  | 0.0% | 1, 2, 1.0 |
| 31 | `sexo` | TEXT |  | 0.0% | Hombre, Mujer, HOMBRE |
| 32 | `ano` | TEXT |  | 0.0% | 2018, 2019, 2020 |
| 33 | `semestre` | TEXT |  | 0.0% | 1, 2, 1.0 |
| 34 | `matriculados_2018` | TEXT |  | 87.1% | 459, 438, 238 |
| 35 | `codigo_del_municipio` | TEXT |  | 85.7% | 11001, 5001, 17001 |
| 36 | `id_cine_campo_amplio` | TEXT |  | 12.9% | 8, 2, 9 |
| 37 | `desc_cine_campo_amplio` | TEXT |  | 12.9% | AGROPECUARIO, SILVICULTURA, PESCA Y VETERINARIA, ARTE Y H... |
| 38 | `id_cine_campo_especifico` | TEXT |  | 12.9% | 81, 84, 21 |
| 39 | `desc_cine_campo_especifico` | TEXT |  | 12.9% | AGROPECUARIO, VETERINARIA, ARTES |
| 40 | `id_cine_codigo_detallado` | TEXT |  | 43.0% | 811, 841, 219 |
| 41 | `desc_cine_codigo_detallado` | TEXT |  | 43.0% | PRODUCCIÓN AGRÍCOLA Y GANADERA, VETERINARIA, ARTES NO CLA... |
| 42 | `matriculados` | TEXT |  | 12.9% | 450, 458, 238 |
| 43 | `id_area_de_conocimiento` | TEXT |  | 57.2% | 8, 1, 2 |
| 44 | `ies_acreditada` | TEXT |  | 41.4% | SI, NO, Si |
| 45 | `programa_acreditado` | TEXT |  | 41.4% | SI, NO, Si |
| 46 | `tipo_ies` | TEXT |  | 70.0% | Principal, Seccional |
| 47 | `id_caracter_ies` | TEXT |  | 70.0% | 4.0, 3.0, 2.0 |
| 48 | `id_modalidad` | TEXT |  | 70.0% | 1.0, 4.0, 2.0 |
| 49 | `modalidad` | TEXT |  | 70.0% | Presencial, Presencial-Virtual, A distancia |
| 50 | `id_cine_campo_detallado` | TEXT |  | 70.0% | 811.0, 841.0, 219.0 |
| 51 | `desc_cine_campo_detallado` | TEXT |  | 70.0% | Producción agrícola y ganadera, Veterinaria, Artes no cla... |

### `matriculados_primer_curso`

**Filas:** 327,084 | **Columnas:** 53

| # | Columna | Tipo | PK | Nulos (%) | Valores de ejemplo |
|---|---|---|---|---|---|
| 1 | `id` | INTEGER | ✅ | 0.0% | 1, 2, 3 |
| 2 | `codigo_de_la_institucion` | TEXT |  | 0.0% | 1101, 1102, 1103 |
| 3 | `ies_padre` | TEXT |  | 0.0% | 1101, 1105, 1106 |
| 4 | `institucion_de_educacion_superior_ies` | TEXT |  | 0.0% | UNIVERSIDAD NACIONAL DE COLOMBIA, UNIVERSIDAD PEDAGOGICA ... |
| 5 | `principal_o_seccional` | TEXT |  | 31.2% | Principal, Seccional, PRINCIPAL |
| 6 | `id_sector_ies` | TEXT |  | 0.0% | 1, 2, 1.0 |
| 7 | `sector_ies` | TEXT |  | 0.0% | OFICIAL, PRIVADA, Oficial |
| 8 | `id_caracter` | TEXT |  | 31.2% | 4, 3, 2 |
| 9 | `caracter_ies` | TEXT |  | 0.0% | Universidad, Institución Universitaria/Escuela Tecnológic... |
| 10 | `codigo_del_departamento_ies` | TEXT |  | 0.0% | 11, 5, 17 |
| 11 | `departamento_de_domicilio_de_la_ies` | TEXT |  | 0.0% | BOGOTA D.C, ANTIOQUIA, CALDAS |
| 12 | `codigo_del_municipio` | TEXT |  | 74.0% | 11001, 5001, 17001 |
| 13 | `municipio_de_domicilio_de_la_ies` | TEXT |  | 0.0% | BOGOTA D.C., MEDELLIN, MANIZALES |
| 14 | `codigo_snies_del_programa` | TEXT |  | 0.0% | 1, 2, 3 |
| 15 | `programa_academico` | TEXT |  | 0.0% | INGENIERIA AGRONOMICA, MEDICINA VETERINARIA, ZOOTECNIA |
| 16 | `id_nivel_academico` | TEXT |  | 0.0% | 1, 2, 1.0 |
| 17 | `nivel_academico` | TEXT |  | 0.0% | PREGRADO, POSGRADO, Pregrado |
| 18 | `id_nivel_de_formacion` | TEXT |  | 0.0% | 6, 10, 1 |
| 19 | `nivel_de_formacion` | TEXT |  | 0.0% | Universitaria, Especialización Médico Quirúrgica, Especia... |
| 20 | `id_metodologia` | TEXT |  | 31.2% | 1, 2, 3 |
| 21 | `metodologia` | TEXT |  | 31.2% | Presencial, Distancia (tradicional), Distancia (virtual) |
| 22 | `id_area` | TEXT |  | 42.8% | 8, 1, 2 |
| 23 | `area_de_conocimiento` | TEXT |  | 0.0% | Ingeniería, arquitectura, urbanismo y afines, Agronomía, ... |
| 24 | `id_nucleo` | TEXT |  | 0.0% | 824, 13, 12 |
| 25 | `nucleo_basico_del_conocimiento_nbc` | TEXT |  | 0.0% | Ingeniería agronómica, pecuaria y afines, Medicina veteri... |
| 26 | `codigo_del_departamento_programa` | TEXT |  | 0.0% | 11, 5, 17 |
| 27 | `departamento_de_oferta_del_programa` | TEXT |  | 0.0% | BOGOTA D.C, ANTIOQUIA, CALDAS |
| 28 | `codigo_del_municipio_programa` | TEXT |  | 0.0% | 11001, 5001, 17001 |
| 29 | `municipio_de_oferta_del_programa` | TEXT |  | 0.0% | BOGOTA D.C., MEDELLIN, MANIZALES |
| 30 | `id_sexo` | TEXT |  | 0.0% | 1, 2, 0 |
| 31 | `sexo` | TEXT |  | 13.5% | Hombre, Mujer, NO INFORMA |
| 32 | `ano` | TEXT |  | 0.0% | 2018, 2019, 2020 |
| 33 | `semestre` | TEXT |  | 0.0% | 1, 2, 1.0 |
| 34 | `primer_curso_2018` | TEXT |  | 87.5% | 72, 54, 30 |
| 35 | `id_cine_campo_amplio` | TEXT |  | 12.5% | 8, 2, 9 |
| 36 | `desc_cine_campo_amplio` | TEXT |  | 12.5% | AGROPECUARIO, SILVICULTURA, PESCA Y VETERINARIA, ARTE Y H... |
| 37 | `id_cine_campo_especifico` | TEXT |  | 12.5% | 81, 84, 21 |
| 38 | `desc_cine_campo_especifico` | TEXT |  | 12.5% | AGROPECUARIO, VETERINARIA, ARTES |
| 39 | `id_cine_codigo_detallado` | TEXT |  | 43.7% | 811, 841, 219 |
| 40 | `desc_cine_codigo_detallado` | TEXT |  | 43.7% | PRODUCCIÓN AGRÍCOLA Y GANADERA, VETERINARIA, ARTES NO CLA... |
| 41 | `primer_curso_2019` | TEXT |  | 86.5% | 64, 58, 22 |
| 42 | `codigo_del_municipio_ies` | TEXT |  | 26.0% | 11001, 5001, 17001 |
| 43 | `id_area_de_conocimiento` | TEXT |  | 57.2% | 8, 1, 2 |
| 44 | `primer_curso` | TEXT |  | 57.2% | 62, 49, 23 |
| 45 | `ies_acreditada` | TEXT |  | 39.9% | SI, NO, Si |
| 46 | `programa_acreditado` | TEXT |  | 39.9% | SI, NO, Si |
| 47 | `tipo_ies` | TEXT |  | 68.8% | Principal, Seccional |
| 48 | `id_caracter_ies` | TEXT |  | 68.8% | 4.0, 3.0, 2.0 |
| 49 | `id_modalidad` | TEXT |  | 68.8% | 1.0, 4.0, 2.0 |
| 50 | `modalidad` | TEXT |  | 68.8% | Presencial, Presencial-Virtual, A distancia |
| 51 | `id_cine_campo_detallado` | TEXT |  | 68.8% | 811.0, 841.0, 219.0 |
| 52 | `desc_cine_campo_detallado` | TEXT |  | 68.8% | Producción agrícola y ganadera, Veterinaria, Artes no cla... |
| 53 | `matriculados_primer_curso` | TEXT |  | 68.8% | 63.0, 49.0, 37.0 |

### `matriculados_unified`

**Filas:** 491,122 | **Columnas:** 50

| # | Columna | Tipo | PK | Nulos (%) | Valores de ejemplo |
|---|---|---|---|---|---|
| 1 | `codigo_de_la_institucion` | TEXT |  | 0.0% | 1101, 1102, 1103 |
| 2 | `ies_padre` | TEXT |  | 0.0% | 1101, 1105, 1106 |
| 3 | `institucion_de_educacion_superior_ies` | TEXT |  | 0.0% | UNIVERSIDAD NACIONAL DE COLOMBIA, UNIVERSIDAD PEDAGOGICA ... |
| 4 | `principal_o_seccional` | TEXT |  | 30.1% | Principal, Seccional, PRINCIPAL |
| 5 | `id_sector_ies` | TEXT |  | 0.0% | 1, 2, 1.0 |
| 6 | `sector_ies` | TEXT |  | 0.0% | OFICIAL, PRIVADA, Oficial |
| 7 | `id_caracter` | TEXT |  | 30.1% | 4, 3, 2 |
| 8 | `caracter_ies` | TEXT |  | 0.0% | Universidad, Institución Universitaria/Escuela Tecnológic... |
| 9 | `codigo_del_departamento_ies` | TEXT |  | 0.0% | 11, 5, 17 |
| 10 | `departamento_de_domicilio_de_la_ies` | TEXT |  | 0.0% | BOGOTA D.C, ANTIOQUIA, CALDAS |
| 11 | `codigo_del_municipio_ies` | TEXT |  | 14.3% | 11001, 5001, 17001 |
| 12 | `municipio_de_domicilio_de_la_ies` | TEXT |  | 0.0% | BOGOTA D.C., MEDELLIN, MANIZALES |
| 13 | `codigo_snies_del_programa` | TEXT |  | 0.0% | 1, 2, 3 |
| 14 | `programa_academico` | TEXT |  | 0.0% | INGENIERIA AGRONOMICA, MEDICINA VETERINARIA, ZOOTECNIA |
| 15 | `id_nivel_academico` | TEXT |  | 0.0% | 1, 2, 1.0 |
| 16 | `nivel_academico` | TEXT |  | 0.0% | PREGRADO, POSGRADO, Pregrado |
| 17 | `id_nivel_de_formacion` | TEXT |  | 0.0% | 6, 10, 1 |
| 18 | `nivel_de_formacion` | TEXT |  | 0.0% | Universitaria, Especialización Médico Quirúrgica, Especia... |
| 19 | `id_metodologia` | TEXT |  | 30.1% | 1, 2, 3 |
| 20 | `metodologia` | TEXT |  | 30.1% | Presencial, Distancia (tradicional), Distancia (virtual) |
| 21 | `id_area` | TEXT |  | 42.8% | 8, 1, 2 |
| 22 | `area_de_conocimiento` | TEXT |  | 0.0% | Ingeniería, arquitectura, urbanismo y afines, Agronomía, ... |
| 23 | `id_nucleo` | TEXT |  | 0.0% | 824, 13, 12 |
| 24 | `nucleo_basico_del_conocimiento_nbc` | TEXT |  | 0.0% | Ingeniería agronómica, pecuaria y afines, Medicina veteri... |
| 25 | `codigo_del_departamento_programa` | TEXT |  | 0.0% | 11, 5, 17 |
| 26 | `departamento_de_oferta_del_programa` | TEXT |  | 0.0% | BOGOTÁ D.C, ANTIOQUIA, CALDAS |
| 27 | `codigo_del_municipio_programa` | TEXT |  | 0.0% | 11001, 5001, 17001 |
| 28 | `municipio_de_oferta_del_programa` | TEXT |  | 0.0% | BOGOTÁ, D.C., MEDELLÍN, MANIZALES |
| 29 | `id_sexo` | TEXT |  | 0.0% | 1, 2, 1.0 |
| 30 | `sexo` | TEXT |  | 0.0% | Hombre, Mujer, HOMBRE |
| 31 | `ano` | REAL |  | 0.0% | 2018.0, 2019.0, 2020.0 |
| 32 | `semestre` | TEXT |  | 0.0% | 1, 2, 1.0 |
| 33 | `matriculados_2018` | TEXT |  | 87.1% | 459, 438, 238 |
| 34 | `codigo_del_municipio` | TEXT |  | 85.7% | 11001, 5001, 17001 |
| 35 | `id_cine_campo_amplio` | TEXT |  | 12.9% | 8, 2, 9 |
| 36 | `desc_cine_campo_amplio` | TEXT |  | 12.9% | AGROPECUARIO, SILVICULTURA, PESCA Y VETERINARIA, ARTE Y H... |
| 37 | `id_cine_campo_especifico` | TEXT |  | 12.9% | 81, 84, 21 |
| 38 | `desc_cine_campo_especifico` | TEXT |  | 12.9% | AGROPECUARIO, VETERINARIA, ARTES |
| 39 | `id_cine_codigo_detallado` | TEXT |  | 43.0% | 811, 841, 219 |
| 40 | `desc_cine_codigo_detallado` | TEXT |  | 43.0% | PRODUCCIÓN AGRÍCOLA Y GANADERA, VETERINARIA, ARTES NO CLA... |
| 41 | `matriculados` | TEXT |  | 12.9% | 450, 458, 238 |
| 42 | `id_area_de_conocimiento` | TEXT |  | 57.2% | 8, 1, 2 |
| 43 | `ies_acreditada` | TEXT |  | 41.4% | SI, NO, Si |
| 44 | `programa_acreditado` | TEXT |  | 41.4% | SI, NO, Si |
| 45 | `tipo_ies` | TEXT |  | 70.0% | Principal, Seccional |
| 46 | `id_caracter_ies` | TEXT |  | 70.0% | 4.0, 3.0, 2.0 |
| 47 | `id_modalidad` | TEXT |  | 70.0% | 1.0, 4.0, 2.0 |
| 48 | `modalidad` | TEXT |  | 70.0% | Presencial, Presencial-Virtual, A distancia |
| 49 | `id_cine_campo_detallado` | TEXT |  | 70.0% | 811.0, 841.0, 219.0 |
| 50 | `desc_cine_campo_detallado` | TEXT |  | 70.0% | Producción agrícola y ganadera, Veterinaria, Artes no cla... |

---

## Comparación entre Grupos de Tablas

### Grupo: `poblacion_estudiantil`

**Tablas comparadas (5):** `admitidos`, `inscritos`, `matriculados`, `matriculados_primer_curso`, `graduados`

#### Columnas comunes a TODAS (47)

- `ano`
- `area_de_conocimiento`
- `caracter_ies`
- `codigo_de_la_institucion`
- `codigo_del_departamento_ies`
- `codigo_del_departamento_programa`
- `codigo_del_municipio`
- `codigo_del_municipio_ies`
- `codigo_del_municipio_programa`
- `codigo_snies_del_programa`
- `departamento_de_domicilio_de_la_ies`
- `departamento_de_oferta_del_programa`
- `desc_cine_campo_amplio`
- `desc_cine_campo_detallado`
- `desc_cine_campo_especifico`
- `desc_cine_codigo_detallado`
- `id_area`
- `id_caracter`
- `id_caracter_ies`
- `id_cine_campo_amplio`
- `id_cine_campo_detallado`
- `id_cine_campo_especifico`
- `id_cine_codigo_detallado`
- `id_metodologia`
- `id_modalidad`
- `id_nivel_academico`
- `id_nivel_de_formacion`
- `id_nucleo`
- `id_sector_ies`
- `id_sexo`
- `ies_acreditada`
- `ies_padre`
- `institucion_de_educacion_superior_ies`
- `metodologia`
- `modalidad`
- `municipio_de_domicilio_de_la_ies`
- `municipio_de_oferta_del_programa`
- `nivel_academico`
- `nivel_de_formacion`
- `nucleo_basico_del_conocimiento_nbc`
- `principal_o_seccional`
- `programa_academico`
- `programa_acreditado`
- `sector_ies`
- `semestre`
- `sexo`
- `tipo_ies`

#### Columnas únicas por tabla

- **`admitidos`** (10): `admisiones_2018`, `admitidos`, `docentes`, `id_maximo_nivel_de_formacion_del_docente`, `id_tiempo_de_dedicacion`, `id_tipo_de_contrato`, `maximo_nivel_de_formacion_del_docente`, `sexo_del_docente`, `tiempo_de_dedicacion_del_docente`, `tipo_de_contrato`
- **`inscritos`** (4): `id_sector`, `ies_sector_ies`, `inscripciones_2018`, `inscritos`
- **`matriculados`** (2): `matriculados`, `matriculados_2018`
- **`matriculados_primer_curso`** (4): `matriculados_primer_curso`, `primer_curso`, `primer_curso_2018`, `primer_curso_2019`
- **`graduados`** (4): `cdigo_del_municipio_programa`, `cine_campo_amplio`, `graduados`, `id_cine_campo_amplio_desc`

#### Columnas parciales (en algunas pero no todas) (25)

| Columna | Presente en |
|---|---|
| `admisiones_2018` | `admitidos` |
| `admitidos` | `admitidos` |
| `cdigo_del_municipio_programa` | `graduados` |
| `cine_campo_amplio` | `graduados` |
| `docentes` | `admitidos` |
| `graduados` | `graduados` |
| `id_area_de_conocimiento` | `matriculados`, `matriculados_primer_curso`, `graduados` |
| `id_cine_campo_amplio_desc` | `graduados` |
| `id_maximo_nivel_de_formacion_del_docente` | `admitidos` |
| `id_sector` | `inscritos` |
| `id_tiempo_de_dedicacion` | `admitidos` |
| `id_tipo_de_contrato` | `admitidos` |
| `ies_sector_ies` | `inscritos` |
| `inscripciones_2018` | `inscritos` |
| `inscritos` | `inscritos` |
| `matriculados` | `matriculados` |
| `matriculados_2018` | `matriculados` |
| `matriculados_primer_curso` | `matriculados_primer_curso` |
| `maximo_nivel_de_formacion_del_docente` | `admitidos` |
| `primer_curso` | `matriculados_primer_curso` |
| `primer_curso_2018` | `matriculados_primer_curso` |
| `primer_curso_2019` | `matriculados_primer_curso` |
| `sexo_del_docente` | `admitidos` |
| `tiempo_de_dedicacion_del_docente` | `admitidos` |
| `tipo_de_contrato` | `admitidos` |

### Grupo: `recurso_humano`

**Tablas comparadas (2):** `docentes`, `administrativos`

#### Columnas comunes a TODAS (26)

- `ano`
- `caracter_ies`
- `codigo_de_la_institucion`
- `codigo_del_departamento_ies`
- `codigo_del_municipio_ies`
- `departamento_de_domicilio_de_la_ies`
- `id_caracter`
- `id_caracter_ies`
- `id_maximo_nivel_de_formacion_del_docente`
- `id_sector_ies`
- `id_sexo`
- `id_tiempo_de_dedicacion`
- `id_tipo_de_contrato`
- `ies_acreditada`
- `ies_padre`
- `institucion_de_educacion_superior_ies`
- `maximo_nivel_de_formacion_del_docente`
- `municipio_de_domicilio_de_la_ies`
- `no_de_docentes`
- `principal_o_seccional`
- `sector_ies`
- `semestre`
- `sexo_del_docente`
- `tiempo_de_dedicacion_del_docente`
- `tipo_de_contrato_del_docente`
- `tipo_ies`

#### Columnas únicas por tabla

- **`docentes`** (29): `admitidos`, `area_de_conocimiento`, `codigo_del_departamento_programa`, `codigo_del_municipio_programa`, `codigo_snies_del_programa`, `departamento_de_oferta_del_programa`, `desc_cine_campo_amplio`, `desc_cine_campo_detallado`, `desc_cine_campo_especifico`, `docentes`, `id_area`, `id_cine_campo_amplio`, `id_cine_campo_detallado`, `id_cine_campo_especifico`, `id_modalidad`, `id_nivel_academico`, `id_nivel_de_formacion`, `id_nucleo`, `modalidad`, `municipio_de_oferta_del_programa`, `nivel_academico`, `nivel_cine`, `nivel_de_formacion`, `nucleo_basico_del_conocimiento_nbc`, `programa_academico`, `programa_acreditado`, `sexo`, `tiempo_de_dedicacion_del_docente_1`, `tipo_de_contrato`
- **`administrativos`** (6): `auxiliar`, `coodigo_de_la_institucion`, `directivo`, `profesional`, `tecnico`, `total`

#### Columnas parciales (en algunas pero no todas) (35)

| Columna | Presente en |
|---|---|
| `admitidos` | `docentes` |
| `area_de_conocimiento` | `docentes` |
| `auxiliar` | `administrativos` |
| `codigo_del_departamento_programa` | `docentes` |
| `codigo_del_municipio_programa` | `docentes` |
| `codigo_snies_del_programa` | `docentes` |
| `coodigo_de_la_institucion` | `administrativos` |
| `departamento_de_oferta_del_programa` | `docentes` |
| `desc_cine_campo_amplio` | `docentes` |
| `desc_cine_campo_detallado` | `docentes` |
| `desc_cine_campo_especifico` | `docentes` |
| `directivo` | `administrativos` |
| `docentes` | `docentes` |
| `id_area` | `docentes` |
| `id_cine_campo_amplio` | `docentes` |
| `id_cine_campo_detallado` | `docentes` |
| `id_cine_campo_especifico` | `docentes` |
| `id_modalidad` | `docentes` |
| `id_nivel_academico` | `docentes` |
| `id_nivel_de_formacion` | `docentes` |
| `id_nucleo` | `docentes` |
| `modalidad` | `docentes` |
| `municipio_de_oferta_del_programa` | `docentes` |
| `nivel_academico` | `docentes` |
| `nivel_cine` | `docentes` |
| `nivel_de_formacion` | `docentes` |
| `nucleo_basico_del_conocimiento_nbc` | `docentes` |
| `profesional` | `administrativos` |
| `programa_academico` | `docentes` |
| `programa_acreditado` | `docentes` |
| `sexo` | `docentes` |
| `tecnico` | `administrativos` |
| `tiempo_de_dedicacion_del_docente_1` | `docentes` |
| `tipo_de_contrato` | `docentes` |
| `total` | `administrativos` |

---

## Frecuencia Global de Columnas

Cuántas tablas contienen cada columna (excluyendo `id` autoincrement):

| Columna | Aparece en N tablas |
|---|---|
| `codigo_de_la_institucion` | 13 |
| `ies_padre` | 13 |
| `institucion_de_educacion_superior_ies` | 13 |
| `principal_o_seccional` | 13 |
| `id_sector_ies` | 13 |
| `sector_ies` | 13 |
| `id_caracter_ies` | 13 |
| `caracter_ies` | 13 |
| `codigo_del_departamento_ies` | 13 |
| `departamento_de_domicilio_de_la_ies` | 13 |
| `codigo_del_municipio_ies` | 13 |
| `municipio_de_domicilio_de_la_ies` | 13 |
| `ano` | 13 |
| `semestre` | 13 |
| `id_caracter` | 13 |
| `id_sexo` | 13 |
| `tipo_ies` | 13 |
| `ies_acreditada` | 13 |
| `codigo_snies_del_programa` | 11 |
| `programa_academico` | 11 |
| `id_nivel_academico` | 11 |
| `nivel_academico` | 11 |
| `id_nivel_de_formacion` | 11 |
| `nivel_de_formacion` | 11 |
| `id_area` | 11 |
| `area_de_conocimiento` | 11 |
| `id_nucleo` | 11 |
| `nucleo_basico_del_conocimiento_nbc` | 11 |
| `codigo_del_departamento_programa` | 11 |
| `departamento_de_oferta_del_programa` | 11 |
| `codigo_del_municipio_programa` | 11 |
| `municipio_de_oferta_del_programa` | 11 |
| `sexo` | 11 |
| `id_cine_campo_amplio` | 11 |
| `desc_cine_campo_amplio` | 11 |
| `id_cine_campo_especifico` | 11 |
| `desc_cine_campo_especifico` | 11 |
| `programa_acreditado` | 11 |
| `id_modalidad` | 11 |
| `modalidad` | 11 |
| `id_cine_campo_detallado` | 11 |
| `desc_cine_campo_detallado` | 11 |
| `id_metodologia` | 9 |
| `metodologia` | 9 |
| `codigo_del_municipio` | 9 |
| `id_cine_codigo_detallado` | 9 |
| `desc_cine_codigo_detallado` | 9 |
| `sexo_del_docente` | 6 |
| `id_maximo_nivel_de_formacion_del_docente` | 6 |
| `maximo_nivel_de_formacion_del_docente` | 6 |
| `id_tiempo_de_dedicacion` | 6 |
| `tiempo_de_dedicacion_del_docente` | 6 |
| `id_tipo_de_contrato` | 6 |
| `id_area_de_conocimiento` | 5 |
| `tipo_de_contrato_del_docente` | 4 |
| `no_de_docentes` | 4 |
| `admitidos` | 4 |
| `tipo_de_contrato` | 4 |
| `docentes` | 4 |
| `auxiliar` | 2 |
| `tecnico` | 2 |
| `profesional` | 2 |
| `directivo` | 2 |
| `total` | 2 |
| `coodigo_de_la_institucion` | 2 |
| `admisiones_2018` | 2 |
| `tiempo_de_dedicacion_del_docente_1` | 2 |
| `nivel_cine` | 2 |
| `graduados` | 2 |
| `id_cine_campo_amplio_desc` | 2 |
| `cine_campo_amplio` | 2 |
| `cdigo_del_municipio_programa` | 2 |
| `inscripciones_2018` | 2 |
| `inscritos` | 2 |
| `id_sector` | 2 |
| `ies_sector_ies` | 2 |
| `matriculados_2018` | 2 |
| `matriculados` | 2 |
| `primer_curso_2018` | 1 |
| `primer_curso_2019` | 1 |
| `primer_curso` | 1 |
| `matriculados_primer_curso` | 1 |
