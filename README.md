# Seminario Ingenieria de Datos

Pipeline ETL para analisis de datos de educacion superior en Colombia (2018-2024). Descarga datos del SNIES, ICFES y PND desde Google Drive, los carga en PostgreSQL, construye un esquema estrella (dimensiones + hechos) y exporta los schemas de vuelta a Drive.

## Requisitos previos

- Python 3.12+
- Docker y Docker Compose
- `uv` (gestor de paquetes Python): https://docs.astral.sh/uv/
- Credenciales OAuth2 de Google Cloud (CLIENT_ID y CLIENT_SECRET)
- Acceso a la carpeta compartida de Google Drive del proyecto

## Estructura del proyecto

```
seminario/
  config/
    globals.py          Constantes, rutas, credenciales, parametros del pipeline
    sources.py          Registro de archivos en Google Drive (IDs)
  etl/
    ingest.py           Descarga archivos desde Google Drive
    transform.py        Limpieza y transformacion en PostgreSQL (server-side SQL)
    quality.py          Verificacion de calidad via SQL (sin cargar datos en memoria)
    dictionary.py       Generacion de diccionarios de datos
    upload.py           Exportacion pg_dump + subida a Google Drive
    pipeline.py         Orquestador local (11 pasos secuenciales)
  scripts/
    create_db.py        Carga Excel/CSV a PostgreSQL (schema raw) con COPY
    create_schemas.py   Creacion de schemas (raw, unified, facts)
    normalize_data.py   Normalizacion de texto via pg_normalize_text()
    create_indexes.py   Creacion declarativa de indices + ANALYZE
    unify_by_year.py    Unificacion de tablas por anio (raw -> unified)
    create_dimensions.py  Tablas de dimensiones del esquema estrella
    create_facts.py     Tablas de hechos del esquema estrella
  utils/
    db.py               Conexiones psycopg2 + SQLAlchemy (pool, retry, keepalive)
    google_auth.py      Autenticacion OAuth2 con Google (4 niveles de refresh)
    logger.py           Logger con RotatingFileHandler
    text.py             Funciones de normalizacion de texto + pg_normalize_text SQL
    schema_helpers.py   Utilidades auxiliares para schemas
  airflow/
    dags/               DAG de Airflow para orquestacion en produccion
    docker-compose.yaml Servicios de Airflow (scheduler + webserver + postgres)
  dashboard/            Streamlit dashboard
  data/
    raw/                Archivos descargados (xlsx, csv)
    processed/          Parquets, lineage, reportes de calidad
    dictionaries/       Diccionarios de datos (JSON + Markdown)
    exports/            Dumps de pg_dump (.sql)
  docker-compose.yml    Servicios principales (postgres, pipeline, dashboard)
  Dockerfile            Multi-stage build con uv
```

## Configuracion

### 1. Variables de entorno

Crear un archivo `.env` en la raiz del proyecto:

```env
CLIENT_ID=tu-client-id.apps.googleusercontent.com
CLIENT_SECRET=tu-client-secret
FOLDER_ID=id-de-carpeta-raiz-drive
DRIVE_UPLOAD_FOLDER_ID=id-de-carpeta-destino-drive

POSTGRES_USER=yeigen
POSTGRES_PASSWORD=tu-password
POSTGRES_DB=seminario
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
```

### 2. Autenticacion con Google Drive

La primera vez se necesita autenticacion interactiva para generar `token.json`:

```bash
# Opcion 1: Autenticacion local (abre navegador)
uv run python reauth.py

# Opcion 2: Verificar estado del token
uv run python reauth.py --check

# Opcion 3: Autenticacion manual (entornos sin navegador)
uv run python reauth_manual.py
```

El archivo `token.json` se genera en la raiz del proyecto. Se monta como volumen en Docker.

## Ejecucion

### Opcion A: Docker Compose (recomendado)

```bash
# Levantar PostgreSQL y ejecutar el pipeline completo
docker compose up postgres -d
docker compose run pipeline python -m etl.pipeline

# Solo pipeline sin ingesta (datos ya descargados)
docker compose run pipeline python -m etl.pipeline --skip-ingest

# Solo pipeline sin subida a Drive
docker compose run pipeline python -m etl.pipeline --skip-upload

# Levantar dashboard Streamlit
docker compose up dashboard
```

### Opcion B: Ejecucion local

```bash
# Instalar dependencias
uv sync

# Ejecutar pipeline completo
uv run python -m etl.pipeline

# Con flags
uv run python -m etl.pipeline --skip-ingest
uv run python -m etl.pipeline --skip-upload
```

**Nota:** Para ejecucion local, PostgreSQL debe estar accesible. Ajustar `POSTGRES_HOST` y `POSTGRES_PORT` en `.env` segun corresponda (ej. `localhost` y `5433`).

### Opcion C: Apache Airflow (produccion)

```bash
# Levantar el stack principal primero
docker compose up postgres -d

# Levantar Airflow
docker compose -f airflow/docker-compose.yaml up -d

# UI disponible en http://localhost:8080
# Credenciales: admin / admin
```

El DAG `seminario_etl_pipeline` se ejecuta manualmente desde la UI de Airflow. Tiene 4 TaskGroups:

1. **ingestion**: Validacion de auth + descarga desde Drive
2. **staging**: Carga a PostgreSQL + transformacion + normalizacion + indices
3. **star_schema**: Unificacion + dimensiones + hechos
4. **delivery**: Calidad + diccionarios (en paralelo) -> subida a Drive

## Pipeline: orden de ejecucion

| Paso | Descripcion | Detalle |
|------|-------------|---------|
| 1 | Ingesta de datos | Descarga xlsx/csv desde Google Drive |
| 2 | Creacion de base de datos | Carga archivos al schema `raw` con COPY |
| 3 | Transformacion y limpieza | Normalizacion server-side via SQL |
| 4 | Normalizacion de datos | pg_normalize_text() sobre columnas de texto |
| 5 | Creacion de indices (raw) | Indices en schema `raw` + ANALYZE |
| 6 | Unificacion por anio | Merge de tablas por anio a schema `unified` |
| 7 | Creacion de dimensiones | 7 tablas de dimension en schema `facts` |
| 8 | Creacion de tablas de hechos | 3 tablas de hechos en schema `facts` |
| 9 | Verificacion de calidad | Checks SQL: vacias, duplicados, nulos, columnas |
| 10 | Diccionarios de datos | Profiling de columnas, genera JSON + Markdown |
| 11 | Export + subida a Drive | pg_dump de 3 schemas -> archivos .sql a Drive |

La calidad se verifica antes de la subida a Drive para asegurar que los datos exportados fueron validados.

## Base de datos

### Schemas

- **raw**: Tablas crudas importadas de Excel/CSV (una por categoria SNIES + PND + ICFES)
- **unified**: Tablas unificadas por anio (ej. `matriculados_unified` con datos 2018-2024)
- **facts**: Esquema estrella con dimensiones y hechos

### Dimensiones

| Tabla | Descripcion |
|-------|-------------|
| dim_institucion | Instituciones de educacion superior |
| dim_geografia | Ubicacion geografica (departamento, municipio) |
| dim_programa | Programas academicos |
| dim_tiempo | Periodos (anio, semestre) |
| dim_sexo | Sexo |
| dim_nivel_formacion_docente | Nivel de formacion de docentes |
| dim_dedicacion_docente | Tipo de dedicacion de docentes |

### Hechos

| Tabla | Descripcion |
|-------|-------------|
| fact_estudiantes | Inscritos, admitidos, matriculados, primer curso, graduados (discriminador: tipo_evento) |
| fact_docentes | Docentes por institucion, programa, dedicacion, nivel |
| fact_administrativos | Personal administrativo por institucion |

## Fuentes de datos

- **SNIES** (7 categorias x 7 anios): administrativos, admitidos, docentes, graduados, inscritos, matriculados, matriculados_primer_curso
- **ICFES**: Saber 3-5-9 (scores de pruebas)
- **PND**: Seguimiento Plan Nacional de Desarrollo

Los archivos fuente se descargan de Google Drive en formato `.xlsx` (SNIES) y `.csv` (ICFES, PND).

Lo que se sube a Drive son **dumps SQL** generados con `pg_dump` (archivos `.sql`), no CSV ni XLSX. Se exportan los 3 schemas:
- `seminario_raw.sql`
- `seminario_unified.sql`
- `seminario_facts.sql`

## Notas tecnicas

- **Idempotencia**: La carga de archivos usa hashes MD5 para evitar re-importaciones. Las tablas usan `CREATE TABLE IF NOT EXISTS` + `TRUNCATE` en lugar de `DROP TABLE`.
- **Rendimiento**: Las inserciones masivas usan `COPY FROM STDIN` (create_db) y `execute_values` (dimensiones, hechos, unificacion). Las transformaciones corren server-side en PostgreSQL via SQL.
- **Calidad**: Los checks de calidad corren via SQL sin cargar datos en memoria (COUNT, COUNT DISTINCT, information_schema).
- **Seguridad**: Conexiones usan `psycopg2.sql.SQL` + `Identifier` para prevenir SQL injection. El contenedor corre con usuario no-root.
- **Resiliencia**: Descargas con retry + exponential backoff, archivos temporales atomicos (.tmp), conexiones con TCP keepalive.
