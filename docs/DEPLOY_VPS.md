# Guía de Despliegue en VPS

> **Servidor:** `192.241.132.222`
> **Última actualización:** Febrero 2026

---

## Tabla de contenido

1. [Arquitectura](#arquitectura)
2. [Prerrequisitos](#prerrequisitos)
3. [Preparación del servidor](#preparación-del-servidor)
4. [Stack principal (docker-compose.yml)](#stack-principal)
5. [Stack de Airflow (airflow/docker-compose.yaml)](#stack-de-airflow)
6. [URLs de acceso](#urls-de-acceso)
7. [Credenciales](#credenciales)
8. [Logs y monitoreo](#logs-y-monitoreo)
9. [Operaciones comunes](#operaciones-comunes)
10. [Solución de problemas](#solución-de-problemas)

---

## Arquitectura

```
┌─────────────────────────────────────────────────────────────────┐
│  VPS 192.241.132.222                                            │
│                                                                 │
│  ┌─── Stack Principal (docker-compose.yml) ──────────────────┐  │
│  │                                                           │  │
│  │  postgres (:5433)      pipeline          dashboard (:8501)│  │
│  │  seminario-postgres    seminario-pipeline seminario-dash   │  │
│  │        │                     │                  │         │  │
│  │        └─────────────────────┴──────────────────┘         │  │
│  │                    red: seminario_default                  │  │
│  └───────────────────────────┬───────────────────────────────┘  │
│                              │ (red compartida)                  │
│  ┌─── Stack Airflow (airflow/docker-compose.yaml) ───────────┐  │
│  │                                                           │  │
│  │  airflow-postgres (:5434)   webserver (:8080)  scheduler  │  │
│  │          │                       │          │             │  │
│  │          └───────────────────────┴──────────┘             │  │
│  │                     red: airflow-net                       │  │
│  │                   + seminario_default (externa)            │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

**Dos instancias de PostgreSQL separadas:**

| Instancia | Puerto host | Base de datos | Propósito |
|-----------|------------|---------------|-----------|
| `seminario-postgres` | **5433** | `seminario` | Datos del pipeline ETL (schemas: raw, unified, facts) |
| `airflow-postgres` | **5434** | `airflow` | Metadatos internos de Airflow |

---

## Prerrequisitos

En el VPS se necesita:

- **Docker Engine** ≥ 24.0
- **Docker Compose** V2 (plugin `docker compose`)
- **Git**
- Al menos **4 GB de RAM** (Airflow recomienda ≥ 4 GB)
- **`token.json`** válido (OAuth2 de Google Drive)
- **`.env`** con credenciales del proyecto

---

## Preparación del servidor

### 1. Conectarse al VPS

```bash
ssh root@192.241.132.222
```

### 2. Clonar el repositorio

```bash
cd /opt
git clone <URL_DEL_REPOSITORIO> seminario
cd seminario
```

### 3. Crear el archivo `.env`

```bash
cat > .env << 'EOF'
CLIENT_ID=your-google-client-id.apps.googleusercontent.com
CLIENT_SECRET=your-google-client-secret
FOLDER_ID=your-google-drive-folder-id
DRIVE_UPLOAD_FOLDER_ID=your-google-drive-upload-folder-id

POSTGRES_USER=your_postgres_user
POSTGRES_PASSWORD=your_postgres_password
POSTGRES_DB=seminario
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
EOF

chmod 600 .env
```

### 4. Subir `token.json` desde la máquina local

El `token.json` se genera localmente con `uv run python reauth.py` (requiere navegador). Una vez generado, subirlo al VPS:

```bash
# Desde tu máquina local:
scp token.json root@192.241.132.222:/opt/seminario/token.json
```

Si el token expiró y no hay acceso a un navegador, usar el método manual:

```bash
# En la máquina local (con navegador):
uv run python reauth_manual.py
# Copiar el código de autorización y seguir las instrucciones
# Luego subir el token.json resultante al VPS
```

### 5. Crear directorios necesarios

```bash
mkdir -p data logs airflow/logs airflow/dags airflow/plugins airflow/config
```

---

## Stack principal

> **Archivo:** `docker-compose.yml` (raíz del proyecto)
> **Servicios:** postgres, pipeline, dashboard, dev

### Levantar todo el stack

```bash
# Levantar todos los servicios (postgres + pipeline + dashboard)
docker compose up -d

# Solo levantar PostgreSQL
docker compose up -d postgres

# Solo ejecutar el pipeline ETL
docker compose run --rm pipeline python -m etl.pipeline

# Pipeline sin ingesta (datos ya descargados)
docker compose run --rm pipeline python -m etl.pipeline --skip-ingest

# Pipeline sin subida a Drive
docker compose run --rm pipeline python -m etl.pipeline --skip-upload

# Solo el dashboard
docker compose up -d dashboard
```

### Servicio de desarrollo

El servicio `dev` monta el código fuente directamente y solo se activa con el profile `dev`:

```bash
# Levantar el entorno de desarrollo
docker compose --profile dev up -d dev

# Ejecutar comandos dentro del contenedor de desarrollo
docker exec -it seminario-dev bash
```

> **Nota:** El servicio `dev` y `dashboard` comparten el puerto 8501. No levantarlos simultáneamente.

### Detener el stack

```bash
# Detener todos los servicios (mantiene volúmenes)
docker compose down

# Detener y eliminar volúmenes (BORRA DATOS de PostgreSQL)
docker compose down -v
```

### Reconstruir imágenes

```bash
# Tras cambios en Dockerfile, pyproject.toml o código fuente
docker compose build --no-cache

# Reconstruir y levantar
docker compose up -d --build
```

---

## Stack de Airflow

> **Archivo:** `airflow/docker-compose.yaml`
> **Servicios:** airflow-postgres, airflow-init, airflow-webserver, airflow-scheduler

### Método 1: Script helper (recomendado)

```bash
# Arrancar (levanta postgres del stack principal + Airflow)
bash scripts/start_airflow.sh

# Detener
bash scripts/start_airflow.sh stop

# Reiniciar
bash scripts/start_airflow.sh restart

# Ver logs
bash scripts/start_airflow.sh logs

# Estado de servicios
bash scripts/start_airflow.sh status
```

### Método 2: Setup completo (primera vez)

```bash
# Setup completo: pre-requisitos + directorios + init + servicios + health check
bash scripts/setup_airflow.sh

# Solo detener
bash scripts/setup_airflow.sh --down

# Solo ver estado
bash scripts/setup_airflow.sh --status
```

### Método 3: Manual con Docker Compose

```bash
# PASO 1: Levantar PostgreSQL del stack principal (los DAGs lo necesitan)
docker compose up -d postgres

# PASO 2: Levantar Airflow
docker compose -f airflow/docker-compose.yaml up -d

# Verificar que todo esté corriendo
docker compose -f airflow/docker-compose.yaml ps
```

> **Importante:** El stack principal debe estar corriendo primero porque Airflow se conecta a `seminario-postgres` a través de la red compartida `seminario_default`.

### Detener Airflow

```bash
# Detener Airflow (mantiene volúmenes)
docker compose -f airflow/docker-compose.yaml down

# Detener y eliminar volúmenes (BORRA metadatos de Airflow)
docker compose -f airflow/docker-compose.yaml down -v
```

### Orden correcto de inicio completo

Para levantar **todo** desde cero:

```bash
# 1. Stack principal (crea la red seminario_default + postgres)
docker compose up -d postgres

# 2. Esperar a que postgres esté healthy
docker compose ps  # verificar que seminario-postgres esté "healthy"

# 3. Stack de Airflow
docker compose -f airflow/docker-compose.yaml up -d

# 4. (Opcional) Dashboard
docker compose up -d dashboard
```

---

## URLs de acceso

| Servicio | URL | Descripción |
|----------|-----|-------------|
| **Dashboard Streamlit** | `http://192.241.132.222:8501` | Dashboard de visualización de datos |
| **Airflow Webserver** | `http://192.241.132.222:8080` | UI de orquestación de Airflow |
| **PostgreSQL (ETL)** | `192.241.132.222:5433` | Base de datos del pipeline |
| **PostgreSQL (Airflow)** | `192.241.132.222:5434` | Base de datos de metadatos de Airflow |

### Conexión a PostgreSQL desde máquina local

```bash
# Base de datos del pipeline ETL
psql -h 192.241.132.222 -p 5433 -U yeigen -d seminario

# Base de datos de metadatos de Airflow
psql -h 192.241.132.222 -p 5434 -U airflow -d airflow
```

---

## Credenciales

### PostgreSQL (pipeline ETL)

| Campo | Valor |
|-------|-------|
| Host | `postgres` (dentro de Docker) / `192.241.132.222` (externo) |
| Puerto | `5432` (dentro de Docker) / `5433` (externo) |
| Usuario | `yeigen` |
| Contraseña | *(definida en `.env` como `POSTGRES_PASSWORD`)* |
| Base de datos | `seminario` |

### PostgreSQL (Airflow metadata)

| Campo | Valor |
|-------|-------|
| Host | `airflow-postgres` (dentro de Docker) / `192.241.132.222` (externo) |
| Puerto | `5432` (dentro de Docker) / `5434` (externo) |
| Usuario | `airflow` |
| Contraseña | `airflow` |
| Base de datos | `airflow` |

### Airflow Webserver

| Campo | Valor |
|-------|-------|
| URL | `http://192.241.132.222:8080` |
| Usuario | `admin` |
| Contraseña | `admin` |

### Google Drive OAuth2

| Campo | Ubicación |
|-------|-----------|
| Client ID | `.env` → `CLIENT_ID` |
| Client Secret | `.env` → `CLIENT_SECRET` |
| Token | `token.json` (raíz del proyecto) |

---

## Logs y monitoreo

### Logs de Docker Compose (stack principal)

```bash
# Todos los servicios
docker compose logs -f

# Solo un servicio específico
docker compose logs -f postgres
docker compose logs -f pipeline
docker compose logs -f dashboard

# Últimas 100 líneas
docker compose logs --tail=100 pipeline
```

### Logs de Docker Compose (Airflow)

```bash
# Todos los servicios de Airflow
docker compose -f airflow/docker-compose.yaml logs -f

# Solo webserver
docker compose -f airflow/docker-compose.yaml logs -f airflow-webserver

# Solo scheduler
docker compose -f airflow/docker-compose.yaml logs -f airflow-scheduler

# Solo init (útil para depurar errores de inicialización)
docker compose -f airflow/docker-compose.yaml logs airflow-init
```

### Logs del pipeline ETL

Los logs del pipeline se escriben en el directorio `logs/` montado como volumen:

```bash
# Listar archivos de log
ls -la logs/

# Ver log más reciente
tail -f logs/*.log
```

### Logs de Airflow (tareas)

Los logs de ejecución de tareas de Airflow se almacenan en `airflow/logs/`:

```bash
# Estructura: airflow/logs/dag_id=<dag>/run_id=<run>/task_id=<task>/attempt=<n>.log
ls -la airflow/logs/

# Ver log de una tarea específica
find airflow/logs/ -name "*.log" -newer airflow/logs/ -exec tail -20 {} +
```

También se pueden consultar desde la UI de Airflow en `http://192.241.132.222:8080` → clic en la tarea → pestaña **Log**.

### Estado de los contenedores

```bash
# Stack principal
docker compose ps

# Airflow
docker compose -f airflow/docker-compose.yaml ps

# Todos los contenedores corriendo
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

### Health checks

```bash
# PostgreSQL del pipeline
docker exec seminario-postgres pg_isready -U yeigen -d seminario

# PostgreSQL de Airflow
docker exec airflow-postgres pg_isready -U airflow -d airflow

# Airflow webserver
curl -s http://192.241.132.222:8080/health | python3 -m json.tool
```

---

## Operaciones comunes

### Ejecutar el pipeline ETL manualmente

```bash
# Pipeline completo (ingesta + transformación + star schema + export)
docker compose run --rm pipeline python -m etl.pipeline

# Sin ingesta (datos ya descargados en data/)
docker compose run --rm pipeline python -m etl.pipeline --skip-ingest

# Sin subida a Drive
docker compose run --rm pipeline python -m etl.pipeline --skip-upload
```

### Ejecutar scripts individuales dentro del contenedor

```bash
# Entrar al contenedor de pipeline
docker exec -it seminario-pipeline bash

# O ejecutar un script directamente
docker compose run --rm pipeline python -m scripts.create_db
docker compose run --rm pipeline python -m scripts.create_schemas
docker compose run --rm pipeline python -m scripts.create_dimensions
docker compose run --rm pipeline python -m scripts.create_facts
```

### Trigger de DAG desde la línea de comandos

```bash
# Trigger manual del DAG de Airflow
docker exec airflow-webserver airflow dags trigger seminario_etl_pipeline

# Listar DAGs
docker exec airflow-webserver airflow dags list

# Estado de la última ejecución
docker exec airflow-webserver airflow dags list-runs -d seminario_etl_pipeline
```

### Backup de PostgreSQL

```bash
# Dump completo de la base del pipeline
docker exec seminario-postgres pg_dump -U yeigen -d seminario > backup_seminario_$(date +%Y%m%d).sql

# Dump por schema
docker exec seminario-postgres pg_dump -U yeigen -d seminario -n raw > backup_raw.sql
docker exec seminario-postgres pg_dump -U yeigen -d seminario -n unified > backup_unified.sql
docker exec seminario-postgres pg_dump -U yeigen -d seminario -n facts > backup_facts.sql

# Restaurar
cat backup_seminario_20260227.sql | docker exec -i seminario-postgres psql -U yeigen -d seminario
```

### Actualizar el código

```bash
# Desde el VPS
cd /opt/seminario

# Obtener últimos cambios
git pull origin main

# Reconstruir imágenes
docker compose build --no-cache

# Reiniciar servicios
docker compose down && docker compose up -d
```

---

## Solución de problemas

### 1. Error de permisos en `airflow/logs/`

**Síntoma:** Airflow falla con `Permission denied` al escribir logs.

**Causa:** El usuario de Airflow dentro del contenedor (UID 50000) no tiene permisos sobre el directorio montado.

**Solución:**

```bash
# Opción A: Dar permisos al UID de Airflow
sudo chown -R 50000:0 airflow/logs airflow/dags airflow/plugins airflow/config

# Opción B: Permisos abiertos (menos seguro, pero funciona)
chmod -R 777 airflow/logs airflow/dags airflow/plugins airflow/config
```

### 2. `token.json` inválido o expirado

**Síntoma:** El pipeline falla con errores de autenticación de Google Drive:
```
google.auth.exceptions.RefreshError: ... Token has been expired or revoked
```

**Causa:** El refresh token de OAuth2 expiró (Google lo revoca después de 7 días si la app está en modo "testing").

**Solución:**

```bash
# 1. Desde la máquina local (con navegador):
uv run python reauth.py

# 2. Subir el nuevo token al VPS:
scp token.json root@192.241.132.222:/opt/seminario/token.json

# 3. Verificar permisos (el contenedor pipeline lo monta como read-only)
chmod 644 token.json

# 4. Re-ejecutar el pipeline
docker compose run --rm pipeline python -m etl.pipeline
```

**Para entornos sin navegador** (alternativa manual):

```bash
# En la máquina local:
uv run python reauth_manual.py
# Seguir las instrucciones para obtener el código de autorización
# Luego copiar el token.json al VPS
```

> **Nota:** Airflow monta `token.json` como read-write (`../token.json:/opt/airflow/seminario/token.json`) para permitir auto-refresh del token. El pipeline lo monta como read-only.

### 3. Red `seminario_default` no existe

**Síntoma:** Airflow falla al iniciar con:
```
Network seminario_default declared as external, but could not be found
```

**Causa:** El stack principal no fue levantado antes que Airflow. La red `seminario_default` la crea `docker-compose.yml` al levantar cualquier servicio.

**Solución:**

```bash
# Levantar al menos postgres del stack principal primero
docker compose up -d postgres

# Luego levantar Airflow
docker compose -f airflow/docker-compose.yaml up -d
```

> **No** crear la red manualmente con `docker network create`. Esto genera labels vacías que rompen Docker Compose. Siempre dejar que `docker compose up` la cree.

### 4. Puerto ocupado

**Síntoma:**
```
Bind for 0.0.0.0:5433 failed: port is already allocated
```

**Solución:**

```bash
# Identificar qué proceso usa el puerto
ss -tlnp | grep 5433

# Si es un contenedor huérfano
docker ps -a | grep 5433
docker rm -f <container_id>

# Si es un servicio del sistema
sudo systemctl stop postgresql  # si hay un postgres local instalado
```

| Puerto | Servicio |
|--------|----------|
| 5433 | PostgreSQL (ETL) |
| 5434 | PostgreSQL (Airflow) |
| 8080 | Airflow webserver |
| 8501 | Dashboard Streamlit |

### 5. Pipeline muere por falta de memoria

**Síntoma:** El contenedor se detiene abruptamente (OOMKilled).

**Solución:**

```bash
# Verificar si fue OOMKilled
docker inspect seminario-pipeline | grep -i oom

# Limitar memoria de otros servicios para liberar RAM
# O agregar swap al VPS:
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
```

### 6. Airflow scheduler detecta zombies

**Síntoma:** Tareas marcadas como `zombie` o `failed` en la UI.

**Causa:** Tareas CPU-bound que no envían heartbeat a tiempo.

**Solución:** Los timeouts ya están configurados en `airflow/docker-compose.yaml`:

```yaml
AIRFLOW__SCHEDULER__TASK_INSTANCE_HEARTBEAT_SEC: "30"       # Heartbeat cada 30s
AIRFLOW__SCHEDULER__TASK_INSTANCE_HEARTBEAT_TIMEOUT: "10"   # Timeout: 30×10 = 300s = 5min
AIRFLOW__SCHEDULER__ZOMBIE_DETECTION_INTERVAL: "120"        # Detectar zombies cada 2min
```

Si persiste, verificar que el VPS tiene suficiente CPU:

```bash
# Ver uso de CPU en tiempo real
docker stats
```

### 7. Airflow init falla o queda en loop

**Síntoma:** `airflow-init` nunca completa y webserver/scheduler no arrancan.

**Solución:**

```bash
# Ver logs de init
docker compose -f airflow/docker-compose.yaml logs airflow-init

# Reiniciar desde cero (elimina metadatos de Airflow, NO los datos del pipeline)
docker compose -f airflow/docker-compose.yaml down -v
docker compose -f airflow/docker-compose.yaml up -d
```

### 8. Dashboard no conecta a PostgreSQL

**Síntoma:** Streamlit muestra error de conexión a la base de datos.

**Causa:** El dashboard depende de que `pipeline` haya terminado exitosamente (`service_completed_successfully`). Si el pipeline no corrió, el dashboard no inicia.

**Solución:**

```bash
# Verificar que postgres esté corriendo y healthy
docker compose ps postgres

# Ejecutar el pipeline primero
docker compose run --rm pipeline python -m etl.pipeline --skip-ingest

# Luego levantar el dashboard
docker compose up -d dashboard
```

### 9. Datos corruptos o pipeline inconsistente

**Solución nuclear:** Reiniciar todo desde cero.

```bash
# Detener todo
docker compose down -v
docker compose -f airflow/docker-compose.yaml down -v

# Eliminar datos descargados (opcional)
rm -rf data/*

# Reconstruir y re-ejecutar
docker compose up -d postgres
docker compose run --rm pipeline python -m etl.pipeline
docker compose up -d dashboard
```

---

## Checklist de despliegue

```
[ ] SSH al VPS funciona
[ ] Docker y Docker Compose instalados
[ ] Repositorio clonado en /opt/seminario
[ ] .env creado con credenciales correctas
[ ] token.json subido y con permisos 644
[ ] Directorios data/, logs/, airflow/logs/ creados
[ ] Permisos de airflow/logs/ ajustados (chown 50000:0)
[ ] docker compose up -d postgres → healthy
[ ] docker compose run pipeline → pipeline completo sin errores
[ ] docker compose up -d dashboard → http://192.241.132.222:8501 accesible
[ ] bash scripts/start_airflow.sh → http://192.241.132.222:8080 accesible
[ ] DAG seminario_etl_pipeline visible en la UI de Airflow
[ ] Firewall permite puertos 8080, 8501 (y opcionalmente 5433, 5434)
```
