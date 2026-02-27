# ============================================================
# Dockerfile — Seminario Ingeniería de Datos
#
# Multi-stage build con uv para reproducibilidad.
# Stages:
#   1. builder  → instala dependencias en .venv
#   2. runtime  → imagen final mínima con solo .venv + código
#
# Build:
#   docker build -t seminario .
#
# Run pipeline:
#   docker run --rm \
#     --env-file .env \
#     -v ./data:/app/data \
#     -v ./logs:/app/logs \
#     -v ./token.json:/app/token.json:ro \
#     seminario python -m etl.pipeline --skip-ingest
#
# Run Streamlit dashboard:
#   docker run --rm -p 8501:8501 \
#     --env-file .env \
#     -v ./data:/app/data \
#     seminario streamlit run dashboard/app.py
# ============================================================

# ── Stage 1: Builder ─────────────────────────────────────────
FROM python:3.12-slim-bookworm AS builder

# Copiar uv desde la imagen oficial
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Instalar dependencias primero (layer caching)
# Solo se invalida cuando pyproject.toml o uv.lock cambian
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-editable

# Copiar el código fuente
COPY . /app

# Instalar el proyecto (sin modo editable para producción)
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-editable

# ── Stage 2: Runtime ─────────────────────────────────────────
FROM python:3.12-slim-bookworm AS runtime

# Metadatos
LABEL maintainer="seminario-team"
LABEL description="Pipeline ETL — Seminario Ingeniería de Datos"

# Crear usuario no-root para seguridad
RUN groupadd --gid 1000 app && \
    useradd --uid 1000 --gid app --shell /bin/bash --create-home app

WORKDIR /app

# Copiar el entorno virtual completo desde builder
COPY --from=builder --chown=app:app /app/.venv /app/.venv

# Copiar solo el código fuente necesario (sin .venv, .git, etc.)
COPY --chown=app:app config/ /app/config/
COPY --chown=app:app etl/ /app/etl/
COPY --chown=app:app scripts/ /app/scripts/
COPY --chown=app:app utils/ /app/utils/
COPY --chown=app:app dashboard/ /app/dashboard/
COPY --chown=app:app main.py /app/main.py
COPY --chown=app:app pyproject.toml /app/pyproject.toml

# Crear directorios para datos y logs (se montan como volúmenes)
RUN mkdir -p /app/data /app/logs && \
    chown -R app:app /app/data /app/logs

# Activar el entorno virtual via PATH
ENV PATH="/app/.venv/bin:$PATH"

# Evitar buffering de Python (logs en tiempo real)
ENV PYTHONUNBUFFERED=1

# Evitar generación de .pyc dentro del contenedor
ENV PYTHONDONTWRITEBYTECODE=1

# Volúmenes para persistencia de datos
VOLUME ["/app/data", "/app/logs"]

# Puerto para Streamlit dashboard
EXPOSE 8501

# Cambiar a usuario no-root
USER app

# Comando por defecto: ejecutar el pipeline completo
CMD ["python", "-m", "etl.pipeline", "--skip-ingest"]
