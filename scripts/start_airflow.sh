#!/usr/bin/env bash
# ============================================================
# scripts/start_airflow.sh — Levanta Apache Airflow
#
# Este script:
#   1. Verifica que Docker esté corriendo
#   2. Asegura que la red seminario_default exista
#   3. Levanta el stack principal de postgres (si no está corriendo)
#   4. Levanta Airflow (postgres metadata + init + webserver + scheduler)
#   5. Espera a que el webserver esté healthy
#
# Uso:
#   bash scripts/start_airflow.sh          # arrancar
#   bash scripts/start_airflow.sh stop     # detener
#   bash scripts/start_airflow.sh restart  # reiniciar
#   bash scripts/start_airflow.sh logs     # ver logs
#   bash scripts/start_airflow.sh status   # estado de servicios
#
# Credenciales UI (http://localhost:8080):
#   Usuario: admin
#   Password: admin
# ============================================================

set -euo pipefail

# ── Colores ──────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# ── Directorio raíz del proyecto ─────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
AIRFLOW_COMPOSE="${PROJECT_ROOT}/airflow/docker-compose.yaml"
MAIN_COMPOSE="${PROJECT_ROOT}/docker-compose.yml"

# ── Funciones helpers ────────────────────────────────────────
info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }

check_docker() {
    if ! command -v docker &>/dev/null; then
        err "Docker no está instalado. Instálalo desde https://docs.docker.com/get-docker/"
        exit 1
    fi
    if ! docker info &>/dev/null; then
        err "Docker no está corriendo. Inícialo primero."
        exit 1
    fi
    ok "Docker está corriendo"
}

ensure_seminario_network() {
    # La red seminario_default la crea el docker-compose.yml principal
    # (networks.default.name: seminario_default) con labels correctas.
    # NO crearla manualmente con "docker network create" porque eso
    # genera labels vacías que rompen docker compose.
    #
    # Estrategia: si la red no existe, levantamos el postgres del stack
    # principal (que la crea). Si ya existe con labels correctas, no hacemos nada.
    if docker network inspect seminario_default &>/dev/null; then
        ok "Red seminario_default ya existe"
    else
        info "Red seminario_default no encontrada, se creará al levantar el stack principal..."
    fi
}

ensure_seminario_postgres() {
    # Verifica si el postgres del pipeline ETL está corriendo.
    # Los DAGs necesitan conectarse a él para leer/escribir datos.
    local pg_status
    pg_status=$(docker inspect -f '{{.State.Status}}' seminario-postgres 2>/dev/null || echo "not_found")

    if [ "$pg_status" = "running" ]; then
        ok "seminario-postgres ya está corriendo"
    elif [ "$pg_status" = "not_found" ]; then
        info "Levantando postgres del pipeline ETL..."
        docker compose -f "${MAIN_COMPOSE}" up -d postgres
        ok "seminario-postgres levantado"
    else
        info "seminario-postgres existe pero está en estado '${pg_status}', reiniciando..."
        docker compose -f "${MAIN_COMPOSE}" up -d postgres
        ok "seminario-postgres reiniciado"
    fi
}

ensure_airflow_dirs() {
    # Airflow necesita que estos directorios existan y tengan permisos correctos
    local dirs=(
        "${PROJECT_ROOT}/airflow/logs"
        "${PROJECT_ROOT}/airflow/dags"
        "${PROJECT_ROOT}/airflow/plugins"
        "${PROJECT_ROOT}/airflow/config"
    )
    for dir in "${dirs[@]}"; do
        mkdir -p "$dir"
    done
    ok "Directorios de Airflow verificados"
}

wait_for_webserver() {
    local max_wait=120
    local elapsed=0
    info "Esperando que Airflow webserver esté listo (max ${max_wait}s)..."

    while [ $elapsed -lt $max_wait ]; do
        if curl -sf http://localhost:8080/health &>/dev/null; then
            echo ""
            ok "Airflow webserver está listo"
            return 0
        fi
        printf "."
        sleep 3
        elapsed=$((elapsed + 3))
    done

    echo ""
    warn "Timeout esperando al webserver. Revisa los logs:"
    warn "  docker compose -f ${AIRFLOW_COMPOSE} logs airflow-webserver"
    return 1
}

# ── Comandos ─────────────────────────────────────────────────
cmd_start() {
    echo -e "${CYAN}══════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  Seminario — Arrancando Apache Airflow${NC}"
    echo -e "${CYAN}══════════════════════════════════════════════════${NC}"
    echo ""

    check_docker
    ensure_airflow_dirs
    ensure_seminario_network
    ensure_seminario_postgres

    echo ""
    info "Levantando servicios de Airflow..."
    docker compose -f "${AIRFLOW_COMPOSE}" up -d

    echo ""
    wait_for_webserver

    echo ""
    echo -e "${GREEN}══════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}  Airflow está corriendo${NC}"
    echo -e "${GREEN}══════════════════════════════════════════════════${NC}"
    echo -e "  UI:          ${CYAN}http://localhost:8080${NC}"
    echo -e "  Usuario:     ${CYAN}admin${NC}"
    echo -e "  Password:    ${CYAN}admin${NC}"
    echo ""
    echo -e "  Comandos útiles:"
    echo -e "    ${YELLOW}bash scripts/start_airflow.sh logs${NC}     — ver logs"
    echo -e "    ${YELLOW}bash scripts/start_airflow.sh status${NC}   — estado"
    echo -e "    ${YELLOW}bash scripts/start_airflow.sh stop${NC}     — detener"
    echo ""
}

cmd_stop() {
    info "Deteniendo servicios de Airflow..."
    docker compose -f "${AIRFLOW_COMPOSE}" down
    ok "Airflow detenido"
}

cmd_restart() {
    cmd_stop
    echo ""
    cmd_start
}

cmd_logs() {
    docker compose -f "${AIRFLOW_COMPOSE}" logs -f --tail=100 airflow-webserver airflow-scheduler
}

cmd_status() {
    echo -e "${CYAN}══════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  Estado de servicios Airflow${NC}"
    echo -e "${CYAN}══════════════════════════════════════════════════${NC}"
    echo ""
    docker compose -f "${AIRFLOW_COMPOSE}" ps
    echo ""

    # Health check rápido
    if curl -sf http://localhost:8080/health &>/dev/null; then
        ok "Webserver respondiendo en http://localhost:8080"
    else
        warn "Webserver NO responde en http://localhost:8080"
    fi
}

cmd_reset() {
    warn "Esto eliminará TODOS los datos de Airflow (metadata DB, logs)."
    read -rp "¿Continuar? [y/N]: " confirm
    if [[ "${confirm}" =~ ^[Yy]$ ]]; then
        info "Deteniendo y eliminando volúmenes..."
        docker compose -f "${AIRFLOW_COMPOSE}" down -v
        ok "Airflow reseteado. Ejecuta 'bash scripts/start_airflow.sh' para reiniciar."
    else
        info "Cancelado."
    fi
}

# ── Main ─────────────────────────────────────────────────────
case "${1:-start}" in
    start)   cmd_start   ;;
    stop)    cmd_stop    ;;
    restart) cmd_restart ;;
    logs)    cmd_logs    ;;
    status)  cmd_status  ;;
    reset)   cmd_reset   ;;
    *)
        echo "Uso: $0 {start|stop|restart|logs|status|reset}"
        exit 1
        ;;
esac
