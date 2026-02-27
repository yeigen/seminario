#!/usr/bin/env bash
# ============================================================
# setup_airflow.sh — Setup comprehensivo de Apache Airflow
#
# Automatiza la instalación y configuración de Airflow via Docker
# Compose para el proyecto Seminario de Ingeniería de Datos.
#
# Es IDEMPOTENTE: puede ejecutarse múltiples veces sin efectos
# secundarios. Cada paso verifica si ya fue completado antes
# de actuar.
#
# Uso:
#   chmod +x scripts/setup_airflow.sh
#   ./scripts/setup_airflow.sh
#
# Flags:
#   --skip-init    Saltar la inicialización de la base de datos
#   --skip-start   Saltar el inicio de servicios
#   --down         Detener todos los servicios y salir
#   --status       Mostrar estado de servicios y salir
#   --help         Mostrar esta ayuda
# ============================================================

set -euo pipefail

# ── Colores y formato ────────────────────────────────────────
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly BLUE='\033[0;34m'
readonly CYAN='\033[0;36m'
readonly BOLD='\033[1m'
readonly NC='\033[0m' # No Color

log_info()    { echo -e "${BLUE}[INFO]${NC}  $*"; }
log_ok()      { echo -e "${GREEN}[OK]${NC}    $*"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $*"; }
log_section() { echo -e "\n${BOLD}${CYAN}═══ $* ═══${NC}\n"; }

# ── Variables de proyecto ────────────────────────────────────
# Resolver PROJECT_ROOT al directorio raíz del proyecto (padre de scripts/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
AIRFLOW_DIR="${PROJECT_ROOT}/airflow"
AIRFLOW_ENV_FILE="${AIRFLOW_DIR}/.env"
COMPOSE_FILE="${AIRFLOW_DIR}/docker-compose.yaml"

readonly AIRFLOW_WEBSERVER_URL="http://localhost:8080"
readonly HEALTH_ENDPOINT="${AIRFLOW_WEBSERVER_URL}/health"
readonly MAX_HEALTH_RETRIES=30
readonly HEALTH_RETRY_INTERVAL=5

# ── Flags ────────────────────────────────────────────────────
SKIP_INIT=false
SKIP_START=false
ACTION_DOWN=false
ACTION_STATUS=false

# ── Parse de argumentos ──────────────────────────────────────
parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --skip-init)   SKIP_INIT=true;   shift ;;
            --skip-start)  SKIP_START=true;  shift ;;
            --down)        ACTION_DOWN=true;  shift ;;
            --status)      ACTION_STATUS=true; shift ;;
            --help|-h)     show_help; exit 0 ;;
            *)
                log_error "Argumento desconocido: $1"
                show_help
                exit 1
                ;;
        esac
    done
}

show_help() {
    cat << 'EOF'
Uso: ./scripts/setup_airflow.sh [OPCIONES]

Opciones:
  --skip-init    Saltar la inicialización de la base de datos
  --skip-start   Saltar el inicio de servicios
  --down         Detener todos los servicios y salir
  --status       Mostrar estado de servicios y salir
  --help, -h     Mostrar esta ayuda

Ejemplos:
  ./scripts/setup_airflow.sh                  # Setup completo
  ./scripts/setup_airflow.sh --skip-init      # Solo levantar servicios
  ./scripts/setup_airflow.sh --down           # Detener todo
  ./scripts/setup_airflow.sh --status         # Ver estado
EOF
}

# ════════════════════════════════════════════════════════════
# FASE 0: Acciones rápidas (--down, --status)
# ════════════════════════════════════════════════════════════

action_down() {
    log_section "Deteniendo servicios de Airflow"
    docker compose -f "${COMPOSE_FILE}" down
    log_ok "Todos los servicios detenidos"
    exit 0
}

action_status() {
    log_section "Estado de servicios Airflow"
    docker compose -f "${COMPOSE_FILE}" ps
    echo ""
    # Verificar salud del webserver
    if curl -sf "${HEALTH_ENDPOINT}" > /dev/null 2>&1; then
        log_ok "Webserver accesible en ${AIRFLOW_WEBSERVER_URL}"
    else
        log_warn "Webserver NO accesible en ${AIRFLOW_WEBSERVER_URL}"
    fi
    exit 0
}

# ════════════════════════════════════════════════════════════
# FASE 1: Pre-requisitos
# ════════════════════════════════════════════════════════════

check_prerequisites() {
    log_section "Fase 1: Verificando pre-requisitos"

    # 1.1 Docker
    if command -v docker &> /dev/null; then
        local docker_version
        docker_version="$(docker --version 2>&1)"
        log_ok "Docker instalado: ${docker_version}"
    else
        log_error "Docker no está instalado."
        log_error "Instalar desde: https://docs.docker.com/engine/install/"
        exit 1
    fi

    # 1.2 Docker daemon corriendo
    if docker info &> /dev/null; then
        log_ok "Docker daemon corriendo"
    else
        log_error "Docker daemon no está corriendo. Ejecutar: sudo systemctl start docker"
        exit 1
    fi

    # 1.3 Docker Compose (v2 integrado en docker CLI)
    if docker compose version &> /dev/null; then
        local compose_version
        compose_version="$(docker compose version 2>&1)"
        log_ok "Docker Compose instalado: ${compose_version}"
    else
        log_error "Docker Compose no está disponible."
        log_error "Se requiere Docker Compose V2 (plugin 'docker compose')."
        log_error "Instalar desde: https://docs.docker.com/compose/install/"
        exit 1
    fi

    # 1.4 Verificar que el archivo docker-compose existe
    if [[ -f "${COMPOSE_FILE}" ]]; then
        log_ok "Archivo docker-compose encontrado: ${COMPOSE_FILE}"
    else
        log_error "No se encontró ${COMPOSE_FILE}"
        log_error "Asegúrate de estar en el directorio correcto del proyecto."
        exit 1
    fi

    # 1.5 Verificar memoria disponible (Airflow recomienda >=4GB)
    local mem_total_kb
    mem_total_kb="$(grep MemTotal /proc/meminfo 2>/dev/null | awk '{print $2}' || echo 0)"
    local mem_total_gb=$(( mem_total_kb / 1024 / 1024 ))
    if [[ ${mem_total_gb} -lt 4 ]]; then
        log_warn "Memoria RAM: ~${mem_total_gb}GB. Airflow recomienda al menos 4GB."
    else
        log_ok "Memoria RAM: ~${mem_total_gb}GB (suficiente)"
    fi
}

# ════════════════════════════════════════════════════════════
# FASE 2: Crear directorios y archivos de configuración
# ════════════════════════════════════════════════════════════

setup_directories() {
    log_section "Fase 2: Configuración de directorios y entorno"

    # 2.1 Directorios de Airflow
    local dirs=(
        "${AIRFLOW_DIR}/dags"
        "${AIRFLOW_DIR}/logs"
        "${AIRFLOW_DIR}/plugins"
        "${AIRFLOW_DIR}/config"
    )

    for dir in "${dirs[@]}"; do
        if [[ -d "${dir}" ]]; then
            log_ok "Directorio ya existe: ${dir#"${PROJECT_ROOT}"/}"
        else
            mkdir -p "${dir}"
            log_ok "Directorio creado: ${dir#"${PROJECT_ROOT}"/}"
        fi
    done

    # 2.2 Logs del proyecto raíz (montados en el container)
    if [[ ! -d "${PROJECT_ROOT}/logs" ]]; then
        mkdir -p "${PROJECT_ROOT}/logs"
        log_ok "Directorio creado: logs/"
    fi

    # 2.3 Data del proyecto raíz
    if [[ ! -d "${PROJECT_ROOT}/data" ]]; then
        mkdir -p "${PROJECT_ROOT}/data"
        log_ok "Directorio creado: data/"
    fi
}

setup_env_file() {
    # 2.4 Archivo .env para Airflow
    if [[ -f "${AIRFLOW_ENV_FILE}" ]]; then
        log_ok "Archivo .env de Airflow ya existe"

        # Verificar que las variables clave estén presentes, actualizar si faltan
        local needs_update=false

        if ! grep -q "^AIRFLOW_UID=" "${AIRFLOW_ENV_FILE}" 2>/dev/null; then
            log_warn "AIRFLOW_UID no encontrado en .env, se agregará"
            needs_update=true
        fi
        if ! grep -q "^FERNET_KEY=" "${AIRFLOW_ENV_FILE}" 2>/dev/null; then
            log_warn "FERNET_KEY no encontrado en .env, se agregará"
            needs_update=true
        fi
        if ! grep -q "^AIRFLOW_SECRET_KEY=" "${AIRFLOW_ENV_FILE}" 2>/dev/null; then
            log_warn "AIRFLOW_SECRET_KEY no encontrado en .env, se agregará"
            needs_update=true
        fi

        if [[ "${needs_update}" == "true" ]]; then
            append_missing_env_vars
        fi
    else
        log_info "Generando archivo .env para Airflow..."
        generate_env_file
        log_ok "Archivo .env de Airflow creado: airflow/.env"
    fi
}

generate_fernet_key() {
    # Genera Fernet key usando Python (misma forma que la doc oficial de Airflow)
    python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" 2>/dev/null \
        || openssl rand -base64 32
}

generate_secret_key() {
    # Genera secret key para el webserver
    python3 -c "import secrets; print(secrets.token_hex(32))" 2>/dev/null \
        || openssl rand -hex 32
}

generate_env_file() {
    local fernet_key
    fernet_key="$(generate_fernet_key)"

    local secret_key
    secret_key="$(generate_secret_key)"

    local airflow_uid
    airflow_uid="$(id -u)"

    cat > "${AIRFLOW_ENV_FILE}" << EOF
# ============================================================
# .env — Variables de entorno para Airflow Docker Compose
#
# Generado automáticamente por setup_airflow.sh
# Fecha: $(date -Iseconds)
#
# ¡NO COMMITEAR ESTE ARCHIVO! Contiene secretos.
# ============================================================

# ── Airflow Core ─────────────────────────────────────────────
AIRFLOW_UID=${airflow_uid}

# Fernet key para cifrado de conexiones y variables
# Generada con: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
FERNET_KEY=${fernet_key}

# Secret key para sesiones del webserver
AIRFLOW_SECRET_KEY=${secret_key}

# ── Imagen de Airflow ────────────────────────────────────────
AIRFLOW_IMAGE_NAME=apache/airflow:2.9.3-python3.12
EOF

    log_info "AIRFLOW_UID=${airflow_uid}"
    log_info "FERNET_KEY generada (cifrado de conexiones)"
    log_info "SECRET_KEY generada (sesiones web)"
}

append_missing_env_vars() {
    echo "" >> "${AIRFLOW_ENV_FILE}"
    echo "# --- Agregado por setup_airflow.sh ($(date -Iseconds)) ---" >> "${AIRFLOW_ENV_FILE}"

    if ! grep -q "^AIRFLOW_UID=" "${AIRFLOW_ENV_FILE}" 2>/dev/null; then
        echo "AIRFLOW_UID=$(id -u)" >> "${AIRFLOW_ENV_FILE}"
        log_ok "AIRFLOW_UID agregado"
    fi

    if ! grep -q "^FERNET_KEY=" "${AIRFLOW_ENV_FILE}" 2>/dev/null; then
        local fernet_key
        fernet_key="$(generate_fernet_key)"
        echo "FERNET_KEY=${fernet_key}" >> "${AIRFLOW_ENV_FILE}"
        log_ok "FERNET_KEY generada y agregada"
    fi

    if ! grep -q "^AIRFLOW_SECRET_KEY=" "${AIRFLOW_ENV_FILE}" 2>/dev/null; then
        local secret_key
        secret_key="$(generate_secret_key)"
        echo "AIRFLOW_SECRET_KEY=${secret_key}" >> "${AIRFLOW_ENV_FILE}"
        log_ok "AIRFLOW_SECRET_KEY generada y agregada"
    fi
}

# ════════════════════════════════════════════════════════════
# FASE 3: Docker networks
# ════════════════════════════════════════════════════════════

setup_docker_networks() {
    log_section "Fase 3: Redes Docker"

    # La red por defecto de docker compose se crea automáticamente,
    # pero si el proyecto necesita una red compartida entre el
    # docker-compose raíz y el de airflow, la creamos.
    local network_name="seminario-network"

    if docker network inspect "${network_name}" &> /dev/null; then
        log_ok "Red Docker '${network_name}' ya existe"
    else
        docker network create "${network_name}" 2>/dev/null && \
            log_ok "Red Docker '${network_name}' creada" || \
            log_warn "No se pudo crear la red '${network_name}' (puede que ya exista)"
    fi
}

# ════════════════════════════════════════════════════════════
# FASE 4: Inicialización de Airflow
# ════════════════════════════════════════════════════════════

initialize_airflow() {
    if [[ "${SKIP_INIT}" == "true" ]]; then
        log_section "Fase 4: Inicialización (SALTADA --skip-init)"
        return 0
    fi

    log_section "Fase 4: Inicializando Airflow"

    log_info "Ejecutando airflow-init (db init + creación de usuario admin)..."
    log_info "Esto puede tardar 1-3 minutos la primera vez..."
    echo ""

    # Pasar variables del .env al compose
    if docker compose --env-file "${AIRFLOW_ENV_FILE}" -f "${COMPOSE_FILE}" up airflow-init --abort-on-container-exit 2>&1; then
        log_ok "Inicialización completada exitosamente"
    else
        local exit_code=$?
        # airflow-init puede fallar si ya fue inicializado; verificamos
        if docker compose -f "${COMPOSE_FILE}" ps --format json 2>/dev/null | grep -q '"airflow-init"'; then
            log_warn "airflow-init terminó con código ${exit_code}, pero puede ser idempotente"
        else
            log_error "Error en la inicialización de Airflow (código: ${exit_code})"
            log_error "Revisar logs con: docker compose -f ${COMPOSE_FILE} logs airflow-init"
            exit 1
        fi
    fi
}

# ════════════════════════════════════════════════════════════
# FASE 5: Levantar servicios
# ════════════════════════════════════════════════════════════

start_services() {
    if [[ "${SKIP_START}" == "true" ]]; then
        log_section "Fase 5: Inicio de servicios (SALTADO --skip-start)"
        return 0
    fi

    log_section "Fase 5: Levantando servicios"

    log_info "Iniciando postgres, webserver y scheduler..."

    docker compose --env-file "${AIRFLOW_ENV_FILE}" -f "${COMPOSE_FILE}" up -d \
        postgres \
        airflow-webserver \
        airflow-scheduler

    log_ok "Servicios iniciados en modo detached"
    echo ""

    # Mostrar estado
    docker compose -f "${COMPOSE_FILE}" ps
}

# ════════════════════════════════════════════════════════════
# FASE 6: Verificación de salud
# ════════════════════════════════════════════════════════════

verify_health() {
    if [[ "${SKIP_START}" == "true" ]]; then
        return 0
    fi

    log_section "Fase 6: Verificación de salud"

    log_info "Esperando a que el webserver esté listo..."
    log_info "Endpoint: ${HEALTH_ENDPOINT}"
    echo ""

    local attempt=0
    while [[ ${attempt} -lt ${MAX_HEALTH_RETRIES} ]]; do
        attempt=$((attempt + 1))

        if curl -sf "${HEALTH_ENDPOINT}" > /dev/null 2>&1; then
            echo ""
            log_ok "Webserver accesible en ${AIRFLOW_WEBSERVER_URL}"

            # Obtener estado detallado del health endpoint
            local health_response
            health_response="$(curl -sf "${HEALTH_ENDPOINT}" 2>/dev/null || echo '{}')"
            if command -v python3 &> /dev/null; then
                echo "${health_response}" | python3 -m json.tool 2>/dev/null || echo "${health_response}"
            else
                echo "${health_response}"
            fi
            echo ""
            return 0
        fi

        printf "\r  Intento %d/%d — esperando %ds..." "${attempt}" "${MAX_HEALTH_RETRIES}" "${HEALTH_RETRY_INTERVAL}"
        sleep "${HEALTH_RETRY_INTERVAL}"
    done

    echo ""
    log_warn "El webserver no respondió después de $(( MAX_HEALTH_RETRIES * HEALTH_RETRY_INTERVAL ))s"
    log_warn "Puede que aún esté iniciando. Verificar manualmente:"
    log_warn "  curl ${HEALTH_ENDPOINT}"
    log_warn "  docker compose -f ${COMPOSE_FILE} logs airflow-webserver"
}

# ════════════════════════════════════════════════════════════
# FASE 7: Resumen final
# ════════════════════════════════════════════════════════════

show_summary() {
    log_section "Setup completado"

    cat << EOF
${GREEN}╔══════════════════════════════════════════════════════════╗
║              Airflow está listo                           ║
╠══════════════════════════════════════════════════════════╣
║                                                          ║
║  UI:           ${BOLD}${AIRFLOW_WEBSERVER_URL}${NC}${GREEN}                  ║
║  Usuario:      ${BOLD}admin${NC}${GREEN}                                     ║
║  Contraseña:   ${BOLD}admin${NC}${GREEN}                                     ║
║                                                          ║
╠══════════════════════════════════════════════════════════╣
║  Comandos útiles:                                        ║
║                                                          ║
║  Estado:       ${NC}./scripts/setup_airflow.sh --status${GREEN}      ║
║  Detener:      ${NC}./scripts/setup_airflow.sh --down${GREEN}        ║
║  Logs web:     ${NC}docker compose -f airflow/docker-compose.yaml logs -f airflow-webserver${GREEN}  ║
║  Logs sched:   ${NC}docker compose -f airflow/docker-compose.yaml logs -f airflow-scheduler${GREEN}  ║
║  Reiniciar:    ${NC}docker compose -f airflow/docker-compose.yaml restart${GREEN}                     ║
║                                                          ║
╚══════════════════════════════════════════════════════════╝${NC}
EOF
}

# ════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════

main() {
    echo -e "${BOLD}${CYAN}"
    cat << 'BANNER'

     █████╗ ██╗██████╗ ███████╗██╗      ██████╗ ██╗    ██╗
    ██╔══██╗██║██╔══██╗██╔════╝██║     ██╔═══██╗██║    ██║
    ███████║██║██████╔╝█████╗  ██║     ██║   ██║██║ █╗ ██║
    ██╔══██║██║██╔══██╗██╔══╝  ██║     ██║   ██║██║███╗██║
    ██║  ██║██║██║  ██║██║     ███████╗╚██████╔╝╚███╔███╔╝
    ╚═╝  ╚═╝╚═╝╚═╝  ╚═╝╚═╝     ╚══════╝ ╚═════╝  ╚══╝╚══╝
             Seminario — Setup Automático

BANNER
    echo -e "${NC}"

    parse_args "$@"

    # Acciones rápidas
    [[ "${ACTION_DOWN}"   == "true" ]] && action_down
    [[ "${ACTION_STATUS}" == "true" ]] && action_status

    # Pipeline completo de setup
    check_prerequisites
    setup_directories
    setup_env_file
    setup_docker_networks
    initialize_airflow
    start_services
    verify_health
    show_summary
}

main "$@"
