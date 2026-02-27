"""
Logger centralizado del proyecto Seminario Ingeniería de Datos.

Configura un logger con salida a consola (INFO) y archivo (DEBUG).
La carpeta de logs se crea automáticamente si no existe.

Uso:
    from utils.logger import logger

    logger.info("Proceso iniciado")
    logger.debug("Detalle interno: %s", variable)
    logger.warning("Archivo no encontrado: %s", path)
    logger.error("Fallo en la conexión: %s", err)
"""

import logging
import sys

from config.globals import LOG_DIR, LOG_FILE, PROJECT_NAME

# ──────────────────────────────────────────────────────────────
# Crear carpeta de logs si no existe
# ──────────────────────────────────────────────────────────────
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ──────────────────────────────────────────────────────────────
# Formato de log
# ──────────────────────────────────────────────────────────────
LOG_FORMAT = "[%(asctime)s] - %(levelname)s - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

# ──────────────────────────────────────────────────────────────
# Console handler — INFO level
# ──────────────────────────────────────────────────────────────
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

# ──────────────────────────────────────────────────────────────
# File handler — DEBUG level
# ──────────────────────────────────────────────────────────────
file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

# ──────────────────────────────────────────────────────────────
# Logger principal
# ──────────────────────────────────────────────────────────────
logger = logging.getLogger(PROJECT_NAME)
logger.setLevel(logging.DEBUG)
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# Evitar propagación al root logger (duplicación de mensajes)
logger.propagate = False
