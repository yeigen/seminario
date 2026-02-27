import logging
import sys
from logging.handlers import RotatingFileHandler

from config.globals import LOG_DIR, LOG_FILE, PROJECT_NAME

LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FORMAT = "[%(asctime)s] - %(levelname)s - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

file_handler = RotatingFileHandler(
    LOG_FILE,
    encoding="utf-8",
    maxBytes=10 * 1024 * 1024,
    backupCount=5,
)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

logger = logging.getLogger(PROJECT_NAME)
logger.setLevel(logging.DEBUG)
logger.addHandler(console_handler)
logger.addHandler(file_handler)
logger.propagate = False
