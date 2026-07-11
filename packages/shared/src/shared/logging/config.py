import sys
import json
import logging
from datetime import datetime, timezone


class JSONFormatter(logging.Formatter):
    """
    Serializa cada log record a una línea JSON con:
    timestamp, level, logger, service, message.
    """

    def __init__(self, service: str):
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "service": self.service,
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        return json.dumps(payload)


def configure_logging(service: str, level: str = "INFO") -> None:
    """
    Configura el root logger (JSON a stdout). Llamar una vez por proceso,
    en el entry point. El resto de módulos solo usan getLogger(__name__).

    Args:
        service: nombre del servicio (p. ej. "ingestion"), se incluye en cada log.
        level: nivel mínimo, como string (p. ej. "INFO"). Case-insensitive.

    Raises:
        ValueError: si `level` no es un nivel de logging válido.
    """
    level_name = level.upper()

    valid_levels = logging.getLevelNamesMapping()

    if level_name not in valid_levels:
        raise ValueError(f"Nivel de logging no válido: {level_name}")

    formatter = JSONFormatter(service)

    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(valid_levels[level_name])
