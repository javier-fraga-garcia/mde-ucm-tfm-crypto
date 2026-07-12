from pydantic import field_validator
from shared.config import BaseSettings


class IngestionSettings(BaseSettings):
    """Configuración del servicio de ingesta de datos de mercado.

    Esta clase carga y valida la configuración necesaria para conectar
    con el WebSocket del proveedor de datos y publicar los eventos
    recibidos en un tópico de Kafka.

    Attributes:
        ws_base_url: URL base del WebSocket desde el que se reciben
            los datos de mercado.
        symbols: Lista de símbolos financieros que se deben suscribir.
        kafka_bootstrap_servers: Dirección de los brokers de Kafka.
        kafka_topic: Nombre del tópico de Kafka donde se publicarán
            los eventos recibidos.
    """

    ws_base_url: str
    symbols: list[str]
    kafka_bootstrap_servers: str
    kafka_topic: str

    @field_validator("symbols", mode="before")
    @classmethod
    def parse_symbols(cls, value: str | list[str]) -> list[str]:
        if isinstance(value, str):
            return [v.strip() for v in value.split(",")]
        return value
