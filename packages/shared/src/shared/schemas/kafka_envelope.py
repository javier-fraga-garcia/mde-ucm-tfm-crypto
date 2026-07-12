from datetime import datetime, timezone
from enum import Enum
from pydantic import BaseModel, Field


class StreamType(str, Enum):
    """Tipo de stream de datos de Binance transportado en el envelope."""

    AGG_TRADE = "aggTrade"
    BOOK_TICKER = "bookTicker"
    DEPTH20 = "depth10"


class KafkaEnvelope(BaseModel):
    """
    Estructura del mensaje publicado en Kafka por parte del sistema de ingesta desde el websocket.

    Attributes:
        ingestion_timestamp: momento en que ingestion recibió el mensaje.
            Se calcula automáticamente si no se pasa explícitamente.
        symbol: par de trading (p. ej. "BTCUSDT").
        stream_type: tipo de stream de Binance del que procede el mensaje.
        raw_payload: JSON crudo de Binance como string, tal cual llega del
            WebSocket, sin parsear.
    """

    ingestion_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    symbol: str
    stream_type: StreamType
    raw_payload: str
