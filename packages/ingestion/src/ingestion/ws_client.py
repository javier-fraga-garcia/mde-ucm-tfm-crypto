import sys
import json
import random
import websockets
import logging
from pydantic import ValidationError
from shared.schemas import KafkaEnvelope, StreamType
from ingestion.producers import Producer

logger = logging.getLogger(__name__)


class BinanceStreamConnector:
    """Conector para los streams WebSocket de Binance.

    Gestiona el ciclo de vida de la conexión con el WebSocket de Binance,
    realiza la suscripción a uno o varios símbolos y publica cada evento
    recibido mediante un productor.

    Attributes:
        base_url: URL del WebSocket de Binance.
        stream_type: Tipo de stream al que se realizará la suscripción.
        symbols: Lista de símbolos financieros que se deben monitorizar.
        producer: Productor encargado de publicar los eventos recibidos.
    """

    def __init__(
        self,
        base_url: str,
        stream_type: StreamType,
        symbols: list[str],
        producer: Producer,
    ):
        self.base_url = base_url
        self.stream_type = stream_type
        self.symbols = symbols
        self.producer = producer

    def _build_tickers(self) -> list[str]:
        """Construye los identificadores de los streams de Binance.

        Returns:
            Lista de streams en el formato requerido por Binance
            (por ejemplo, ``btcusdt@trade``).
        """
        return [f"{t.lower()}@{self.stream_type.value}" for t in self.symbols]

    async def run(self) -> None:
        """Inicia la conexión con Binance y procesa los eventos recibidos.

        El método establece la conexión WebSocket, envía el mensaje de
        suscripción y permanece escuchando eventos hasta que la conexión
        se cierre. Cada mensaje válido se encapsula en un
        ``KafkaEnvelope`` y se envía al productor configurado.

        Raises:
            websockets.ConnectionClosed: Si el servidor cierra la conexión.
            SystemExit: Si ocurre un error inesperado durante la ejecución.
        """
        logger.info(f"Conectado a stream {self.stream_type.value}...")
        try:
            async with websockets.connect(self.base_url) as ws:
                subscribe_msg = json.dumps(
                    {
                        "method": "SUBSCRIBE",
                        "params": self._build_tickers(),
                        "id": random.randint(1, 1000),
                    }
                )

                await ws.send(subscribe_msg)
                confirmation = json.loads(await ws.recv())
                logger.info(f"Suscrito: {confirmation}")

                async for msg in ws:
                    try:
                        parsed = json.loads(msg)
                        symbol = parsed["data"]["s"]
                        envelope = KafkaEnvelope(
                            symbol=symbol,
                            stream_type=self.stream_type,
                            raw_payload=msg,
                        )
                        logger.info(envelope.model_dump_json())
                        # self.producer.publish(envelope)
                    except (json.JSONDecodeError, KeyError, ValidationError) as e:
                        logger.error(f"Mensaje mal formado: {e}")
                        continue
        except websockets.ConnectionClosed:
            raise
        except Exception as e:
            logger.critical(f"Error inesperado {e}\nCerrando conexión...")
            sys.exit(1)
