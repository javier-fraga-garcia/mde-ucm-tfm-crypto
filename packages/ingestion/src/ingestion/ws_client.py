import sys
import json
import random
import websockets
import logging
from pydantic import ValidationError
from shared.schemas import KafkaEnvelope, StreamType

logger = logging.getLogger(__name__)


class BinanceStreamConnector:
    def __init__(self, base_url: str, stream_type: StreamType, symbols: list[str]):
        self.base_url = base_url
        self.stream_type = stream_type
        self.symbols = symbols

    def _build_tickers(self) -> list[str]:
        return [f"{t.lower()}@{self.stream_type.value}" for t in self.symbols]

    async def run(self) -> None:
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
                    except (json.JSONDecodeError, KeyError, ValidationError) as e:
                        logger.error(f"Mensaje mal formado: {e}")
                        continue
        except websockets.ConnectionClosed:
            raise
        except Exception as e:
            logger.critical(f"Error inesperado {e}\nCerrando conexión...")
            sys.exit(1)
