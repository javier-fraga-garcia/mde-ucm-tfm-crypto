import sys
import asyncio
import logging
from ingestion.ws_client import BinanceStreamConnector
from ingestion.producers import KafkaProducer
from shared.logging import configure_logging
from shared.schemas import StreamType

configure_logging("ingestion", level="INFO")
logger = logging.getLogger(__name__)


async def main():
    producer = KafkaProducer(bootstrap_servers="localhost:9092", topic="kafka-envelope")
    await producer.connect()
    try:
        connectors = [
            BinanceStreamConnector(
                base_url="wss://stream.binance.com:9443/stream",
                stream_type=s,
                symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT"],
                producer=producer,
            )
            for s in StreamType
        ]
        await asyncio.gather(*(con.run() for con in connectors))
    finally:
        await producer.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("El usuario interrumpió la ejecución. Saliendo del proceso...")
        sys.exit(0)
