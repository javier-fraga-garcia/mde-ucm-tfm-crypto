import sys
import asyncio
import logging
from ingestion.ws_client import BinanceStreamConnector
from ingestion.producers import KafkaProducer
from ingestion.config import IngestionSettings
from shared.logging import configure_logging
from shared.schemas import StreamType

settings = IngestionSettings()

configure_logging("ingestion", level=settings.log_level)
logger = logging.getLogger(__name__)


async def main():
    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers, topic=settings.kafka_topic
    )
    await producer.connect()
    try:
        connectors = [
            BinanceStreamConnector(
                base_url=settings.ws_base_url,
                stream_type=s,
                symbols=settings.symbols,
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
    except Exception as e:
        logger.error("Error inesperado. Saliendo del programa")
        logger.error(e)
        sys.exit(1)
