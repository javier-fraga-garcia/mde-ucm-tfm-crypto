import sys
import asyncio
import logging
from ingestion.ws_client import BinanceStreamConnector
from shared.logging import configure_logging
from shared.schemas import StreamType

configure_logging('ingestion')
logger = logging.getLogger(__name__)

if __name__ == '__main__':
    try:
        connector = BinanceStreamConnector(base_url='wss://stream.binance.com:9443/stream', stream_type=StreamType.AGG_TRADE, symbols=['BTCUSDT'])
        asyncio.run(connector.run())
    except KeyboardInterrupt:
        logger.info('El usuario interrumpio la ejecución...\nSaliendo del proceso...')
        sys.exit(0)