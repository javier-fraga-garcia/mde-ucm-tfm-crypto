import sys
import logging

from shared.logging import configure_logging
from lakehouse.config import LakehouseSettings
from lakehouse.readers import DeltaReader
from lakehouse.writers import DeltaWriter
from lakehouse.ingestors.silver import (
    SilverDepth10Ingestor,
    SilverBookTickerIngestor,
    SilverAggTradeIngestor,
)
from lakehouse.utils import get_or_create_spark_session

settings = LakehouseSettings()

configure_logging("silver_job", level=settings.log_level)
logger = logging.getLogger(__name__)


def run():
    try:
        pass
    except KeyboardInterrupt:
        logger.info("Proceso detenido por el usuario. Saliendo...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error desconocido: {e}")
        sys.exit(1)


if __name__ == "__main__":
    run()
