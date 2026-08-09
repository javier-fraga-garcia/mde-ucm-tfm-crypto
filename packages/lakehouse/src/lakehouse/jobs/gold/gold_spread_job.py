import sys
import logging

from shared.logging import configure_logging
from lakehouse.config import LakehouseSettings
from lakehouse.readers import DeltaReader
from lakehouse.writers import DeltaWriter
from lakehouse.ingestors.gold import GoldSpreadIngestor
from lakehouse.utils import get_or_create_spark_session

settings = LakehouseSettings()

configure_logging("gold_spread_job", level=settings.log_level)
logger = logging.getLogger(__name__)


def run():
    try:
        logger.info("Iniciando GoldSpread Job...")
        spark = get_or_create_spark_session(
            "gold_spread_job",
            settings.s3_endpoint_url,
            settings.s3_access_key,
            settings.s3_secret_key,
        )
        reader = DeltaReader(spark, settings.silver_book_ticker_table_path)
        writer = DeltaWriter(
            spark,
            settings.gold_spread_table_path,
            settings.gold_spread_checkpoint_path,
            settings.gold_spread_trigger_interval,
        )
        ingestor = GoldSpreadIngestor(reader, writer)
        ingestor.run()
    except KeyboardInterrupt:
        logger.info("Proceso detenido por el usuario. Saliendo...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error desconocido: {e}")
        sys.exit(1)


if __name__ == "__main__":
    run()
