import sys
import logging

from shared.logging import configure_logging
from lakehouse.config import LakehouseSettings
from lakehouse.readers import DeltaReader
from lakehouse.writers import DeltaWriter
from lakehouse.ingestors.gold import GoldVolatilityIngestor
from lakehouse.utils import get_or_create_spark_session

settings = LakehouseSettings()

configure_logging("gold_volatility_job", level=settings.log_level)
logger = logging.getLogger(__name__)


def run():
    try:
        logger.info("Iniciando GoldVolatility Job...")
        spark = get_or_create_spark_session(
            "gold_volatility_job",
            settings.s3_endpoint_url,
            settings.s3_access_key,
            settings.s3_secret_key,
        )
        reader = DeltaReader(spark, settings.silver_agg_trade_table_path)
        writer = DeltaWriter(
            spark,
            settings.gold_volatility_table_path,
            settings.gold_volatility_checkpoint_path,
            settings.gold_volatility_trigger_interval,
        )
        ingestor = GoldVolatilityIngestor(reader, writer)
        ingestor.run()
    except KeyboardInterrupt:
        logger.info("Proceso detenido por el usuario. Saliendo...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error desconocido: {e}")
        sys.exit(1)


if __name__ == "__main__":
    run()
