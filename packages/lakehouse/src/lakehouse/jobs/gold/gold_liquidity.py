import sys
import logging

from shared.logging import configure_logging
from lakehouse.config import LakehouseSettings
from lakehouse.readers import DeltaReader
from lakehouse.writers import DeltaWriter
from lakehouse.ingestors.gold import GoldLiquidityIngestor
from lakehouse.utils import get_or_create_spark_session

settings = LakehouseSettings()

configure_logging("gold_liquidity_job", level=settings.log_level)
logger = logging.getLogger(__name__)


def run():
    try:
        logger.info("Iniciando GoldLiquidity Job...")
        spark = get_or_create_spark_session(
            "gold_liquidity_job",
            settings.s3_endpoint_url,
            settings.s3_access_key,
            settings.s3_secret_key,
        )
        reader = DeltaReader(spark, settings.silver_depth10_table_path)
        writer = DeltaWriter(
            spark,
            settings.gold_liquidity_table_path,
            settings.gold_liquidity_checkpoint_path,
            settings.gold_liquidity_trigger_interval,
        )
        ingestor = GoldLiquidityIngestor(reader, writer)
        ingestor.run()
    except KeyboardInterrupt:
        logger.info("Proceso detenido por el usuario. Saliendo...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error desconocido: {e}")
        sys.exit(1)


if __name__ == "__main__":
    run()
