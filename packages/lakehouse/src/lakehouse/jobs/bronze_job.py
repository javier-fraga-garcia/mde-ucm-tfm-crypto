import sys
import logging

from lakehouse.config import LakehouseSettings
from shared.logging import configure_logging
from lakehouse.readers import KafkaReader
from lakehouse.writers import DeltaWriter
from lakehouse.ingestors import BronzeIngestor
from lakehouse.utils import get_or_create_spark_session

settings = LakehouseSettings()

configure_logging("bronze_job", level=settings.log_level)
logger = logging.getLogger(__name__)


def run():
    try:
        logger.info("Iniciando BronzeJob...")

        spark = get_or_create_spark_session(
            "bronze_job",
            s3_endpoint_url=settings.s3_endpoint_url,
            s3_access_key=settings.s3_access_key,
            s3_secret_key=settings.s3_secret_key,
        )
        reader = KafkaReader(
            spark, settings.kafka_bootstrap_servers, settings.kafka_topic
        )
        writer = DeltaWriter(
            spark,
            settings.bronze_table_path,
            settings.bronze_checkpoint_path,
            settings.bronze_trigger_interval,
        )

        ingestor = BronzeIngestor(reader=reader, writer=writer)
        ingestor.run()
    except KeyboardInterrupt:
        logger.info("Proceso detenido por el usuario. Saliendo...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error desconocido: {e}")
        sys.exit(1)


if __name__ == "__main__":
    run()
