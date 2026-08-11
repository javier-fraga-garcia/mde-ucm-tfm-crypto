import sys
import time
import logging

from shared.logging import configure_logging
from serving.config import ServingSettings
from serving.sync import get_connection, sync_table

settings = ServingSettings()

configure_logging("serving_main", level=settings.log_level)
logger = logging.getLogger(__name__)


def run_sync_loop(settings: ServingSettings) -> None:
    con = get_connection(settings)
    tables = [
        (
            settings.gold_volatility_table_path,
            "gold_volatility",
            ["symbol", "window_start"],
        ),
        (settings.gold_spread_table_path, "gold_spread", ["symbol", "window_start"]),
        (
            settings.gold_liquidity_table_path,
            "gold_liquidity",
            ["symbol", "side", "window_start"],
        ),
    ]
    while True:
        for delta_path, pg_table, key_columns in tables:
            try:
                synced = sync_table(con, delta_path, pg_table, key_columns)
                if synced:
                    logger.info(f"Sincronizada tabla {pg_table}")
            except Exception as e:
                logger.error(f"Error sincronizando {pg_table}: {e}")
        time.sleep(settings.sync_interval_seconds)


if __name__ == "__main__":
    try:
        run_sync_loop(settings)
    except KeyboardInterrupt:
        logger.info("Proceso detenido por el usuario. Saliendo...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error desconocido: {e}")
        sys.exit(1)
