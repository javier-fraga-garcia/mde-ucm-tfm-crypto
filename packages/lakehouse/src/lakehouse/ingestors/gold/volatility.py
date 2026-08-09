from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, window, col, stddev, sum, count

from lakehouse.ingestors.base import Ingestor


class GoldVolatilityIngestor(Ingestor):
    """Ingestor de la capa Gold: volatilidad de precios por ventana de 5 minutos."""

    def transform(self, df: DataFrame) -> DataFrame:
        """Calcula métricas de volatilidad agregadas por símbolo y ventana temporal.
        Args:
            df: DataFrame leído de la tabla Silver agg_trade.
        Returns:
            DataFrame con volatilidad agregada por ventana de 5 minutos.
        """
        aggregated_df = (
            df.withWatermark("trade_timestamp", "10 minutes")
            .groupBy(window(col("trade_timestamp"), "5 minutes"), col("symbol"))
            .agg(
                avg("price").alias("avg_price"),
                stddev("price").alias("stddev_price"),
                sum("quantity").alias("total_volume"),
                count("symbol").alias("trade_count"),
            )
        )

        return aggregated_df.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("symbol"),
            col("avg_price"),
            col("stddev_price"),
            col("total_volume"),
            col("trade_count"),
        )
