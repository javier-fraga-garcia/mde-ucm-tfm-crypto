from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, min, max, window, col

from lakehouse.ingestors.base import Ingestor


class GoldSpreadIngestor(Ingestor):
    """Ingestor de la capa Gold: spread bid-ask por ventana de 5 minutos."""

    def transform(self, df: DataFrame) -> DataFrame:
        """Calcula métricas de spread agregadas por símbolo y ventana temporal.
        Args:
            df: DataFrame leído de la tabla Silver book_ticker.
        Returns:
            DataFrame con el spread agregado por ventana de 5 minutos.
        """

        aggregated_df = (
            df.withColumn("spread", col("best_ask_price") - col("best_bid_price"))
            .withColumn(
                "mid_price", (col("best_bid_price") + col("best_ask_price")) / 2
            )
            .withWatermark("ingestion_timestamp", "10 minutes")
            .groupBy(window(col("ingestion_timestamp"), "5 minutes"), col("symbol"))
            .agg(
                avg("spread").alias("avg_spread"),
                min("spread").alias("min_spread"),
                max("spread").alias("max_spread"),
                avg("mid_price").alias("avg_mid_price"),
            )
        )

        return aggregated_df.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("symbol"),
            col("avg_spread"),
            col("min_spread"),
            col("max_spread"),
            col("avg_mid_price"),
        )
