from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, window, col

from lakehouse.ingestors.base import Ingestor


class GoldLiquidityIngestor(Ingestor):
    """Ingestor de la capa Gold: liquidez media por nivel de precio y ventana de 5 minutos."""

    def transform(self, df: DataFrame) -> DataFrame:
        """Calcula la cantidad media disponible por nivel de precio, agregada por
        símbolo, lado del book (bid/ask) y ventana temporal.
        Args:
            df: DataFrame leído de la tabla Silver depth10.
        Returns:
            DataFrame con la liquidez media agregada por ventana de 5 minutos.
        """
        aggregated_df = (
            df.withWatermark("ingestion_timestamp", "10 minutes")
            .groupBy(
                window(col("ingestion_timestamp"), "5 minutes"),
                col("symbol"),
                col("side"),
            )
            .agg(
                avg("quantity").alias("avg_depth_quantity"),
            )
        )

        return aggregated_df.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("symbol"),
            col("side"),
            col("avg_depth_quantity"),
        )
