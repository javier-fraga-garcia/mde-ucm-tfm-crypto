from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, posexplode, lit
from pyspark.sql.types import DecimalType

from lakehouse.ingestors.base import Ingestor
from lakehouse.readers.schemas import DEPTH10_RAW_SCHEMA


class SilverDepth10Ingestor(Ingestor):
    """Ingestor de la capa Silver para el stream depth10."""

    def transform(self, df: DataFrame) -> DataFrame:
        """Filtra, parsea, tipa y aplana los niveles del order book de Bronze.

        Args:
            df: DataFrame leído de la tabla Bronze.

        Returns:
            DataFrame con una fila por nivel de precio (bid/ask).
        """
        filtered_df = df.filter(col("stream_type") == "depth10")

        parsed_df = filtered_df.select(
            col("symbol"),
            col("ingestion_timestamp"),
            from_json(col("raw_payload"), DEPTH10_RAW_SCHEMA).alias("payload"),
        )

        valid_df = parsed_df.filter(col("payload").isNotNull())

        bids_df = valid_df.select(
            col("symbol"),
            col("ingestion_timestamp"),
            col("payload.lastUpdateId").alias("last_update_id"),
            lit("bid").alias("side"),
            posexplode(col("payload.bids")).alias("level", "raw_level"),
        )

        asks_df = valid_df.select(
            col("symbol"),
            col("ingestion_timestamp"),
            col("payload.lastUpdateId").alias("last_update_id"),
            lit("ask").alias("side"),
            posexplode(col("payload.asks")).alias("level", "raw_level"),
        )

        combined_df = bids_df.unionByName(asks_df)

        return combined_df.select(
            col("symbol"),
            col("ingestion_timestamp"),
            col("last_update_id"),
            col("side"),
            col("level"),
            col("raw_level")[0].cast(DecimalType(20, 8)).alias("price"),
            col("raw_level")[1].cast(DecimalType(20, 8)).alias("quantity"),
        )
