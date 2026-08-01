from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import DecimalType

from lakehouse.ingestors.base import Ingestor
from lakehouse.readers.schemas import BOOK_TICKER_RAW_SCHEMA


class SilverBookTickerIngestor(Ingestor):
    """Ingestor de la capa Silver para el stream bookTicker."""

    def transform(self, df: DataFrame) -> DataFrame:
        """Filtra, parsea y tipa los eventos bookTicker de Bronze.

        Args:
            df: DataFrame leído de la tabla Bronze.

        Returns:
            DataFrame con los campos de bookTicker tipados.
        """
        filtered_df = df.filter(col("stream_type") == "bookTicker")

        parsed_df = filtered_df.select(
            col("symbol"),
            col("ingestion_timestamp"),
            from_json(col("raw_payload"), BOOK_TICKER_RAW_SCHEMA).alias("payload"),
        )

        return parsed_df.select(
            col("symbol"),
            col("ingestion_timestamp"),
            col("payload.u").alias("update_id"),
            col("payload.b").cast(DecimalType(20, 8)).alias("best_bid_price"),
            col("payload.B").cast(DecimalType(20, 8)).alias("best_bid_quantity"),
            col("payload.a").cast(DecimalType(20, 8)).alias("best_ask_price"),
            col("payload.A").cast(DecimalType(20, 8)).alias("best_ask_quantity"),
        )
