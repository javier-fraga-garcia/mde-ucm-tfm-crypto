from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, get_json_object
from pyspark.sql.types import DecimalType

from lakehouse.ingestors.base import Ingestor
from lakehouse.readers.schemas import AGG_TRADE_RAW_SCHEMA


class SilverAggTradeIngestor(Ingestor):
    """Ingestor de la capa Silver para el stream aggTrade."""

    def transform(self, df: DataFrame) -> DataFrame:
        """Filtra, parsea y tipa los eventos aggTrade de Bronze.

        Args:
            df: DataFrame leído de la tabla Bronze.

        Returns:
            DataFrame con los campos de aggTrade tipados.
        """
        filtered_df = df.filter(col("stream_type") == "aggTrade")

        parsed_df = filtered_df.select(
            col("symbol"),
            col("ingestion_timestamp"),
            from_json(
                get_json_object(col("raw_payload"), "$.data"), AGG_TRADE_RAW_SCHEMA
            ).alias("payload"),
        )

        valid_df = parsed_df.filter(col("payload").isNotNull())

        return valid_df.select(
            col("symbol"),
            col("ingestion_timestamp"),
            col("payload.e").alias("event_type"),
            (col("payload.E") / 1000).cast("timestamp").alias("event_timestamp"),
            col("payload.a").alias("agg_trade_id"),
            col("payload.p").cast(DecimalType(20, 8)).alias("price"),
            col("payload.q").cast(DecimalType(20, 8)).alias("quantity"),
            col("payload.f").alias("first_trade_id"),
            col("payload.l").alias("last_trade_id"),
            (col("payload.T") / 1000).cast("timestamp").alias("trade_timestamp"),
            col("payload.m").alias("is_buyer_maker"),
            col("payload.M").alias("is_best_match"),
        )
