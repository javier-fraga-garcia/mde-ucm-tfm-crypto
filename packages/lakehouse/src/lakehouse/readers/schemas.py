from pyspark.sql.types import StructType, StructField, StringType, TimestampType

KAFKA_ENVELOPE_SPARK_SCHEMA = StructType(
    [
        StructField("ingestion_timestamp", TimestampType(), nullable=False),
        StructField("symbol", StringType(), nullable=False),
        StructField("stream_type", StringType(), nullable=False),
        StructField("raw_payload", StringType(), nullable=False),
    ]
)
