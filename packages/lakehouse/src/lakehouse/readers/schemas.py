from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType,
    BooleanType,
    ArrayType,
)

KAFKA_ENVELOPE_SPARK_SCHEMA = StructType(
    [
        StructField("ingestion_timestamp", TimestampType(), nullable=False),
        StructField("symbol", StringType(), nullable=False),
        StructField("stream_type", StringType(), nullable=False),
        StructField("raw_payload", StringType(), nullable=False),
    ]
)

AGG_TRADE_RAW_SCHEMA = StructType(
    [
        StructField("e", StringType(), nullable=False),
        StructField("E", LongType(), nullable=False),
        StructField("s", StringType(), nullable=False),
        StructField("a", LongType(), nullable=False),
        StructField("p", StringType(), nullable=False),
        StructField("q", StringType(), nullable=False),
        StructField("f", LongType(), nullable=False),
        StructField("l", LongType(), nullable=False),
        StructField("T", LongType(), nullable=False),
        StructField("m", BooleanType(), nullable=False),
        StructField("M", BooleanType(), nullable=False),
    ]
)

BOOK_TICKER_RAW_SCHEMA = StructType(
    [
        StructField("u", LongType(), nullable=False),
        StructField("s", StringType(), nullable=False),
        StructField("b", StringType(), nullable=False),
        StructField("B", StringType(), nullable=False),
        StructField("a", StringType(), nullable=False),
        StructField("A", StringType(), nullable=False),
    ]
)

DEPTH10_LEVEL_SCHEMA = ArrayType(StringType())

DEPTH10_RAW_SCHEMA = StructType(
    [
        StructField("lastUpdateId", LongType(), nullable=False),
        StructField("bids", ArrayType(DEPTH10_LEVEL_SCHEMA), nullable=False),
        StructField("asks", ArrayType(DEPTH10_LEVEL_SCHEMA), nullable=False),
    ]
)
