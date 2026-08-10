"""Tests de contrato: verifican que los StructType parsean payloads reales de Binance."""

import json
from pyspark.sql.functions import from_json, get_json_object

from lakehouse.readers.schemas import (
    AGG_TRADE_RAW_SCHEMA,
    BOOK_TICKER_RAW_SCHEMA,
    DEPTH10_RAW_SCHEMA,
)

AGG_TRADE_SAMPLE = json.dumps(
    {
        "stream": "solusdt@aggTrade",
        "data": {
            "e": "aggTrade",
            "E": 1785061176911,
            "s": "SOLUSDT",
            "a": 664665614,
            "p": "75.14000000",
            "q": "0.88600000",
            "f": 2013737931,
            "l": 2013737931,
            "T": 1785061176911,
            "m": False,
            "M": True,
        },
    }
)

BOOK_TICKER_SAMPLE = json.dumps(
    {
        "stream": "solusdt@bookTicker",
        "data": {
            "u": 29644019475,
            "s": "SOLUSDT",
            "b": "75.13000000",
            "B": "263.75600000",
            "a": "75.14000000",
            "A": "587.56600000",
        },
    }
)

DEPTH10_SAMPLE = json.dumps(
    {
        "stream": "solusdt@depth10",
        "data": {
            "lastUpdateId": 29644019473,
            "bids": [["75.13000000", "269.75600000"], ["75.12000000", "587.12600000"]],
            "asks": [["75.14000000", "587.56600000"], ["75.15000000", "620.98300000"]],
        },
    }
)


def test_agg_trade_schema_parses_real_payload_without_loss(spark):
    df = spark.createDataFrame([(AGG_TRADE_SAMPLE,)], "raw_payload STRING")

    parsed = df.select(
        from_json(get_json_object("raw_payload", "$.data"), AGG_TRADE_RAW_SCHEMA).alias(
            "payload"
        )
    )
    row = parsed.select("payload.*").collect()[0]

    assert row["e"] == "aggTrade"
    assert row["s"] == "SOLUSDT"
    assert row["p"] == "75.14000000"
    assert row["q"] == "0.88600000"
    assert row["m"] is False
    assert row["M"] is True


def test_book_ticker_schema_parses_real_payload_without_loss(spark):
    df = spark.createDataFrame([(BOOK_TICKER_SAMPLE,)], "raw_payload STRING")

    parsed = df.select(
        from_json(
            get_json_object("raw_payload", "$.data"), BOOK_TICKER_RAW_SCHEMA
        ).alias("payload")
    )
    row = parsed.select("payload.*").collect()[0]

    assert row["s"] == "SOLUSDT"
    assert row["b"] == "75.13000000"
    assert row["B"] == "263.75600000"
    assert row["a"] == "75.14000000"
    assert row["A"] == "587.56600000"


def test_depth10_schema_parses_real_payload_without_loss(spark):
    df = spark.createDataFrame([(DEPTH10_SAMPLE,)], "raw_payload STRING")

    parsed = df.select(
        from_json(get_json_object("raw_payload", "$.data"), DEPTH10_RAW_SCHEMA).alias(
            "payload"
        )
    )
    row = parsed.select("payload.*").collect()[0]

    assert row["lastUpdateId"] == 29644019473
    assert row["bids"][0] == ["75.13000000", "269.75600000"]
    assert row["asks"][1] == ["75.15000000", "620.98300000"]


def test_agg_trade_schema_rejects_envelope_without_data_extraction(spark):
    """Confirma el bug real detectado: parsear raw_payload directo (sin
    extraer 'data' primero) produce todos los campos nulos."""
    df = spark.createDataFrame([(AGG_TRADE_SAMPLE,)], "raw_payload STRING")

    parsed = df.select(from_json("raw_payload", AGG_TRADE_RAW_SCHEMA).alias("payload"))
    row = parsed.select("payload.*").collect()[0]

    assert row["e"] is None
    assert row["p"] is None
