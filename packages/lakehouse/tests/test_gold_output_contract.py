"""Tests de contrato: verifican que el schema de salida de Gold no varía inesperadamente."""

from datetime import datetime
from decimal import Decimal

from lakehouse.ingestors.gold.volatility import GoldVolatilityIngestor

EXPECTED_COLUMNS = {
    "window_start",
    "window_end",
    "symbol",
    "avg_price",
    "stddev_price",
    "total_volume",
    "trade_count",
}


def test_volatility_output_schema_matches_contract(spark):
    input_df = spark.createDataFrame(
        [
            (
                "BTCUSDT",
                "aggTrade",
                datetime(2026, 1, 1, 10, 0),
                1,
                Decimal("100.0"),
                Decimal("1.0"),
                1,
                1,
                True,
                True,
            ),
        ],
        "symbol STRING, event_type STRING, trade_timestamp TIMESTAMP, agg_trade_id LONG, "
        "price DECIMAL(20,8), quantity DECIMAL(20,8), first_trade_id LONG, last_trade_id LONG, "
        "is_buyer_maker BOOLEAN, is_best_match BOOLEAN",
    )

    ingestor = GoldVolatilityIngestor(reader=None, writer=None)
    result_df = ingestor.transform(input_df)

    assert set(result_df.columns) == EXPECTED_COLUMNS
