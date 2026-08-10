"""Tests de GoldSpreadIngestor."""

from unittest.mock import MagicMock, patch

from lakehouse.ingestors.gold.spread import GoldSpreadIngestor


def _make_df_chain():
    df = MagicMock()
    with_spread_df = MagicMock()
    with_mid_price_df = MagicMock()
    watermarked_df = MagicMock()
    grouped_df = MagicMock()
    aggregated_df = MagicMock()
    result_df = MagicMock()

    df.withColumn.return_value = with_spread_df
    with_spread_df.withColumn.return_value = with_mid_price_df
    with_mid_price_df.withWatermark.return_value = watermarked_df
    watermarked_df.groupBy.return_value = grouped_df
    grouped_df.agg.return_value = aggregated_df
    aggregated_df.select.return_value = result_df

    return (
        df,
        with_spread_df,
        with_mid_price_df,
        watermarked_df,
        grouped_df,
        aggregated_df,
        result_df,
    )


def test_transform_derives_spread_and_mid_price_columns():
    df, with_spread_df, *_rest = _make_df_chain()

    with (
        patch("lakehouse.ingestors.gold.spread.col", return_value=MagicMock()),
        patch("lakehouse.ingestors.gold.spread.window", return_value=MagicMock()),
        patch("lakehouse.ingestors.gold.spread.avg", return_value=MagicMock()),
        patch("lakehouse.ingestors.gold.spread.min", return_value=MagicMock()),
        patch("lakehouse.ingestors.gold.spread.max", return_value=MagicMock()),
    ):
        ingestor = GoldSpreadIngestor(reader=MagicMock(), writer=MagicMock())
        ingestor.transform(df)

    df.withColumn.assert_called_once()
    with_spread_df.withColumn.assert_called_once()

    spread_call_args = df.withColumn.call_args.args
    mid_price_call_args = with_spread_df.withColumn.call_args.args
    assert spread_call_args[0] == "spread"
    assert mid_price_call_args[0] == "mid_price"


def test_transform_groups_by_window_and_symbol():
    (
        df,
        with_spread_df,
        with_mid_price_df,
        watermarked_df,
        grouped_df,
        aggregated_df,
        result_df,
    ) = _make_df_chain()

    with (
        patch("lakehouse.ingestors.gold.spread.col", return_value=MagicMock()),
        patch("lakehouse.ingestors.gold.spread.window", return_value=MagicMock()),
        patch("lakehouse.ingestors.gold.spread.avg", return_value=MagicMock()),
        patch("lakehouse.ingestors.gold.spread.min", return_value=MagicMock()),
        patch("lakehouse.ingestors.gold.spread.max", return_value=MagicMock()),
    ):
        ingestor = GoldSpreadIngestor(reader=MagicMock(), writer=MagicMock())
        result = ingestor.transform(df)

    with_mid_price_df.withWatermark.assert_called_once_with(
        "ingestion_timestamp", "10 minutes"
    )
    grouped_df.agg.assert_called_once()
    assert result is result_df
