"""Tests de GoldVolatilityIngestor."""

from unittest.mock import MagicMock, patch

from lakehouse.ingestors.gold.volatility import GoldVolatilityIngestor


def _make_df_chain():
    df = MagicMock()
    watermarked_df = MagicMock()
    grouped_df = MagicMock()
    aggregated_df = MagicMock()
    result_df = MagicMock()

    df.withWatermark.return_value = watermarked_df
    watermarked_df.groupBy.return_value = grouped_df
    grouped_df.agg.return_value = aggregated_df
    aggregated_df.select.return_value = result_df

    return df, watermarked_df, grouped_df, aggregated_df, result_df


def test_transform_applies_watermark_on_trade_timestamp():
    df, *_rest = _make_df_chain()

    with (
        patch("lakehouse.ingestors.gold.volatility.col", return_value=MagicMock()),
        patch("lakehouse.ingestors.gold.volatility.window", return_value=MagicMock()),
        patch("lakehouse.ingestors.gold.volatility.avg", return_value=MagicMock()),
        patch("lakehouse.ingestors.gold.volatility.stddev", return_value=MagicMock()),
        patch("lakehouse.ingestors.gold.volatility.sum", return_value=MagicMock()),
        patch("lakehouse.ingestors.gold.volatility.count", return_value=MagicMock()),
    ):
        ingestor = GoldVolatilityIngestor(reader=MagicMock(), writer=MagicMock())
        ingestor.transform(df)

    df.withWatermark.assert_called_once_with("trade_timestamp", "10 minutes")


def test_transform_groups_by_window_and_symbol():
    df, watermarked_df, grouped_df, aggregated_df, result_df = _make_df_chain()

    with (
        patch("lakehouse.ingestors.gold.volatility.col", return_value=MagicMock()),
        patch("lakehouse.ingestors.gold.volatility.window", return_value=MagicMock()),
        patch("lakehouse.ingestors.gold.volatility.avg", return_value=MagicMock()),
        patch("lakehouse.ingestors.gold.volatility.stddev", return_value=MagicMock()),
        patch("lakehouse.ingestors.gold.volatility.sum", return_value=MagicMock()),
        patch("lakehouse.ingestors.gold.volatility.count", return_value=MagicMock()),
    ):
        ingestor = GoldVolatilityIngestor(reader=MagicMock(), writer=MagicMock())
        result = ingestor.transform(df)

    watermarked_df.groupBy.assert_called_once()
    grouped_df.agg.assert_called_once()
    aggregated_df.select.assert_called_once()
    assert result is result_df
