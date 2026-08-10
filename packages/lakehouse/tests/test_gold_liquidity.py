"""Tests de GoldLiquidityIngestor."""

from unittest.mock import MagicMock, patch

from lakehouse.ingestors.gold.liquidity import GoldLiquidityIngestor


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


def test_transform_applies_watermark_on_ingestion_timestamp():
    df, *_rest = _make_df_chain()

    with (
        patch("lakehouse.ingestors.gold.liquidity.col", return_value=MagicMock()),
        patch("lakehouse.ingestors.gold.liquidity.window", return_value=MagicMock()),
        patch("lakehouse.ingestors.gold.liquidity.avg", return_value=MagicMock()),
    ):
        ingestor = GoldLiquidityIngestor(reader=MagicMock(), writer=MagicMock())
        ingestor.transform(df)

    df.withWatermark.assert_called_once_with("ingestion_timestamp", "10 minutes")


def test_transform_groups_by_window_symbol_and_side():
    df, watermarked_df, grouped_df, aggregated_df, result_df = _make_df_chain()

    with (
        patch("lakehouse.ingestors.gold.liquidity.col", return_value=MagicMock()),
        patch("lakehouse.ingestors.gold.liquidity.window", return_value=MagicMock()),
        patch("lakehouse.ingestors.gold.liquidity.avg", return_value=MagicMock()),
    ):
        ingestor = GoldLiquidityIngestor(reader=MagicMock(), writer=MagicMock())
        result = ingestor.transform(df)

    watermarked_df.groupBy.assert_called_once()
    grouped_df.agg.assert_called_once()
    assert result is result_df
