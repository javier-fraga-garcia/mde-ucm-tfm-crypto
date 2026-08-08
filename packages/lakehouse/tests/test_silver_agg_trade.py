"""Tests de SilverAggTradeIngestor."""

from unittest.mock import MagicMock, patch

from lakehouse.ingestors.silver.agg_trade import SilverAggTradeIngestor


def _make_df_chain():
    df = MagicMock()
    filtered_df = MagicMock()
    parsed_df = MagicMock()
    valid_df = MagicMock()
    result_df = MagicMock()

    df.filter.return_value = filtered_df
    filtered_df.select.return_value = parsed_df
    parsed_df.filter.return_value = valid_df
    valid_df.select.return_value = result_df

    return df, filtered_df, parsed_df, valid_df, result_df


def test_transform_filters_by_stream_type():
    df, filtered_df, *_rest = _make_df_chain()

    with (
        patch("lakehouse.ingestors.silver.agg_trade.col", return_value=MagicMock()),
        patch("lakehouse.ingestors.silver.agg_trade.from_json", return_value=MagicMock()),
        patch("lakehouse.ingestors.silver.agg_trade.get_json_object", return_value=MagicMock()),
    ):
        ingestor = SilverAggTradeIngestor(reader=MagicMock(), writer=MagicMock())
        ingestor.transform(df)

    df.filter.assert_called_once()


def test_transform_extracts_data_field_before_parsing():
    df, *_rest = _make_df_chain()

    with (
        patch("lakehouse.ingestors.silver.agg_trade.col", return_value=MagicMock()),
        patch("lakehouse.ingestors.silver.agg_trade.from_json", return_value=MagicMock()) as mock_from_json,
        patch("lakehouse.ingestors.silver.agg_trade.get_json_object", return_value=MagicMock()) as mock_get_json_object,
    ):
        ingestor = SilverAggTradeIngestor(reader=MagicMock(), writer=MagicMock())
        ingestor.transform(df)

    mock_get_json_object.assert_called_once()
    args, _kwargs = mock_get_json_object.call_args
    assert args[1] == "$.data"
    mock_from_json.assert_called_once()


def test_transform_discards_rows_with_null_payload():
    df, filtered_df, parsed_df, valid_df, result_df = _make_df_chain()

    with (
        patch("lakehouse.ingestors.silver.agg_trade.col", return_value=MagicMock()),
        patch("lakehouse.ingestors.silver.agg_trade.from_json", return_value=MagicMock()),
        patch("lakehouse.ingestors.silver.agg_trade.get_json_object", return_value=MagicMock()),
    ):
        ingestor = SilverAggTradeIngestor(reader=MagicMock(), writer=MagicMock())
        result = ingestor.transform(df)

    parsed_df.filter.assert_called_once()
    valid_df.select.assert_called_once()
    assert result is result_df