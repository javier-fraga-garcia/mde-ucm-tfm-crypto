"""Tests de SilverDepth10Ingestor."""

from unittest.mock import MagicMock, patch

from lakehouse.ingestors.silver.depth10 import SilverDepth10Ingestor


def _make_df_chain():
    df = MagicMock()
    filtered_df = MagicMock()
    parsed_df = MagicMock()
    valid_df = MagicMock()
    bids_df = MagicMock()
    asks_df = MagicMock()
    combined_df = MagicMock()
    result_df = MagicMock()

    df.filter.return_value = filtered_df
    filtered_df.select.return_value = parsed_df
    parsed_df.filter.return_value = valid_df
    valid_df.select.side_effect = [bids_df, asks_df]
    bids_df.unionByName.return_value = combined_df
    combined_df.select.return_value = result_df

    return (
        df,
        filtered_df,
        parsed_df,
        valid_df,
        bids_df,
        asks_df,
        combined_df,
        result_df,
    )


def test_transform_filters_by_stream_type():
    df, filtered_df, *_rest = _make_df_chain()

    with (
        patch("lakehouse.ingestors.silver.depth10.col", return_value=MagicMock()),
        patch("lakehouse.ingestors.silver.depth10.from_json", return_value=MagicMock()),
        patch(
            "lakehouse.ingestors.silver.depth10.get_json_object",
            return_value=MagicMock(),
        ),
        patch("lakehouse.ingestors.silver.depth10.lit", return_value=MagicMock()),
        patch(
            "lakehouse.ingestors.silver.depth10.posexplode", return_value=MagicMock()
        ),
    ):
        ingestor = SilverDepth10Ingestor(reader=MagicMock(), writer=MagicMock())
        ingestor.transform(df)

    df.filter.assert_called_once()


def test_transform_unions_bids_and_asks():
    df, filtered_df, parsed_df, valid_df, bids_df, asks_df, combined_df, result_df = (
        _make_df_chain()
    )

    with (
        patch("lakehouse.ingestors.silver.depth10.col", return_value=MagicMock()),
        patch("lakehouse.ingestors.silver.depth10.from_json", return_value=MagicMock()),
        patch(
            "lakehouse.ingestors.silver.depth10.get_json_object",
            return_value=MagicMock(),
        ),
        patch(
            "lakehouse.ingestors.silver.depth10.lit", return_value=MagicMock()
        ) as mock_lit,
        patch(
            "lakehouse.ingestors.silver.depth10.posexplode", return_value=MagicMock()
        ),
    ):
        ingestor = SilverDepth10Ingestor(reader=MagicMock(), writer=MagicMock())
        result = ingestor.transform(df)

    assert valid_df.select.call_count == 2
    bids_df.unionByName.assert_called_once_with(asks_df)
    combined_df.select.assert_called_once()
    assert result is result_df

    lit_calls = [call.args[0] for call in mock_lit.call_args_list]
    assert "bid" in lit_calls
    assert "ask" in lit_calls
