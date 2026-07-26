"""Tests de BronzeIngestor: passthrough de transform y orquestación de run()."""

from unittest.mock import MagicMock

from lakehouse.ingestors.bronze import BronzeIngestor


def test_transform_returns_input_unchanged():
    ingestor = BronzeIngestor(reader=MagicMock(), writer=MagicMock())
    sentinel_df = object()

    result = ingestor.transform(sentinel_df)

    assert result is sentinel_df


def test_run_orchestrates_read_transform_write():
    reader = MagicMock()
    writer = MagicMock()
    df = MagicMock()
    reader.read.return_value = df

    ingestor = BronzeIngestor(reader=reader, writer=writer)
    ingestor.run()

    reader.read.assert_called_once()
    writer.write.assert_called_once_with(df)
