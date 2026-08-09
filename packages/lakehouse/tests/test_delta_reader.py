"""Tests de DeltaReader."""

from unittest.mock import MagicMock

from lakehouse.readers.delta_reader import DeltaReader


def test_read_configures_delta_source_correctly():
    spark = MagicMock()
    builder = MagicMock()
    result_df = MagicMock()

    spark.readStream.format.return_value = builder
    builder.load.return_value = result_df

    reader = DeltaReader(spark, table_path="s3a://bucket/bronze")
    result = reader.read()

    spark.readStream.format.assert_called_once_with("delta")
    builder.load.assert_called_once_with("s3a://bucket/bronze")
    assert result is result_df
