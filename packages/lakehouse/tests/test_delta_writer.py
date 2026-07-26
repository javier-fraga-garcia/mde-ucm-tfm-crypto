"""Tests de DeltaWriter, mockeando la cadena de builders de Spark."""

from unittest.mock import MagicMock

from lakehouse.writers.delta_writer import DeltaWriter


def _make_writer(partition_by=None):
    spark = MagicMock()
    return DeltaWriter(
        spark,
        table_path="s3a://bucket/table",
        checkpoint_path="s3a://bucket/checkpoint",
        trigger_interval="10 seconds",
        partition_by=partition_by,
    )


def _mock_df_with_builder():
    df = MagicMock()
    builder = MagicMock()
    query = MagicMock()

    df.writeStream.format.return_value = builder
    builder.outputMode.return_value = builder
    builder.option.return_value = builder
    builder.trigger.return_value = builder
    builder.partitionBy.return_value = builder
    builder.start.return_value = query

    return df, builder, query


def test_write_configures_streaming_writer_correctly():
    writer = _make_writer()
    df, builder, query = _mock_df_with_builder()

    writer.write(df)

    df.writeStream.format.assert_called_once_with("delta")
    builder.outputMode.assert_called_once_with("append")
    builder.option.assert_called_once_with("checkpointLocation", "s3a://bucket/checkpoint")
    builder.trigger.assert_called_once_with(processingTime="10 seconds")
    builder.start.assert_called_once_with(path="s3a://bucket/table")
    query.awaitTermination.assert_called_once()


def test_write_applies_partition_by_when_set():
    writer = _make_writer(partition_by="symbol")
    df, builder, _query = _mock_df_with_builder()

    writer.write(df)

    builder.partitionBy.assert_called_once_with("symbol")


def test_write_skips_partition_by_when_none():
    writer = _make_writer(partition_by=None)
    df, builder, _query = _mock_df_with_builder()

    writer.write(df)

    builder.partitionBy.assert_not_called()