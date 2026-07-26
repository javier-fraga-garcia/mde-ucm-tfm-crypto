"""Tests de KafkaReader, mockeando la cadena de builders de Spark."""

from unittest.mock import MagicMock, patch

from lakehouse.readers.kafka_reader import KafkaReader


def _make_reader():
    spark = MagicMock()
    builder = MagicMock()
    raw_df = MagicMock()
    json_df = MagicMock()
    envelope_df = MagicMock()
    result_df = MagicMock()

    spark.readStream.format.return_value = builder
    builder.option.return_value = builder
    builder.load.return_value = raw_df
    raw_df.select.return_value = json_df
    json_df.select.return_value = envelope_df
    envelope_df.select.return_value = result_df

    reader = KafkaReader(spark, bootstrap_servers="fake:9092", topic="fake-topic")
    return spark, builder, raw_df, json_df, envelope_df, result_df, reader


def test_read_configures_kafka_source_with_expected_options():
    spark, builder, *_rest, reader = _make_reader()

    with (
        patch("lakehouse.readers.kafka_reader.col", return_value=MagicMock()),
        patch("lakehouse.readers.kafka_reader.from_json", return_value=MagicMock()),
    ):
        reader.read()

    spark.readStream.format.assert_called_once_with("kafka")
    builder.option.assert_any_call("kafka.bootstrap.servers", "fake:9092")
    builder.option.assert_any_call("subscribe", "fake-topic")
    builder.option.assert_any_call("startingOffsets", "earliest")
    builder.load.assert_called_once()


def test_read_chains_the_expected_selects_and_returns_result():
    _spark, _builder, raw_df, json_df, envelope_df, result_df, reader = _make_reader()

    with (
        patch("lakehouse.readers.kafka_reader.col", return_value=MagicMock()),
        patch("lakehouse.readers.kafka_reader.from_json", return_value=MagicMock()),
    ):
        result = reader.read()

    raw_df.select.assert_called_once()
    json_df.select.assert_called_once()
    envelope_df.select.assert_called_once()
    assert result is result_df
