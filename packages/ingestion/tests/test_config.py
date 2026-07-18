"""Tests de IngestionSettings, principalmente el parseo del campo symbols."""

from ingestion.config import IngestionSettings


def test_symbols_parsed_from_csv_string():
    settings = IngestionSettings(
        ws_base_url="wss://example.com",
        symbols="BTCUSDT,ETHUSDT,SOLUSDT",
        kafka_bootstrap_servers="localhost:9092",
        kafka_topic="test-topic",
    )

    assert settings.symbols == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


def test_symbols_accepts_list_directly():
    settings = IngestionSettings(
        ws_base_url="wss://example.com",
        symbols=["BTCUSDT", "ETHUSDT"],
        kafka_bootstrap_servers="localhost:9092",
        kafka_topic="test-topic",
    )

    assert settings.symbols == ["BTCUSDT", "ETHUSDT"]


def test_symbols_strips_whitespace():
    settings = IngestionSettings(
        ws_base_url="wss://example.com",
        symbols="BTCUSDT, ETHUSDT , SOLUSDT",
        kafka_bootstrap_servers="localhost:9092",
        kafka_topic="test-topic",
    )

    assert settings.symbols == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
