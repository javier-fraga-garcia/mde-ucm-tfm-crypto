"""Tests de KafkaEnvelope y StreamType."""

import time

import pytest
from pydantic import ValidationError

from shared.schemas import KafkaEnvelope, StreamType


def test_envelope_builds_with_valid_fields():
    envelope = KafkaEnvelope(
        symbol="BTCUSDT",
        stream_type=StreamType.AGG_TRADE,
        raw_payload='{"fake": "data"}',
    )

    assert envelope.symbol == "BTCUSDT"
    assert envelope.stream_type == StreamType.AGG_TRADE
    assert envelope.raw_payload == '{"fake": "data"}'


def test_envelope_rejects_invalid_stream_type():
    with pytest.raises(ValidationError):
        KafkaEnvelope(
            symbol="BTCUSDT",
            stream_type="not-a-valid-stream-type",
            raw_payload="{}",
        )


def test_ingestion_timestamp_default_factory_generates_new_value_each_time():
    envelope_1 = KafkaEnvelope(
        symbol="BTCUSDT",
        stream_type=StreamType.AGG_TRADE,
        raw_payload="{}",
    )
    time.sleep(0.01)
    envelope_2 = KafkaEnvelope(
        symbol="BTCUSDT",
        stream_type=StreamType.AGG_TRADE,
        raw_payload="{}",
    )

    assert envelope_1.ingestion_timestamp != envelope_2.ingestion_timestamp
