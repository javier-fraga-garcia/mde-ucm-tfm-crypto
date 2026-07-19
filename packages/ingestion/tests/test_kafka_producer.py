"""Tests de KafkaProducer, usando mock de AIOKafkaProducer"""

from unittest.mock import AsyncMock, patch

from ingestion.producers import KafkaProducer
from shared.schemas import KafkaEnvelope, StreamType


def _make_envelope(symbol: str = "BTCUSDT") -> KafkaEnvelope:
    return KafkaEnvelope(
        symbol=symbol,
        stream_type=StreamType.AGG_TRADE,
        raw_payload='{"fake": "data"}',
    )


async def test_connect_creates_and_starts_producer():
    with patch(
        "ingestion.producers.kafka_producer.AIOKafkaProducer"
    ) as mock_producer_class:
        mock_instance = AsyncMock()
        mock_producer_class.return_value = mock_instance

        producer = KafkaProducer(bootstrap_servers="fake:9092", topic="test-topic")
        await producer.connect()

        mock_producer_class.assert_called_once_with(bootstrap_servers="fake:9092")
        mock_instance.start.assert_awaited_once()


async def test_close_stops_producer():
    with patch(
        "ingestion.producers.kafka_producer.AIOKafkaProducer"
    ) as mock_producer_class:
        mock_instance = AsyncMock()
        mock_producer_class.return_value = mock_instance

        producer = KafkaProducer(bootstrap_servers="fake:9092", topic="test-topic")
        await producer.connect()
        await producer.close()

        mock_instance.stop.assert_awaited_once()


async def test_publish_sends_correct_topic_and_value():
    with patch(
        "ingestion.producers.kafka_producer.AIOKafkaProducer"
    ) as mock_producer_class:
        mock_instance = AsyncMock()
        mock_producer_class.return_value = mock_instance

        producer = KafkaProducer(bootstrap_servers="fake:9092", topic="test-topic")
        await producer.connect()

        envelope = _make_envelope()
        await producer.publish(envelope)

        mock_instance.send_and_wait.assert_awaited_once_with(
            topic="test-topic",
            key=b"BTCUSDT",
            value=envelope.model_dump_json().encode("utf-8"),
        )


async def test_publish_uses_symbol_as_key():
    with patch(
        "ingestion.producers.kafka_producer.AIOKafkaProducer"
    ) as mock_producer_class:
        mock_instance = AsyncMock()
        mock_producer_class.return_value = mock_instance

        producer = KafkaProducer(bootstrap_servers="fake:9092", topic="test-topic")
        await producer.connect()

        envelope = _make_envelope(symbol="ETHUSDT")
        await producer.publish(envelope)

        _, kwargs = mock_instance.send_and_wait.call_args
        assert kwargs["key"] == b"ETHUSDT"
