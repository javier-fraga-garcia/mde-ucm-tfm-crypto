"""Tests de BinanceStreamConnector."""

import json
import pytest
import websockets
from unittest.mock import AsyncMock, MagicMock, patch

from ingestion.ws_client import BinanceStreamConnector
from shared.schemas import StreamType


def _make_connector(producer=None) -> BinanceStreamConnector:
    return BinanceStreamConnector(
        base_url="wss://fake.example.com/stream",
        stream_type=StreamType.AGG_TRADE,
        symbols=["BTCUSDT", "ETHUSDT"],
        producer=producer or AsyncMock(),
    )


def test_build_tickers_formats_symbols_correctly():
    connector = _make_connector()

    tickers = connector._build_tickers()

    assert tickers == ["btcusdt@aggTrade", "ethusdt@aggTrade"]


async def test_run_publishes_valid_message():
    producer = AsyncMock()
    connector = _make_connector(producer=producer)

    fake_message = json.dumps(
        {
            "stream": "btcusdt@aggTrade",
            "data": {"e": "aggTrade", "s": "BTCUSDT", "p": "63000.00"},
        }
    )
    confirmation = json.dumps({"result": None, "id": 1})

    mock_ws = MagicMock()
    mock_ws.__aenter__.return_value = mock_ws
    mock_ws.__aexit__.return_value = None
    mock_ws.send = AsyncMock()
    mock_ws.recv = AsyncMock(return_value=confirmation)
    mock_ws.__aiter__.return_value = iter([fake_message])

    with patch("ingestion.ws_client.websockets.connect", return_value=mock_ws):
        await connector.run()

    producer.publish.assert_awaited_once()
    published_envelope = producer.publish.call_args.args[0]
    assert published_envelope.symbol == "BTCUSDT"
    assert published_envelope.stream_type == StreamType.AGG_TRADE
    assert published_envelope.raw_payload == fake_message


async def test_run_skips_malformed_message_without_publishing():
    producer = AsyncMock()
    connector = _make_connector(producer=producer)

    malformed_message = "not-valid-json"
    confirmation = json.dumps({"result": None, "id": 1})

    mock_ws = MagicMock()
    mock_ws.__aenter__.return_value = mock_ws
    mock_ws.__aexit__.return_value = None
    mock_ws.send = AsyncMock()
    mock_ws.recv = AsyncMock(return_value=confirmation)
    mock_ws.__aiter__.return_value = iter([malformed_message])

    with patch("ingestion.ws_client.websockets.connect", return_value=mock_ws):
        await connector.run()

    producer.publish.assert_not_awaited()


async def test_run_reraises_after_exhausting_retries():
    producer = AsyncMock()
    connector = _make_connector(producer=producer)

    mock_ws = MagicMock()
    mock_ws.__aenter__.return_value = mock_ws
    mock_ws.__aexit__.return_value = None
    mock_ws.send = AsyncMock()
    mock_ws.recv = AsyncMock(return_value='{"result": null, "id": 1}')
    mock_ws.__aiter__.side_effect = websockets.ConnectionClosed(None, None)

    with (
        patch("ingestion.ws_client.websockets.connect", return_value=mock_ws),
        patch("asyncio.sleep", new_callable=AsyncMock),
    ):
        with pytest.raises(websockets.ConnectionClosed):
            await connector.run()

    assert mock_ws.__aenter__.call_count == 30
