"""Tests de serving.sync: conexión DuckDB y sincronización de tablas."""

from unittest.mock import MagicMock, patch

import duckdb

from serving.sync import get_connection, sync_table, _table_exists


def _make_settings():
    settings = MagicMock()
    settings.s3_access_key = "test"
    settings.s3_secret_key = "test"
    settings.s3_endpoint_url = "http://floci:4566"
    settings.timescale_database = "serving"
    settings.timescale_user = "serving"
    settings.timescale_password = "serving"
    settings.timescale_host = "timescaledb"
    settings.timescale_port = 5432
    return settings


def test_get_connection_installs_extensions_and_attaches_postgres():
    settings = _make_settings()
    mock_con = MagicMock()

    with patch("serving.sync.duckdb.connect", return_value=mock_con):
        result = get_connection(settings)

    executed_statements = [call.args[0] for call in mock_con.execute.call_args_list]

    assert any("INSTALL delta" in stmt for stmt in executed_statements)
    assert any("INSTALL httpfs" in stmt for stmt in executed_statements)
    assert any("INSTALL postgres" in stmt for stmt in executed_statements)
    assert any("CREATE SECRET" in stmt for stmt in executed_statements)
    assert any("ATTACH" in stmt for stmt in executed_statements)
    assert result is mock_con


def test_table_exists_returns_true_when_query_succeeds():
    mock_con = MagicMock()
    mock_con.execute.return_value = None

    assert _table_exists(mock_con, "s3://lakehouse/gold/volatility") is True


def test_table_exists_returns_false_on_io_exception():
    mock_con = MagicMock()
    mock_con.execute.side_effect = duckdb.IOException("table not found")

    assert _table_exists(mock_con, "s3://lakehouse/gold/volatility") is False


def test_sync_table_returns_false_and_skips_insert_when_table_missing():
    mock_con = MagicMock()

    with patch("serving.sync._table_exists", return_value=False):
        result = sync_table(
            mock_con,
            "s3://lakehouse/gold/volatility",
            "gold_volatility",
            ["symbol", "window_start"],
        )

    assert result is False
    insert_calls = [
        call
        for call in mock_con.execute.call_args_list
        if "INSERT INTO" in call.args[0]
    ]
    assert len(insert_calls) == 0


def test_sync_table_inserts_and_returns_true_when_table_exists():
    mock_con = MagicMock()

    with patch("serving.sync._table_exists", return_value=True):
        result = sync_table(
            mock_con,
            "s3://lakehouse/gold/volatility",
            "gold_volatility",
            ["symbol", "window_start"],
        )

    assert result is True
    insert_calls = [
        call
        for call in mock_con.execute.call_args_list
        if "INSERT INTO" in call.args[0]
    ]
    assert len(insert_calls) == 1
    assert "gold_volatility" in insert_calls[0].args[0]
    assert "symbol = pg.symbol" in insert_calls[0].args[0]
    assert "window_start = pg.window_start" in insert_calls[0].args[0]
