CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE gold_volatility (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    avg_price NUMERIC(20, 8),
    stddev_price NUMERIC(20, 8),
    total_volume NUMERIC(20, 8),
    trade_count BIGINT,
    PRIMARY KEY (symbol, window_start)
);
SELECT create_hypertable('gold_volatility', 'window_start');

CREATE TABLE gold_spread (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    avg_spread NUMERIC(20, 8),
    min_spread NUMERIC(20, 8),
    max_spread NUMERIC(20, 8),
    avg_mid_price NUMERIC(20, 8),
    PRIMARY KEY (symbol, window_start)
);
SELECT create_hypertable('gold_spread', 'window_start');

CREATE TABLE gold_liquidity (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    avg_depth_quantity NUMERIC(20, 8),
    PRIMARY KEY (symbol, side, window_start)
);
SELECT create_hypertable('gold_liquidity', 'window_start');