-- Schema for crypto streaming pipeline: Flink OHLCV JDBC sink + dashboard API.

CREATE TABLE IF NOT EXISTS ohlcv (
    symbol        TEXT        NOT NULL,
    window_start  TIMESTAMPTZ NOT NULL,
    window_end    TIMESTAMPTZ NOT NULL,
    open_price    DOUBLE PRECISION NOT NULL,
    high_price    DOUBLE PRECISION NOT NULL,
    low_price     DOUBLE PRECISION NOT NULL,
    close_price   DOUBLE PRECISION NOT NULL,
    volume        DOUBLE PRECISION NOT NULL,
    trade_count   BIGINT      NOT NULL,
    PRIMARY KEY (symbol, window_start)
);

CREATE INDEX IF NOT EXISTS idx_ohlcv_symbol_window_end
    ON ohlcv (symbol, window_end DESC);

-- Latest candle per symbol (WebSocket ticker / latest_ohlcv)
CREATE OR REPLACE VIEW latest_ohlcv AS
SELECT DISTINCT ON (symbol)
    symbol,
    close_price,
    window_end
FROM ohlcv
ORDER BY symbol, window_end DESC;

-- Raw trades for dashboard tail (optional; main trade path is Kafka + lake Parquet)
CREATE TABLE IF NOT EXISTS raw_trades (
    trade_id    TEXT PRIMARY KEY,
    symbol      TEXT NOT NULL,
    price       DOUBLE PRECISION NOT NULL,
    quantity    DOUBLE PRECISION NOT NULL,
    side        TEXT NOT NULL,
    event_time  BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_trades_symbol_time
    ON raw_trades (symbol, event_time DESC);
