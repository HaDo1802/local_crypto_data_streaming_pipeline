"""
PyFlink job for the serving layer.

Pipeline:
  Kafka trades -> Flink SQL -> Postgres raw_trades + Postgres ohlcv

The dashboard reads both tables directly from Postgres:
  - `raw_trades` for the latest trade tape
  - `ohlcv` for candlestick windows and summary stats
"""

from __future__ import annotations

import os

from pyflink.common import Configuration
from pyflink.table import EnvironmentSettings, TableEnvironment


def env(name: str, default: str) -> str:
    return os.getenv(name, default).strip()


KAFKA_BROKERS = env("BOOTSTRAP_SERVERS", "kafka:9092")
TRADES_TOPIC = env("TRADES_TOPIC", "trades")
POSTGRES_URL = env("POSTGRES_URL", "jdbc:postgresql://postgres:5432/crypto")
POSTGRES_USER = env("POSTGRES_USER", "trader")
POSTGRES_PASSWORD = env("POSTGRES_PASSWORD", "trader")
WINDOW_SECONDS = max(1, int(env("WINDOW_SECONDS", "10")))
ENABLE_CONSOLE_SINK = env("ENABLE_CONSOLE_SINK", "false").lower() == "true"


def sql_literal(value: str) -> str:
    return value.replace("'", "''")


def execute(table_env: TableEnvironment, statement: str) -> None:
    table_env.execute_sql(statement)


def kafka_source_ddl(brokers: str, topic: str) -> str:
    """Return the Kafka source DDL for raw trades."""
    return f"""
    CREATE TABLE trades (
        trade_id STRING,
        symbol STRING,
        price DOUBLE,
        quantity DOUBLE,
        side STRING,
        event_time BIGINT,
        ts AS TO_TIMESTAMP_LTZ(event_time, 3),
        WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{sql_literal(topic)}',
        'properties.bootstrap.servers' = '{sql_literal(brokers)}',
        'properties.group.id' = 'flink-price-aggregator',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json',
        'json.ignore-parse-errors' = 'true'
    )
    """


def raw_trades_sink_ddl(pg_url: str, pg_user: str, pg_password: str) -> str:
    """Return the JDBC sink DDL for raw trades."""
    return f"""
    CREATE TABLE raw_trades_sink (
        trade_id STRING,
        symbol STRING,
        price DOUBLE,
        quantity DOUBLE,
        side STRING,
        event_time BIGINT,
        PRIMARY KEY (trade_id) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = '{sql_literal(pg_url)}',
        'table-name' = 'raw_trades',
        'username' = '{sql_literal(pg_user)}',
        'password' = '{sql_literal(pg_password)}',
        'driver' = 'org.postgresql.Driver'
    )
    """


def ohlcv_sink_ddl(pg_url: str, pg_user: str, pg_password: str) -> str:
    """Return the JDBC sink DDL for OHLCV candles."""
    return f"""
    CREATE TABLE ohlcv_sink (
        symbol STRING,
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        open_price DOUBLE,
        high_price DOUBLE,
        low_price DOUBLE,
        close_price DOUBLE,
        volume DOUBLE,
        trade_count BIGINT,
        PRIMARY KEY (symbol, window_start) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = '{sql_literal(pg_url)}',
        'table-name' = 'ohlcv',
        'username' = '{sql_literal(pg_user)}',
        'password' = '{sql_literal(pg_password)}',
        'driver' = 'org.postgresql.Driver'
    )
    """


def console_sink_ddl() -> str:
    """Return the print connector sink DDL for debugging."""
    return """
    CREATE TABLE console_sink (
        symbol STRING,
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        open_price DOUBLE,
        high_price DOUBLE,
        low_price DOUBLE,
        close_price DOUBLE,
        volume DOUBLE,
        trade_count BIGINT
    ) WITH (
        'connector' = 'print'
    )
    """


def raw_trades_view_sql() -> str:
    """Return the SQL for the normalized raw trades view."""
    return """
    SELECT
        trade_id,
        UPPER(symbol) AS symbol,
        price,
        quantity,
        side,
        event_time
    FROM trades
    """


def ohlcv_view_sql(window_seconds: int) -> str:
    """Return the SQL for the windowed OHLCV view."""
    return f"""
    -- Use MIN_BY/MAX_BY on event-time `ts` so open/close are stable even
    -- when arrival order differs from the true event order under load.
    SELECT
        UPPER(symbol) AS symbol,
        CAST(window_start AS TIMESTAMP(3)) AS window_start,
        CAST(window_end AS TIMESTAMP(3)) AS window_end,
        MIN_BY(price, ts) AS open_price,
        MAX(price) AS high_price,
        MIN(price) AS low_price,
        MAX_BY(price, ts) AS close_price,
        ROUND(SUM(quantity), 6) AS volume,
        COUNT(*) AS trade_count
    FROM TABLE(
        TUMBLE(TABLE trades, DESCRIPTOR(ts), INTERVAL '{window_seconds}' SECOND)
    )
    GROUP BY symbol, window_start, window_end
    """


def build_table_env() -> TableEnvironment:
    config = Configuration()
    config.set_string("pipeline.name", "crypto-price-aggregator")
    config.set_string("parallelism.default", "3")
    config.set_string("table.exec.source.idle-timeout", "5 s")

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = TableEnvironment.create(settings)
    table_env.get_config().add_configuration(config)
    return table_env


def register_tables(table_env: TableEnvironment) -> None:
    # Build DDL in named helpers so each SQL contract is easier to read and
    # test independently than large inline f-strings inside orchestration code.
    execute(table_env, kafka_source_ddl(KAFKA_BROKERS, TRADES_TOPIC))
    execute(table_env, raw_trades_sink_ddl(POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD))
    execute(table_env, ohlcv_sink_ddl(POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD))

    if ENABLE_CONSOLE_SINK:
        execute(table_env, console_sink_ddl())


def register_views(table_env: TableEnvironment) -> None:
    table_env.create_temporary_view(
        "raw_trades_view",
        table_env.sql_query(raw_trades_view_sql()),
    )

    table_env.create_temporary_view(
        "ohlcv_view",
        table_env.sql_query(ohlcv_view_sql(WINDOW_SECONDS)),
    )


def main() -> None:
    table_env = build_table_env()
    register_tables(table_env)
    register_views(table_env)

    print("Price aggregator starting")
    print(f"  Kafka: {KAFKA_BROKERS} topic={TRADES_TOPIC}")
    print(f"  Postgres: {POSTGRES_URL}")
    print(f"  Window: {WINDOW_SECONDS}s")

    statement_set = table_env.create_statement_set()
    statement_set.add_insert("raw_trades_sink", table_env.from_path("raw_trades_view"))
    statement_set.add_insert("ohlcv_sink", table_env.from_path("ohlcv_view"))

    if ENABLE_CONSOLE_SINK:
        statement_set.add_insert("console_sink", table_env.from_path("ohlcv_view"))

    result = statement_set.execute()
    result.get_job_client().get_job_execution_result().result()


if __name__ == "__main__":
    main()
