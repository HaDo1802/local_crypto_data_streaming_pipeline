"""
price_aggregator.py — Kafka trades -> Flink SQL -> Postgres raw_trades + ohlcv.

Simplified learning version:
  - Keep all SQL contracts in one file (easy to scan)
  - Keep only one optional flag (console sink)
  - Remove helper indirection that is nice in larger production codebases
"""

import os

from pyflink.common import Configuration
from pyflink.table import EnvironmentSettings, TableEnvironment

KAFKA_BROKERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092").strip()
TRADES_TOPIC = os.getenv("TRADES_TOPIC", "trades").strip()
POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/crypto").strip()
POSTGRES_USER = os.getenv("POSTGRES_USER", "trader").strip()
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "trader").strip()
WINDOW_SECONDS = max(1, int(os.getenv("WINDOW_SECONDS", "10")))
ENABLE_CONSOLE_SINK = os.getenv("ENABLE_CONSOLE_SINK", "false").lower() == "true"


def q(value: str) -> str:
    return value.replace("'", "''")


def main() -> None:
    config = Configuration()
    config.set_string("pipeline.name", "crypto-price-aggregator")
    config.set_string("parallelism.default", "3")
    config.set_string("table.exec.source.idle-timeout", "5 s")

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = TableEnvironment.create(settings)
    t_env.get_config().add_configuration(config)

    t_env.execute_sql(
        f"""
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
            'topic' = '{q(TRADES_TOPIC)}',
            'properties.bootstrap.servers' = '{q(KAFKA_BROKERS)}',
            'properties.group.id' = 'flink-price-aggregator',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
        """
    )

    t_env.execute_sql(
        f"""
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
            'url' = '{q(POSTGRES_URL)}',
            'table-name' = 'raw_trades',
            'username' = '{q(POSTGRES_USER)}',
            'password' = '{q(POSTGRES_PASSWORD)}',
            'driver' = 'org.postgresql.Driver'
        )
        """
    )

    t_env.execute_sql(
        f"""
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
            'url' = '{q(POSTGRES_URL)}',
            'table-name' = 'ohlcv',
            'username' = '{q(POSTGRES_USER)}',
            'password' = '{q(POSTGRES_PASSWORD)}',
            'driver' = 'org.postgresql.Driver'
        )
        """
    )

    if ENABLE_CONSOLE_SINK:
        t_env.execute_sql(
            """
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
            ) WITH ('connector' = 'print')
            """
        )

    # Keep event-time based open/close so candles stay correct under out-of-order delivery.
    t_env.create_temporary_view(
        "raw_trades_view",
        t_env.sql_query(
            """
            SELECT trade_id, UPPER(symbol) AS symbol, price, quantity, side, event_time
            FROM trades
            """
        ),
    )
    t_env.create_temporary_view(
        "ohlcv_view",
        t_env.sql_query(
            f"""
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
                TUMBLE(TABLE trades, DESCRIPTOR(ts), INTERVAL '{WINDOW_SECONDS}' SECOND)
            )
            GROUP BY symbol, window_start, window_end
            """
        ),
    )

    print("Price aggregator starting")
    print(f"Kafka: {KAFKA_BROKERS} topic={TRADES_TOPIC}")
    print(f"Postgres: {POSTGRES_URL}")
    print(f"Window: {WINDOW_SECONDS}s")

    statement_set = t_env.create_statement_set()
    statement_set.add_insert("raw_trades_sink", t_env.from_path("raw_trades_view"))
    statement_set.add_insert("ohlcv_sink", t_env.from_path("ohlcv_view"))
    if ENABLE_CONSOLE_SINK:
        statement_set.add_insert("console_sink", t_env.from_path("ohlcv_view"))

    result = statement_set.execute()
    result.get_job_client().get_job_execution_result().result()


if __name__ == "__main__":
    main()
