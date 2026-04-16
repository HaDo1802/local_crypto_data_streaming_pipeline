"""
price_aggregator.py — PyFlink job that computes per-symbol OHLCV candles
from the `trades` Kafka topic and writes results to PostgreSQL.

Architecture:
  Kafka (trades) ──► Flink Tumbling Window (1 min) ──► Postgres (ohlcv)

Uses the Flink SQL / Table API for concise, declarative stream processing.
The OHLCV computation uses:
  - FIRST_VALUE(price) as open
  - MAX(price)         as high
  - MIN(price)         as low
  - LAST_VALUE(price)  as close
  - SUM(quantity)      as volume
  - COUNT(*)           as trade_count

NOTE: FIRST_VALUE/LAST_VALUE in Flink SQL streaming mode require retraction
support. If you encounter issues, replace with MIN/MAX for a simpler demo.
"""

import os

from pyflink.common import Configuration
from pyflink.table import EnvironmentSettings, TableEnvironment

# ── Config from environment ──────────────────────────────────────────────────
KAFKA_BROKERS = os.getenv("BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
TRADES_TOPIC = os.getenv("TRADES_TOPIC", "trades")
POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/crypto")
POSTGRES_USER = os.getenv("POSTGRES_USER", "trader")
POSTGRES_PASS = os.getenv("POSTGRES_PASSWORD", "trader")
WINDOW_MINUTES = int(os.getenv("WINDOW_MINUTES", "1"))

# ── Table environment ─────────────────────────────────────────────────────────
config = Configuration()
config.set_string("pipeline.name", "CryptoPriceAggregator")

env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = TableEnvironment.create(env_settings)
t_env.get_config().add_configuration(config)

# Parallelism: each partition maps to 1 Flink task (3 Kafka partitions)
t_env.get_config().set("parallelism.default", "3")
t_env.get_config().set("table.exec.source.idle-timeout", "5000ms")

# ── Source: trades from Kafka ─────────────────────────────────────────────────
t_env.execute_sql(f"""
CREATE TABLE trades (
    trade_id   STRING,
    symbol     STRING,
    price      DOUBLE,
    quantity   DOUBLE,
    side       STRING,
    event_time BIGINT,                        -- epoch milliseconds from generator
    -- Derive a proper TIMESTAMP_LTZ for event-time semantics
    ts AS TO_TIMESTAMP_LTZ(event_time, 3),
    WATERMARK FOR ts AS ts - INTERVAL '10' SECOND
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = '{TRADES_TOPIC}',
    'properties.bootstrap.servers'  = '{KAFKA_BROKERS}',
    'properties.group.id'           = 'flink-price-aggregator',
    'scan.startup.mode'             = 'earliest-offset',
    'format'                        = 'json',
    'json.ignore-parse-errors'      = 'true'
)
""")

# ── Sink: OHLCV to PostgreSQL ─────────────────────────────────────────────────
t_env.execute_sql(f"""
CREATE TABLE ohlcv_sink (
    symbol       STRING,
    window_start TIMESTAMP(3),
    window_end   TIMESTAMP(3),
    open_price   DOUBLE,
    high_price   DOUBLE,
    low_price    DOUBLE,
    close_price  DOUBLE,
    volume       DOUBLE,
    trade_count  BIGINT,
    PRIMARY KEY (symbol, window_start) NOT ENFORCED
) WITH (
    'connector'  = 'jdbc',
    'url'        = '{POSTGRES_URL}',
    'table-name' = 'ohlcv',
    'username'   = '{POSTGRES_USER}',
    'password'   = '{POSTGRES_PASS}',
    'driver'     = 'org.postgresql.Driver'
)
""")

# ── Also echo to console for observability ───────────────────────────────────
t_env.execute_sql("""
CREATE TABLE console_sink (
    symbol       STRING,
    window_start TIMESTAMP(3),
    window_end   TIMESTAMP(3),
    open_price   DOUBLE,
    high_price   DOUBLE,
    low_price    DOUBLE,
    close_price  DOUBLE,
    volume       DOUBLE,
    trade_count  BIGINT
) WITH (
    'connector' = 'print'
)
""")

# ── OHLCV aggregation using tumbling event-time windows ──────────────────────
ohlcv_query = f"""
SELECT
    symbol,
    TUMBLE_START(ts, INTERVAL '{WINDOW_MINUTES}' MINUTE)   AS window_start,
    TUMBLE_END(ts,   INTERVAL '{WINDOW_MINUTES}' MINUTE)   AS window_end,
    FIRST_VALUE(price)   AS open_price,
    MAX(price)           AS high_price,
    MIN(price)           AS low_price,
    LAST_VALUE(price)    AS close_price,
    ROUND(SUM(quantity), 6) AS volume,
    COUNT(*)             AS trade_count
FROM trades
GROUP BY
    symbol,
    TUMBLE(ts, INTERVAL '{WINDOW_MINUTES}' MINUTE)
"""

# Register the aggregated view so both sinks share the same computation
t_env.create_temporary_view("ohlcv_view", t_env.sql_query(ohlcv_query))

print(f"Price aggregator starting:")
print(f"  Kafka:    {KAFKA_BROKERS}  topic={TRADES_TOPIC}")
print(f"  Postgres: {POSTGRES_URL}")
print(f"  Window:   {WINDOW_MINUTES} minute(s) tumbling")

# Submit both sink inserts concurrently via StatementSet
stmt_set = t_env.create_statement_set()
stmt_set.add_insert("ohlcv_sink",   t_env.from_path("ohlcv_view"))
stmt_set.add_insert("console_sink", t_env.from_path("ohlcv_view"))

result = stmt_set.execute()

# Block until job finishes (or container is killed)
result.get_job_client().get_job_execution_result().result()
