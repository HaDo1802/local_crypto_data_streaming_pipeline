"""
trade_enricher.py — PyFlink stream-stream join between trades and orderbook
snapshots.  Writes enriched records (trade + spread context) back to Kafka.

Architecture:
  Kafka (trades)    ──┐
                      ├──► Flink Stream-Stream Join ──► Kafka (trades-enriched)
  Kafka (orderbook) ──┘

Join condition:
  trade.symbol = orderbook.symbol
  AND orderbook.event_time BETWEEN trade.event_time - 5s AND trade.event_time + 5s

~10% of trades intentionally have no matching orderbook snapshot (gap in
generator), so those records will not appear in the output — this exercises
the inner join semantics and watermark-driven state eviction.
"""

import os

from pyflink.common import Configuration
from pyflink.table import EnvironmentSettings, TableEnvironment

# ── Config ───────────────────────────────────────────────────────────────────
KAFKA_BROKERS = os.getenv("BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
TRADES_TOPIC = os.getenv("TRADES_TOPIC", "trades")
ORDERBOOK_TOPIC = os.getenv("ORDERBOOK_TOPIC", "orderbook")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "trades-enriched")

# ── Environment ───────────────────────────────────────────────────────────────
config = Configuration()
config.set_string("pipeline.name", "CryptoTradeEnricher")

env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = TableEnvironment.create(env_settings)
t_env.get_config().add_configuration(config)
t_env.get_config().set("parallelism.default", "3")
t_env.get_config().set("table.exec.source.idle-timeout", "5000ms")

# ── Source: trades ────────────────────────────────────────────────────────────
t_env.execute_sql(f"""
CREATE TABLE trades (
    trade_id   STRING,
    symbol     STRING,
    price      DOUBLE,
    quantity   DOUBLE,
    side       STRING,
    event_time BIGINT,
    ts AS TO_TIMESTAMP_LTZ(event_time, 3),
    WATERMARK FOR ts AS ts - INTERVAL '10' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = '{TRADES_TOPIC}',
    'properties.bootstrap.servers' = '{KAFKA_BROKERS}',
    'properties.group.id'          = 'flink-trade-enricher-trades',
    'scan.startup.mode'            = 'earliest-offset',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'true'
)
""")

# ── Source: orderbook ─────────────────────────────────────────────────────────
t_env.execute_sql(f"""
CREATE TABLE orderbook (
    symbol     STRING,
    bid_price  DOUBLE,
    bid_size   DOUBLE,
    ask_price  DOUBLE,
    ask_size   DOUBLE,
    event_time BIGINT,
    ts AS TO_TIMESTAMP_LTZ(event_time, 3),
    WATERMARK FOR ts AS ts - INTERVAL '10' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = '{ORDERBOOK_TOPIC}',
    'properties.bootstrap.servers' = '{KAFKA_BROKERS}',
    'properties.group.id'          = 'flink-trade-enricher-ob',
    'scan.startup.mode'            = 'earliest-offset',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'true'
)
""")

# ── Sink: enriched trades → Kafka (JSON) ──────────────────────────────────────
t_env.execute_sql(f"""
CREATE TABLE trades_enriched_sink (
    trade_id   STRING,
    symbol     STRING,
    price      DOUBLE,
    quantity   DOUBLE,
    side       STRING,
    bid_price  DOUBLE,
    ask_price  DOUBLE,
    spread     DOUBLE,
    trade_time TIMESTAMP(3),
    PRIMARY KEY (trade_id) NOT ENFORCED
) WITH (
    'connector'                    = 'upsert-kafka',
    'topic'                        = '{OUTPUT_TOPIC}',
    'properties.bootstrap.servers' = '{KAFKA_BROKERS}',
    'key.format'                   = 'json',
    'value.format'                 = 'json'
)
""")

# ── Stream-stream join: trade ⋈ orderbook within ±5 second event-time window ─
print(f"Trade enricher starting:")
print(f"  Kafka:  {KAFKA_BROKERS}")
print(f"  Input:  {TRADES_TOPIC} + {ORDERBOOK_TOPIC}")
print(f"  Output: {OUTPUT_TOPIC}")

t_env.execute_sql("""
INSERT INTO trades_enriched_sink
SELECT
    t.trade_id,
    t.symbol,
    t.price,
    t.quantity,
    t.side,
    ob.bid_price,
    ob.ask_price,
    ROUND(ob.ask_price - ob.bid_price, 6)  AS spread,
    t.ts                                   AS trade_time
FROM trades AS t
INNER JOIN orderbook FOR SYSTEM_TIME AS OF t.ts AS ob
    ON t.symbol = ob.symbol
""").wait()
