"""
generator.py — produces simulated crypto trade and order-book events to Kafka.

Trade events carry a realistic random-walk price.  Order-book snapshots carry
the best bid/ask around that price.  10% of trades intentionally have no
corresponding order-book update, exercising gap handling in the enricher.
"""

import json
import math
import os
import random
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict

import jsonschema
from confluent_kafka import Producer

# ── Config ───────────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TRADES_TOPIC = os.getenv("TRADES_TOPIC", "trades")
ORDERBOOK_TOPIC = os.getenv("ORDERBOOK_TOPIC", "orderbook")
INTERVAL_SEC = float(os.getenv("TRADE_INTERVAL_SEC", "0.3"))
SYMBOLS_RAW = os.getenv("SYMBOLS", "BTC-USD,ETH-USD,SOL-USD,BNB-USD,XRP-USD")
SYMBOLS = [s.strip() for s in SYMBOLS_RAW.split(",")]

TRADE_SCHEMA_PATH = os.getenv("TRADE_SCHEMA_PATH", "/app/schemas/trade.schema.json")
OB_SCHEMA_PATH = os.getenv("OB_SCHEMA_PATH", "/app/schemas/orderbook.schema.json")

# Base prices for simulated symbols
BASE_PRICES: Dict[str, float] = {
    "BTC-USD": 65_000.0,
    "ETH-USD": 3_200.0,
    "SOL-USD": 150.0,
    "BNB-USD": 580.0,
    "XRP-USD": 0.60,
}

# Typical spread as % of price
SPREAD_PCT: Dict[str, float] = {
    "BTC-USD": 0.01,
    "ETH-USD": 0.02,
    "SOL-USD": 0.05,
    "BNB-USD": 0.03,
    "XRP-USD": 0.10,
}

# Volatility — std-dev of log-return per tick
VOLATILITY: Dict[str, float] = {
    "BTC-USD": 0.0008,
    "ETH-USD": 0.0010,
    "SOL-USD": 0.0015,
    "BNB-USD": 0.0012,
    "XRP-USD": 0.0020,
}


@dataclass
class SymbolState:
    price: float
    spread_pct: float
    volatility: float
    tick_sizes: Dict[str, float] = field(default_factory=dict)

    def next_price(self) -> float:
        """Geometric Brownian Motion step."""
        log_ret = random.gauss(0, self.volatility)
        self.price = self.price * math.exp(log_ret)
        return round(self.price, 6)


def _load_schema(path: str) -> dict:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}  # schema validation optional if file missing


TRADE_SCHEMA = _load_schema(TRADE_SCHEMA_PATH)
OB_SCHEMA = _load_schema(OB_SCHEMA_PATH)


def build_trade(symbol: str, state: SymbolState) -> dict:
    price = state.next_price()
    qty = round(random.uniform(0.001, 5.0), 6)
    return {
        "trade_id": str(uuid.uuid4()),
        "symbol": symbol,
        "price": price,
        "quantity": qty,
        "side": random.choice(["buy", "sell"]),
        "event_time": int(time.time() * 1000),  # epoch ms
    }


def build_orderbook(symbol: str, mid_price: float, spread_pct: float) -> dict:
    half_spread = mid_price * spread_pct / 100
    bid = round(mid_price - half_spread, 6)
    ask = round(mid_price + half_spread, 6)
    bid_size = round(random.uniform(0.1, 10.0), 4)
    ask_size = round(random.uniform(0.1, 10.0), 4)
    return {
        "symbol": symbol,
        "bid_price": bid,
        "bid_size": bid_size,
        "ask_price": ask,
        "ask_size": ask_size,
        "event_time": int(time.time() * 1000),
    }


def validate(payload: dict, schema: dict) -> None:
    if schema:
        jsonschema.validate(instance=payload, schema=schema)


def delivery_report(err, msg) -> None:
    if err:
        print(f"[delivery-error] topic={msg.topic()} error={err}")
        return
    key = msg.key().decode() if msg.key() else "-"
    print(
        f"[delivered] topic={msg.topic():18s} key={key[:16]:16s} "
        f"partition={msg.partition()} offset={msg.offset()}"
    )


def main() -> None:
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    # Initialize per-symbol state
    states: Dict[str, SymbolState] = {}
    for sym in SYMBOLS:
        base = BASE_PRICES.get(sym, 100.0)
        states[sym] = SymbolState(
            price=base,
            spread_pct=SPREAD_PCT.get(sym, 0.05),
            volatility=VOLATILITY.get(sym, 0.001),
        )

    print(f"Generator starting | brokers={BOOTSTRAP_SERVERS}")
    print(f"Symbols: {SYMBOLS}")
    print(f"Interval: {INTERVAL_SEC}s")

    sent = 0
    try:
        while True:
            symbol = random.choice(SYMBOLS)
            state = states[symbol]

            # --- trade event ---
            trade = build_trade(symbol, state)
            validate(trade, TRADE_SCHEMA)
            producer.poll(0)
            producer.produce(
                topic=TRADES_TOPIC,
                key=trade["symbol"].encode(),
                value=json.dumps(trade).encode(),
                callback=delivery_report,
            )

            # --- order-book snapshot (90% of the time) ---
            if random.random() < 0.9:
                ob = build_orderbook(symbol, state.price, state.spread_pct)
                validate(ob, OB_SCHEMA)
                producer.produce(
                    topic=ORDERBOOK_TOPIC,
                    key=ob["symbol"].encode(),
                    value=json.dumps(ob).encode(),
                    callback=delivery_report,
                )

            sent += 1
            if sent % 100 == 0:
                print(f"[generator] {sent} trade events sent")

            time.sleep(INTERVAL_SEC)

    except KeyboardInterrupt:
        print("Generator stopped.")
    finally:
        remaining = producer.flush(timeout=10)
        if remaining:
            print(f"Warning: {remaining} messages unconfirmed on shutdown.")


if __name__ == "__main__":
    main()
