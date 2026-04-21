"""
generator.py — Binance WebSocket → Kafka producer (simplified learning version)
"""

import json
import os
import time

import websocket
from confluent_kafka import Producer

# ── Config ────────────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:19092")
TRADES_TOPIC      = os.getenv("TRADES_TOPIC", "trades")
BINANCE_WSS_BASE  = os.getenv("BINANCE_WSS_BASE", "wss://data-stream.binance.vision")
SYMBOLS_RAW       = os.getenv("SYMBOLS", "btcusdt,ethusdt,solusdt,bnbusdt,xrpusdt")
SYMBOLS           = [s.strip().lower() for s in SYMBOLS_RAW.split(",") if s.strip()]

# ── Transform ─────────────────────────────────────────────────────────────────
def map_binance_trade(data: dict) -> dict:
    """Binance trade event → internal schema."""
    sym  = str(data["s"])
    side = "sell" if data["m"] else "buy"   # m=True means buyer is market maker → seller aggressed
    return {
        "trade_id":   f"{sym}-{data['t']}",
        "symbol":     sym,
        "price":      float(data["p"]),     # Binance sends price as string
        "quantity":   float(data["q"]),     # same
        "side":       side,
        "event_time": int(data["T"]),       # millisecond timestamp — used by Flink for windowing
    }

# ── WebSocket message handler ─────────────────────────────────────────────────
def make_on_message(producer: Producer):
    """Return the WebSocket callback that transforms and publishes each trade."""
    def on_message(_ws, raw: str):
        try:
            msg  = json.loads(raw)
            data = msg.get("data", msg)     # unwrap combined stream envelope
            if data.get("e") != "trade":
                return                      # skip non-trade events (e.g. ping)

            out = map_binance_trade(data)

            # Kafka producer is generated here!
            producer.produce(
                topic=TRADES_TOPIC,
                key=out["symbol"].encode(), # key routes same symbol to same partition always
                value=json.dumps(out).encode(),
            )
        except Exception as exc:
            print(f"[skip] {exc}")

    return on_message

# ── Main loop ─────────────────────────────────────────────────────────────────
def main():
    streams  = "/".join(f"{s}@trade" for s in SYMBOLS)
    url      = f"{BINANCE_WSS_BASE}/stream?streams={streams}"
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    print(f"Connecting to Binance | symbols={SYMBOLS}")
    print(f"Kafka brokers: {BOOTSTRAP_SERVERS}")

    backoff = 5.0
    while True:
        ws = websocket.WebSocketApp(
            url,
            on_message=make_on_message(producer),
            on_error=lambda _ws, e: print(f"[ws-error] {e}"),
            on_close=lambda _ws, c, m: print(f"[ws-closed] code={c}"),
        )
        ws.run_forever(ping_interval=20, ping_timeout=10)
        # run_forever blocks until connection drops, then falls through here
        print(f"[reconnecting in {backoff:.0f}s]")
        time.sleep(backoff)
        backoff = min(backoff * 1.5, 120.0)

    producer.flush(timeout=10)   # drain on clean exit (ctrl+c hits the while loop's KeyboardInterrupt)

if __name__ == "__main__":
    main()