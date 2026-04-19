"""
generator.py — Binance WebSocket → Kafka producer (trades only).

Subscribes to `<symbol>@trade` via a combined stream, maps payloads to the
internal trade JSON contract, and publishes to Kafka for Flink / lakehouse.

Local check (no Kafka):

  cd jobs/producer && pip install -r requirements.txt
  python generator.py --dry-run

Optional: limit how many mapped events to print, then exit:

  python generator.py --dry-run --max-events 25
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Optional

import jsonschema
from confluent_kafka import Producer

log = logging.getLogger(__name__)

try:
    import websocket
except ImportError as e:  # pragma: no cover
    raise SystemExit(
        "Install dependencies: pip install -r requirements.txt"
    ) from e

# ── Config ───────────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:19092")
TRADES_TOPIC = os.getenv("TRADES_TOPIC", "trades")

# Binance spot combined stream base (see Binance WebSocket API docs)
BINANCE_WSS_BASE = os.getenv("BINANCE_WSS_BASE", "wss://stream.binance.com:9443").rstrip("/")

# Comma-separated *lowercase* symbols, e.g. btcusdt,ethusdt (Binance stream names)
SYMBOLS_RAW = os.getenv(
    "SYMBOLS",
    "btcusdt,ethusdt,solusdt,bnbusdt,xrpusdt",
)
SYMBOLS = [s.strip().lower() for s in SYMBOLS_RAW.split(",") if s.strip()]


def _resolve_schema_path(env_key: str, filename: str) -> str:
    p = os.getenv(env_key)
    if p:
        return p
    local_candidate = Path(__file__).resolve().parent / "schemas" / filename
    if local_candidate.is_file():
        return str(local_candidate)

    repo_root = Path(__file__).resolve().parent
    for _ in range(3):
        candidate = repo_root / "schemas" / filename
        if candidate.is_file():
            return str(candidate)
        repo_root = repo_root.parent

    return f"/app/schemas/{filename}"


TRADE_SCHEMA_PATH = _resolve_schema_path("TRADE_SCHEMA_PATH", "trade.schema.json")


def _load_schema(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


TRADE_SCHEMA = _load_schema(TRADE_SCHEMA_PATH)


def build_combined_stream_url(symbols: list[str]) -> str:
    """One connection: streams=<sym>@trade/<sym>@trade/..."""
    streams = "/".join(f"{sym}@trade" for sym in symbols)
    return f"{BINANCE_WSS_BASE}/stream?streams={streams}"


def map_binance_trade(data: dict) -> dict:
    """Binance `trade` event → internal trade record."""
    sym = str(data["s"])
    tid = data["t"]
    # m: is buyer the market maker? True → aggressive seller
    side = "sell" if data["m"] else "buy"
    return {
        "trade_id": f"{sym}-{tid}",
        "symbol": sym,
        "price": float(data["p"]),
        "quantity": float(data["q"]),
        "side": side,
        "event_time": int(data["T"]),
    }


def parse_trade_message(raw: str) -> Optional[dict]:
    """
    Returns inner trade payload or None.
    Handles combined stream wrapper: {"stream":"btcusdt@trade","data":{...}}
    """
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError:
        return None

    data = msg.get("data", msg)
    if isinstance(data, list) or not isinstance(data, dict):
        return None

    if data.get("e") == "trade":
        return data
    return None


def validate(payload: dict, schema: dict) -> None:
    if schema:
        jsonschema.validate(instance=payload, schema=schema)


def delivery_report_verbose(err, msg) -> None:
    if err:
        print(f"[delivery-error] topic={msg.topic()} error={err}", file=sys.stderr)
        return
    key = msg.key().decode() if msg.key() else "-"
    print(
        f"[delivered] topic={msg.topic():18s} key={key[:16]:16s} "
        f"partition={msg.partition()} offset={msg.offset()}"
    )


def make_delivery_summary_callback():
    """Single-threaded producer callback: count successes, log every 1000 deliveries."""
    total_delivered = [0]

    def delivery_report(err, msg) -> None:
        if err:
            print(f"[delivery-error] topic={msg.topic()} error={err}", file=sys.stderr)
            return
        total_delivered[0] += 1
        n = total_delivered[0]
        if n % 1000 == 0:
            log.info(
                "Delivered %d messages | latest topic=%s partition=%d offset=%d",
                n,
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

    return delivery_report


def run_kafka_loop(producer: Producer, verbose_delivery: bool = False) -> None:
    url = build_combined_stream_url(SYMBOLS)
    print(f"Binance → Kafka | brokers={BOOTSTRAP_SERVERS}")
    print(f"Symbols: {SYMBOLS}")
    print(f"WebSocket: {url[:80]}…")
    delivery_cb = delivery_report_verbose if verbose_delivery else make_delivery_summary_callback()

    def on_message(_ws: Any, message: str) -> None:
        raw_payload = parse_trade_message(message)
        if not raw_payload:
            return
        try:
            out = map_binance_trade(raw_payload)
            validate(out, TRADE_SCHEMA)
            producer.poll(0)
            producer.produce(
                topic=TRADES_TOPIC,
                key=out["symbol"].encode(),
                value=json.dumps(out).encode(),
                callback=delivery_cb,
            )
        except Exception as exc:  # noqa: BLE001 — stream must stay up
            print(f"[skip] {exc} | payload snippet={str(raw_payload)[:120]}", file=sys.stderr)

    def on_error(_ws: Any, error: Any) -> None:
        print(f"[ws-error] {error}", file=sys.stderr)

    def on_close(_ws: Any, close_status_code: Any, close_msg: Any) -> None:
        print(f"[ws-closed] code={close_status_code} msg={close_msg}")

    backoff = 5.0
    while True:
        try:
            ws = websocket.WebSocketApp(
                url,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except KeyboardInterrupt:
            print("Generator stopped.")
            break
        except Exception as exc:  # noqa: BLE001
            print(f"[ws] reconnecting in {backoff:.0f}s: {exc}", file=sys.stderr)
            time.sleep(backoff)
            backoff = min(backoff * 1.5, 120.0)
            continue
        backoff = 5.0
        print("[ws] connection ended, reconnecting…", file=sys.stderr)
        time.sleep(backoff)


def run_dry_run(max_events: int) -> None:
    url = build_combined_stream_url(SYMBOLS)
    print(f"Dry run (no Kafka) — printing up to {max_events} trades", flush=True)
    print(f"WebSocket: {url}\n", flush=True)

    seen = 0
    last_error: list[str] = []

    def on_message(_ws: Any, message: str) -> None:
        nonlocal seen
        if seen >= max_events:
            _ws.close()
            return
        raw_payload = parse_trade_message(message)
        if not raw_payload:
            return
        try:
            out = map_binance_trade(raw_payload)
            validate(out, TRADE_SCHEMA)
        except Exception as exc:  # noqa: BLE001
            print(f"[invalid] {exc} | {raw_payload!r}\n")
            return
        seen += 1
        print(f"[{seen}/{max_events}] trade: {json.dumps(out)}")

    def on_open(_ws: Any) -> None:
        print("[ws] connected — waiting for Binance messages…\n")

    def on_error(_ws: Any, error: Any) -> None:
        msg = str(error)
        last_error.append(msg)
        print(f"[ws-error] {msg}", file=sys.stderr)
        if "451" in msg or "restricted" in msg.lower():
            print(
                "\nHint: Binance may block WebSocket from your region (HTTP 451). "
                "Try another network/VPN, or set BINANCE_WSS_BASE to a reachable endpoint.",
                file=sys.stderr,
            )

    ws = websocket.WebSocketApp(
        url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
    )
    ws.run_forever(ping_interval=20, ping_timeout=10)
    if seen == 0 and last_error:
        print("\nNo events received; fix WebSocket errors above and retry.", file=sys.stderr)
        sys.exit(1)
    print("\nDone.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Binance WebSocket → Kafka producer")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not connect to Kafka; print mapped trade JSON from Binance",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=20,
        help="With --dry-run, stop after this many trades (default: 20)",
    )
    parser.add_argument(
        "--verbose-delivery",
        action="store_true",
        help="Kafka mode: log every message delivery to stdout (default: summary every 1000)",
    )
    args = parser.parse_args()

    if not SYMBOLS:
        print("SYMBOLS is empty; set SYMBOLS env (e.g. btcusdt,ethusdt)", file=sys.stderr)
        sys.exit(1)

    if args.dry_run:
        run_dry_run(max(1, args.max_events))
        return

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [generator] %(levelname)s %(message)s",
    )
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    try:
        run_kafka_loop(producer, verbose_delivery=args.verbose_delivery)
    finally:
        remaining = producer.flush(timeout=10)
        if remaining:
            print(f"Warning: {remaining} messages unconfirmed on shutdown.")


if __name__ == "__main__":
    main()
