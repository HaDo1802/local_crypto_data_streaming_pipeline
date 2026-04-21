"""
lake_writer.py — Kafka trades -> batched Parquet files in MinIO.
"""

import io
import json
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.client import Config
from confluent_kafka import Consumer, KafkaError

# ── Config ───────────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
TRADES_TOPIC = os.getenv("TRADES_TOPIC", "trades")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "lakehouse")
FLUSH_INTERVAL_SEC = int(os.getenv("FLUSH_INTERVAL_SEC", "60"))
FLUSH_BATCH_SIZE = int(os.getenv("FLUSH_BATCH_SIZE", "500"))

TRADE_PA_SCHEMA = pa.schema([
    pa.field("trade_id",   pa.string()),
    pa.field("symbol",     pa.string()),
    pa.field("price",      pa.float64()),
    pa.field("quantity",   pa.float64()),
    pa.field("side",       pa.string()),
    pa.field("event_time", pa.int64()),
    pa.field("written_at", pa.int64()),
])


def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def upload_parquet(client, records: list[dict], symbol: str, dt: str, part_n: int) -> str:
    arrays = {field.name: [] for field in TRADE_PA_SCHEMA}
    now_ms = int(time.time() * 1000)
    for rec in records:
        arrays["trade_id"].append(rec.get("trade_id"))
        arrays["symbol"].append(rec.get("symbol"))
        arrays["price"].append(rec.get("price"))
        arrays["quantity"].append(rec.get("quantity"))
        arrays["side"].append(rec.get("side"))
        arrays["event_time"].append(rec.get("event_time"))
        arrays["written_at"].append(now_ms)

    table = pa.table({k: pa.array(v) for k, v in arrays.items()}, schema=TRADE_PA_SCHEMA)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    size_kb = buf.tell() / 1024
    buf.seek(0)

    key = f"trades/dt={dt}/symbol={symbol}/part-{part_n:05d}.parquet"
    client.upload_fileobj(buf, MINIO_BUCKET, key)
    print(f"[uploaded] s3://{MINIO_BUCKET}/{key} ({len(records)} rows, {size_kb:.1f} KB)")
    return key


def main() -> None:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "lake-writer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TRADES_TOPIC])
    print(f"Lake writer started | topic={TRADES_TOPIC} flush={FLUSH_INTERVAL_SEC}s/{FLUSH_BATCH_SIZE} rows")

    minio = get_minio_client()
    buffers: dict[str, list[dict]] = defaultdict(list)
    part_counter: dict[tuple[str, str], int] = defaultdict(int)
    last_flush = time.time()
    total_written = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[kafka-error] {msg.error()}")
            else:
                try:
                    trade = json.loads(msg.value().decode("utf-8"))
                    symbol = trade.get("symbol", "UNKNOWN")
                    buffers[symbol].append(trade)
                except Exception as exc:
                    print(f"[skip] malformed message: {exc}")

            size_flush = any(len(rows) >= FLUSH_BATCH_SIZE for rows in buffers.values())
            time_flush = (time.time() - last_flush) >= FLUSH_INTERVAL_SEC

            if (size_flush or time_flush) and buffers:
                dt = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
                flush_written = 0
                flush_failed = False

                for symbol, rows in buffers.items():
                    if not rows:
                        continue

                    try:
                        part_counter[(dt, symbol)] += 1
                        upload_parquet(minio, rows, symbol, dt, part_counter[(dt, symbol)])
                        flush_written += len(rows)
                    except Exception as exc:
                        print(f"[upload-failed] symbol={symbol} rows={len(rows)} error={exc}")
                        flush_failed = True
                        break

                if flush_failed:
                    last_flush = time.time()
                    continue

                consumer.commit()
                total_written += flush_written
                buffers = defaultdict(list)
                last_flush = time.time()
                print(f"[flush] wrote={flush_written} total={total_written}")

    except KeyboardInterrupt:
        print("Lake writer stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
