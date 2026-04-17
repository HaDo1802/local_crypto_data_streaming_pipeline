"""
lake_writer.py — consumes trade events from Kafka, batches them in memory,
and writes columnar Parquet files to MinIO (S3-compatible lakehouse).

Storage layout:
  lakehouse/trades/dt=YYYY-MM-DD/symbol=<SYM>/part-<n>.parquet

This gives a Hive-style partition layout that tools like DuckDB, Trino,
or Spark can query directly using partition pruning.

Design decisions:
  - Confluent kafka consumer (same lib as generator — single dependency)
  - pyarrow for in-process Parquet serialisation (no Spark overhead)
  - boto3 for S3-API uploads to MinIO
  - Two flush triggers: time-based (FLUSH_INTERVAL_SEC) and size-based
    (FLUSH_BATCH_SIZE), whichever fires first
"""

import io
import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [lake-writer] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Pyarrow schema matching trade events ────────────────────────────────────
TRADE_PA_SCHEMA = pa.schema([
    pa.field("trade_id",   pa.string()),
    pa.field("symbol",     pa.string()),
    pa.field("price",      pa.float64()),
    pa.field("quantity",   pa.float64()),
    pa.field("side",       pa.string()),
    pa.field("event_time", pa.int64()),
    pa.field("written_at", pa.int64()),   # epoch ms — set by lake writer
])


class MinIOUploader:
    def __init__(self) -> None:
        self.client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )
        self.part_counter: Dict[str, int] = defaultdict(int)

    def upload_parquet(self, records: List[dict], symbol: str, dt: str) -> str:
        """Serialise records to Parquet and upload to MinIO. Returns the S3 key."""
        # Convert list-of-dicts → columnar Arrow table
        arrays = {field.name: [] for field in TRADE_PA_SCHEMA}
        now_ms = int(time.time() * 1000)
        for rec in records:
            for col_name in ["trade_id", "symbol", "price", "quantity", "side", "event_time"]:
                arrays[col_name].append(rec.get(col_name))
            arrays["written_at"].append(now_ms)

        table = pa.table(
            {k: pa.array(v) for k, v in arrays.items()},
            schema=TRADE_PA_SCHEMA,
        )

        # Write to an in-memory buffer — no temp files needed
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        size_kb = buf.tell() / 1024
        buf.seek(0)

        self.part_counter[(dt, symbol)] += 1
        part_n = self.part_counter[(dt, symbol)]
        key = f"trades/dt={dt}/symbol={symbol}/part-{part_n:05d}.parquet"

        self.client.upload_fileobj(buf, MINIO_BUCKET, key)
        log.info(
            "Uploaded  s3://%s/%s  (%d rows, %.1f KB)",
            MINIO_BUCKET, key, len(records), size_kb,
        )
        return key


def main() -> None:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "lake-writer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([TRADES_TOPIC])
    log.info("Lake writer started | topic=%s  flush=%ss / %s rows",
             TRADES_TOPIC, FLUSH_INTERVAL_SEC, FLUSH_BATCH_SIZE)

    uploader = MinIOUploader()

    # Buffer: symbol → list of trade dicts for current flush window
    buffers: Dict[str, List[dict]] = defaultdict(list)
    last_flush = time.time()
    total_written = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                pass  # no new messages — check flush timer below
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("Kafka error: %s", msg.error())
            else:
                try:
                    trade = json.loads(msg.value().decode("utf-8"))
                    symbol = trade.get("symbol", "UNKNOWN")
                    buffers[symbol].append(trade)
                except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                    log.warning("Skipping malformed message: %s", exc)

            # Determine if any buffer has hit the size threshold
            size_flush = any(len(rows) >= FLUSH_BATCH_SIZE for rows in buffers.values())
            time_flush = (time.time() - last_flush) >= FLUSH_INTERVAL_SEC

            if (size_flush or time_flush) and buffers:
                dt = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
                for symbol, rows in buffers.items():
                    if rows:
                        uploader.upload_parquet(rows, symbol, dt)
                        total_written += len(rows)

                buffers = defaultdict(list)
                last_flush = time.time()
                log.info("Flush complete. Total rows written: %d", total_written)

    except KeyboardInterrupt:
        log.info("Lake writer stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
