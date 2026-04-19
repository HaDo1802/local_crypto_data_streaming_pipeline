"""
FastAPI serving layer for the streaming demo.

The API intentionally stays thin:
  - Postgres is the serving database for dashboard reads
  - MinIO is exposed for lakehouse observability
  - WebSockets publish the latest close per symbol
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import asyncpg
import boto3
from botocore.client import Config
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from pydantic import BaseModel


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [api] %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger(__name__)


DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://trader:trader@postgres:5432/crypto")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "lakehouse")
DASHBOARD_PATH = Path("/app/dashboard/index.html")

PRICE_POLL_INTERVAL_SECONDS = 2


class OHLCVCandle(BaseModel):
    symbol: str
    window_start: str
    window_end: str
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    trade_count: int


class TradeRecord(BaseModel):
    trade_id: str
    symbol: str
    price: float
    quantity: float
    side: str
    event_time: int


class LakeFile(BaseModel):
    key: str
    size_kb: float
    last_modified: str


class ConnectionManager:
    def __init__(self) -> None:
        self.active: set[WebSocket] = set()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.active.add(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        self.active.discard(websocket)

    async def broadcast(self, payload: dict[str, Any]) -> None:
        stale: list[WebSocket] = []
        for websocket in self.active:
            try:
                await websocket.send_json(payload)
            except Exception:
                stale.append(websocket)

        for websocket in stale:
            self.disconnect(websocket)


manager = ConnectionManager()


def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


async def price_broadcast_loop(pool: asyncpg.Pool) -> None:
    while True:
        try:
            if manager.active:
                async with pool.acquire() as conn:
                    rows = await conn.fetch(
                        """
                        SELECT symbol, close_price, window_end
                        FROM latest_ohlcv
                        ORDER BY symbol
                        """
                    )

                payload = {
                    "type": "price_update",
                    "prices": [
                        {
                            "symbol": row["symbol"],
                            "price": row["close_price"],
                            "updated_at": row["window_end"].isoformat(),
                        }
                        for row in rows
                    ],
                }
                await manager.broadcast(payload)
            await asyncio.sleep(PRICE_POLL_INTERVAL_SECONDS)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("price_broadcast_loop failed | retry_after_s=5")
            await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
    broadcaster = asyncio.create_task(price_broadcast_loop(pool))
    app.state.db_pool = pool
    app.state.broadcaster = broadcaster
    try:
        yield
    finally:
        broadcaster.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await broadcaster
        await pool.close()


app = FastAPI(
    title="Crypto Streaming Dashboard API",
    version="2.0.0",
    lifespan=lifespan,
)


def get_pool() -> asyncpg.Pool:
    return app.state.db_pool


@app.get("/healthz")
async def healthz():
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute("SELECT 1")
    return {"status": "ok"}


@app.get("/healthz/ready")
async def healthz_ready():
    """Readiness: Postgres (dashboard reads) and MinIO (lake) must both answer."""
    failures: dict[str, str] = {}
    pool = get_pool()
    try:
        async with pool.acquire() as conn:
            await conn.execute("SELECT 1")
    except Exception as exc:
        failures["postgres"] = str(exc)

    try:
        client = get_minio_client()
        client.head_bucket(Bucket=MINIO_BUCKET)
    except Exception as exc:
        failures["minio"] = str(exc)

    if failures:
        return JSONResponse(
            status_code=503,
            content={"status": "not_ready", "dependencies": failures},
        )
    return {"status": "ready", "postgres": "ok", "minio": "ok"}


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    if DASHBOARD_PATH.is_file():
        return FileResponse(DASHBOARD_PATH)
    return HTMLResponse("<h1>Dashboard not found.</h1>", status_code=500)


@app.get("/api/symbols", response_model=list[str])
async def list_symbols():
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT DISTINCT symbol FROM ohlcv ORDER BY symbol")
    return [row["symbol"] for row in rows]


@app.get("/api/ohlcv/{symbol}", response_model=list[OHLCVCandle])
async def get_ohlcv(symbol: str, limit: int = Query(default=60, ge=1, le=500)):
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                symbol,
                window_start,
                window_end,
                open_price,
                high_price,
                low_price,
                close_price,
                volume,
                trade_count
            FROM ohlcv
            WHERE symbol = $1
            ORDER BY window_start DESC
            LIMIT $2
            """,
            symbol.upper(),
            limit,
        )

    if not rows:
        raise HTTPException(status_code=404, detail=f"No OHLCV data for {symbol.upper()}")

    return [
        OHLCVCandle(
            symbol=row["symbol"],
            window_start=row["window_start"].isoformat(),
            window_end=row["window_end"].isoformat(),
            open_price=row["open_price"],
            high_price=row["high_price"],
            low_price=row["low_price"],
            close_price=row["close_price"],
            volume=row["volume"],
            trade_count=row["trade_count"],
        )
        for row in reversed(rows)
    ]


@app.get("/api/trades/{symbol}/latest", response_model=list[TradeRecord])
async def get_latest_trades(symbol: str, limit: int = Query(default=50, ge=1, le=200)):
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT trade_id, symbol, price, quantity, side, event_time
            FROM raw_trades
            WHERE symbol = $1
            ORDER BY event_time DESC
            LIMIT $2
            """,
            symbol.upper(),
            limit,
        )

    if not rows:
        raise HTTPException(status_code=404, detail=f"No trades found for {symbol.upper()}")

    return [TradeRecord(**dict(row)) for row in rows]


@app.get("/api/stats")
async def pipeline_stats():
    pool = get_pool()
    async with pool.acquire() as conn:
        total_candles = await conn.fetchval("SELECT COUNT(*) FROM ohlcv")
        total_trades = await conn.fetchval("SELECT COUNT(*) FROM raw_trades")
        active_symbols = await conn.fetchval("SELECT COUNT(DISTINCT symbol) FROM ohlcv")
        latest_candle = await conn.fetchval("SELECT MAX(window_end) FROM ohlcv")

    return {
        "total_candles": total_candles,
        "total_trades_stored": total_trades,
        "active_symbols": active_symbols,
        "latest_candle_at": latest_candle.isoformat() if latest_candle else None,
    }


@app.get("/api/lake/files", response_model=list[LakeFile])
async def list_lake_files(prefix: str = "trades/", max_keys: int = Query(default=50, ge=1, le=200)):
    try:
        client = get_minio_client()
        response = client.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix, MaxKeys=max_keys)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"MinIO unavailable: {exc}") from exc

    return [
        LakeFile(
            key=obj["Key"],
            size_kb=round(obj["Size"] / 1024, 2),
            last_modified=obj["LastModified"].isoformat(),
        )
        for obj in response.get("Contents", [])
    ]


@app.websocket("/ws/prices")
async def websocket_prices(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
