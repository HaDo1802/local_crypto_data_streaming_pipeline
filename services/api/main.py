"""
main.py — FastAPI backend for the crypto streaming dashboard.

Endpoints:
  GET  /                          → serves the dashboard HTML
  GET  /api/symbols               → list of active symbols
  GET  /api/ohlcv/{symbol}        → paginated OHLCV candles from Postgres
  GET  /api/trades/{symbol}/latest → latest N raw trades
  GET  /api/stats                 → pipeline-wide stats (candle count, etc.)
  GET  /api/lake/files            → list Parquet files in MinIO lakehouse
  WS   /ws/prices                 → live price tick stream via WebSocket
"""

import asyncio
import json
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator, List, Optional

import boto3
from botocore.client import Config
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import asyncpg

# ── Config ───────────────────────────────────────────────────────────────────
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://trader:trader@postgres:5432/crypto",
)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "lakehouse")

DASHBOARD_PATH = Path("/app/dashboard/index.html")

# ── Pydantic models ──────────────────────────────────────────────────────────
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


# ── DB connection pool ────────────────────────────────────────────────────────
_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    return _pool


# ── MinIO client ─────────────────────────────────────────────────────────────
def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


# ── WebSocket manager ─────────────────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self.active: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, data: dict):
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


manager = ConnectionManager()


# ── Lifespan: background price broadcaster ───────────────────────────────────
async def price_broadcast_loop():
    """Polls Postgres for the latest close price every 2 seconds and
    broadcasts to all connected WebSocket clients."""
    while True:
        try:
            pool = await get_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT symbol, close_price, window_end
                    FROM   latest_ohlcv
                    ORDER BY symbol
                """)
            if rows and manager.active:
                payload = {
                    "type": "price_update",
                    "prices": [
                        {
                            "symbol": r["symbol"],
                            "price": r["close_price"],
                            "updated_at": str(r["window_end"]),
                        }
                        for r in rows
                    ],
                }
                await manager.broadcast(payload)
        except Exception:
            pass  # DB may not be ready yet
        await asyncio.sleep(2)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    asyncio.create_task(price_broadcast_loop())
    yield
    global _pool
    if _pool:
        await _pool.close()


# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Crypto Streaming Dashboard API",
    version="1.0.0",
    lifespan=lifespan,
)


# ── Routes ────────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def dashboard():
    if DASHBOARD_PATH.exists():
        return FileResponse(DASHBOARD_PATH)
    return HTMLResponse("<h1>Dashboard not found. Mount services/dashboard at /app/dashboard.</h1>")


@app.get("/api/symbols", response_model=List[str])
async def list_symbols():
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT DISTINCT symbol FROM ohlcv ORDER BY symbol")
    return [r["symbol"] for r in rows]


@app.get("/api/ohlcv/{symbol}", response_model=List[OHLCVCandle])
async def get_ohlcv(
    symbol: str,
    limit: int = Query(default=60, le=500),
):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT symbol, window_start, window_end,
                   open_price, high_price, low_price, close_price,
                   volume, trade_count
            FROM   ohlcv
            WHERE  symbol = $1
            ORDER  BY window_start DESC
            LIMIT  $2
            """,
            symbol.upper(), limit,
        )
    if not rows:
        raise HTTPException(status_code=404, detail=f"No OHLCV data for {symbol}")
    return [
        OHLCVCandle(
            symbol=r["symbol"],
            window_start=str(r["window_start"]),
            window_end=str(r["window_end"]),
            open_price=r["open_price"],
            high_price=r["high_price"],
            low_price=r["low_price"],
            close_price=r["close_price"],
            volume=r["volume"],
            trade_count=r["trade_count"],
        )
        for r in reversed(rows)  # chronological order for charts
    ]


@app.get("/api/trades/{symbol}/latest", response_model=List[TradeRecord])
async def get_latest_trades(
    symbol: str,
    limit: int = Query(default=50, le=200),
):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT trade_id, symbol, price, quantity, side, event_time
            FROM   raw_trades
            WHERE  symbol = $1
            ORDER  BY event_time DESC
            LIMIT  $2
            """,
            symbol.upper(), limit,
        )
    if not rows:
        raise HTTPException(status_code=404, detail=f"No trades found for {symbol}")
    return [TradeRecord(**dict(r)) for r in rows]


@app.get("/api/stats")
async def pipeline_stats():
    pool = await get_pool()
    async with pool.acquire() as conn:
        ohlcv_count = await conn.fetchval("SELECT COUNT(*) FROM ohlcv")
        trade_count = await conn.fetchval("SELECT COUNT(*) FROM raw_trades")
        symbols = await conn.fetchval("SELECT COUNT(DISTINCT symbol) FROM ohlcv")
        latest_candle = await conn.fetchval(
            "SELECT MAX(window_end) FROM ohlcv"
        )
    return {
        "total_candles": ohlcv_count,
        "total_trades_stored": trade_count,
        "active_symbols": symbols,
        "latest_candle_at": str(latest_candle) if latest_candle else None,
    }


@app.get("/api/lake/files", response_model=List[LakeFile])
async def list_lake_files(prefix: str = "trades/", max_keys: int = Query(default=50, le=200)):
    try:
        client = get_minio_client()
        response = client.list_objects_v2(
            Bucket=MINIO_BUCKET,
            Prefix=prefix,
            MaxKeys=max_keys,
        )
        files = []
        for obj in response.get("Contents", []):
            files.append(LakeFile(
                key=obj["Key"],
                size_kb=round(obj["Size"] / 1024, 2),
                last_modified=obj["LastModified"].isoformat(),
            ))
        return files
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"MinIO unavailable: {exc}")


# ── WebSocket: live price stream ──────────────────────────────────────────────
@app.websocket("/ws/prices")
async def websocket_prices(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # Keep-alive — actual data pushed by broadcast loop
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
