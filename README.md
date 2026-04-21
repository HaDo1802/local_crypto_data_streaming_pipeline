# Crypto Streaming Platform

Clean local streaming architecture using Binance crypto trades API, Kafka, Flink, MinIO, Postgres.

## Architecture

![architecture](image/architecture.png)

## Run

1. Copy the environment file.

```bash
cp .env.example .env
```

2. Start the full platform.

COMPOSE_BAKE helps optimize the spin up for containers in complex setup

```bash
COMPOSE_BAKE=true docker compose up -d --build
```

3. Open the UIs.

- Dashboard: `http://localhost:8000`
- Kafka UI: `http://localhost:8080`
- Flink UI: `http://localhost:8082`
- MinIO Console: `http://localhost:9001`

## Expected Output

After startup, the expected steady-state is:

- `generator` is publishing Binance trade events into Kafka
- `price-aggregator` is running as one continuous Flink job
- `lake-writer` is writing Parquet files into MinIO
- the dashboard is reading candles and latest trades from Postgres
- Kafka UI shows the `trades` topic and the `lake-writer` consumer group
- Flink UI shows a running job named `crypto-price-aggregator`

Useful check:

```bash
docker compose ps
```

You should see these services up:

- `kafka`
- `kafka-ui`
- `flink-jobmanager`
- `flink-taskmanager`
- `price-aggregator`
- `generator`
- `lake-writer`
- `postgres`
- `minio`
- `api`

## Screenshots

### Flink runtime after load stabilizes

![Flink Stable Runtime](image/flink.png)

### Dashboard

![Dashboard](image/dashboard.png)

### MinIO

![MinIO](image/minio.png)
