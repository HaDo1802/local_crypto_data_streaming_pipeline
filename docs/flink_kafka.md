```text
KAFKA SIDE                              FLINK SIDE
(lake-writer consumer)                  (price-aggregator job)
──────────────────────────────────────────────────────────────────────

Kafka Broker                            Flink JobManager
┌─────────────────────┐                 ┌──────────────────────────┐
│  topic: trades      │                 │  Checkpoint Coordinator  │
│  partition 0 ──────────────────────────────────────────────────► │
│  partition 1 ──────────────────────────────────────────────────► │
│  partition 2 ──────────────────────────────────────────────────► │
│                     │                 │  every 30s:              │
│  __consumer_offsets │                 │  sends barrier signal ──►│
│  ┌──────────────┐   │                 └──────────────────────────┘
│  │ lake-writer  │   │                           │
│  │ part0: 4821  │   │                           │ checkpoint barrier
│  │ part1: 3902  │   │                           │ flows downstream
│  │ part2: 5103  │◄──│──────────┐                ▼
│  └──────────────┘   │          │      Flink TaskManager
│                     │          │      ┌──────────────────────────┐
│  ┌──────────────┐   │          │      │  Slot 0 (partition 0)    │
│  │flink-price-  │   │          │      │  KafkaSource state:      │
│  │aggregator    │   │          │      │    offset: 48291         │
│  │ (absent /    │   │          │      │  WindowOperator state:   │
│  │  zero lag)   │   │          │      │   BTCUSDT partial window │
│  │ part0: 0     │   │          │      │    [67432, 67431, ...]   │
│  │ part1: 0     │   │          │      │  JDBCSink state:         │
│  │ part2: 0     │   │          │      │    pending upserts       │
│  └──────────────┘   │          │      ├──────────────────────────┤
└─────────────────────┘          │      │  Slot 1 (partition 1)    │
         ▲                       │      │  KafkaSource state:      │
         │                       │      │    offset: 39847         │
         │                       │      │  WindowOperator state:   │
         │                       │      │   ETHUSDT partial window │
┌────────┴────────────┐          │      └──────────────────────────┘
│  lake-writer        │          │                 │
│  Python process     │          │                 │ on barrier received:
│                     │          │                 │ all 3 slots flush state
│  enable.auto.commit │          │                 ▼
│    = False          │          │      ┌───────────────────────────┐
│                     │          │      │  Checkpoint Storage       │
│  T+0s:  poll msgs   │          │      │  /tmp/flink-checkpoints   │
│  T+60s: 500 records │          │      │  (Docker named volume)    │
│         OR 60s hit  │          │      │                           │
│  → upload Parquet   │          │      │  checkpoint-42/           │
│    to MinIO         │          │      │  ├── part-0-offset: 48291 │
│  → if upload OK:    │          │      │  ├── part-1-offset: 39847 │
│    consumer.commit()│──────────┘      │  ├── part-2-offset: 51203 │
│    → writes to      │                 │  ├── window-state-slot0   │
│      __consumer     │                 │  ├── window-state-slot1   │
│      _offsets       │                 │  └── jdbc-sink-state      │
│  → if upload fails: │                 │                           │
│    skip commit,     │                 │  checkpoint-43/ (30s later)
│    retry next cycle │                 │  ├── part-0-offset: 51847 │
└─────────────────────┘                 │  └── ...                  │
                                        └───────────────────────────┘
```
NOTE: Flink commits offsets to __consumer_offsets only after each
successful checkpoint (~every 30s). Before the first checkpoint
completes, the flink-price-aggregator group is absent from Kafka UI.
After that it appears and updates in 30s jumps.

Flink uses checkpoint state (/tmp/flink-checkpoints) for recovery —
not __consumer_offsets. The Kafka commits are observability-only.

```text
WHAT KAFKA UI READS:
┌─────────────────────────────────────────────────────────────────┐
│  reads __consumer_offsets topic only                            │
│                                                                 │
│  lake-writer group:                                             │
│    partition 0  latest: 5200  committed: 4821  lag: 379  ✓      │
│    partition 1  latest: 4100  committed: 3902  lag: 198  ✓      │
│    partition 2  latest: 5400  committed: 5103  lag: 297  ✓      │
│                                                                 │
│  flink-price-aggregator group:                                  │
│    absent before first checkpoint (~30s after startup)          │
│    after that: updates every 30s, offsets lag behind lake-writer│
│    because Flink only commits on checkpoint completion          │
│                                                                 │
│  Kafka UI can see Flink — but only in 30s snapshots,            │
│  not in real time like lake-writer.                             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

````
CRASH RECOVERY BEHAVIOR:
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│  lake-writer crashes mid-cycle (after poll, before flush)       │
│    → restarts, reads from last committed offset (e.g. 4821)     │
│    → replays messages that arrived since last commit            │
│    → those messages may be uploaded to MinIO twice              │
│    → guarantee: at-least-once                                   │
│                                                                 │
│  lake-writer crashes after upload, before consumer.commit()     │
│    → restarts, re-reads the same batch from Kafka               │
│    → uploads a duplicate Parquet file to MinIO                  │
│    → this is the known at-least-once trade-off                  │
│                                                                 │
│  flink job crashes mid-window                                   │
│    → JobManager restores checkpoint-42 entirely                 │
│    → resets Kafka read position to offsets in checkpoint        │
│      (48291, 39847, 51203) — ignores __consumer_offsets         │
│    → discards any partial window state after checkpoint-42      │
│    → replays events from those offsets forward                  │
│    → Postgres JDBC upserts by trade_id / (symbol, window_start) │
│    → no duplicate rows in ohlcv or raw_trades tables            │
│    → guarantee: exactly-once                                    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```