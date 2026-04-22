# Docker Compose Architecture

## 1. General Architecture

```text
┌──────────────────────────── Docker Host (physical laptop) ─────────────────────────┐
│                                                                                    │
│  ┌──────────────┐   ┌──────────────┐   ┌─────────────┐   ┌──────────────────────┐  │
│  │   Kafka      │   │  Postgres    │   │   MinIO     │   │ generator /          │  │
│  │ (container)  │   │ (container)  │   │ (container) │   │ lake-writer / api    │  │
│  │ heap:512m    │   │ mem:512m     │   │ mem:256m    │   │ mem:128-192m         │  │
│  │ + page cache │   │              │   │ storage     │   │                      │  │
│  └──────────────┘   └──────────────┘   └─────────────┘   └──────────────────────┘  │
│                                                                                    │
│  ┌──────────────────────────── Flink Cluster ───────────────────────────────────┐  │
│  │                                                                              │  │
│  │   ┌──────────────────────────────┐                                           │  │
│  │   │   JobManager (container)     │                                           │  │
│  │   │                              │                                           │  │
│  │   │  process.size: 384m          │                                           │  │
│  │   │  checkpoint: 30s             │                                           │  │
│  │   │  state.backend: hashmap      │                                           │  │
│  │   │                              │                                           │  │
│  │   │  → schedules tasks           │                                           │  │
│  │   │  → coordinates checkpoints   │                                           │  │
│  │   └──────────────┬───────────────┘                                           │  │
│  │                  │ assigns tasks                                             │  │
│  │                  ▼                                                           │  │
│  │   ┌──────────────────────────────┐                                           │  │
│  │   │ TaskManager (container)      │                                           │  │
│  │   │                              │                                           │  │
│  │   │  process.size: 1g            │                                           │  │
│  │   │  slots: 3                    │                                           │  │
│  │   │  parallelism: 3              │                                           │  │
│  │   │                              │                                           │  │
│  │   │  → executes streaming tasks  │                                           │  │
│  │   │  → processes Kafka data      │                                           │  │
│  │   └──────────────────────────────┘                                           │  │
│  │                                                                              │  │
│  │   ┌──────────────────────────────┐                                           │  │
│  │   │ price-aggregator (container) │                                           │  │
│  │   │                              │                                           │  │
│  │   │ submits job → exits          │                                           │  │
│  │   └──────────────────────────────┘                                           │  │
│  │                                                                              │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                    │
└────────────────────────────────────────────────────────────────────────────────────┘
```
---

## 2. The Kafka Broker

### Estimate Data Volume
From Binance trade streams:
~5–20 events/sec per symbol (normal), higher during spikes
5 symbols → ~30–150 events/sec (steady), spikes higher
Event size: ~200–400 bytes (JSON)
Peak estimate:

100 events/sec × 400 bytes ≈ 40 KB/sec

Even with spikes:

~100 KB/sec range
==> 1 broker is sufficient
### Kafka Memory Model
Kafka is not a typical JVM application. Most JVM services are sized by their heap alone. Kafka is different because its read and write performance depends heavily on the **OS page cache**, which lives outside the JVM heap entirely.

```text
Kafka container (container)
│
├── JVM Heap (512m)
│     ├── request handling (producers / consumers)
│     ├── metadata (topics, partitions, offsets)
│     ├── KRaft controller state
│     └── internal buffers
│
└── OS Page Cache (remaining memory)
      ├── cached log segments (recent data)
      ├── serves reads from memory instead of disk
      └── critical for high throughput
```

Kafka writes data to disk, but:

OS automatically caches recent data in memory (page cache)
Consumers read from memory instead of disk
This is why Kafka achieves high throughput + low latency

If page cache is too small:

frequent disk reads
higher latency
consumer lag increases

The practical rule:

```
Total memory ≥ heap + (1–2× heap for page cache)
```

**One additional thing to be aware of with KRaft mode.** This broker is running both as a broker and a controller (`KAFKA_PROCESS_ROLES: broker,controller`). In older Kafka deployments you would have a separate ZooKeeper quorum. Here the controller metadata is embedded in the same JVM. The heap budget covers both roles.

---

## 3. The Flink Cluster — Separating Control Plane from Data Plane

Flink also has a clear separation between a control process and a data process. Similar to Drives/Cluster Manager and Executor in Spark, Flink has Job Manager, Task Manager, and Task Slot

```text
price-aggregator (container)
(one-shot submitter)
        │
        │  flink run ...
        ▼
┌──────────────────────────────────────────────────────────┐
│                    Flink Cluster                         │
│                                                          │
│  ┌──────────────────────────────┐                        │
│  │   JobManager (control plane) │      (BRAIN)           │
│  │                              │                        │
│  │  - receives job graph        │                        │
│  │  - builds execution plan     │                        │
│  │  - assigns tasks to slots    │                        │
│  │  - coordinates checkpoints   │                        │
│  │                              │                        │
│  │  process.size: 384m          │                        │
│  └──────────────┬───────────────┘                        │
│                 │ assigns subtasks                       │
│                 ▼                                        │
│  ┌────────────────────────────────────────────────────┐  │
│  │ TaskManager (data plane)          (MUSCLE)         │  │
│  │                                                    │  │
│  │  process.size: 1g                                  │  │
│  │  slots: 3                                          │  │
│  │                                                    │  │
│  │   ┌──────────┐  ┌──────────┐  ┌──────────┐         │  │
│  │   │  Slot 0  │  │  Slot 1  │  │  Slot 2  │         │  │
│  │   │          │  │          │  │          │         │  │
│  │   │ source   │  │ source   │  │ source   │         │  │
│  │   │ → window │  │ → window │  │ → window │         │  │
│  │   │ → sink   │  │ → sink   │  │ → sink   │         │  │
│  │   └──────────┘  └──────────┘  └──────────┘         │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
```

### Flink Architecture

**JobManager**

- Technical role: control plane
- Practical role: receives the job graph, builds the execution plan, schedules subtasks, and coordinates checkpoints
- Memory behavior: small footprint compared with the data plane because it manages metadata, not the live event stream

**TaskManager**

- Technical role: worker / data plane
- Practical role: executes operators, pulls from Kafka, maintains window state, and pushes to sinks
- Memory behavior: this is where most resources belong because this JVM does the actual data work

**Task Slot**

- Technical role: unit of execution capacity inside a TaskManager
- Practical role: gives Flink a way to place parallel subtasks inside the worker process
- Memory behavior: slots are logical execution units within the same JVM (not isolated processes); they share the TaskManager process, while some resources such as managed memory are accounted for in a slot-aware way

### Spark vs Flink — Architecture Mapping

| Spark                            | Flink                            |
| ---------------------------------| ---------------------------------|
| **Driver**                      | **JobManager**                  |
|          **Executor JVM**        | **TaskManager JVM**             |
| **Core / Task / Thread**                   | **Task Slot / Subtask**          |

**Spark:**

- Driver → Executors → Tasks (batch or micro-batch)

**Flink:**

- JobManager → TaskManagers → Slots → Operators (continuous stream)
---

### TaskManager Memory 

 Flink's memory model divides the process into named regions. Flink enforces these regions internally and will refuse to start if the numbers do not add up.

**Docker Compose Flink Configuration**

| **Property**                    | **Value**                    |
| -------------------------------| -----------------------------|
| `TASKMANAGER_MEMORY_PROCESS_SIZE` | `1g`                          |
| `TASKMANAGER_MEMORY_TASK_HEAP_SIZE` | `512m`                        |
| `TASKMANAGER_MEMORY_MANAGED_SIZE` | `160m`                        |
| `TASKMANAGER_MEMORY_NETWORK_MIN` | `96m`                         |
| `TASKMANAGER_MEMORY_NETWORK_MAX` | `96m`                         |
```text
taskmanager.memory.process.size = 1g  
│
├── task heap:         512m   ← set explicitly
│     Your operators run here.
│     Window state (hashmap backend) lives here.
│     All 3 active slots share this pool.
│     If this fills up → OutOfMemoryError → TM dies → job restarts from last checkpoint.
│     If one slot spikes → it can consume most heap
├── managed memory:    160m   ← set explicitly                                 
│     Off-heap region for RocksDB state and heavy sort operators.
│     We are using hashmap backend, so this is mostly unused.
│     It is still reserved because some operators (sort, hash-join) allocate here.
│     Keeping it above 0 avoids surprising failures if the job grows.
│  -----> **Evenly divided per slot: 160m / 3 slots ≈ ~53m per slot**
├── network buffers:    96m   ← set explicitly (min == max for stable sizing)
│     Shuffle and transport buffers between operators.
│     For a linear pipeline (source → window → sink) with no shuffles, this is low pressure.
│     Fixed min/max prevents runtime resizing surprises.
│
└── JVM overhead:     ~256m   ← Flink calculates this as (process.size - explicit regions)
      JVM metaspace, code cache, thread stacks, GC overhead.
      This is real memory the JVM consumes outside the heap regions.
      You cannot compress it away. If you do not leave room for it,
      the container OOMs at the OS level even when Flink thinks memory is fine.

Total explicitly set:   512 + 160 + 96 = 768m
Flink process budget:   1024m
JVM overhead room:      1024 - 768 = 256m
```
The managed memory is smaller and task heap is bigger is becasue in the set up, we choose `state.backend: hashmap` in Flink taskmanager set up, meaning most of the state data would be stored in the heap instead managed memory


### Flink’s Design Fits Continuous Streaming

Flink is designed for a job that stays alive and keeps state for as long as the stream is alive. That is fundamentally different from a micro-batch engine that processes a bounded input, finishes, and releases memory in Spark Streaming.

```text
continuous stream:
Kafka → Source → KeyBy → Window → Sink
           │
           └── state kept here per key / per window
data → flows through operators
state → accumulates inside TaskManager
```
- events pass through operators and are discarded
- state does not disappear after one record
- windows keep accumulating values
- keyed aggregates persist across time
- late events may reopen or update prior results
```text
batch-style job:
read data → process → finish → release memory
```
- data is processed one record at a time
- state persists across micro-batches but is processed in discrete intervals
- one-time processing and release

So in streaming, memory must remain bounded and predictable over time, not just sufficient for a single burst.

If state growth is not controlled, the failure mode is straightforward:

```text
more keys / longer windows / slower sink
        ↓
more state buffered in the TaskManager
        ↓
more heap pressure
        ↓
OutOfMemoryError
        ↓
TaskManager crash and job restart
```

That is exactly why the state backend choice matters so much in Flink.

In this project we chose:

```text
state.backend = hashmap
```

That means:

- state is stored primarily in JVM heap
- TaskManager heap is the main pressure point
- as data volume, key cardinality, or window length grows, heap demand grows with it

This is the main reason the task heap was increased in the current setup. With `hashmap`, growing state means growing heap pressure. If this job evolves toward much larger state, we would switch to RocksDB plus a different managed-memory profile.

### Checkpointing Is the Mechanism Behind Exactly-Once

Checkpointing is not just a recovery convenience. In Flink it is the mechanism that ties source position, operator state, and sink consistency together.

```text
Flink triggers checkpoint
  → all operators snapshot their state
  → Kafka source records current offset in the snapshot
  → JDBC sink flushes buffered rows and commits the transaction

If the TaskManager crashes after the checkpoint:
  → Flink restores from the last checkpoint
  → Kafka source seeks back to the offset in the snapshot
  → events between the snapshot and the crash are replayed
  → JDBC upserts by trade_id / (symbol, window_start)
  → no duplicate logical rows in Postgres
```

The important point is that Flink is not only remembering "where it was in Kafka." It is restoring the full streaming state of the job:

- source offsets
- window state
- buffered operator state
- sink progress

That is what makes Flink suitable for continuous, stateful processing rather than a sequence of isolated micro-batches.



### Spark vs Flink Memory Model

| Aspect | Spark | Flink |
| --- | --- | --- |
| Primary unit | Executor JVM | TaskManager with task slots |
| Heap usage | Shared heap across tasks in the executor | Task heap shared in the TaskManager |
| Managed/off-heap model | More pool-oriented | More explicitly partitioned and accounted |
| Isolation | Lower | Higher for managed-memory-style resources |
| Tradeoff | More flexible | More predictable |

The practical takeaway:

- Spark is more dynamic because tasks draw from a larger shared pool
- Flink is more structured because it explicitly accounts for execution regions inside the TaskManager
- in this project, the important shared pressure point is still the TaskManager heap, because the `hashmap` state backend keeps state in JVM heap
- that is why Flink memory tuning here is primarily about state stability over time, not just one-time task execution speed

---

### Slots, Parallelism, and Partition Alignment

`PARALLELISM` is the shared knob that ties this local setup together, for this project setup, we control our parallelism by the `PARALLELISM` environment variable in the `docker-compose.yaml`

```text
TaskManager : JVM process
Slots : execution capacity inside TaskManager
Parallelism = total slots = (# TaskManagers) × (slots per TM): total number of subtasks
```
In this setup
slots per TaskManager = 3
TaskManagers = 1
==> parallelism = 3
- 3 subtasks → run on 3 slots → inside 1 TaskManager

What parallelism actually means in Flink:
- **Partitions** = how Kafka splits the topic log
- **Slots** = execution capacity inside the TaskManager JVM
- **Parallelism** = how many operator subtasks Flink creates

The important relationship is:

- Kafka assigns topic partitions to **source subtasks**
- Flink schedules those subtasks into available **task slots**
- a `1:1` mapping only happens when partitions, subtasks, and slots are intentionally aligned

```text
trades topic
├── partition-0 ──► source subtask-0 ──► slot-0
├── partition-1 ──► source subtask-1 ──► slot-1
└── partition-2 ──► source subtask-2 ──► slot-2
```

This alignment is a convention in this project, not something Kafka or Flink enforces automatically.

Why it helps here:

- every Kafka partition has an active source subtask
- every source subtask has a slot to run in
- the topology is easier to reason about when debugging lag and backpressure

> For a simple single-TaskManager local pipeline, start with `partitions = parallelism = slots`, explaining why we set up `parallelism.default: ${PARALLELISM}` in `docker-compose.yaml`



### What Consumer Groups Actually Mean Here

Two independent consumer groups read the same `trades` topic:

```text
trades topic (3 partitions)
│
├── Consumer group: flink-price-aggregator   (Flink JDBC pipeline)
│     parallelism = 3 → 3 subtasks, each owning one partition
│     offset management: checkpoint-based (Flink controls this)
│
└── Consumer group: lake-writer              (Parquet writer)
      1 consumer process reading all 3 partitions
      offset management: Kafka auto-commit
```

Each group maintains its own offsets. Kafka treats them completely
independently — Flink can be fully caught up while lake-writer lags,
or vice versa. Neither affects the other. This is the standard
fan-out pattern: two downstream systems consume the same raw stream
without any coupling between them.

---

### Why `flink-price-aggregator` Does Not Appear in Kafka UI

You might notice that `flink-price-aggregator` is consuming from the `trades` topic but does not show up — or shows no offset movement — in Kafka UI at startup.
This is not a bug. It comes down to when each consumer commits offsets, and what mechanism drives that decision.

The two consumers use different offset commit strategies:

In the jobs/consumer/lakehouse/lake_writer.py, we have
```text
# lake_writer.py
consumer = Consumer({
    "enable.auto.commit": False,   # manual control
})
# ...
consumer.commit()   # called only after upload_parquet() succeeds
```
That leads to
```text
lake-writer (manual commit after MinIO flush)
  poll messages into memory buffer
  → accumulate until 500 records or 60 seconds
  → upload Parquet file to MinIO
  → only if upload succeeds: consumer.commit() to Kafka
  → Kafka UI shows progress in ~60s jumps
```

In the jobs/consumer/lakehouse/flink_price_aggregator.py, we have
```text
# price_aggregator.py
) WITH (
    'connector' = 'kafka',
    'properties.group.id' = 'flink-price-aggregator',
    'scan.startup.mode' = 'earliest-offset',
    # no 'properties.enable.auto.commit' = 'false' here
    # Flink manages this internally when checkpointing is on
)
```
and in docker-compose.yaml, we set up
```yaml
  # docker-compose.yaml — flink-taskmanager
execution.checkpointing.interval: 30s
execution.checkpointing.mode: EXACTLY_ONCE
```
that leads to
```text
flink-price-aggregator (checkpoint-based commit)
  poll messages
  → process events, accumulate window state internally
  → hold offsets inside Flink checkpoint state
  → every 30s: checkpoint triggers
      → snapshot window state to /tmp/flink-checkpoints volume
      → snapshot current Kafka partition offsets
      → JDBC sink flushes and commits to Postgres
      → on full checkpoint success: commit those same offsets to Kafka
  → Kafka UI shows progress in ~30s jumps, or nothing before
    the first checkpoint completes
```

Both consumers use manual offset commits — neither uses Kafka's built-in auto-commit. The difference is what triggers the commit:
- `lake-writer` commits after a successful MinIO upload
- `flink-price-aggregator` commits after a successful Flink checkpoint

---

#### Where Flink Actually Stores Offsets

This is the part worth being precise about. Flink maintains offsets in two places:

```text
Primary (source of truth for recovery):
  checkpoint state → flink_checkpoints volume
  → this is what Flink reads when restarting after a crash
  → stored alongside window state and operator state
  → completely invisible to Kafka UI

Secondary (observability side-effect):
  __consumer_offsets → Kafka's internal topic
  → written by Flink after each checkpoint completes
  → this is what Kafka UI reads to show consumer group progress
  → Flink does NOT use this for recovery — it is purely for external visibility
```

On recovery, Flink ignores `__consumer_offsets` entirely and seeks Kafka partitions back to the offsets stored in the last checkpoint. The commit to `__consumer_offsets` exists only so that tooling like Kafka UI and consumer lag monitors can observe Flink's progress from the outside.

---

#### Why Flink Couples Offsets to Checkpoints

Flink must guarantee three things simultaneously:

- no event is lost
- no event is processed twice
- operator state (window aggregates) and Kafka position are always consistent

To achieve this, a checkpoint snapshots all three atomically:

```text
checkpoint triggers every 30s
  → Flink snapshots partial OHLCV window state
  → Flink snapshots current Kafka partition offsets
  → JDBC sink flushes buffered rows, commits transaction to Postgres
  → only if ALL of the above succeed:
      offsets are written to checkpoint volume (primary)
      offsets are committed to __consumer_offsets (secondary)

If the TaskManager crashes before the next checkpoint:
  → Flink restores window state from the last checkpoint
  → Kafka source seeks back to the offsets in that checkpoint
  → events between the checkpoint and the crash are replayed
  → Postgres upserts by trade_id / (symbol, window_start) → idempotent
  → no duplicate candles, no missing candles
```
#### How Flink Manages Offsets in Flow
```text
T+0s    Flink starts consuming
        → flink-price-aggregator group: absent in Kafka UI
          (no checkpoint has completed yet, nothing committed)

T+30s   First checkpoint completes
        → Flink commits offsets to __consumer_offsets
        → flink-price-aggregator group: NOW appears in Kafka UI
        → offsets show the position as of checkpoint-1

T+60s   Second checkpoint completes
        → offsets update again
        → group shows progress, updates every 30s
```

If Flink committed offsets immediately like a normal consumer, a crash after the offset commit but before the checkpoint would mean: Kafka thinks those
events were processed, but Flink's window state never saw them. The OHLCV candle would be silently incomplete with no way to recover it.

---

#### Summary

| Consumer | Commit trigger | Written to `__consumer_offsets` | Source of truth for recovery |
|---|---|---|---|
| `lake-writer` | after MinIO upload succeeds | yes, immediately | `__consumer_offsets` |
| `flink-price-aggregator` | after checkpoint succeeds (~30s) | yes, as side-effect | checkpoint state volume |

You can check[Check the Flink Kafka documentation](docs/flink_kafka.md) for more details and visuals.