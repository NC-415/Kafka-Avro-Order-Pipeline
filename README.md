# Kafka + Avro Order Pipeline — Design & Implementation Plan

Assignment: build a Kafka system that produces and consumes **order messages** using
**Avro serialization**, with **real-time aggregation (running average of prices)**,
**retry logic** for temporary failures, and a **Dead Letter Queue (DLQ)** for
permanently failed messages. Demonstrated live, submitted as a Git repository.

This document is the plan. It states what will be built, why each decision was made,
what the decision costs, and how it will be demonstrated and defended.

---

## 1. Scope and non-goals

**In scope**

| Requirement | How it is met |
|---|---|
| Avro serialization | Confluent Schema Registry + `AvroSerializer` / `AvroDeserializer`, wire format with magic byte + schema ID |
| Real-time aggregation | Incremental (Welford) running mean, global and per product, updated per record |
| Retry logic | Exponential backoff with full jitter, bounded attempt budget, only for retryable failures |
| Dead Letter Queue | Separate `orders.DLQ` topic, original bytes preserved, failure context in record headers |
| Live demonstration | Fault injection flags on the producer + a DLQ inspector tool + Kafka UI |
| Git repository | Structure in §3, `.gitignore`, native Windows batch scripts |

**Explicit non-goals** (state these up front rather than be caught by them)

- Not exactly-once end-to-end. The pipeline is **at-least-once** — see §7.
- Aggregation state is in-memory and not fault tolerant — see §9.
- Single broker, replication factor 1. Correct for a laptop, wrong for production.
- No authentication/TLS. `PLAINTEXT` only.

---

## 2. Correction to the brief before anything is built

The brief specifies `price` as Avro **`float`**. In Avro, `float` is IEEE-754
**binary32** — about 7 significant decimal digits
([Avro specification, primitive types](https://avro.apache.org/docs/1.11.1/specification/#primitive-types)).
This is the wrong type for money. Verified round-trip through the actual schema:

```
in : 199.99
out: 199.99000549316406
```

The error is per-record, and a running average over thousands of records
accumulates it. **The plan is to implement `float` exactly as specified** — the
brief is the requirement — but to document the defect and the fix, because an
examiner asking "is this schema correct?" is a predictable question.

The correct production schema would be:

```json
{
  "name": "price",
  "type": { "type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2 }
}
```

Second-best and much simpler: `long` storing minor units (cents), which removes
floating point from the money path entirely.

---

## 3. Repository structure

```
kafka-orders-avro/
├── start-kafka.bat           Starts Kafka (KRaft mode) and creates topics
├── start-schema-registry.bat Starts Confluent Schema Registry
├── stop-all.bat              Stops Kafka and Schema Registry
├── Makefile                  every command used in the live demo
├── requirements.txt
├── .env.example              all tunables, documented
├── .gitignore
├── README.md
├── schemas/
│   └── order.avsc            the contract — single source of truth
└── src/
    ├── config.py             env-driven settings, producer/consumer configs
    ├── errors.py             TransientError / PermanentError / RetriesExhausted
    ├── stats.py              incremental running average, global + per product
    ├── producer.py           Avro producer with fault injection flags
    ├── consumer.py           deserialize → validate → retry → DLQ → aggregate → commit
    └── dlq_inspector.py      reads the DLQ and decodes it for the demo
```

Rationale: the schema lives in its own directory because it is a **contract**, not
code. Producer and consumer both load the same file, so they cannot drift.

---

## 4. Data model

`schemas/order.avsc`, namespace `com.assignment.orders`, record `Order`:

| Field | Avro type | Notes |
|---|---|---|
| `orderId` | `string` | Also used as the **message key** |
| `product` | `string` | Aggregation grouping key |
| `price` | `float` | Per brief; see §2 |

**Message key = `orderId`.** Kafka guarantees ordering only within a partition, and
the default partitioner hashes the key, so keying by `orderId` puts every event for
one order on one partition. An unkeyed record would be round-robined and lose that
guarantee. ([Kafka design — message delivery and ordering](https://kafka.apache.org/documentation/#semantics))

**Schema Registry subject:** `orders-value`, compatibility level **BACKWARD** — a new
schema must be readable by consumers still on the old one, which is the safe default
when consumers are upgraded after producers.
([Schema Registry compatibility types](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html))

---

## 5. Topology

```mermaid
flowchart LR
    subgraph PRODUCER["producer.py"]
        direction TB
        B[Build Order] --> I[Inject Faults]
    end

    subgraph KAFKA["Kafka"]
        direction TB
        O_TOPIC[("Topic: orders\n(3 partitions)")]
        DLQ_TOPIC[("Topic: orders.DLQ\n(original bytes + headers)")]
    end

    subgraph CONSUMER["consumer.py"]
        direction TB
        D[1. Deserialize] --> V[2. Validate]
        V -- "Valid" --> S[3. Sink]
        S -- "TransientError" --> R{"Retry with\nBackoff"}
        R -- "Success" --> S
        S -- "Success" --> A[4. Aggregate]
        A --> C[5. Commit Offset (manual)]
    end

    subgraph STATS["stats.py"]
        AVG["Running Average\n(global + per product)"]
    end

    subgraph INSPECTOR["dlq_inspector.py"]
        DI[Read and Decode]
    end

    %% Data Flow
    PRODUCER -- "Avro wire format" --> O_TOPIC
    O_TOPIC --> D
    
    V -- "PermanentError" --> DLQ_TOPIC
    R -- "Permanent failure" --> DLQ_TOPIC
    
    A -. "update" .-> AVG
    
    DLQ_TOPIC --> DI
```

Topics created by the `init-topics` service, never by auto-creation
(`KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`), so partition counts are explicit:

| Topic | Partitions | RF | Retention |
|---|---|---|---|
| `orders` | 3 | 1 | broker default (7 days) |
| `orders.DLQ` | 3 | 1 | 14 days — a DLQ that expires before anyone reads it is useless |

### Logic Flow

```text
                ┌───────────────────┐
                │     Producer      │
                │  Creates orders   │
                │  (Avro payload)   │
                └─────────┬─────────┘
                          │
                          ▼
                ┌───────────────────┐
                │       Kafka       │
                │                   │
                │   orders topic    │
                └─────────┬─────────┘
                          │
                          ▼
                ┌───────────────────┐
                │     Consumer      │
                │  Processes order  │
                └─────────┬─────────┘
                          │
              ┌───────────┴───────────┐
              ▼                       ▼
         Successfully             Processing
          processed                 fails
              │                       │
              │                       ▼
              │                Transient Error?
              │               (e.g., HTTP 503)
              │                 ┌─────┴─────┐
              │                 ▼           ▼
              │                Yes          No (Permanent)
              │                 │           │
              │                 ▼           │
              │          Retry processing   │
              │           with backoff      │
              │                 │           │
              │           ┌─────┴─────┐     │
              │           ▼           ▼     │
              │        Success     Failure  │
              │           │           │     │
              └───────────┤           └─────┤
                          ▼                 ▼
                  Calculate running    Dead Letter Queue
                  average of prices      (orders.DLQ)
                          │                 │
                          ▼                 ▼
                    Commit offset     Commit offset
```

---

## 6. Failure taxonomy — the central design decision

Every failure is classified **before** deciding what to do with it:

| Class | Examples | Action | Why |
|---|---|---|---|
| `PermanentError` | negative price, blank product, un-deserializable bytes ("poison pill") | **Straight to DLQ, zero retries** | Retrying cannot make a negative price positive. Retrying wastes 4× the work and blocks the partition 4× longer. |
| `TransientError` | sink timeout, HTTP 503/429, connection reset | **Retry with backoff, then DLQ** | The message is fine; the environment is temporarily unhealthy. |

Collapsing these into one "retry everything" path is the classic failure mode: a
single malformed record stalls its partition indefinitely. This distinction is the
thing to be able to explain in the viva.

### Retry policy

```
raw   = min(BACKOFF_CAP_S, BACKOFF_BASE_S × 2^(attempt−1))
delay = uniform(0, raw)          # full jitter
```

Defaults: `MAX_ATTEMPTS=4` (1 initial + 3 retries), `base=0.5 s`, `cap=8 s`.

Jitter is not decoration. Without it, all consumers that failed at the same instant
retry at the same instant and hammer the recovering service in synchronised waves.
([AWS Architecture Blog — Exponential Backoff and Jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/))

**Constraint to be aware of:** retries block the poll loop. Worst case here is
0.5+1+2 ≈ 3.5 s of sleeping plus jitter, far under the default
`max.poll.interval.ms` of 300 000 ms. If the cap or attempt count were raised much,
the consumer would be evicted from the group mid-record and the batch would be
reprocessed. ([Kafka consumer configs](https://kafka.apache.org/documentation/#consumerconfigs))

### In-place retry vs. retry-topic — why in-place was chosen

| | In-place blocking retry (chosen) | Retry-topic / delay-topic pattern |
|---|---|---|
| Ordering | Preserved within the partition | Broken — retried records overtake fresh ones |
| Head-of-line blocking | Yes, partition stalls during backoff | No, partition keeps moving |
| Infrastructure | None beyond the two topics | `orders.retry.5s`, `.30s`, `.5m` … plus their consumers |
| Consumer-group eviction risk | Real, bounded by `max.poll.interval.ms` | None |
| Fit for this assignment | Retries are sub-second and the brief asks for a DLQ, not a tiered retry ladder | Over-engineered here |

The retry-topic pattern is the right answer at scale
([Uber Engineering — reliable reprocessing and dead letter queues](https://www.uber.com/en-US/blog/reliable-reprocessing/));
it is deliberately out of scope, not overlooked.

---

## 7. Delivery semantics

**Producer side:** `acks=all` + `enable.idempotence=true`. Idempotence removes
duplicates caused by the producer's own internal retries and prevents silent
reordering; `acks=all` prevents loss on leader failover.
([Kafka producer configs](https://kafka.apache.org/documentation/#producerconfigs))

**Consumer side:** `enable.auto.commit=false`, and the offset is committed
**only after** the record has either been aggregated or durably written to the DLQ
(with a blocking `flush`). If the DLQ write is not acknowledged, the offset is not
committed and the record is redelivered.

The resulting guarantee is **at-least-once**. On a crash between processing and
commit, a record is reprocessed and counted twice in the running average. Making
this exactly-once requires the aggregation state and the offset commit to move in
one transaction — i.e. Kafka Streams with `processing.guarantee=exactly_once_v2`.
([Kafka Streams — processing guarantees](https://kafka.apache.org/documentation/streams/core-concepts#streams_processing_guarantee))
Out of scope; stated rather than hidden.

---

## 8. DLQ record format

The DLQ carries the **original value bytes, unchanged**, with all diagnostics in
Kafka record headers:

| Header | Content |
|---|---|
| `x-original-topic` / `-partition` / `-offset` / `-key` | exact provenance for replay |
| `x-failure-stage` | `deserialize` \| `validate` \| `sink` |
| `x-error-type`, `x-error-message` | exception class and message |
| `x-attempts` | how many times it was tried |
| `x-consumer-group`, `x-failed-at` | who failed it, when (UTC ISO-8601) |

Why not wrap the payload in a `DlqEnvelope` Avro record?

1. A poison pill **cannot be deserialized**, therefore cannot be re-serialized into
   an envelope. An envelope scheme breaks on exactly the failure class a DLQ exists
   to catch.
2. Byte-identical preservation means a fixed record can be replayed into `orders`
   verbatim.
3. Headers are metadata and are searchable without decoding the body.

---

## 9. Aggregation design

Update rule: `mean += (x − mean) / n` (Welford), global and per product, plus
count/min/max.

Chosen over a running sum ÷ count because the accumulator in the naive form grows
without bound and loses precision relative to the individual prices — and this
pipeline is already on 32-bit floats (§2), so the numerically better option is taken
where it costs nothing. ([Knuth, *TAOCP* Vol. 2, §4.2.2](https://www-cs-faculty.stanford.edu/~knuth/taocp.html);
[Welford 1962](https://doi.org/10.1080/00401706.1962.10490022))

**Limitation, stated deliberately:** the state is a Python object in one process. If
the consumer restarts, the running average restarts. It is also unbounded-window —
there is no time window, it averages everything ever seen. Production answers: a
changelog-backed Kafka Streams `KTable`, or an external key-value store keyed by
product.

---

## 10. Live demonstration runbook

This system runs natively on Windows without Docker. You will need multiple terminal windows (e.g., PowerShell or Command Prompt). Total run time ≈ 3 minutes.

### Step 1: Start Infrastructure
Open **Terminal 1** and start Kafka:
```cmd
.\start-kafka.bat
```
*(Wait until you see "Topics created: orders, orders.DLQ")*

Open **Terminal 2** and start Schema Registry:
```cmd
.\start-schema-registry.bat
```
*(Wait until you see "Starting Schema Registry on http://localhost:8081")*

### Step 2: Register Schema & Start Dashboard
Open **Terminal 3** and run:
```cmd
python register-schema.py
.\run-dashboard.bat
```
Then, open your web browser to **http://localhost:8080** to view the real-time monitoring dashboard.

### Step 3: Start Consumer
Open **Terminal 4** and start the consumer:
```cmd
.\run-consumer.bat
```
*(It will sit idle, waiting for messages)*

### Step 4: Run Producer (Fault Injection Demo)
Open **Terminal 5** and run the producer with a fixed seed and fault injection:
```cmd
.\run-demo.bat
```
*(This produces 40 records: ~12% business-invalid, ~8% poison pills. It also simulates transient downstream failures causing retries.)*

### Step 5: Observe & Inspect
1. **Watch the dashboard (http://localhost:8080):** You will see live messages flowing, the real-time aggregation table updating, and DLQ/Retry counts increasing.
2. **Watch the consumer (Terminal 4):** You will see `[ok]`, `[retry]`, and `[DLQ]` log lines.
3. **Inspect the Dead Letter Queue (DLQ):** Once finished, in Terminal 5 run:
   ```cmd
   make dlq
   ```
   *(Or click "Load DLQ Records" in the web dashboard to see exactly why messages failed, along with their headers and decoded payloads).*

### Step 6: Cleanup
When finished, open a new terminal or press `Ctrl+C` in Terminal 3 and run:
```cmd
.\stop-all.bat
```

`--seed 42` makes the demo reproducible, so the same failure pattern appears every
run — no live-demo roulette.

**Fault injection flags**

- `--invalid-rate` → schema-valid but business-invalid (negative price / blank
  product). Exercises `PermanentError` → DLQ with **zero** retries.
- `--poison-rate` → raw non-Avro JSON bytes with no magic byte. Exercises the
  deserialization failure path → DLQ.
- `SINK_FAILURE_RATE` (env, default 0.25) → probability the simulated downstream
  raises `TransientError`. Exercises the retry path.

---

## 11. Verification plan

Logic that does not need a broker is checked directly:

- Avro round-trip through the real `order.avsc` (this is what surfaced the float32
  defect in §2).
- `validate()` raises `PermanentError` for blank `orderId`, blank `product`,
  `price <= 0`, and `NaN`.
- `backoff_delay(attempt)` stays within `[0, min(cap, base·2^(n−1))]` for all n.
- `PermanentError` is retried **zero** times; `TransientError` is retried exactly
  `MAX_ATTEMPTS − 1` times and then raises `RetriesExhausted`.
- Running mean over 1000 random prices matches `sum/len` to < 1e-6.

End-to-end checks needing the stack: `orders` count = `processed + dead_lettered`;
consumer-group lag returns to 0; DLQ record count matches the consumer's
`dead_lettered` counter.

---

## 12. Known limitations

| # | Limitation | Consequence | Production fix |
|---|---|---|---|
| 1 | `price` is `float32` | ~1e-5 error per price, accumulates in the average | `bytes`+`decimal` logical type, or `long` cents |
| 2 | At-least-once | Duplicate counting after a crash | Kafka Streams `exactly_once_v2` |
| 3 | In-memory aggregation | State lost on restart | Changelog-backed state store |
| 4 | Blocking retry | Head-of-line blocking during backoff | Retry-topic ladder |
| 5 | RF=1, single broker | No fault tolerance | RF≥3, `min.insync.replicas=2` |
| 6 | No DLQ replay tool | Manual recovery only | Replay CLI reading `x-original-*` headers |
| 7 | `PLAINTEXT`, no auth | Anyone on the network can read/write | TLS + SASL + ACLs |

Items 1–4 are the ones most likely to be asked about.

---

## 13. Submission checklist

- [ ] Repo initialised, meaningful commit history (not one "final" commit)
- [ ] `start-kafka.bat` and `start-schema-registry.bat` work on a native Windows machine
- [ ] `README.md` (this file) committed
- [ ] `schemas/order.avsc` matches the brief field-for-field
- [ ] Demo runbook rehearsed once end-to-end with `--seed 42`
- [ ] Screenshot or recording of the DLQ inspector output
- [ ] `.env.example` committed, `.env` git-ignored

---

## 14. References

- [Apache Avro 1.11 specification](https://avro.apache.org/docs/1.11.1/specification/)
- [Apache Kafka documentation — design & configuration](https://kafka.apache.org/documentation/)
- [Kafka Streams — core concepts and processing guarantees](https://kafka.apache.org/documentation/streams/core-concepts)
- [Confluent Schema Registry — schema evolution and compatibility](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html)
- [Confluent wire format for serialized messages](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format)
- [`confluent-kafka-python` API documentation](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)
- [KRaft — Kafka without ZooKeeper](https://developer.confluent.io/learn/kraft/)
- [AWS Architecture Blog — Exponential Backoff and Jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/)
- [Uber Engineering — Reliable Reprocessing and Dead Letter Queues](https://www.uber.com/en-US/blog/reliable-reprocessing/)
- [Welford, B. P. (1962), *Technometrics* 4(3), 419–420](https://doi.org/10.1080/00401706.1962.10490022)
