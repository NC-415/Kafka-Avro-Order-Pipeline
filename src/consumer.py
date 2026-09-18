"""
consumer.py
───────────
Avro consumer for the order pipeline.

Processing pipeline (per record)
─────────────────────────────────
  1. Deserialize  — AvroDeserializer; on failure → PermanentError → DLQ
  2. Validate     — business rules; on failure → PermanentError → DLQ
  3. Sink         — simulated downstream; may raise TransientError → retry
  4. Aggregate    — Welford running mean, global + per product
  5. Commit       — manual offset commit after DLQ flush or aggregation

Retry policy (§6)
──────────────────
  raw   = min(BACKOFF_CAP_S, BACKOFF_BASE_S × 2^(attempt−1))
  delay = uniform(0, raw)     # full jitter

  Jitter prevents the thundering-herd problem when multiple consumers fail
  simultaneously. Reference: AWS Architecture Blog — Exponential Backoff and
  Jitter (https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/)

DLQ record format (§8)
────────────────────────
  Value  : original value bytes, unchanged (byte-identical preservation)
  Headers: x-original-topic/partition/offset/key, x-failure-stage,
           x-error-type, x-error-message, x-attempts,
           x-consumer-group, x-failed-at

Delivery semantics (§7)
─────────────────────────
  enable.auto.commit=false.  Offset committed only after the record is
  aggregated OR after the DLQ write is flushed.  Result: at-least-once.

Usage
─────
  python -m src.consumer
"""

from __future__ import annotations

import random
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

import src.config as cfg
from src.errors import PermanentError, RetriesExhausted, TransientError
from src.stats import Stats
from src.utils import backoff_delay, validate
from src.dashboard import push_event, set_stats_ref


# ── Simulated downstream sink ─────────────────────────────────────────────────

def sink(order: dict) -> None:
    """
    Simulate writing the order to a downstream service (e.g. a database or API).

    Raises TransientError with probability SINK_FAILURE_RATE to exercise the
    retry path.  In production this would be the actual I/O call.
    """
    if random.random() < cfg.SINK_FAILURE_RATE:
        raise TransientError(
            f"Simulated sink failure for orderId={order['orderId']!r}"
        )
    # Successful sink — in a real system, persist the record here.


# ── DLQ writer ────────────────────────────────────────────────────────────────

def _build_dlq_headers(
    msg,
    stage: str,
    error: Exception,
    attempts: int,
) -> list[tuple[str, bytes]]:
    """
    Build the Kafka record header list for a DLQ message.

    Using the original bytes as the value and all diagnostics in headers
    means: (a) a poison pill can still be stored (it has no decodable payload),
    and (b) a fixed record can be replayed verbatim — see README §8.
    """
    original_key = msg.key() or b""
    return [
        ("x-original-topic",     msg.topic().encode()),
        ("x-original-partition", str(msg.partition()).encode()),
        ("x-original-offset",    str(msg.offset()).encode()),
        ("x-original-key",       original_key),
        ("x-failure-stage",      stage.encode()),
        ("x-error-type",         type(error).__name__.encode()),
        ("x-error-message",      str(error).encode()),
        ("x-attempts",           str(attempts).encode()),
        ("x-consumer-group",     cfg.CONSUMER_GROUP.encode()),
        ("x-failed-at",          datetime.now(timezone.utc).isoformat().encode()),
    ]


def send_to_dlq(
    dlq_producer: Producer,
    msg,
    stage: str,
    error: Exception,
    attempts: int,
) -> None:
    """
    Publish the original message bytes to the DLQ with diagnostic headers.

    Calls flush() before returning so the offset commit in the caller is only
    issued after the DLQ write is durably acknowledged.  If the DLQ write is
    not acknowledged the offset will not be committed and the record will be
    redelivered — see README §7.
    """
    headers = _build_dlq_headers(msg, stage, error, attempts)
    dlq_producer.produce(
        topic=cfg.DLQ_TOPIC,
        key=msg.key(),
        value=msg.value(),   # original bytes, unchanged
        headers=headers,
    )
    dlq_producer.flush()   # blocking — guarantees durability before offset commit


# ── Core processing pipeline ──────────────────────────────────────────────────

def process_message(
    msg,
    avro_deserializer: AvroDeserializer,
    dlq_producer: Producer,
    stats: Stats,
) -> tuple[str, int]:
    """
    Run the five-stage pipeline for one Kafka message.

    Returns
    -------
    (outcome, attempts)
        outcome  : "ok" | "dlq"
        attempts : number of sink attempts made (always 1 for permanent failures)
    """
    original_key = (msg.key() or b"").decode(errors="replace")

    # ── Stage 1: Deserialize ──────────────────────────────────────────────────
    try:
        order = avro_deserializer(
            msg.value(),
            SerializationContext(msg.topic(), MessageField.VALUE),
        )
    except Exception as exc:
        print(f"  [DLQ] deserialize  key={original_key[:16]}…  {exc!r}")
        send_to_dlq(dlq_producer, msg, "deserialize", PermanentError(str(exc)), 1)
        push_event({"type": "dlq", "stage": "deserialize", "key": original_key, "attempts": 1, "error": str(exc)})
        return "dlq", 1

    # ── Stage 2: Validate ─────────────────────────────────────────────────────
    try:
        validate(order)
    except PermanentError as exc:
        print(f"  [DLQ] validate     key={original_key[:16]}…  {exc}")
        send_to_dlq(dlq_producer, msg, "validate", exc, 1)
        push_event({"type": "dlq", "stage": "validate", "key": original_key, "attempts": 1, "error": str(exc)})
        return "dlq", 1

    # ── Stage 3: Sink (with retry) ────────────────────────────────────────────
    last_exc: Optional[TransientError] = None
    for attempt in range(1, cfg.MAX_ATTEMPTS + 1):
        try:
            sink(order)
            last_exc = None
            break   # success
        except TransientError as exc:
            last_exc = exc
            if attempt < cfg.MAX_ATTEMPTS:
                delay = backoff_delay(attempt)
                print(
                    f"  [retry {attempt}/{cfg.MAX_ATTEMPTS - 1}] "
                    f"key={original_key[:16]}…  "
                    f"sleeping {delay:.2f}s  {exc}"
                )
                push_event({"type": "retry", "attempt": attempt, "max_retries": cfg.MAX_ATTEMPTS - 1, "key": original_key, "delay": delay, "error": str(exc)})
                time.sleep(delay)
            else:
                exhausted = RetriesExhausted(exc, attempt)
                print(
                    f"  [DLQ] retries_exhausted  key={original_key[:16]}…  "
                    f"attempts={attempt}  {exc}"
                )
                send_to_dlq(dlq_producer, msg, "sink", exhausted, attempt)
                push_event({"type": "dlq", "stage": "sink", "key": original_key, "attempts": attempt, "error": str(exc)})
                return "dlq", attempt

    # ── Stage 4: Aggregate ────────────────────────────────────────────────────
    stats.update(order["price"], order["product"])
    print(
        f"  [ok]  key={original_key[:16]}…  "
        f"product={order['product']:<14} "
        f"price={order['price']:>8.2f}  "
        f"global_mean={stats.global_mean:>8.4f}  n={stats.global_count}"
    )
    push_event({
        "type": "ok",
        "key": original_key,
        "product": order["product"],
        "price": order["price"],
        "global_mean": stats.global_mean,
        "global_count": stats.global_count,
    })
    return "ok", 1


# ── Consumer loop ─────────────────────────────────────────────────────────────

def run() -> None:
    """Start the consumer loop.  Exits cleanly on Ctrl-C / SIGTERM."""
    schema_str = cfg.SCHEMA_PATH.read_text(encoding="utf-8")
    sr_client = SchemaRegistryClient(cfg.schema_registry_conf())
    avro_deserializer = AvroDeserializer(
        sr_client,
        schema_str,
        lambda obj, ctx: obj,   # identity: pass through as a dict
    )

    consumer = Consumer(cfg.consumer_conf())
    consumer.subscribe([cfg.ORDERS_TOPIC])

    # DLQ producer uses acks=all too; idempotence is not needed for one-off writes.
    dlq_conf = cfg.producer_conf()
    dlq_conf.pop("on_delivery", None)
    dlq_producer = Producer(dlq_conf)

    stats = Stats()
    set_stats_ref(stats)  # share with dashboard for /api/stats
    processed = dead_lettered = 0

    print(f"Consumer group : {cfg.CONSUMER_GROUP}")
    print(f"Subscribed to  : {cfg.ORDERS_TOPIC}")
    print(f"DLQ topic      : {cfg.DLQ_TOPIC}")
    print(f"Retry policy   : MAX_ATTEMPTS={cfg.MAX_ATTEMPTS}  "
          f"base={cfg.BACKOFF_BASE_S}s  cap={cfg.BACKOFF_CAP_S}s")
    print(f"Sink fail rate : {cfg.SINK_FAILURE_RATE:.0%}")
    print()
    print("Waiting for messages … (Ctrl-C to stop)\n")

    # Graceful shutdown flag
    _running = True

    def _shutdown(signum, frame):
        nonlocal _running
        _running = False

    signal.signal(signal.SIGTERM, _shutdown)

    try:
        while _running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            outcome, attempts = process_message(
                msg, avro_deserializer, dlq_producer, stats
            )

            if outcome == "ok":
                processed += 1
            else:
                dead_lettered += 1

            # ── Stage 5: Commit offset ────────────────────────────────────────
            # Only reached after the record is aggregated *or* after the DLQ
            # write has been flushed (flush() is blocking in send_to_dlq).
            consumer.commit(message=msg, asynchronous=False)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        dlq_producer.flush()

    print(f"\nShutdown.  processed={processed}  dead_lettered={dead_lettered}  "
          f"total={processed + dead_lettered}")
    stats.print_table()


def main() -> None:
    run()


if __name__ == "__main__":
    main()
