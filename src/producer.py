"""
producer.py
───────────
Avro producer for the order pipeline.

Produces N order records to the `orders` topic, keyed by orderId, using the
Confluent Schema Registry wire format (magic byte 0x00 + 4-byte schema ID +
Avro binary payload).

Fault injection
───────────────
  --invalid-rate   Fraction of records that are schema-valid but business-invalid
                   (negative price or blank product).  Exercises the PermanentError
                   → DLQ path in the consumer with zero retries.

  --poison-rate    Fraction of records that are raw JSON bytes with no Avro magic
                   byte.  The consumer's AvroDeserializer will fail, raising a
                   PermanentError → DLQ (poison-pill path).

Producer config
───────────────
  acks=all + enable.idempotence=true — see README §7 and config.py.

Usage
─────
  python -m src.producer                       # 40 records, 2/s, no faults
  python -m src.producer --count 40 --seed 42 --invalid-rate 0.12 --poison-rate 0.08
"""

from __future__ import annotations

import argparse
import io
import json
import random
import time
import uuid
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

import src.config as cfg

# ── Products used for random order generation ─────────────────────────────────

_PRODUCTS = [
    "Widget-A",
    "Widget-B",
    "Gadget-X",
    "Gadget-Y",
    "Doohickey",
    "Thingamajig",
]

_PRICE_RANGE = (0.01, 999.99)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load_schema_str() -> str:
    """Read the Avro schema file and return its JSON string."""
    return cfg.SCHEMA_PATH.read_text(encoding="utf-8")


def _make_poison_pill(order_id: str) -> bytes:
    """
    Return raw JSON bytes with no Avro magic byte.

    The consumer's AvroDeserializer expects the Confluent wire format
    (0x00 + 4-byte schema ID + Avro binary).  Sending plain JSON will cause
    an immediate deserialization failure → PermanentError → DLQ.
    """
    payload = {"orderId": order_id, "product": "POISON", "price": 0.0}
    return json.dumps(payload).encode()


def _delivery_report(err, msg) -> None:
    """Callback invoked by the producer's poll loop after each message is acknowledged."""
    if err:
        print(f"  [DELIVER ERROR] {err}")
    else:
        print(
            f"  [ok] topic={msg.topic()} partition={msg.partition()} "
            f"offset={msg.offset()} key={msg.key().decode()}"
        )


# ── Main ──────────────────────────────────────────────────────────────────────

def produce(
    count: int = 40,
    rate: float = 2.0,
    seed: int | None = None,
    invalid_rate: float = 0.0,
    poison_rate: float = 0.0,
) -> None:
    """
    Produce `count` order records to the `orders` topic.

    Parameters
    ----------
    count:        number of records to produce
    rate:         records per second
    seed:         RNG seed for reproducible fault injection
    invalid_rate: fraction of records that are business-invalid (negative price
                  or blank product) — valid Avro bytes, semantically wrong
    poison_rate:  fraction of records that are raw JSON (no magic byte)
    """
    rng = random.Random(seed)
    schema_str = _load_schema_str()

    sr_client = SchemaRegistryClient(cfg.schema_registry_conf())
    avro_serializer = AvroSerializer(
        sr_client,
        schema_str,
        lambda obj, ctx: obj,   # identity: dict is already the right shape
    )
    string_serializer = StringSerializer("utf_8")

    raw_producer = Producer(cfg.producer_conf())

    print(f"Producing {count} records to '{cfg.ORDERS_TOPIC}' "
          f"at {rate}/s  seed={seed}  "
          f"invalid_rate={invalid_rate:.0%}  poison_rate={poison_rate:.0%}")
    print(f"Schema Registry: {cfg.SCHEMA_REGISTRY_URL}")
    print()

    interval = 1.0 / rate if rate > 0 else 0.0
    produced = invalid = poison = 0

    for i in range(count):
        order_id = str(uuid.uuid4())
        roll = rng.random()

        # ── Poison pill (raw bytes, no magic byte) ────────────────────────────
        if roll < poison_rate:
            raw_bytes = _make_poison_pill(order_id)
            raw_producer.produce(
                topic=cfg.ORDERS_TOPIC,
                key=string_serializer(order_id),
                value=raw_bytes,
                on_delivery=_delivery_report,
            )
            print(f"  [poison] #{i+1:03d}  key={order_id[:8]}…")
            poison += 1

        # ── Business-invalid record (valid Avro, but semantically wrong) ──────
        elif roll < poison_rate + invalid_rate:
            # Randomly choose: negative price or blank product
            if rng.random() < 0.5:
                order = {
                    "orderId": order_id,
                    "product": rng.choice(_PRODUCTS),
                    "price": float(-rng.uniform(0.01, 100.0)),   # negative
                }
                reason = f"price={order['price']:.2f}"
            else:
                order = {
                    "orderId": order_id,
                    "product": "",   # blank — fails validate()
                    "price": float(rng.uniform(*_PRICE_RANGE)),
                }
                reason = "product=''"

            raw_producer.produce(
                topic=cfg.ORDERS_TOPIC,
                key=string_serializer(order_id),
                value=avro_serializer(
                    order,
                    SerializationContext(cfg.ORDERS_TOPIC, MessageField.VALUE),
                ),
                on_delivery=_delivery_report,
            )
            print(f"  [invalid] #{i+1:03d}  key={order_id[:8]}…  {reason}")
            invalid += 1

        # ── Normal valid record ───────────────────────────────────────────────
        else:
            order = {
                "orderId": order_id,
                "product": rng.choice(_PRODUCTS),
                "price": float(rng.uniform(*_PRICE_RANGE)),
            }
            raw_producer.produce(
                topic=cfg.ORDERS_TOPIC,
                key=string_serializer(order_id),
                value=avro_serializer(
                    order,
                    SerializationContext(cfg.ORDERS_TOPIC, MessageField.VALUE),
                ),
                on_delivery=_delivery_report,
            )
            print(
                f"  [send]   #{i+1:03d}  key={order_id[:8]}…  "
                f"product={order['product']:<14} price={order['price']:>8.2f}"
            )
            produced += 1

        raw_producer.poll(0)

        if interval > 0 and i < count - 1:
            time.sleep(interval)

    # Flush and wait for all delivery callbacks.
    raw_producer.flush()

    print()
    print("─" * 50)
    print(f"Done.  sent={produced}  invalid={invalid}  poison={poison}  "
          f"total={count}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Produce Avro-encoded order messages to Kafka."
    )
    parser.add_argument("--count", type=int, default=40,
                        help="Number of records to produce (default: 40)")
    parser.add_argument("--rate", type=float, default=2.0,
                        help="Records per second (default: 2.0)")
    parser.add_argument("--seed", type=int, default=None,
                        help="RNG seed for reproducible fault injection")
    parser.add_argument("--invalid-rate", type=float, default=0.0,
                        dest="invalid_rate",
                        help="Fraction of business-invalid records (default: 0.0)")
    parser.add_argument("--poison-rate", type=float, default=0.0,
                        dest="poison_rate",
                        help="Fraction of raw-JSON poison-pill records (default: 0.0)")
    args = parser.parse_args()

    produce(
        count=args.count,
        rate=args.rate,
        seed=args.seed,
        invalid_rate=args.invalid_rate,
        poison_rate=args.poison_rate,
    )


if __name__ == "__main__":
    main()
