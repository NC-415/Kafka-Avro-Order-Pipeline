"""
dlq_inspector.py
────────────────
Reads the orders.DLQ topic from the beginning and prints every record with its
diagnostic headers and a best-effort payload decode.

Used in demo step 6 (make dlq) — see README §10.

Output format (per record)
──────────────────────────
  ─── DLQ record #N ─────────────────────────────────────────────
  Partition : 0   Offset: 12
  Headers:
    x-original-topic     : orders
    x-original-partition : 1
    x-original-offset    : 7
    x-original-key       : <uuid>
    x-failure-stage      : validate
    x-error-type         : PermanentError
    x-error-message      : Non-positive price -42.5 for orderId=...
    x-attempts           : 1
    x-consumer-group     : orders-consumer-group
    x-failed-at          : 2024-01-15T10:23:45.123456+00:00
  Payload (Avro):
    orderId : ...
    product : ...
    price   : ...
  ──────────────────────────────────────────────────────────────

Usage
─────
  python -m src.dlq_inspector
"""

from __future__ import annotations

import sys
from uuid import UUID

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

import src.config as cfg

_DIVIDER = "─" * 62


def _decode_header_value(v: bytes) -> str:
    try:
        return v.decode("utf-8")
    except Exception:
        return v.hex()


def _try_decode_payload(
    value: bytes | None,
    avro_deserializer: AvroDeserializer,
    topic: str,
) -> str:
    """
    Attempt to decode the record value as Avro.

    The DLQ stores the original bytes unchanged.  If they have the Confluent
    magic byte (0x00) we can decode them; if not (poison pill), we fall back to
    a hex dump.
    """
    if value is None:
        return "  <null value>"

    # Confluent wire format starts with magic byte 0x00
    if len(value) > 0 and value[0] == 0x00:
        try:
            order = avro_deserializer(
                value,
                SerializationContext(topic, MessageField.VALUE),
            )
            lines = []
            for k, v in order.items():
                lines.append(f"    {k:<10}: {v!r}")
            return "\n".join(lines)
        except Exception as exc:
            return f"    <Avro decode failed: {exc}>"
    else:
        # Poison pill — no magic byte; show as text or hex
        try:
            text = value.decode("utf-8")
            return f"    <raw bytes, UTF-8> {text[:200]}"
        except Exception:
            return f"    <raw bytes, hex>   {value[:80].hex()}"


def inspect() -> None:
    """Read orders.DLQ from the beginning and print every record."""
    schema_str = cfg.SCHEMA_PATH.read_text(encoding="utf-8")
    sr_client = SchemaRegistryClient(cfg.schema_registry_conf())
    avro_deserializer = AvroDeserializer(
        sr_client,
        schema_str,
        lambda obj, ctx: obj,
    )

    # Use a unique group so we always read from offset 0.
    import uuid as _uuid
    group_id = f"dlq-inspector-{_uuid.uuid4()}"

    consumer_conf = {
        **cfg.consumer_conf(),
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([cfg.DLQ_TOPIC])

    print(f"DLQ Inspector — reading {cfg.DLQ_TOPIC!r} from the beginning")
    print(f"Schema Registry: {cfg.SCHEMA_REGISTRY_URL}")
    print()

    count = 0
    # Give the consumer up to 10 s total to drain the topic.
    timeout_total = 10.0
    elapsed = 0.0
    poll_interval = 1.0

    try:
        while elapsed < timeout_total:
            msg = consumer.poll(timeout=poll_interval)
            elapsed += poll_interval

            if msg is None:
                # If we've seen at least one record and there's nothing new,
                # assume we've reached the end.
                if count > 0:
                    break
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    if count > 0:
                        break
                    continue
                print(f"Kafka error: {msg.error()}", file=sys.stderr)
                continue

            count += 1
            elapsed = 0.0   # reset timeout on each record

            key_str = (msg.key() or b"").decode(errors="replace")
            headers = dict(msg.headers() or [])

            print(_DIVIDER)
            print(f"DLQ record #{count}")
            print(f"  Partition : {msg.partition()}   Offset: {msg.offset()}")
            print(f"  Key       : {key_str[:36]}")
            print("  Headers:")
            for hk in [
                "x-original-topic",
                "x-original-partition",
                "x-original-offset",
                "x-original-key",
                "x-failure-stage",
                "x-error-type",
                "x-error-message",
                "x-attempts",
                "x-consumer-group",
                "x-failed-at",
            ]:
                raw_val = headers.get(hk, b"<missing>")
                print(f"    {hk:<28}: {_decode_header_value(raw_val)}")

            print("  Payload:")
            payload_str = _try_decode_payload(
                msg.value(), avro_deserializer, cfg.ORDERS_TOPIC
            )
            print(payload_str)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

    print(_DIVIDER)
    if count == 0:
        print("No records found in the DLQ.")
    else:
        print(f"\nTotal DLQ records: {count}")


def main() -> None:
    inspect()


if __name__ == "__main__":
    main()
