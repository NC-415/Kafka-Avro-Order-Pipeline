"""
config.py
─────────
Env-driven settings and confluent-kafka client configuration.

All tunables are read from the environment (or a .env file loaded at import
time via python-dotenv).  Production callers should set these via their
secrets manager; for local dev, copy .env.example to .env and edit.

References
----------
- Kafka producer configs: https://kafka.apache.org/documentation/#producerconfigs
- Kafka consumer configs: https://kafka.apache.org/documentation/#consumerconfigs
- confluent-kafka-python API: https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env from the project root (parent of src/).
_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_ROOT / ".env", override=False)


def _get(key: str, default: str) -> str:
    return os.environ.get(key, default)


# ── Connectivity ──────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP: str = _get("KAFKA_BOOTSTRAP", "localhost:9092")
SCHEMA_REGISTRY_URL: str = _get("SCHEMA_REGISTRY_URL", "http://localhost:8081")

# ── Topic names ───────────────────────────────────────────────────────────────

ORDERS_TOPIC: str = "orders"
DLQ_TOPIC: str = "orders.DLQ"

# ── Consumer ──────────────────────────────────────────────────────────────────

CONSUMER_GROUP: str = _get("CONSUMER_GROUP", "orders-consumer-group")

# ── Retry policy (§6) ─────────────────────────────────────────────────────────

MAX_ATTEMPTS: int = int(_get("MAX_ATTEMPTS", "4"))       # 1 initial + 3 retries
BACKOFF_BASE_S: float = float(_get("BACKOFF_BASE_S", "0.5"))
BACKOFF_CAP_S: float = float(_get("BACKOFF_CAP_S", "8.0"))

# ── Fault injection ───────────────────────────────────────────────────────────

SINK_FAILURE_RATE: float = float(_get("SINK_FAILURE_RATE", "0.0"))

# ── Schema path ───────────────────────────────────────────────────────────────

SCHEMA_PATH: Path = _ROOT / "schemas" / "order.avsc"


# ── confluent-kafka config dicts ──────────────────────────────────────────────

def producer_conf() -> dict:
    """
    Return a confluent-kafka producer config dict.

    acks=all + enable.idempotence=true:
        - Prevents silent loss on leader failover (acks=all).
        - Removes duplicates from the producer's own internal retries and
          prevents silent reordering (idempotence).
        - See README §7.
    """
    return {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks": "all",
        "enable.idempotence": True,
    }


def consumer_conf() -> dict:
    """
    Return a confluent-kafka consumer config dict.

    enable.auto.commit=false:
        The offset is committed manually, only *after* the record has been
        either aggregated or durably written to the DLQ (with a blocking flush).
        This gives at-least-once semantics — see README §7.
    """
    return {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }


def schema_registry_conf() -> dict:
    """Return a Schema Registry client config dict."""
    return {"url": SCHEMA_REGISTRY_URL}
