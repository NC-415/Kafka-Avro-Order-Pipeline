"""
dashboard.py
────────────
Real-time web dashboard for the Kafka order pipeline.

Replaces the Docker-only Kafka UI with a custom FastAPI app that provides:
  - REST API for topic metadata, consumer lag, DLQ records, aggregation stats
  - WebSocket for live message streaming to the browser
  - Serves a single-page HTML/CSS/JS frontend

Usage
─────
  python -m src.dashboard          # http://localhost:8080
  DASHBOARD_PORT=9090 python -m src.dashboard
"""

from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.admin import AdminClient, ClusterMetadata
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

import src.config as cfg

# ── Configuration ─────────────────────────────────────────────────────────────

DASHBOARD_PORT = int(os.environ.get("DASHBOARD_PORT", "8080"))
STATIC_DIR = Path(__file__).parent / "static"

# ── Shared event bus ──────────────────────────────────────────────────────────
# The consumer pushes events here; the WebSocket broadcasts them to browsers.

_event_log: deque[dict] = deque(maxlen=500)  # ring buffer of recent events
_ws_clients: set[WebSocket] = set()
_event_lock = threading.Lock()


def push_event(event: dict) -> None:
    """
    Called by consumer.py to push a processing event to the dashboard.

    Thread-safe. Non-blocking. If no dashboard is running, this is a no-op
    (the deque just accumulates in memory until maxlen evicts old entries).
    """
    event.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
    with _event_lock:
        _event_log.append(event)
    # Schedule broadcast to WebSocket clients (fire-and-forget)
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(_broadcast(event))
    except RuntimeError:
        pass  # no event loop — dashboard not running, ignore


async def _broadcast(event: dict) -> None:
    """Send an event to all connected WebSocket clients."""
    dead = set()
    msg = json.dumps(event)
    for ws in _ws_clients.copy():
        try:
            await ws.send_text(msg)
        except Exception:
            dead.add(ws)
    _ws_clients.difference_update(dead)


# ── Kafka helpers ─────────────────────────────────────────────────────────────

def _get_admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": cfg.KAFKA_BOOTSTRAP})


def _get_topic_info() -> dict[str, Any]:
    """Get topic metadata and message counts."""
    admin = _get_admin()
    metadata: ClusterMetadata = admin.list_topics(timeout=10)

    topics_info = {}
    for topic_name in [cfg.ORDERS_TOPIC, cfg.DLQ_TOPIC]:
        topic_meta = metadata.topics.get(topic_name)
        if not topic_meta:
            topics_info[topic_name] = {"exists": False}
            continue

        partitions = list(topic_meta.partitions.keys())
        # Get high watermarks to estimate message count
        consumer = Consumer({
            "bootstrap.servers": cfg.KAFKA_BOOTSTRAP,
            "group.id": f"dashboard-count-{int(time.time())}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        total_messages = 0
        for pid in partitions:
            lo, hi = consumer.get_watermark_offsets(
                TopicPartition(topic_name, pid), timeout=5
            )
            total_messages += hi - lo
        consumer.close()

        topics_info[topic_name] = {
            "exists": True,
            "partitions": len(partitions),
            "messages": total_messages,
        }

    return topics_info


def _get_consumer_lag() -> dict[str, Any]:
    """Get consumer group lag for the orders consumer group."""
    admin = _get_admin()
    try:
        # Use a consumer to get committed offsets
        consumer = Consumer({
            "bootstrap.servers": cfg.KAFKA_BOOTSTRAP,
            "group.id": cfg.CONSUMER_GROUP,
            "enable.auto.commit": False,
        })

        metadata = admin.list_topics(timeout=10)
        topic_meta = metadata.topics.get(cfg.ORDERS_TOPIC)
        if not topic_meta:
            consumer.close()
            return {"error": "Topic not found"}

        total_lag = 0
        partition_lag = []
        for pid in topic_meta.partitions:
            tp = TopicPartition(cfg.ORDERS_TOPIC, pid)
            committed = consumer.committed([tp], timeout=5)
            lo, hi = consumer.get_watermark_offsets(tp, timeout=5)

            committed_offset = committed[0].offset if committed[0].offset >= 0 else 0
            lag = hi - committed_offset
            total_lag += lag
            partition_lag.append({
                "partition": pid,
                "committed": committed_offset,
                "end": hi,
                "lag": lag,
            })

        consumer.close()
        return {
            "group": cfg.CONSUMER_GROUP,
            "total_lag": total_lag,
            "partitions": partition_lag,
        }
    except Exception as exc:
        return {"error": str(exc)}


def _read_dlq_records(limit: int = 100) -> list[dict]:
    """Read DLQ records from the beginning."""
    schema_str = cfg.SCHEMA_PATH.read_text(encoding="utf-8")
    sr_client = SchemaRegistryClient(cfg.schema_registry_conf())
    avro_deserializer = AvroDeserializer(
        sr_client, schema_str, lambda obj, ctx: obj,
    )

    import uuid as _uuid
    consumer = Consumer({
        "bootstrap.servers": cfg.KAFKA_BOOTSTRAP,
        "group.id": f"dashboard-dlq-{_uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([cfg.DLQ_TOPIC])

    records = []
    empty_polls = 0
    try:
        while len(records) < limit and empty_polls < 3:
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                empty_polls += 1
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    empty_polls += 1
                    continue
                continue
            empty_polls = 0

            key_str = (msg.key() or b"").decode(errors="replace")
            headers = {}
            for hk, hv in (msg.headers() or []):
                try:
                    headers[hk] = hv.decode("utf-8")
                except Exception:
                    headers[hk] = hv.hex()

            # Try to decode payload
            payload = None
            value = msg.value()
            if value and len(value) > 0 and value[0] == 0x00:
                try:
                    payload = avro_deserializer(
                        value,
                        SerializationContext(cfg.ORDERS_TOPIC, MessageField.VALUE),
                    )
                except Exception:
                    pass
            elif value:
                try:
                    payload = {"raw": value.decode("utf-8")}
                except Exception:
                    payload = {"hex": value[:80].hex()}

            records.append({
                "partition": msg.partition(),
                "offset": msg.offset(),
                "key": key_str[:36],
                "headers": headers,
                "payload": payload,
            })
    finally:
        consumer.close()

    return records


# ── Shared stats reference ────────────────────────────────────────────────────
# The consumer sets this so the dashboard can read aggregation state.

_stats_ref = None


def set_stats_ref(stats) -> None:
    """Called by consumer.py to share its Stats instance with the dashboard."""
    global _stats_ref
    _stats_ref = stats


# ── FastAPI app ───────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield

app = FastAPI(title="Kafka Order Pipeline Dashboard", lifespan=lifespan)


@app.get("/", response_class=HTMLResponse)
async def index():
    """Serve the dashboard SPA."""
    html_path = STATIC_DIR / "index.html"
    if html_path.exists():
        return FileResponse(html_path, media_type="text/html")
    return HTMLResponse("<h1>Dashboard</h1><p>static/index.html not found</p>")


@app.get("/api/topics")
async def api_topics():
    """Topic metadata and message counts."""
    try:
        return JSONResponse(_get_topic_info())
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)


@app.get("/api/lag")
async def api_lag():
    """Consumer group lag."""
    try:
        return JSONResponse(_get_consumer_lag())
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)


@app.get("/api/dlq")
async def api_dlq():
    """Read all DLQ records."""
    try:
        records = _read_dlq_records()
        return JSONResponse({"count": len(records), "records": records})
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)


@app.get("/api/stats")
async def api_stats():
    """Current aggregation state."""
    if _stats_ref is None:
        return JSONResponse({
            "status": "no_consumer",
            "message": "Consumer not connected. Start the consumer first.",
        })
    return JSONResponse(_stats_ref.summary())


@app.get("/api/events")
async def api_events():
    """Recent event log (last 500 events)."""
    with _event_lock:
        events = list(_event_log)
    return JSONResponse({"count": len(events), "events": events})


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    """WebSocket for real-time event streaming."""
    await ws.accept()
    _ws_clients.add(ws)
    try:
        # Send recent events as catch-up
        with _event_lock:
            for event in _event_log:
                await ws.send_text(json.dumps(event))
        # Keep alive — client disconnect triggers exception
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        _ws_clients.discard(ws)


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    import uvicorn
    print(f"Starting dashboard on http://localhost:{DASHBOARD_PORT}")
    print(f"Static files: {STATIC_DIR}")
    uvicorn.run(app, host="0.0.0.0", port=DASHBOARD_PORT, log_level="info")


if __name__ == "__main__":
    main()
