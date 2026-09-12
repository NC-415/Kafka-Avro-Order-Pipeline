# Makefile — every command used in the live demonstration (README §10)
#
# Four terminals, ≈3 minutes total:
#   Terminal 0:  make up       (keep running)
#   Terminal 1:  make schema
#   Terminal 2:  make consumer (keep running; Ctrl-C to see final stats)
#   Terminal 3:  make demo     (40 records, seed 42, faults injected)
#   After demo:  make dlq      (inspect dead-lettered records)
#   Browser:     http://localhost:8080
#
# Usage on Windows: install GNU Make (e.g. via winget install GnuWin32.Make)
# or run the commands inside the recipe manually.

PYTHON   ?= python
PIP      ?= pip

.PHONY: help up down clean install schema consumer producer demo dlq test

# ── Default target ────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  make up        Start Kafka, Schema Registry, Kafka UI, init topics"
	@echo "  make down      Stop and remove containers (keeps volumes)"
	@echo "  make clean     Stop containers AND remove volumes (full reset)"
	@echo "  make install   pip install -r requirements.txt"
	@echo "  make schema    Register schemas/order.avsc with Schema Registry"
	@echo "  make consumer  Start the order consumer (Ctrl-C for final stats)"
	@echo "  make demo      Produce 40 records with seed=42, 12%% invalid, 8%% poison"
	@echo "  make dlq       Inspect dead-lettered records in orders.DLQ"
	@echo "  make test      Run the unit-test suite"
	@echo ""

# ── Docker stack ──────────────────────────────────────────────────────────────
up:
	docker compose up -d
	@echo ""
	@echo "Stack is up. Kafka UI: http://localhost:8080"
	@echo "Waiting for topics to be created by init-topics …"
	@sleep 5

down:
	docker compose down

clean:
	docker compose down -v
	@echo "Volumes removed."

# ── Python dependencies ───────────────────────────────────────────────────────
install:
	$(PIP) install -r requirements.txt

# ── Schema registration ───────────────────────────────────────────────────────
# Uses curl to POST the schema directly to the Schema Registry REST API.
# The subject name is 'orders-value' (topic name + '-value').
schema:
	@echo "Registering schemas/order.avsc as subject 'orders-value' …"
	@SCHEMA=$$(cat schemas/order.avsc | python -c "import sys,json; print(json.dumps({'schema': sys.stdin.read()}))") && \
	curl -s -X POST \
	  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
	  --data "$$SCHEMA" \
	  http://localhost:8081/subjects/orders-value/versions | python -m json.tool
	@echo ""
	@echo "Registered schemas:"
	@curl -s http://localhost:8081/subjects | python -m json.tool

# ── Consumer (run in its own terminal) ───────────────────────────────────────
consumer:
	SINK_FAILURE_RATE=0.25 $(PYTHON) -m src.consumer

# ── Producer — normal run (no faults) ────────────────────────────────────────
producer:
	$(PYTHON) -m src.producer --count 20 --rate 2

# ── Demo run — reproducible fault injection (seed=42) ────────────────────────
# 40 records at 2/s, 12% business-invalid, 8% poison-pill
# SINK_FAILURE_RATE=0.25 is set for the consumer via the consumer target above.
demo:
	$(PYTHON) -m src.producer \
	  --count 40 \
	  --rate 2 \
	  --seed 42 \
	  --invalid-rate 0.12 \
	  --poison-rate 0.08

# ── DLQ inspector ─────────────────────────────────────────────────────────────
dlq:
	$(PYTHON) -m src.dlq_inspector

# ── Tests ─────────────────────────────────────────────────────────────────────
test:
	$(PYTHON) -m pytest tests/ -v
