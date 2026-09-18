# Makefile — every command used in the live demonstration (README §10)
#
# Demonstration flow (≈3 minutes):
#   Terminal 0:  start-kafka.bat              (keep running)
#   Terminal 1:  start-schema-registry.bat    (keep running)
#   Terminal 2:  make schema
#   Terminal 3:  make dashboard               (open http://localhost:8080)
#   Terminal 4:  make consumer                (keep running; Ctrl-C for stats)
#   Terminal 5:  make demo                    (40 records, seed 42, faults)
#   After demo:  make dlq                    (inspect DLQ records)
#
# Usage on Windows: install GNU Make (e.g. via winget install GnuWin32.Make)
# or run the commands inside the recipe manually.

PYTHON   ?= python
PIP      ?= pip

.PHONY: help up down clean install schema consumer producer demo dlq dashboard test

# ── Default target ────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  make up        Start Kafka + Schema Registry (native, no Docker)"
	@echo "  make down      Stop Kafka + Schema Registry"
	@echo "  make clean     Stop services AND delete Kafka logs (full reset)"
	@echo "  make install   pip install -r requirements.txt"
	@echo "  make schema    Register schemas/order.avsc with Schema Registry"
	@echo "  make consumer  Start the order consumer (Ctrl-C for final stats)"
	@echo "  make demo      Produce 40 records with seed=42, 12%% invalid, 8%% poison"
	@echo "  make dlq       Inspect dead-lettered records in orders.DLQ"
	@echo "  make dashboard Start the real-time web dashboard (http://localhost:8080)"
	@echo "  make test      Run the unit-test suite"
	@echo ""

# ── Kafka + Schema Registry (native) ─────────────────────────────────────────
up:
	@echo "Starting Kafka and Schema Registry..."
	@echo "Run these in separate terminals:"
	@echo "  Terminal 1: start-kafka.bat"
	@echo "  Terminal 2: start-schema-registry.bat"
	@echo ""
	@echo "Or use 'make up-kafka' and 'make up-sr' individually."

up-kafka:
	start-kafka.bat

up-sr:
	start-schema-registry.bat

down:
	stop-all.bat

clean:
	stop-all.bat
	@echo "Removing Kafka logs..."
	@if exist "D:\kafka\kafka-logs" rmdir /s /q "D:\kafka\kafka-logs"
	@echo "Clean complete. Run start-kafka.bat to re-format storage."

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

# ── Dashboard ─────────────────────────────────────────────────────────────────
dashboard:
	$(PYTHON) -m src.dashboard

# ── Tests ─────────────────────────────────────────────────────────────────────
test:
	$(PYTHON) -m pytest tests/ -v
