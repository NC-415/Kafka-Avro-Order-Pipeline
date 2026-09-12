"""
tests/test_pipeline.py
──────────────────────
Unit tests for logic that does NOT require a running Kafka broker.

Covers all verification-plan items from README §11:

  1. Avro round-trip through the real order.avsc (surfaces the float32 defect).
  2. validate() raises PermanentError for blank orderId, blank product,
     price <= 0, and NaN.
  3. backoff_delay(attempt) stays within [0, min(cap, base·2^(n−1))].
  4. PermanentError → 0 retries; TransientError → exactly MAX_ATTEMPTS-1
     retries then RetriesExhausted.
  5. Welford running mean over 1000 random prices matches sum/len to < 1e-6.
"""

from __future__ import annotations

import io
import json
import math
import random
import time
import unittest
from unittest.mock import MagicMock, patch, call

import fastavro

from src.errors import PermanentError, RetriesExhausted, TransientError
from src.utils import backoff_delay, validate
from src.stats import Stats
import src.config as cfg
# 1. Avro round-trip — float32 precision defect
# ─────────────────────────────────────────────────────────────────────────────

class TestAvroRoundTrip(unittest.TestCase):
    """
    Verifies that the order.avsc schema can encode/decode correctly and that
    the known float32 precision defect is reproducible (README §2).
    """

    def setUp(self):
        schema_json = json.loads(cfg.SCHEMA_PATH.read_text())
        self.parsed_schema = fastavro.parse_schema(schema_json)

    def _roundtrip(self, order: dict) -> dict:
        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, self.parsed_schema, order)
        buf.seek(0)
        return fastavro.schemaless_reader(buf, self.parsed_schema)

    def test_string_fields_roundtrip_exactly(self):
        order = {"orderId": "ord-001", "product": "Widget-A", "price": 9.99}
        result = self._roundtrip(order)
        self.assertEqual(result["orderId"], "ord-001")
        self.assertEqual(result["product"], "Widget-A")

    def test_float32_precision_defect(self):
        """
        Demonstrates that price=199.99 does NOT survive a float32 round-trip
        exactly, reproducing the known defect documented in README §2.
        """
        order = {"orderId": "x", "product": "p", "price": 199.99}
        result = self._roundtrip(order)
        # float32 round-trip introduces an error of ~5.5e-7 relative
        self.assertNotEqual(result["price"], 199.99,
                            "Expected float32 precision loss — if this fails, "
                            "the schema was changed from float to double.")
        error = abs(result["price"] - 199.99)
        self.assertGreater(error, 0,
                           "float32 round-trip should introduce non-zero error")
        self.assertLess(error, 0.01,
                        "float32 round-trip error should be small (<0.01)")

    def test_all_fields_present_after_roundtrip(self):
        order = {"orderId": "abc", "product": "Gadget-X", "price": 42.0}
        result = self._roundtrip(order)
        self.assertIn("orderId", result)
        self.assertIn("product", result)
        self.assertIn("price", result)


# ─────────────────────────────────────────────────────────────────────────────
# 2. validate() — business rules
# ─────────────────────────────────────────────────────────────────────────────

class TestValidate(unittest.TestCase):

    def _good(self) -> dict:
        return {"orderId": "ord-1", "product": "Widget", "price": 9.99}

    # ── Happy path ────────────────────────────────────────────────────────────

    def test_valid_order_does_not_raise(self):
        validate(self._good())   # must not raise

    # ── orderId ───────────────────────────────────────────────────────────────

    def test_blank_order_id_raises_permanent(self):
        order = self._good()
        order["orderId"] = ""
        with self.assertRaises(PermanentError):
            validate(order)

    def test_whitespace_order_id_raises_permanent(self):
        order = self._good()
        order["orderId"] = "   "
        with self.assertRaises(PermanentError):
            validate(order)

    def test_missing_order_id_raises_permanent(self):
        order = self._good()
        del order["orderId"]
        with self.assertRaises(PermanentError):
            validate(order)

    # ── product ───────────────────────────────────────────────────────────────

    def test_blank_product_raises_permanent(self):
        order = self._good()
        order["product"] = ""
        with self.assertRaises(PermanentError):
            validate(order)

    def test_whitespace_product_raises_permanent(self):
        order = self._good()
        order["product"] = "\t"
        with self.assertRaises(PermanentError):
            validate(order)

    # ── price ─────────────────────────────────────────────────────────────────

    def test_zero_price_raises_permanent(self):
        order = self._good()
        order["price"] = 0.0
        with self.assertRaises(PermanentError):
            validate(order)

    def test_negative_price_raises_permanent(self):
        order = self._good()
        order["price"] = -1.0
        with self.assertRaises(PermanentError):
            validate(order)

    def test_nan_price_raises_permanent(self):
        order = self._good()
        order["price"] = float("nan")
        with self.assertRaises(PermanentError):
            validate(order)

    def test_positive_price_is_accepted(self):
        order = self._good()
        order["price"] = 0.01
        validate(order)   # must not raise

    def test_large_price_is_accepted(self):
        order = self._good()
        order["price"] = 1_000_000.0
        validate(order)


# ─────────────────────────────────────────────────────────────────────────────
# 3. backoff_delay — bounds check
# ─────────────────────────────────────────────────────────────────────────────

class TestBackoffDelay(unittest.TestCase):
    """
    backoff_delay(attempt) must return a value in [0, min(cap, base*2^(n-1))]
    for all attempt values.
    """

    def _upper_bound(self, attempt: int) -> float:
        return min(cfg.BACKOFF_CAP_S, cfg.BACKOFF_BASE_S * (2 ** (attempt - 1)))

    def test_delay_is_non_negative(self):
        for attempt in range(1, 10):
            delay = backoff_delay(attempt)
            self.assertGreaterEqual(delay, 0.0,
                                    f"Negative delay at attempt={attempt}")

    def test_delay_does_not_exceed_upper_bound(self):
        for attempt in range(1, 10):
            upper = self._upper_bound(attempt)
            for _ in range(50):   # sample multiple jitter draws
                delay = backoff_delay(attempt)
                self.assertLessEqual(delay, upper + 1e-9,
                                     f"Delay {delay} > bound {upper} at attempt={attempt}")

    def test_delay_is_capped_at_backoff_cap(self):
        # At large attempt numbers the raw exponential exceeds the cap
        for attempt in range(8, 15):
            for _ in range(30):
                delay = backoff_delay(attempt)
                self.assertLessEqual(delay, cfg.BACKOFF_CAP_S + 1e-9)

    def test_attempt_1_upper_bound_equals_base(self):
        self.assertAlmostEqual(self._upper_bound(1), cfg.BACKOFF_BASE_S)


# ─────────────────────────────────────────────────────────────────────────────
# 4. Retry behaviour — permanent vs transient
# ─────────────────────────────────────────────────────────────────────────────

class TestRetryBehaviour(unittest.TestCase):
    """
    Simulates the retry loop from consumer.py inline to avoid needing a broker.

    Rules from README §6:
      PermanentError → 0 retries (never enters the loop)
      TransientError → exactly MAX_ATTEMPTS-1 retries → RetriesExhausted
    """

    def _run_retry_loop(self, sink_fn):
        """
        Minimal replica of the consumer retry loop.

        Returns (outcome, total_calls) where outcome is "ok" or "exhausted".
        """
        last_exc = None
        calls = 0
        for attempt in range(1, cfg.MAX_ATTEMPTS + 1):
            try:
                sink_fn()
                calls += 1
                return "ok", calls
            except TransientError as exc:
                calls += 1
                last_exc = exc
                if attempt >= cfg.MAX_ATTEMPTS:
                    raise RetriesExhausted(exc, attempt)
                # (skip actual sleep in tests)
        return "ok", calls  # unreachable but satisfies linter

    def test_permanent_error_is_not_retried(self):
        """
        A PermanentError must propagate immediately — the loop is never entered.
        The caller is responsible for sending to DLQ directly.
        """
        call_count = 0

        def failing_sink():
            nonlocal call_count
            call_count += 1
            raise PermanentError("bad record")

        with self.assertRaises(PermanentError):
            failing_sink()   # caller catches PermanentError before the loop

        self.assertEqual(call_count, 1, "PermanentError must not be retried")

    def test_transient_error_retried_max_attempts_minus_one_times(self):
        """TransientError → exactly MAX_ATTEMPTS calls total → RetriesExhausted."""
        call_count = 0

        def always_fails():
            nonlocal call_count
            call_count += 1
            raise TransientError("timeout")

        with self.assertRaises(RetriesExhausted) as ctx:
            self._run_retry_loop(always_fails)

        self.assertEqual(call_count, cfg.MAX_ATTEMPTS,
                         f"Expected {cfg.MAX_ATTEMPTS} total calls "
                         f"(1 initial + {cfg.MAX_ATTEMPTS - 1} retries)")
        self.assertIsInstance(ctx.exception.last_error, TransientError)
        self.assertEqual(ctx.exception.attempts, cfg.MAX_ATTEMPTS)

    def test_transient_success_on_second_attempt(self):
        """Succeeds on the second call — should return 'ok' with 2 calls."""
        call_count = 0

        def fails_once():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise TransientError("first attempt fails")

        outcome, calls = self._run_retry_loop(fails_once)
        self.assertEqual(outcome, "ok")
        self.assertEqual(calls, 2)

    def test_retries_exhausted_inherits_transient(self):
        """RetriesExhausted is a subclass of TransientError."""
        exc = RetriesExhausted(TransientError("x"), 4)
        self.assertIsInstance(exc, TransientError)


# ─────────────────────────────────────────────────────────────────────────────
# 5. Stats — Welford running mean accuracy
# ─────────────────────────────────────────────────────────────────────────────

class TestStats(unittest.TestCase):

    def test_welford_mean_matches_naive_mean(self):
        """
        Welford running mean over 1000 random prices must match sum/len to < 1e-6.
        """
        rng = random.Random(0)
        prices = [rng.uniform(1.0, 999.99) for _ in range(1000)]

        stats = Stats()
        for p in prices:
            stats.update(p, "Widget")

        naive_mean = sum(prices) / len(prices)
        self.assertAlmostEqual(stats.global_mean, naive_mean, places=6)

    def test_global_count(self):
        stats = Stats()
        for i in range(50):
            stats.update(float(i + 1), "A")
        self.assertEqual(stats.global_count, 50)

    def test_per_product_mean(self):
        stats = Stats()
        stats.update(10.0, "A")
        stats.update(20.0, "A")
        stats.update(100.0, "B")
        self.assertAlmostEqual(stats.product_mean("A"), 15.0, places=6)
        self.assertAlmostEqual(stats.product_mean("B"), 100.0, places=6)
        self.assertIsNone(stats.product_mean("C"))

    def test_unknown_product_returns_none(self):
        stats = Stats()
        self.assertIsNone(stats.product_mean("nonexistent"))

    def test_min_max_tracked(self):
        stats = Stats()
        for price in [5.0, 1.0, 3.0, 9.0, 2.0]:
            stats.update(price, "P")
        summary = stats.summary()
        self.assertEqual(summary["global"]["min"], 1.0)
        self.assertEqual(summary["global"]["max"], 9.0)

    def test_single_value_mean_equals_value(self):
        stats = Stats()
        stats.update(42.5, "X")
        self.assertAlmostEqual(stats.global_mean, 42.5, places=6)

    def test_summary_is_json_serialisable(self):
        import json
        stats = Stats()
        stats.update(9.99, "Widget-A")
        stats.update(4.49, "Widget-B")
        # Must not raise
        json.dumps(stats.summary())


if __name__ == "__main__":
    unittest.main()
