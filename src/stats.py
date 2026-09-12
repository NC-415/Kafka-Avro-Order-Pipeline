"""
stats.py
────────
Incremental (Welford) running mean, global and per product.

Why Welford instead of running_sum / count?
    The naive sum can grow without bound and loses precision relative to
    individual prices — especially important here because the pipeline already
    operates on 32-bit floats (README §2).  Welford's one-pass algorithm keeps
    the accumulator numerically stable at no extra cost.

    Reference: Knuth, TAOCP Vol. 2, §4.2.2; Welford 1962,
    Technometrics 4(3) doi:10.1080/00401706.1962.10490022

Update rule (per bucket):
    mean  +=  (x − mean) / n
    min    =  min(min, x)
    max    =  max(max, x)
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Dict


@dataclass
class Bucket:
    """Running statistics for a single aggregation bucket (global or per-product)."""

    count: int = 0
    mean: float = 0.0
    min_val: float = float("inf")
    max_val: float = float("-inf")

    def update(self, x: float) -> None:
        """Incorporate one new price value using the Welford algorithm."""
        self.count += 1
        self.mean += (x - self.mean) / self.count
        if x < self.min_val:
            self.min_val = x
        if x > self.max_val:
            self.max_val = x

    def as_dict(self) -> dict:
        return {
            "count": self.count,
            "mean": round(self.mean, 6),
            "min": round(self.min_val, 6) if self.count else None,
            "max": round(self.max_val, 6) if self.count else None,
        }


class Stats:
    """
    Aggregation state: one global bucket and one bucket per product.

    This state lives in memory and is NOT fault-tolerant.  If the consumer
    process restarts, all counts reset to zero.  See README §9 for the
    production fix (changelog-backed KTable or external KV store).
    """

    def __init__(self) -> None:
        self._global: Bucket = Bucket()
        self._by_product: Dict[str, Bucket] = {}

    def update(self, price: float, product: str) -> None:
        """
        Incorporate one record into both the global and per-product buckets.

        Parameters
        ----------
        price:   the record's price field (float32 from Avro)
        product: the record's product field, used as the grouping key
        """
        self._global.update(price)
        if product not in self._by_product:
            self._by_product[product] = Bucket()
        self._by_product[product].update(price)

    @property
    def global_mean(self) -> float:
        """Current global running average."""
        return self._global.mean

    @property
    def global_count(self) -> int:
        """Total number of records processed."""
        return self._global.count

    def product_mean(self, product: str) -> float | None:
        """Running average for a specific product, or None if not seen."""
        bucket = self._by_product.get(product)
        return bucket.mean if bucket else None

    def summary(self) -> dict:
        """
        Return a JSON-serialisable summary dict.

        Called on consumer shutdown (Ctrl-C) to print the final aggregation
        table.
        """
        return {
            "global": self._global.as_dict(),
            "by_product": {
                product: bucket.as_dict()
                for product, bucket in sorted(self._by_product.items())
            },
        }

    def print_table(self) -> None:
        """Print a human-readable aggregation table to stdout."""
        print("\n" + "═" * 70)
        print(f"{'FINAL AGGREGATION':^70}")
        print("═" * 70)
        g = self._global
        print(f"  Global — count={g.count:>6}  mean={g.mean:>10.4f}"
              f"  min={g.min_val:>10.4f}  max={g.max_val:>10.4f}")
        print("─" * 70)
        print(f"  {'Product':<20} {'Count':>6}  {'Mean':>10}  {'Min':>10}  {'Max':>10}")
        print("─" * 70)
        for product, bucket in sorted(self._by_product.items()):
            print(
                f"  {product:<20} {bucket.count:>6}  {bucket.mean:>10.4f}"
                f"  {bucket.min_val:>10.4f}  {bucket.max_val:>10.4f}"
            )
        print("═" * 70)
        print("\nJSON summary:")
        print(json.dumps(self.summary(), indent=2))
