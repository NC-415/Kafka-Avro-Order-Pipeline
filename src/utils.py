"""
utils.py
────────
Pure-Python utilities shared by consumer.py and the test suite.

Kept in a separate module so the unit tests can import backoff_delay() and
validate() without pulling in the confluent_kafka C extension, which requires
a compiled wheel and is not available in all CI environments.
"""

from __future__ import annotations

import math
import random

import src.config as cfg
from src.errors import PermanentError


def backoff_delay(attempt: int) -> float:
    """
    Compute a full-jitter exponential back-off delay for the given attempt number.

    Parameters
    ----------
    attempt: 1-based attempt number (1 = first retry, not the initial try)

    Returns
    -------
    A delay in seconds drawn from uniform(0, min(CAP, BASE × 2^(attempt−1))).

    Full jitter prevents the thundering-herd problem: without it, all consumers
    that failed at the same instant retry at the same instant and hammer the
    recovering service in synchronised waves.
    Reference: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/

    Stays well under max.poll.interval.ms (300 s) for the defaults:
    worst case ≈ 0.5 + 1 + 2 = 3.5 s total sleep plus jitter.
    """
    raw = min(cfg.BACKOFF_CAP_S, cfg.BACKOFF_BASE_S * (2 ** (attempt - 1)))
    return random.uniform(0, raw)


def validate(order: dict) -> None:
    """
    Apply business-rule validation to a deserialized order dict.

    Raises PermanentError for any violation — retrying cannot make a negative
    price positive or fill in a missing product name.  See README §6.

    Rules
    -----
    - orderId must be a non-empty string
    - product must be a non-empty string
    - price must be a finite, positive number (> 0, not NaN)
    """
    order_id = order.get("orderId", "")
    product = order.get("product", "")
    price = order.get("price", None)

    if not isinstance(order_id, str) or not order_id.strip():
        raise PermanentError(f"Blank or missing orderId: {order_id!r}")

    if not isinstance(product, str) or not product.strip():
        raise PermanentError(f"Blank or missing product: {product!r}")

    if price is None:
        raise PermanentError("Missing price field")

    if not isinstance(price, (int, float)):
        raise PermanentError(f"Non-numeric price: {price!r}")

    if math.isnan(price):
        raise PermanentError(f"NaN price for orderId={order_id!r}")

    if price <= 0:
        raise PermanentError(
            f"Non-positive price {price!r} for orderId={order_id!r}"
        )
