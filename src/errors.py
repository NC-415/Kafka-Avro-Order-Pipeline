"""
errors.py
─────────
Failure taxonomy for the order pipeline.

Every failure is classified as either Transient or Permanent *before* deciding
what to do with the record. This is the central design decision — see README §6.

    PermanentError  → straight to DLQ, zero retries
    TransientError  → retry with exponential back-off, then DLQ
    RetriesExhausted → subclass of TransientError, raised when the attempt
                       budget is spent; then routed to DLQ like a permanent failure
"""


class PermanentError(Exception):
    """
    The record itself is the problem.

    Examples:
        - Negative or zero price
        - Blank orderId or product
        - NaN price
        - Un-deserializable bytes ("poison pill") — no amount of retrying can
          make a malformed Avro payload deserializable.

    Action: write to DLQ immediately, do NOT retry.
    """


class TransientError(Exception):
    """
    The record is fine; the environment is temporarily unhealthy.

    Examples:
        - Simulated downstream sink timeout
        - HTTP 503 / 429 from an external service
        - Transient connection reset

    Action: retry with exponential back-off + full jitter, then DLQ.
    """


class RetriesExhausted(TransientError):
    """
    Raised by the retry loop when MAX_ATTEMPTS have been consumed without success.

    Inherits from TransientError so callers can catch either class, but signals
    that the attempt budget is gone and the record must be dead-lettered.
    """

    def __init__(self, last_error: TransientError, attempts: int) -> None:
        self.last_error = last_error
        self.attempts = attempts
        super().__init__(
            f"Exhausted {attempts} attempt(s). Last error: {last_error!r}"
        )
