"""Base class for "asynchronous report" APIs.

Most ad-tech / marketing reporting APIs don't return data synchronously. You
submit a report request, poll its status until it's ready, then download the
rows. Every connector repeats that loop, and every hand-written copy gets one
of these wrong:

* **No overall deadline.** A report stuck in "running" holds an Airflow
  worker slot forever. Here every connector has a hard `max_wait`, and every
  HTTP call has its own timeout.
* **Retrying the wrong errors.** 429/5xx and network errors are retried with
  exponential backoff + jitter (honouring `Retry-After`); 4xx means our request
  is wrong and fails immediately, so a bad credential doesn't turn into 30
  minutes of retries.
* **Trusting the final payload.** Downloaded rows are validated
  (`transform.py`) before anything reaches the warehouse.

Concrete connectors implement only `submit`, `status` and `download`.
"""

from __future__ import annotations

import logging
import random
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from enum import StrEnum

import httpx

log = logging.getLogger(__name__)

RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}


class ReportState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"


class ConnectorError(RuntimeError):
    """Non-retryable failure: bad request, bad credentials, report failed."""


class ReportTimeout(ConnectorError):
    """The report didn't finish within max_wait."""


@dataclass(frozen=True)
class RetryPolicy:
    attempts: int = 5
    base_delay: float = 1.0
    max_delay: float = 60.0

    def delay(self, attempt: int, retry_after: str | None = None) -> float:
        if retry_after and retry_after.isdigit():
            return min(float(retry_after), self.max_delay)
        return min(self.max_delay, self.base_delay * 2**attempt) * random.uniform(0.5, 1.0)


class AsyncReportConnector(ABC):
    name: str = "base"

    def __init__(
        self,
        client: httpx.Client,
        retry: RetryPolicy | None = None,
        poll_interval: float = 10.0,
        max_wait: float = 30 * 60,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ):
        if client.timeout.read is None:
            raise ValueError("httpx.Client must have a read timeout; unbounded calls hang workers")
        self.client = client
        self.retry = retry or RetryPolicy()
        self.poll_interval = poll_interval
        self.max_wait = max_wait
        self._sleep = sleep
        self._clock = clock

    # --- implemented by each connector -------------------------------------
    @abstractmethod
    def submit(self, day: date) -> str:
        """Request a report for one day; return its id."""

    @abstractmethod
    def status(self, report_id: str) -> ReportState:
        """Current state of a submitted report."""

    @abstractmethod
    def download(self, report_id: str) -> list[dict]:
        """Raw rows of a finished report."""

    # --- shared machinery ----------------------------------------------------
    def request(self, method: str, url: str, **kwargs) -> httpx.Response:
        """HTTP call with retries on transient failures only."""
        for attempt in range(self.retry.attempts):
            try:
                response = self.client.request(method, url, **kwargs)
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                if attempt == self.retry.attempts - 1:
                    raise ConnectorError(f"{self.name}: {method} {url} failed: {exc}") from exc
                wait = self.retry.delay(attempt)
                log.warning("%s: %s, retrying in %.1fs", self.name, exc.__class__.__name__, wait)
                self._sleep(wait)
                continue

            if response.status_code < 400:
                return response
            if response.status_code not in RETRYABLE_STATUS or attempt == self.retry.attempts - 1:
                raise ConnectorError(f"{self.name}: {method} {url} -> {response.status_code}: {response.text[:300]}")
            wait = self.retry.delay(attempt, response.headers.get("Retry-After"))
            log.warning("%s: HTTP %s, retrying in %.1fs", self.name, response.status_code, wait)
            self._sleep(wait)
        raise AssertionError("unreachable")

    def wait_until_done(self, report_id: str) -> None:
        deadline = self._clock() + self.max_wait
        while True:
            state = self.status(report_id)
            if state is ReportState.DONE:
                return
            if state is ReportState.FAILED:
                raise ConnectorError(f"{self.name}: report {report_id} failed upstream")
            if self._clock() + self.poll_interval > deadline:
                raise ReportTimeout(f"{self.name}: report {report_id} not ready after {self.max_wait:.0f}s")
            self._sleep(self.poll_interval)

    def fetch(self, day: date) -> list[dict]:
        """submit → poll (bounded) → download."""
        report_id = self.submit(day)
        log.info("%s: submitted report %s for %s", self.name, report_id, day)
        self.wait_until_done(report_id)
        rows = self.download(report_id)
        log.info("%s: downloaded %d rows for %s", self.name, len(rows), day)
        return rows
