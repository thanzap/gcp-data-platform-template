from datetime import date

import httpx
import pytest

from pipelines.connectors.base import ConnectorError, ReportTimeout, RetryPolicy
from pipelines.connectors.example_ssp import ExampleSSPConnector

DAY = date(2026, 11, 9)


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds

    def __call__(self):
        return self.now


def make(handler, clock=None, **kw):
    clock = clock or FakeClock()
    client = httpx.Client(base_url="https://api.example-ssp.test", transport=httpx.MockTransport(handler),
                          timeout=httpx.Timeout(5.0))
    conn = ExampleSSPConnector(client, retry=RetryPolicy(attempts=3, base_delay=1, max_delay=4),
                               poll_interval=10, sleep=clock.sleep, clock=clock, **kw)
    return conn, clock


def ssp_api(statuses, pages, fail_first=0):
    """A fake SSP: returns `statuses` in order on polls, then serves `pages`."""
    state = {"polls": 0, "failures": fail_first, "calls": []}

    def handler(request: httpx.Request) -> httpx.Response:
        state["calls"].append((request.method, request.url.path))
        if state["failures"]:
            state["failures"] -= 1
            return httpx.Response(503, text="busy")
        if request.method == "POST":
            return httpx.Response(200, json={"report_id": "r-1"})
        if request.url.path.endswith("/rows"):
            i = int(request.url.params.get("page_token", 0))
            nxt = str(i + 1) if i + 1 < len(pages) else None
            return httpx.Response(200, json={"rows": pages[i], "next_page_token": nxt})
        s = statuses[min(state["polls"], len(statuses) - 1)]
        state["polls"] += 1
        return httpx.Response(200, json={"status": s})

    return handler, state


def test_full_flow_polls_then_downloads_all_pages():
    handler, state = ssp_api(["queued", "running", "completed"], [[{"a": 1}], [{"a": 2}], [{"a": 3}]])
    conn, clock = make(handler)
    assert conn.fetch(DAY) == [{"a": 1}, {"a": 2}, {"a": 3}]
    assert state["polls"] == 3 and clock.sleeps == [10, 10]


def test_transient_errors_are_retried_with_backoff():
    handler, state = ssp_api(["completed"], [[{"a": 1}]], fail_first=2)
    conn, clock = make(handler)
    assert conn.fetch(DAY) == [{"a": 1}]
    assert len(clock.sleeps) == 2 and clock.sleeps[1] > 0


def test_retry_after_header_is_honoured():
    calls = iter([httpx.Response(429, headers={"Retry-After": "3"}), httpx.Response(200, json={"report_id": "x"})])
    conn, clock = make(lambda r: next(calls))
    assert conn.submit(DAY) == "x" and clock.sleeps == [3.0]


def test_client_errors_fail_immediately_without_retry():
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(401, text="bad token")

    conn, clock = make(handler)
    with pytest.raises(ConnectorError, match="401"):
        conn.fetch(DAY)
    assert len(seen) == 1 and clock.sleeps == []


def test_stuck_report_hits_the_deadline_instead_of_hanging():
    handler, _ = ssp_api(["running"], [])
    conn, clock = make(handler, max_wait=60)
    with pytest.raises(ReportTimeout):
        conn.fetch(DAY)
    assert clock.now <= 60


def test_failed_report_and_unknown_status_raise():
    conn, _ = make(ssp_api(["error"], [])[0])
    with pytest.raises(ConnectorError, match="failed upstream"):
        conn.fetch(DAY)
    conn, _ = make(ssp_api(["weird"], [])[0])
    with pytest.raises(ConnectorError, match="unknown report status"):
        conn.fetch(DAY)


def test_client_without_timeout_is_refused():
    with pytest.raises(ValueError, match="read timeout"):
        ExampleSSPConnector(httpx.Client(timeout=None))
