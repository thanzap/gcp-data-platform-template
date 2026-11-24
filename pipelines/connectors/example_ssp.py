"""Connector for a (fictional) SSP reporting API, the kind every ad-tech data team integrates:

    POST /v1/reports            {"date": "2026-11-09", "dimensions": [...], "metrics": [...]}
         → {"report_id": "r-123"}
    GET  /v1/reports/{id}       → {"status": "queued" | "running" | "completed" | "error"}
    GET  /v1/reports/{id}/rows  → {"rows": [...], "next_page_token": "..."}

Auth is a bearer token read from Secret Manager by the caller (never from code or env files).
"""

from __future__ import annotations

from datetime import date

from pipelines.connectors.base import AsyncReportConnector, ConnectorError, ReportState

_STATES = {
    "queued": ReportState.PENDING,
    "running": ReportState.RUNNING,
    "completed": ReportState.DONE,
    "error": ReportState.FAILED,
}

DIMENSIONS = ["site_domain", "ad_unit", "country"]
METRICS = ["impressions", "revenue", "currency"]


class ExampleSSPConnector(AsyncReportConnector):
    name = "example_ssp"

    def submit(self, day: date) -> str:
        body = {"date": day.isoformat(), "dimensions": DIMENSIONS, "metrics": METRICS}
        payload = self.request("POST", "/v1/reports", json=body).json()
        if "report_id" not in payload:
            raise ConnectorError(f"{self.name}: no report_id in response: {payload}")
        return str(payload["report_id"])

    def status(self, report_id: str) -> ReportState:
        raw = self.request("GET", f"/v1/reports/{report_id}").json().get("status", "")
        if raw not in _STATES:
            raise ConnectorError(f"{self.name}: unknown report status {raw!r}")
        return _STATES[raw]

    def download(self, report_id: str) -> list[dict]:
        rows, token = [], None
        for _ in range(1000):  # hard page cap: a looping token must not spin forever
            params = {"page_token": token} if token else {}
            page = self.request("GET", f"/v1/reports/{report_id}/rows", params=params).json()
            rows.extend(page.get("rows", []))
            token = page.get("next_page_token")
            if not token:
                return rows
        raise ConnectorError(f"{self.name}: more than 1000 pages for report {report_id}")
