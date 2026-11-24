"""Daily SSP revenue: API → GCS → BigQuery (idempotent MERGE) → quality gate.

One DAG run = one reporting day (the data interval). Safe to re-run or
backfill: every step overwrites exactly its own (day, source) slice.

Conventions this repo follows for every DAG:
* heavy imports (google-cloud-*, httpx) happen inside tasks, keeping DAG
  parsing fast on Composer's scheduler;
* every task has an execution_timeout, so nothing can hold a worker slot forever;
* raw API output lands in GCS first: reloading never needs to call the API again;
* failures alert with *what* failed (quality-gate details), not just "task failed".
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import Variable, dag, task

SOURCE = "example_ssp"
DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
}


def _cfg(key: str) -> str:
    # Set per environment (dev / prod) as Airflow Variables, e.g. from Terraform outputs.
    return Variable.get(key)


def notify_failure(context) -> None:
    """Failure callback: post the task, run and exception to the team channel."""
    import json
    import urllib.request

    ti = context["task_instance"]
    text = f":red_circle: {ti.dag_id}.{ti.task_id} failed for {context['ds']}: {context.get('exception')}"
    url = Variable.get("alerts_webhook_url", default=None)
    if url:
        req = urllib.request.Request(url, data=json.dumps({"text": text}).encode(),
                                     headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10)


@dag(
    dag_id="ssp_revenue_daily",
    schedule="0 6 * * *",  # partners finalise yesterday's numbers by ~05:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=3,
    default_args={**DEFAULT_ARGS, "on_failure_callback": notify_failure},
    tags=["revenue", "ingestion", SOURCE],
    doc_md=__doc__,
)
def ssp_revenue_daily():
    @task(execution_timeout=timedelta(minutes=45))
    def extract(ds=None) -> str:
        """Fetch the day's report, validate it, and write NDJSON to GCS. Returns the gs:// URI."""
        import json
        from datetime import date
        from decimal import Decimal

        import httpx
        from google.cloud import secretmanager, storage

        from pipelines.connectors.example_ssp import ExampleSSPConnector
        from pipelines.transform import normalise

        day = date.fromisoformat(ds)
        token = secretmanager.SecretManagerServiceClient().access_secret_version(
            name=_cfg("example_ssp_token_secret")  # projects/…/secrets/…/versions/latest
        ).payload.data.decode()

        with httpx.Client(
            base_url=_cfg("example_ssp_base_url"),
            headers={"Authorization": f"Bearer {token}"},
            timeout=httpx.Timeout(30.0, connect=10.0),
        ) as client:
            raw = ExampleSSPConnector(client, max_wait=30 * 60).fetch(day)

        fx = {k: Decimal(v) for k, v in json.loads(_cfg("fx_to_eur")).items()}
        result = normalise(raw, day, SOURCE, fx_to_eur=fx)

        uri = f"gs://{_cfg('raw_bucket')}/{SOURCE}/date={ds}/rows.ndjson"
        bucket, path = uri[5:].split("/", 1)
        storage.Client().bucket(bucket).blob(path).upload_from_string(
            "\n".join(json.dumps(r) for r in result.rows), content_type="application/x-ndjson"
        )
        return uri

    @task(execution_timeout=timedelta(minutes=20))
    def load(uri: str, ds=None, run_id=None) -> int:
        from google.cloud import bigquery

        from pipelines.bigquery import load_day

        return load_day(
            bigquery.Client(), uri,
            target=_cfg("revenue_table"),            # project.marts.revenue_daily
            staging_dataset=_cfg("staging_dataset"),  # project.staging
            day=ds, source=SOURCE, run_id=run_id,
        )

    @task(execution_timeout=timedelta(minutes=10))
    def quality_gate(_merged: int, ds=None) -> list[dict]:
        from google.cloud import bigquery

        from pipelines.quality import checks_for, run_gate

        client = bigquery.Client()

        def run_query(sql: str) -> dict:
            return dict(next(iter(client.query(sql).result(timeout=120))))

        return run_gate(run_query, checks_for(_cfg("revenue_table"), ds, SOURCE))

    quality_gate(load(extract()))


ssp_revenue_daily()
