"""Idempotent loading into a partitioned BigQuery table.

Pattern: load the day's file from GCS into a per-run staging table, then
MERGE it into the target in one statement, scoped to that day and source.
Re-running the same day (backfill, retry, a fixed upstream export) replaces
exactly that slice: no duplicates, and no window where the day is half-loaded,
because MERGE is atomic.

The SQL builders are pure functions so they can be unit-tested and reviewed;
the thin `load_day` wrapper is the only part that talks to BigQuery.
"""

from __future__ import annotations

from pipelines.transform import KEY, SCHEMA


def merge_sql(target: str, staging: str, day: str, source: str) -> str:
    cols = [c for c, _ in SCHEMA]
    on = " AND ".join(f"T.{k} = S.{k}" for k in KEY)
    updates = ", ".join(f"{c} = S.{c}" for c in cols if c not in KEY)
    return (
        f"MERGE `{target}` T\n"
        f"USING (SELECT * FROM `{staging}` WHERE date = DATE '{day}' AND source = '{source}') S\n"
        f"ON {on} AND T.date = DATE '{day}'\n"  # constant date on T enables partition pruning
        f"WHEN MATCHED THEN UPDATE SET {updates}\n"
        f"WHEN NOT MATCHED BY TARGET THEN INSERT ({', '.join(cols)}) VALUES ({', '.join('S.' + c for c in cols)})\n"
        f"WHEN NOT MATCHED BY SOURCE AND T.date = DATE '{day}' AND T.source = '{source}' THEN DELETE"
    )


def staging_table(dataset: str, source: str, day: str, run_id: str) -> str:
    safe = "".join(ch if ch.isalnum() else "_" for ch in f"{source}_{day}_{run_id}")
    return f"{dataset}._stg_{safe}"[:1024]


def load_day(client, gcs_uri: str, target: str, staging_dataset: str, day: str, source: str, run_id: str) -> int:
    """GCS NDJSON → staging (expires in 1 day) → MERGE into target. Returns rows merged."""
    from google.cloud import bigquery

    staging = staging_table(staging_dataset, source, day, run_id)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=[bigquery.SchemaField(name, typ) for name, typ in SCHEMA],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    client.load_table_from_uri(gcs_uri, staging, job_config=job_config).result(timeout=600)
    client.query(f"ALTER TABLE `{staging}` SET OPTIONS (expiration_timestamp = "
                 f"TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY))").result(timeout=60)
    job = client.query(merge_sql(target, staging, day, source))
    job.result(timeout=600)
    return job.num_dml_affected_rows or 0
