"""Data-quality gates that run *after* the load and *before* anything downstream
(dashboards, billing, anomaly detection) reads the day.

Each check is a SQL query returning one row; the gate fails the task with all
failed checks listed, so the alert says what is wrong rather than just "failed".
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Check:
    name: str
    sql: str            # must return a single row with a boolean column `ok` and a `detail` column
    blocking: bool = True


def checks_for(table: str, day: str, source: str, min_volume_ratio: float = 0.5) -> list[Check]:
    """Standard checks for one (day, source) slice of a daily fact table."""
    where = f"date = DATE '{day}' AND source = '{source}'"
    return [
        Check(
            "not_empty",
            f"SELECT COUNT(*) > 0 AS ok, CONCAT(CAST(COUNT(*) AS STRING), ' rows') AS detail "
            f"FROM `{table}` WHERE {where}",
        ),
        Check(
            "unique_key",
            f"SELECT COUNT(*) = 0 AS ok, CONCAT(CAST(COUNT(*) AS STRING), ' duplicate keys') AS detail FROM ("
            f"SELECT 1 FROM `{table}` WHERE {where} "
            f"GROUP BY date, source, site_domain, ad_unit, country HAVING COUNT(*) > 1)",
        ),
        Check(
            "no_null_metrics",
            f"SELECT COUNTIF(impressions IS NULL OR revenue_eur IS NULL) = 0 AS ok, "
            f"CONCAT(CAST(COUNTIF(impressions IS NULL OR revenue_eur IS NULL) AS STRING), ' null metrics') AS detail "
            f"FROM `{table}` WHERE {where}",
        ),
        Check(
            # Volume vs the same weekday last week. Catches partial exports, which
            # load "successfully" with a fraction of the rows.
            "volume_vs_last_week",
            f"WITH t AS (SELECT SUM(impressions) AS imps FROM `{table}` WHERE {where}), "
            f"p AS (SELECT SUM(impressions) AS imps FROM `{table}` "
            f"WHERE date = DATE_SUB(DATE '{day}', INTERVAL 7 DAY) AND source = '{source}') "
            f"SELECT p.imps IS NULL OR t.imps >= {min_volume_ratio} * p.imps AS ok, "
            f"CONCAT(CAST(t.imps AS STRING), ' vs ', IFNULL(CAST(p.imps AS STRING), 'n/a'), ' last week') AS detail "
            f"FROM t, p",
        ),
    ]


class QualityGateFailed(RuntimeError):
    pass


def run_gate(run_query, checks: list[Check]) -> list[dict]:
    """`run_query(sql) -> dict` executes one check (BigQuery in the DAG, a fake in tests)."""
    results = []
    for check in checks:
        row = run_query(check.sql)
        results.append({"check": check.name, "ok": bool(row["ok"]), "detail": row.get("detail"),
                        "blocking": check.blocking})
    failed = [r for r in results if not r["ok"] and r["blocking"]]
    if failed:
        raise QualityGateFailed("; ".join(f"{r['check']}: {r['detail']}" for r in failed))
    return results
