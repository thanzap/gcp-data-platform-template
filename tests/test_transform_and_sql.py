from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from pipelines.bigquery import merge_sql, staging_table
from pipelines.quality import Check, QualityGateFailed, checks_for, run_gate
from pipelines.transform import TooManyRejects, normalise

DAY = date(2026, 11, 9)
FX = {"EUR": Decimal("1"), "USD": Decimal("0.92")}
NOW = datetime(2026, 11, 10, 6, tzinfo=UTC)


def test_normalise_converts_currency_and_cleans_dimensions():
    rows = [{"site_domain": "https://www.Example.com/path", "ad_unit": "top", "country": "gr",
             "impressions": "1000", "revenue": "2.50", "currency": "usd"}]
    (row,) = normalise(rows, DAY, "example_ssp", FX, loaded_at=NOW).rows
    assert row == {"date": "2026-11-09", "source": "example_ssp", "site_domain": "example.com", "ad_unit": "top",
                   "country": "GR", "impressions": 1000, "revenue_eur": "2.300000",
                   "loaded_at": "2026-11-10T06:00:00+00:00"}


def test_a_few_bad_rows_are_rejected_and_reported():
    good = [{"site_domain": "a.com", "impressions": 1, "revenue": 1}] * 199
    bad = [{"site_domain": "a.com", "impressions": -5, "revenue": 1}]
    result = normalise(good + bad, DAY, "s", FX)
    assert len(result.rows) == 199 and result.rejects[0][1] == "negative metric"


def test_format_change_upstream_stops_the_load():
    rows = [{"domain": "a.com", "imps": 1, "rev": 1}] * 10  # renamed fields
    with pytest.raises(TooManyRejects, match="10 of 10"):
        normalise(rows, DAY, "s", FX)


def test_unknown_currency_is_rejected_not_guessed():
    result = normalise([{"site_domain": "a.com", "impressions": 1, "revenue": 1, "currency": "JPY"}] * 1,
                       DAY, "s", FX, max_reject_ratio=1.0)
    assert result.rejects[0][1] == "no FX rate for JPY"


def test_merge_is_scoped_to_one_day_and_source():
    sql = merge_sql("p.marts.revenue_daily", "p.staging._stg_x", "2026-11-09", "example_ssp")
    assert "WHERE date = DATE '2026-11-09' AND source = 'example_ssp'" in sql
    assert "T.date = DATE '2026-11-09'" in sql  # partition pruning on target
    assert "WHEN NOT MATCHED BY SOURCE AND T.date = DATE '2026-11-09' AND T.source = 'example_ssp' THEN DELETE" in sql
    assert "UPDATE SET impressions = S.impressions, revenue_eur = S.revenue_eur, loaded_at = S.loaded_at" in sql


def test_staging_table_name_is_safe():
    assert staging_table("p.staging", "ssp", "2026-11-09", "manual__2026-11-10T06:00:00+00:00") == \
        "p.staging._stg_ssp_2026_11_09_manual__2026_11_10T06_00_00_00_00"


def test_quality_gate_reports_every_failed_check():
    answers = {"not_empty": {"ok": True, "detail": "10 rows"},
               "unique_key": {"ok": False, "detail": "3 duplicate keys"},
               "no_null_metrics": {"ok": True, "detail": "0"},
               "volume_vs_last_week": {"ok": False, "detail": "100 vs 1000 last week"}}
    checks = checks_for("p.marts.revenue_daily", "2026-11-09", "example_ssp")
    by_sql = {c.sql: answers[c.name] for c in checks}
    with pytest.raises(QualityGateFailed) as err:
        run_gate(by_sql.__getitem__, checks)
    assert "unique_key: 3 duplicate keys" in str(err.value) and "volume_vs_last_week" in str(err.value)


def test_non_blocking_checks_only_warn():
    result = run_gate(lambda sql: {"ok": False, "detail": "x"}, [Check("soft", "SELECT 1", blocking=False)])
    assert result[0]["ok"] is False
