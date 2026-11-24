"""DAG integrity: every DAG imports cleanly, has an owner, retries, and per-task timeouts.

Runs only where Airflow is installed (CI's `airflow` job, or `uv run --group airflow pytest`).
"""

import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag  # noqa: E402


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="dags", include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}
    assert "ssp_revenue_daily" in dagbag.dags


def test_conventions(dagbag):
    for dag_id, dag in dagbag.dags.items():
        assert dag.tags, f"{dag_id} has no tags"
        assert dag.catchup is False, f"{dag_id}: enable catchup explicitly, per backfill"
        for t in dag.tasks:
            assert t.execution_timeout is not None, f"{dag_id}.{t.task_id} has no execution_timeout"
            assert t.retries >= 1, f"{dag_id}.{t.task_id} has no retries"
            assert t.owner != "airflow", f"{dag_id}.{t.task_id} has no owner"


def test_task_order(dagbag):
    dag = dagbag.dags["ssp_revenue_daily"]
    assert dag.get_task("extract").downstream_task_ids == {"load"}
    assert dag.get_task("load").downstream_task_ids == {"quality_gate"}
