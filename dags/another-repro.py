"""
Repro DAG for QA ForbiddenSessionUseError / mapped XCom callback issue.

Purpose:
- Create mapped task outputs.
- Pass mapped outputs into a downstream reduce task.
- Trigger a DAG-level on_success_callback.
- In Remote Execution, callback processing may cause the dag-processor
  execution API to call get_mapped_xcom_by_slice(), which attempts
  session.execute() and can raise ForbiddenSessionUseError.
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG, task


def dag_success_callback(context):
    """
    DAG-level callback.

    The callback itself does not need to access XCom directly. The repro is
    intended to trigger Airflow/RE callback context handling after mapped
    XComs exist for this DAG run.
    """
    dag_run = context["dag_run"]

    print(f"[callback] DAG succeeded: {dag_run.dag_id}")
    print(f"[callback] Run ID: {dag_run.run_id}")
    print(f"[callback] State: {dag_run.state}")


@task
def generate_items():
    return [1, 2, 3, 4, 5]


@task
def mapped_task(item: int):
    result = item * 10
    print(f"[mapped_task] item={item}, result={result}")
    return result


@task
def reduce_results(results: list[int]):
    print(f"[reduce_results] got mapped results: {results}")
    return sum(results)


with DAG(
    dag_id="reproduce_qa_forbidden_session_xcom",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    on_success_callback=dag_success_callback,
    tags=["repro", "qa", "mapped-xcom", "remote-execution"],
) as dag:
    items = generate_items()
    mapped_results = mapped_task.expand(item=items)
    reduce_results(mapped_results)