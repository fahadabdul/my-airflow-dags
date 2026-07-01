"""
DAG to reproduce QA ForbiddenSessionUseError + structlog recursion bug.
Mapped tasks produce XCom, dag-level on_success_callback triggers
callback processing in the dag-processor which hits get_mapped_xcom_by_slice.
"""

from airflow.sdk import DAG, task
from datetime import datetime


def dag_success_callback(context):
    """
    DAG-level callback that fires on success.
    The callback itself doesn't need to do much - the bug is triggered
    when the dag-processor processes this callback and the execution API
    tries to read mapped XCom via session.execute().
    """
    print(f"DAG succeeded: {context['dag_run'].dag_id}")
    run_id = context["dag_run"].run_id
    print(f"Run ID: {run_id}")


@task
def generate_list():
    return [1, 2, 3, 4, 5]


@task
def process_item(item):
    return item * 10


@task
def collect_results(results):
    print(f"Got results: {results}")
    return sum(results)


with DAG(
    dag_id="reproduce_qa_xcom_bug",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    on_success_callback=dag_success_callback,
) as dag:
    items = generate_list()
    processed = process_item.expand(item=items)
    collect_results(processed)