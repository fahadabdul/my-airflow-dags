from datetime import datetime, timedelta
from airflow.sdk import DAG, task


@task
def slow_task(task_num):
    import time
    time.sleep(300)
    return f"task_{task_num}_done"


with DAG(
    dag_id="repro_ui_freeze_remote_exec",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
    max_active_tasks=50,
    doc_md="Repro DAG: 60 parallel 5-min tasks to test UI behavior during active remote exec runs.",
) as dag:
    for i in range(60):
        slow_task.override(task_id=f"slow_task_{i}")(task_num=i)
