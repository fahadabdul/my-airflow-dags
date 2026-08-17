import os
from datetime import datetime
from airflow.sdk import DAG, task

if os.path.exists("/tmp/break_dag"):
    raise Exception("Simulated Vault connectivity timeout")

with DAG(
    dag_id="test_cache_import_error",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    @task
    def hello():
        return "hello"

    hello()
