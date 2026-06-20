from airflow.sdk import dag, task
from datetime import datetime
import json

@dag(schedule=None, start_date=datetime(2024, 1, 1))
def xcom_playground():

    @task
    def produce_simple():
        """Returns a simple string. Automatically pushed to XCom."""
        return "hello from task 1"

    @task
    def produce_dict():
        """Returns a dict. Gets serialized to JSON in the XCom backend."""
        return {
            "customer": "Acme Corp",
            "dag_count": 42,
            "healthy": True,
        }

    @task
    def produce_list():
        """Returns a list."""
        return ["scheduler", "worker", "triggerer", "dag-processor"]

    @task
    def consume_all(simple_val, dict_val, list_val):
        """Receives XCom from all three upstream tasks."""
        print(f"Simple value: {simple_val}")
        print(f"Dict value: {json.dumps(dict_val, indent=2)}")
        print(f"List value: {list_val}")
        print(f"Customer name from dict: {dict_val['customer']}")
        return f"consumed {len(list_val)} components for {dict_val['customer']}"

    s = produce_simple()
    d = produce_dict()
    l = produce_list()
    result = consume_all(s, d, l)

xcom_playground()