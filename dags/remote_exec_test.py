from airflow.decorators import dag, task
from datetime import datetime


@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["remote-execution-test"],
)
def remote_exec_test():

    @task
    def task_1():
        print("Hello from Task 1 — running on a Remote Execution Agent!")
        return "hello from task_1"

    @task
    def task_2(msg: str):
        print(f"Task 2 received via XCom: '{msg}'")
        print("If you see this, XCom + MinIO are working correctly!")

    @task
    def task_3():
        print("Task 3 running independently — no upstream dependency")

    result = task_1()
    task_2(result)   # tests XCom (MinIO backend)
    task_3()         # runs in parallel with task_2


remote_exec_test()