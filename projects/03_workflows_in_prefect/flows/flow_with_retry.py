from prefect import task, flow
import random


@task(name="task_1", description="we test retry mechanism in this task ", retries=3, retry_delay_seconds=10)
def task1():
    print("Task 1: start")
    available_src = random.random()
    if available_src < 0.7:
        raise RuntimeError("No enough resource. Please hold!")
    return "Allocated required resource. Start workflow"


@task(name="task_2", description="task 2 will log message received from task1")
def task2(input_str: str) -> None:
    print("Task 2: start")
    print(f"{input_str}")


@flow(name="Flow_with_retry", log_prints=True)
def flow_with_retry():
    # run task1
    result = task1()
    print("Completed Task 1")

    # run task2
    task2(wait_for=[task1],input_str=result)
    print("Completed Task 2")


if __name__ == "__main__":
    flow_with_retry()
