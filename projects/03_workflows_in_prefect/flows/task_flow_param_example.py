from prefect import task, flow


@task(
    name="task1",
    description="task 1 of my workflow",
    retries=3,
    retry_delay_seconds=30,
    timeout_seconds=120,
    persist_result=False,
    log_prints=True,)
def task1(input_data: str)->str:
    msg = f"receive data: {input_data}"
    print(f"Task1: The input value is {input_data}")
    return msg


@task(
    name="task2",
    description="task 2 of my workflow",
    retries=3,
    retry_delay_seconds=30,
    timeout_seconds=120,
    persist_result=False,
    log_prints=True,)
def task2():
    print(f"Task2: Running task 2")


@task(
    name="task3",
    description="task 3 of my workflow",
    retries=3,
    retry_delay_seconds=30,
    timeout_seconds=120,
    persist_result=False,
    log_prints=True,)
def task3():
    print(f"Task3: Running task 3")


@flow(
    name="workflow_with_params",
    description="this a test workflow",
    version="1.0.0",
    retries=2,
    retry_delay_seconds=60,
    persist_result=True,
    log_prints=True,
    validate_parameters=True,
    timeout_seconds=600,
    task_runner=None,  # e.g., ConcurrentTaskRunner
)
def my_flow(param1: str, param2: int = 0):
    r1 =task1(param1)
    task2()
    task3()


if __name__ == "__main__":
    p1 = "some sample data"
    my_flow(p1)