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
    print(f"The input value is {input_data}")
    return msg


@flow(
    name="data_ingestion_flow",
    description="Download and clean data daily",
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