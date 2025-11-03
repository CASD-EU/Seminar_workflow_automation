import logging
from prefect import task, flow
import random
from prefect.logging import get_run_logger


@task(name="task_1", description="we test retry mechanism in this task ", retries=3, retry_delay_seconds=10)
def task1():
    logger = get_run_logger()
    logger.setLevel(logging.DEBUG)
    logger.debug("Task 1: start")
    available_src = random.random()
    if available_src < 0.7:
        error_msg = "No enough resource. Please hold!"
        logger.error(f"task1: {error_msg}")
        raise RuntimeError(error_msg)
    success_msg = "Allocated required resource. Start workflow"
    logger.info(success_msg)
    return success_msg


@task(name="task_2", description="task 2 will log message received from task1")
def task2(input_str: str) -> None:
    logger = get_run_logger()
    logger.setLevel(logging.DEBUG)
    logger.debug("Task 2: start")
    logger.info(f"{input_str}")


@flow(name="Flow_with_log")
def flow_with_log():
    # config logger
    logger = get_run_logger()
    logger.setLevel(logging.DEBUG)
    # run task1
    result = task1()
    logger.debug("Completed Task 1")

    # run task2
    task2(wait_for=[task1],input_str=result)
    logger.debug("Completed Task 2")


if __name__ == "__main__":
    flow_with_log()