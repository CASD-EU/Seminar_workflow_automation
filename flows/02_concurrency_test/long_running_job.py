from time import sleep
from typing import List

from prefect import flow, task

@task
def extract()->List[int]:
    data = [1, 2, 3]
    msg = f"Task1: Extracting data: {data}"
    print(msg)
    sleep(30)
    return data

@task
def transform(data:List[int])->List[int]:
    print(f"Task2: Transforming data: {data}")
    sleep(30)
    return [x * 10 for x in data]

@task
def load(data:List[int]):
    sleep(30)
    print(f"Task3: Loading result {data}")

@flow
def start_long_running_flow():
    # task 1
    data = extract()
    # task 2
    clean = transform(data)
    # task 3
    load(clean)


if __name__ == "__main__":
    start_long_running_flow()