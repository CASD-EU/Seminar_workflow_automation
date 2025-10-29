from typing import List

from prefect import flow, task

@task
def extract()->List[int]:
    data = [1, 2, 3]
    msg = f"Task1: Extracting data: {data}"
    print(msg)
    return data

@task
def transform(data:List[int])->List[int]:
    print(f"Task2: Transforming data: {data}")
    return [x * 10 for x in data]

@task
def load(data:List[int]):
    print(f"Task3: Loading result {data}")

@flow
def etl_flow():
    # task 1
    data = extract()
    # task 2
    clean = transform(data)
    # task 3
    load(clean)


if __name__ == "__main__":
    etl_flow()