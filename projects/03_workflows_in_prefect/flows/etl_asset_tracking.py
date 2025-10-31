from prefect import flow, task, get_run_logger
from prefect.assets import materialize


@materialize("file://pengfei_data/raw_data.csv")
@task(log_prints=True)
def extract():
    data = [1, 2, 3]
    msg = f"Task1: Extracting data: {data}"
    logger = get_run_logger()
    logger.info(msg)
    print(msg)
    return data

@materialize("file://pengfei_data/clean_data.csv", asset_deps=["file://pengfei_data/raw_data.csv"])
@task(log_prints=True)
def transform(data):
    print(f"Task2: Transforming data: {data}")
    return [x * 10 for x in data]

@task(log_prints=True)
def load(data):
    print(f"Task3: Loading result {data}")

@flow
def etl_asset_tracking():
    # task 1
    data = extract()
    # task 2
    clean = transform(data)
    # task 3
    load(clean)


if __name__ == "__main__":
    etl_asset_tracking()