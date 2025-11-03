from prefect import flow, task


@task(name="Task_1", description="task 1 will divide 10 by the given number x with error handling", log_prints=True)
def task1(x: int) -> int:
    print("Running Task 1")
    try:
        result = int(10 / x)
        print(f"The normal output is {result}")
        return result
    except ZeroDivisionError:
        print("The error handling output is the default value 0")
        return 0  # Fallback value or recovery action


@task(name="Task_2", description="task 2 will divide 10 by the given number x without error handling", log_prints=True)
def task2(x: int) -> int:
    print("Running Task 2")
    return int(10 / x)


@task(name="Task_3", description="task 3 will multiple the result of task1 and task2 x by 10", log_prints=True)
def task3(y1: int, y2: int) -> None:
    print("Running Task 3")
    print(f"Receive value from task_1: {y1}, corresponding result is: {10 * y1}")
    print(f"Receive value from task_2: {y2}, corresponding result is: {10 * y2}")


@flow(name="Flow_with_error", description="This workflow contains a task which can raise ZeroDivisionErro ",
      version="1.0.0", log_prints=True)
def handle_error_flow(x: int):
    t1_resu = task1(x)
    print("Completed Task 1")
    t2_resu = task2(x)
    print("Completed Task 2")
    task3(t1_resu, t2_resu)
    print("Completed Task 3")


if __name__ == "__main__":
    handle_error_flow(0)
