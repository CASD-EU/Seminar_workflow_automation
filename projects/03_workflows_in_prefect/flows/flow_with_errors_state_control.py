from prefect import flow, task
from prefect.states import Completed, Failed


@task(name="Task_1", description="task 1 will divide 10 by the given number x with error handling", log_prints=True)
def task1(x: int) -> int:
    print("Task 1: Start")
    try:
        result = int(10 / x)
        print(f"The normal output is {result}")
        return result
    except ZeroDivisionError:
        print("The error handling output is the default value 0")
        return 0  # Fallback value or recovery action


@task(name="Task_2", description="task 2 will divide 10 by the given number x without error handling", log_prints=True)
def task2(x: int) -> int:
    print("Task 2: Start")
    return int(10 / x)


@task(name="Task_3", description="task 3 will multiple the result of task1 and task2 x by 10", log_prints=True)
def task3(y1: int, y2: int) -> None:
    print("Task 3: Start")
    print(f"Receive value from task_1: {y1}, corresponding result is: {10 * y1}")
    print(f"Receive value from task_2: {y2}, corresponding result is: {10 * y2}")


@flow(name="Flow_with_error_state_control", description="This workflow contains a task which can raise ZeroDivisionError, we handle the error with task state",
      version="1.0.0", log_prints=True)
def handle_error_flow(x: int):
    # run task 1
    t1_state_resu = task1(x, return_state=True)
    t1_resu = t1_state_resu.result()
    print("Completed Task 1")
    # tun task 2, need to handle exception
    t2_state_resu = task2(x, return_state=True)
    print(f"t2 result with state: {t2_state_resu}")
    if t2_state_resu.is_completed():
        t2_resu = t2_state_resu.result()
    elif t2_state_resu.is_failed():
        print("Main flow: The task 2 has failed, use the default value 0")
        t2_resu = 0
    else:
        print(f"Main flow: unkown state: {t2_state_resu.name}")
        t2_resu = 0
    print("Completed Task 2")
    # run task3
    # try to replace the below line with task3(wait_for=[t1_resu, t2_resu]), see what happens
    task3(wait_for=[t1_resu, t2_resu],y1=t1_resu,y2=t2_resu)
    print("Completed Task 3")


if __name__ == "__main__":
    handle_error_flow(0)
