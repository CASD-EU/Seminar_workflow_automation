from prefect import flow, task

# ----- LEVEL 1: Base Tasks -----
@task(name="Task_1", log_prints=True)
def task1():
    print("Running Task 1")
    return "output_from_task1"


@task(name="Subtask_2a", log_prints=True)
def subtask2a():
    print("Running Subtask 2a")
    return "result_2a"


@task(name="Subtask_2b", log_prints=True)
def subtask2b():
    print("Running Subtask 2b")
    return "result_2b"


@task(name="Task_3", log_prints=True)
def task3(result1, result2):
    print(f"Running Task 3 after Task 1 and Task 2")
    print(f" → Got from Task 1: {result1}")
    print(f" → Got from Task 2: {result2}")
    return "final_result"


# ----- LEVEL 2: Task 2 as a Subflow -----
@flow(name="subflow_for_task_2", log_prints=True)
def task2():
    print("Starting Task 2 Flow")
    res2a = subtask2a()
    res2b = subtask2b()
    print("Completed Task 2 Flow")
    return f"combined({res2a}, {res2b})"


# ----- LEVEL 3: Main Orchestration Flow -----
@flow(name="Main_Flow", log_prints=True)
def main_flow():
    print("=== Main Flow Started ===")

    # Run the first task
    result1 = task1()
    # Run the second `task`
    # you can notice task2 is actual a flow
    # When you call a flow inside another flow, it becomes subflow
    result2 = task2()

    # Task 3 depends on both Task 1 and Task 2
    task3(wait_for=[result1, result2], result1=result1, result2=result2)

    print("=== Main Flow Completed ===")


if __name__ == "__main__":
    main_flow()