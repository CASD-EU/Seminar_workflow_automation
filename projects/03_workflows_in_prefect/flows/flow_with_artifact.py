from prefect import flow, task
from prefect.artifacts import create_markdown_artifact


# common function
def get_words(input_str: str) -> list[str]:
    # Strip leading/trailing spaces, then split by space
    words = input_str.strip().split(" ")
    # Filter out empty entries (for consecutive spaces)
    return [w for w in words if w]


# ----- LEVEL 1: Base Tasks -----
@task(name="Task_1", description="task 1 will count total words", log_prints=True)
def task1(input_str: str) -> int:
    print("Running Task 1")
    words = get_words(input_str)
    print("Completed Task 1")
    return len(words)


@task(name="Subtask_2a", description="task 2a will convert string to a list of words", log_prints=True)
def subtask2a(input_str: str) -> list[str]:
    print("Running Subtask 2a")
    return get_words(input_str)


@task(name="Subtask_2b", description="task 2b will remove duplicates in the list", log_prints=True)
def subtask2b(words: list[str]) -> list[str]:
    print("Running Subtask 2b")
    seen = set()
    unique_words = []
    for word in words:
        if word not in seen:
            unique_words.append(word)
            seen.add(word)
    return unique_words


@task(name="Task_3", description="show a report with total words and words list", log_prints=True)
def task3(result1, result2):
    print(f"Running Task 3 after Task 1 and Task 2")
    markdown_report = f"""# Book processing report

## Summary

In this report, we count total words and show unique words list.

| word count        | unique word list |
|:--------------|-------:|
| {result1} | {result2} |
"""
    create_markdown_artifact(
        key="word-report",
        markdown=markdown_report,
        description="book processing report",
    )
    print("Completed Task 3")


# ----- LEVEL 2: Task 2 as a Subflow -----
@flow(name="subflow_for_task_2", log_prints=True)
def task2(input_str: str) -> list[str]:
    print("Starting Task 2 Flow")
    res2a = subtask2a(input_str)
    res2b = subtask2b(res2a)
    print("Completed Task 2 Flow")
    return res2b


# ----- LEVEL 3: Main Orchestration Flow -----
@flow(name="Flow_with_artifact", description="This workflow read a book, then show total words count and unique words",
      version="1.0.0", log_prints=True)
def main_flow(book_str: str) -> None:
    print("=== Main Flow Started ===")

    # Run the first task
    result1 = task1(book_str)
    # Run the second `task`
    # you can notice task2 is actual a flow
    # When you call a flow inside another flow, it becomes subflow
    result2 = task2(book_str)

    # Task 3 depends on both Task 1 and Task 2
    task3(wait_for=[result1, result2], result1=result1, result2=result2)

    print("=== Main Flow Completed ===")


if __name__ == "__main__":
    test_str = "people needs to eat more fruits. people needs to do more sports"
    main_flow(test_str)
