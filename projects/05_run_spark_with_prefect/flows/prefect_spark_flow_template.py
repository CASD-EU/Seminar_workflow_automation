import logging

from prefect import flow, task, get_run_logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# the partial file path works, because the prefect worker and spark runs on mode local
# if the prefect worker and spark worker runs on remote server. The path won't work.
data_dir = "../../data"
# data_dir = "C:/Users/PLIU/Documents/git/Seminar_workflow_automation/data"

# you need to change the username value. So the spark.local.dir file path is dedicated to your environment to avoid file access conflict.
user_name = "pengfei"


@task(name="task_1",
      description="task 1 read a text file, count numbers of unique words, then write result in a csv file")
def wordcount_task(source_file: str, out_file: str):
    spark = (
        SparkSession.builder
        .appName("prefect_wordcount")
        .master("local[4]")  # Limit CPU usage
        .config("spark.local.dir", f"{data_dir}/spark_temp/{user_name}")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    df = spark.read.text(source_file)
    counts = df.rdd.flatMap(lambda x: x[0].split()) \
        .map(lambda w: (w, 1)) \
        .reduceByKey(lambda a, b: a + b)
    counts.toDF(["word", "count"]).write.mode("overwrite").csv(out_file)
    spark.stop()


@task(name="task_2", description="task 2 reads the output csv file of task 1, filter the results with a given list")
def filter_task(source_file: str, out_file: str, target_words: list[str]):
    spark = (
        SparkSession.builder
        .appName("prefect_word_filter")
        .master("local[4]")  # Limit CPU usage
        .config("spark.local.dir", f"{data_dir}/spark_temp/{user_name}")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    schema = StructType([
        StructField("word", StringType(), True),
        StructField("count", IntegerType(), True)])

    df = spark.read.csv(source_file, header=False, schema=schema)
    result = df.filter(col("word").isin(target_words))
    result.write.mode("overwrite").csv(out_file)
    spark.stop()


@flow(name="spark_wordcount_flow",
      description="This workflow read plain text file and count words, we handle the error with task state",
      version="1.0.0")
def main_flow(target_words: list[str]):
    # set up logger
    logger = get_run_logger()
    logger.setLevel(logging.INFO)
    # run task 1
    src_file1 = f"{data_dir}/source/word_raw.txt"
    out_file1 = f"{data_dir}/out/wc_out"
    t1_state = wordcount_task(src_file1, out_file1, return_state=True)
    # check task 1 state, before start task2
    if t1_state.is_completed():
        # run task 2
        out_file2 = f"{data_dir}/out/flow_out"
        filter_task(out_file1, out_file2, target_words)
    else:
        logger.error("The task 1 does not complete with success, no need to run the task 2.")


if __name__ == "__main__":
    target_words = ["data", "file"]
    main_flow(target_words)
