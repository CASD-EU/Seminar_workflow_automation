import logging

from prefect import flow, task, get_run_logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

import datetime
import tempfile
from pyspark.sql import SparkSession
from pathlib import Path
import time
import shutil

# the partial file path works, because the prefect worker and spark runs on mode local
# if the prefect worker and spark worker runs on remote server. The path won't work.
data_dir = "../../data"

# you need to change the username value. So the spark.local.dir file path is dedicated to your environment to avoid file access conflict.
user_name = "pengfei"


def build_spark_temp_dir() -> str:
    """
    This function creates a temporary directory for Spark temporary files. Each Spark session uses a unique temp folder will avoid
    file-lock conflicts on Windows. Folder can be safely deleted after spark.stop().
    An example output C:/Users/alice/AppData/Local/spark_temp/20250329_124501_839201
    :return: the temporary directory path in string
    """
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    base = Path(tempfile.gettempdir())
    spark_temp_dir = base / "spark_temp" / f"spark_{ts}"
    spark_temp_dir.mkdir(parents=True, exist_ok=True)
    return spark_temp_dir.as_posix()


def build_spark_session(app_name: str, extra_jar_folder_path: str | None = None, vcore_number: int = 4,
                        driver_memory: str = "4g", local_dir_path: str | None = None):
    """
    Create or reuse a SparkSession with standardized configuration.

    :param vcore_number:
    :param app_name:
    :param extra_jar_folder_path:
    :param driver_memory:
    :param local_dir_path:
    :return:
    """
    # create a standard spark session builder
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(f"local[{vcore_number}]")
        .config("spark.driver.memory", driver_memory)
        # enable AQE
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # give a partition size advice.
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
        # set AQE partition range
        .config("spark.sql.adaptive.maxNumPostShufflePartitions", "100")
        .config("spark.sql.adaptive.minNumPostShufflePartitions", "1")
        # increase worker timeout
        .config("spark.network.timeout", "800s")
        .config("spark.executor.heartbeatInterval", "90s")
        .config("spark.sql.sources.commitProtocolClass",
                "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
        # JVM memory allocation
        .config("spark.driver.maxResultSize", "4g")  # Avoid OOM on collect()
        # Shuffle & partition tuning
        .config("spark.sql.files.maxPartitionBytes", "128m")  # Avoid large partitions in memory
        .config("spark.reducer.maxSizeInFlight", "48m")  # Limit shuffle buffer
        # Unified memory management
        .config("spark.memory.fraction", "0.7")  # Reduce pressure on execution memory
        .config("spark.memory.storageFraction", "0.3")  # Smaller cache area
        # Spill to disk early instead of crashing
        .config("spark.shuffle.spill", "true")
        .config("spark.shuffle.spill.compress", "true")
        .config("spark.shuffle.compress", "true")
        # optimize jvm GC
        .config("spark.driver.extraJavaOptions",
                "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:+HeapDumpOnOutOfMemoryError")
        # Use Kryo serializer
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # Optional: buffer size for serialization
        .config("spark.kryoserializer.buffer", "64m")
        .config("spark.kryoserializer.buffer.max", "512m")
    )

    # Ensure local_dir exists
    # if local dir path are not provided, generate a standard temp folder
    if local_dir_path is None:
        local_dir_path = build_spark_temp_dir()
    else:
        local_dir = Path(local_dir_path)
        local_dir.mkdir(parents=True, exist_ok=True)
    builder = builder.config("spark.local.dir", local_dir_path)

    # Load extra JARs only when present
    if extra_jar_folder_path:
        jar_dir = Path(extra_jar_folder_path)
        jar_files = [
            str(p) for p in jar_dir.iterdir()
            if p.is_file() and p.suffix == ".jar"
        ]
        if jar_files:
            builder = builder.config("spark.jars", ",".join(jar_files))

    return builder.getOrCreate(), local_dir_path

def safe_delete(target_path: str, retries: int = 5, delay: float = 0.5):
    target = Path(target_path)

    for attempt in range(1, retries + 1):
        if not target.exists():
            return True     # Already deleted

        try:
            shutil.rmtree(target)
        except PermissionError:
            time.sleep(delay * attempt)   # exponential backoff

        # Double-check if deletion actually succeeded
        if not target.exists():
            return True

    return False  # Explicit failure after retries


@task(name="task_1",
      description="task 1 read a text file, count numbers of unique words, then write result in a csv file")
def wordcount_task(source_file: str, out_file: str):
    spark, spark_temp_dir_path = build_spark_session("my-wordcount-demo", driver_memory="6g")

    df = spark.read.text(source_file)
    words = df.select(explode(split(col(df.columns[0]), "\\s+")).alias("word"))
    counts = words.groupBy("word").count()
    counts.write.mode("overwrite").csv(out_file)
    spark.stop()
    safe_delete(spark_temp_dir_path, retries=5, delay=0.5)


@task(name="task_2", description="task 2 reads the output csv file of task 1, filter the results with a given list")
def filter_task(source_file: str, out_file: str, target_words: list[str]):
    spark, spark_temp_dir_path = build_spark_session("my-wordcount-demo", driver_memory="6g")
    schema = StructType([
        StructField("word", StringType(), True),
        StructField("count", IntegerType(), True)])

    df = spark.read.csv(source_file, header=False, schema=schema)
    result = df.filter(col("word").isin(target_words))
    result.write.mode("overwrite").csv(out_file)
    spark.stop()
    safe_delete(spark_temp_dir_path, retries=5, delay=0.5)


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
