from prefect import flow, task
from pyspark.sql import SparkSession

data_dir = "C:/Users/PLIU/Documents/git/Seminar_workflow_automation/data"

@task
def run_wordcount(source_file:str, out_file:str):
    spark = (
        SparkSession.builder
        .appName("prefect_wordcount")
        .master("local[4]")  # Limit CPU usage
        .config("spark.local.dir", f"{data_dir}/spark_temp/pengfei")
        .getOrCreate()
    )

    df = spark.read.text(source_file)
    counts = df.rdd.flatMap(lambda x: x[0].split()) \
                   .map(lambda w: (w, 1)) \
                   .reduceByKey(lambda a, b: a + b)
    counts.toDF(["word", "count"]).write.mode("overwrite").csv(out_file)
    spark.stop()

@flow(name="spark_wordcount_flow")
def main_flow():
    src_file = f"{data_dir}/source/word_raw.txt"
    out_file= f"{data_dir}/out/wc_flow_out"
    run_wordcount(src_file,out_file)

if __name__ == "__main__":
    main_flow()