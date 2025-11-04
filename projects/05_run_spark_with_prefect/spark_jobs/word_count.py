from pyspark.sql import SparkSession
import sys
from pathlib import Path

def main():
    # Validate arguments
    if len(sys.argv) < 2:
        print("Usage: spark-submit word_count.py <input_file> [<output_dir>]")
        sys.exit(1)

    input_path = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else str(Path(input_path).parent / "output_wordcount")

    # Initialize Spark session
    spark = (
        SparkSession.builder
        .appName("WordCountJob")
        .getOrCreate()
    )

    # Read text file
    text_rdd = spark.sparkContext.textFile(input_path)

    # Split by space, filter empty, and count words
    counts = (
        text_rdd.flatMap(lambda line: line.split(" "))
        .filter(lambda word: word.strip() != "")
        .map(lambda word: (word.strip(), 1))
        .reduceByKey(lambda a, b: a + b)
    )

    # Convert to DataFrame
    df = spark.createDataFrame(counts, ["word", "count"])

    # Save result
    df.write.mode("overwrite").csv(output_dir)

    print(f"✅ Word count completed. Results saved to: {output_dir}")

    spark.stop()

if __name__ == "__main__":
    main()
