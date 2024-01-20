from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col

spark = SparkSession.builder.appName("wordcount").master("local[*]").getOrCreate()

input_path = "src/resources/exo1/data.csv"
output_path = "data/exo1/output"

def calculate_word_count(df):
    result_df = (
        df.select(explode(split(col("text"), " ")).alias("word")) \
          .groupBy("word") \
          .count()
    )
    return result_df

if __name__ == "__main__":

    df = spark.read.option("header", "true").csv(input_path)
    result_df = calculate_word_count(df)

    result_df.write.partitionBy("count").parquet(output_path, mode="overwrite")
