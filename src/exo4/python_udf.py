from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType

def main():
    spark = SparkSession.builder \
        .appName("UDF") \
        .master("local[*]") \
        .config('spark.jars', 'src/resources/exo4/udf.jar') \
        .getOrCreate()

    data_path = "src/resources/exo4/sell.csv"
    df = spark.read.option("header", "true").csv(data_path)
    df = df.withColumn("category", col("category").cast(IntegerType()))

    def categorize_category(category):
        if category < 6:
            return "food"
        else:
            return "furniture"

    categorize_category_udf = udf(categorize_category, StringType())
    df = df.withColumn("category_name", categorize_category_udf(df["category"]))

if __name__ == "__main__":
    main()
