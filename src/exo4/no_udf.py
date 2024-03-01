from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

def main():
    spark = SparkSession.builder \
        .appName("NoUDF") \
        .master("local[*]") \
        .getOrCreate()

    data_path = "src/resources/exo4/sell.csv"
    df = spark.read.option("header", "true").csv(data_path)
    df = df.withColumn("category", col("category").cast("int"))

    # Ajouter la nouvelle colonne avec des conditions
    df = df.withColumn("category_name", when(col("category") < 6, "food").otherwise("furniture"))

if __name__ == "__main__":
    main()
