from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.column import Column, _to_java_column, _to_seq

spark = SparkSession.builder.appName("UDF").master("local[*]").config('spark.jars', 'src/resources/exo4/udf.jar').getOrCreate()

data_path = "src/resources/exo4/sell.csv/*"
df = spark.read.option("header", "true").csv(data_path)
df = df.withColumn("category", col("category").cast(IntegerType()))

# Définition de la fonction UDF
def categorize_category(category):
    if category < 6:
        return "food"
    else:
        return "furniture"

# Enregistrer la fonction UDF
categorize_category_udf = udf(categorize_category, StringType())

# Ajouter une nouvelle colonne en utilisant l'UDF
df = df.withColumn("category_name", categorize_category_udf(df["category"]))
