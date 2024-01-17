from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col

# Créer une SparkSession
spark = SparkSession.builder.appName("wordcount").master("local[*]").getOrCreate()

# Lire le fichier CSV
input_path = "file:/Users/romaindehaese/Desktop/Cours\ 4IABD\ -\ ESGI/Spark\ Core/Spark_Core/src/resources/exo1/data.csv"
df = spark.read.option("header", "true").csv(input_path)

# Appliquer la fonction wordcount
result_df = (
    df
    .select(explode(split(col("text"), " ")).alias("word"))
    .groupBy("word")
    .count()
)

# Afficher le résultat
result_df.show(truncate=False)

# Écrire le résultat en format Parquet
output_path = "/Users/romaindehaese/Desktop/Cours 4IABD - ESGI/Spark Core/Spark_Core/src/resources/exo1/output"
result_df.write.partitionBy("count").parquet(output_path, mode="overwrite")
