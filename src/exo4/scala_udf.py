from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.column import Column, _to_java_column, _to_seq

spark = SparkSession.builder.appName("UDF").master("local[*]").config('spark.jars', 'src/resources/exo4/udf.jar').getOrCreate()

data_path = "src/resources/exo4/sell.csv/*"
df = spark.read.option("header", "true").csv(data_path)
df = df.withColumn("category", col("category").cast(IntegerType()))

# Définir la fonction Python pour utiliser l'UDF Scala
def addCategoryName(col):
    # on récupère le SparkContext
    sc = spark.sparkContext
    # Via sc._jvm on peut accéder à des fonctions Scala
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    # On retourne un objet colonne avec l'application de notre udf Scala
    return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))

# Ajouter la nouvelle colonne en utilisant l'UDF Scala
df = df.withColumn("category_name", addCategoryName(col("category")))