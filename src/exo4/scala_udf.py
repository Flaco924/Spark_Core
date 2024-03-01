from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.column import Column, _to_java_column, _to_seq

def main():
    spark = SparkSession.builder \
        .appName("UDF") \
        .master("local[*]") \
        .config('spark.jars', 'src/resources/exo4/udf.jar') \
        .getOrCreate()

    data_path = "src/resources/exo4/sell.csv"
    df = spark.read.option("header", "true").csv(data_path)
    df = df.withColumn("category", col("category").cast(IntegerType()))

    # Définir la fonction Python pour utiliser l'UDF Scala
    def addCategoryName(col):
        # Récupérer le SparkContext
        sc = spark.sparkContext
        # Via sc._jvm, accéder aux fonctions Scala
        add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
        # Retourner un objet colonne avec l'application de l'UDF Scala
        return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))

    # Ajouter la nouvelle colonne en utilisant l'UDF Scala
    df = df.withColumn("category_name", addCategoryName(col("category")))

if __name__ == "__main__":
    main()
