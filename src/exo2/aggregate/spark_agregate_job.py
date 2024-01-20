from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("LoadJob").master("local[*]").getOrCreate()

def computePopulationByDepartement(clean_df):
    # Calcul de la population par département
    population_by_departement_df = (
        clean_df
        .groupBy("departement")
        .agg({"name": "count"})
        .withColumnRenamed("count(name)", "nb_people")
        .orderBy("departement")
    )

    return population_by_departement_df

if __name__ == "__main__":

    clean_df = spark.read.parquet("data/exo2/output/*")

    # Calcul de la population par département
    population_df = computePopulationByDepartement(clean_df)
    
    population_df.write.csv("data/exo2/agregate", header=True, mode="overwrite")