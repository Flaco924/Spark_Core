import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from ..exo2.clean.spark_clean_job import joinClientsVilles, departement4Caracteres, specifiteCorse

spark = SparkSession.builder.appName("IntegrationTestClean").master("local[*]").getOrCreate()

def test_integration(spark):
    # Given
    clients_data = [
        ("Pierre", 25, "20190", "Ajaccio"),
        ("Paul", 30, "97206", "Fort-de-France"),
        ("Jacques", 22, "75001", "Paris")
    ]

    villes_data = [
        ("20190", "Ajaccio"),
        ("97206", "Fort-de-France"),
        ("75001", "Paris")
    ]

    clients_df = spark.createDataFrame(clients_data, ["name", "age", "zip", "city"])
    villes_df = spark.createDataFrame(villes_data, ["zip", "city"])

    # When
    joined_df = joinClientsVilles(clients_df, villes_df)
    df_with0 = departement4Caracteres(joined_df)
    df_departement = df_with0.withColumn("departement", df_with0["zip"].substr(1, 2))
    df_outre_mer = df_departement.withColumn("departement", when(df_departement["departement"] == "97", substring(df_departement["zip"], 1, 3)) \
                              .otherwise(df_departement["departement"]))
    df_finale = specifiteCorse(df_outre_mer)

    # Then
    expected_data = [
        ("Pierre", 25, "20190", "Ajaccio", "2A"),
        ("Paul", 30, "97206", "Fort-de-France", "972"),
        ("Jacques", 22, "75001", "Paris", "75")
    ]

    expected_df = spark.createDataFrame(expected_data, ["name", "age", "zip", "city", "departement"])

    assert df_finale.orderBy("zip").collect() == expected_df.orderBy("zip").collect()

# Exécution
test_integration(spark)
