import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from exo2.aggregate.spark_aggregate_job import computePopulationByDepartement

spark = SparkSession.builder.master("local[*]").appName("IntegrationTest").getOrCreate()

def test_data():
    # Df de test
    data = [("John", 25, "75001", "Paris", "75"),
            ("Jane", 30, "13001", "Marseille", "13"),
            ("Bob", 22, "69001", "Lyon", "69")]

    schema = ["name", "age", "zip", "city", "departement"]

    test_df = spark.createDataFrame(data, schema=schema)

    return test_df

def test_compute_population_by_departement(test_data):
    result_df = computePopulationByDepartement(test_data)

    # Vérif
    expected_data = [("13", 1), ("69", 1), ("75", 1)]
    expected_schema = ["departement", "nb_people"]
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    assert result_df.collect() == expected_df.collect()
