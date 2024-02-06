import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.exo2.aggregate.spark_agregate_job import computePopulationByDepartement

spark = SparkSession.builder.master("local[*]").appName("IntegrationTestAgg").getOrCreate()

def test_data():
    # Df de test
    data = [("John", 25, "75001", "Paris"),
            ("Jane", 30, "13001", "Marseille"),
            ("Bob", 22, "69001", "Lyon")]

    schema = ["name", "age", "zip", "city"]

    test_df = spark.createDataFrame(data, schema=schema)

    return test_df

def test_compute_population_by_departement(test_data):
    result_df = computePopulationByDepartement(test_data)

    # Vérif
    expected_data = [("13001", 1), ("69001", 1), ("75001", 1)]
    expected_schema = ["departement", "nb_people"]
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    assert result_df.collect() == expected_df.collect()
