import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from src.exo2.clean.spark_clean_job import joinClientsVilles

class JoinClientsVillesTest(unittest.TestCase):
    spark = SparkSession.builder.master("local[*]").appName("Test").getOrCreate()

    def test_join_clients_villes(self):
        # Given
        clients_data = [("John", 25, "12345"), ("Alice", 30, "67890")]
        villes_data = [("12345", "Paris"), ("67890", "Berlin")]

        clients_df = self.spark.createDataFrame(clients_data, ["name", "age", "zip"])
        villes_df = self.spark.createDataFrame(villes_data, ["zip", "city"])

        # When
        actual = joinClientsVilles(clients_df, villes_df)

        # Then
        expected = self.spark.createDataFrame([Row(name="John", age=25, zip="12345", city="Paris"),
                                               Row(name="Alice", age=30, zip="67890", city="Berlin")])
        self.assertTrue(actual.collect() == expected.collect())

if __name__ == "__main__":
    unittest.main()