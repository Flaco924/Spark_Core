import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from ..exo2.clean.spark_clean_job import departement4Caracteres

class Departement4CaracteresTest(unittest.TestCase):
    spark = SparkSession.builder.master("local[*]").appName("Test").getOrCreate()

    def test_departement_4_caracteres(self):
        # Given
        df = self.spark.createDataFrame([Row(zip="1234"), Row(zip="56789")])

        # When
        actual = departement4Caracteres(df)

        # Then
        expected = self.spark.createDataFrame([Row(zip="01234"), Row(zip="56789")])
        self.assertTrue(actual.collect() == expected.collect())

if __name__ == "__main__":
    unittest.main()