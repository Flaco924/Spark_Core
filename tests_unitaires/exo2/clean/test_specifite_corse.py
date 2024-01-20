import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from exo2.clean.spark_clean_job import specifiteCorse

class SpecifiteCorseTest(unittest.TestCase):
    spark = SparkSession.builder.master("local[*]").appName("Test").getOrCreate()

    def test_specifite_corse(self):
        # Given
        df = self.spark.createDataFrame([Row(departement="20", zip=20180), Row(departement="20", zip=20200)])

        # When
        actual = specifiteCorse(df)

        # Then
        expected = self.spark.createDataFrame([Row(departement="2A", zip=20180), Row(departement="2B", zip=20200)])
        self.assertTrue(actual.collect() == expected.collect())

if __name__ == "__main__":
    unittest.main()