import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from exo2.aggregate.spark_agregate_job import computePopulationByDepartement

class ComputePopulationByDepartementTest(unittest.TestCase):
    spark = SparkSession.builder.master("local[*]").appName("Test").getOrCreate()

    def test_compute_population_by_departement(self):
        # Given
        df = self.spark.createDataFrame([Row(departement="75"), Row(departement="2B"), Row(departement="75")])

        # When
        actual = computePopulationByDepartement(df)

        # Then
        expected = self.spark.createDataFrame([Row(departement="2B", nb_people=1),
                                               Row(departement="75", nb_people=2)])
        self.assertTrue(actual.collect() == expected.collect())

if __name__ == "__main__":
    unittest.main()