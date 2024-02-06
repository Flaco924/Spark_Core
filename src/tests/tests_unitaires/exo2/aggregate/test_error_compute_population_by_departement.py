import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from src.exo2.aggregate.spark_agregate_job import computePopulationByDepartement

class ComputePopulationByDepartementErrorTest(unittest.TestCase):
    spark = SparkSession.builder.master("local[*]").appName("Test").getOrCreate()

    def test_compute_population_by_departement_with_error(self):
        # Given
        df = self.spark.createDataFrame([Row(departement="75"), Row(departement="2B"), Row(departement=None)])

        # When
        with self.assertRaises(ValueError) as context:
            computePopulationByDepartement(df)

        # Then
        self.assertTrue("Invalid value found in 'departement' column: null or empty" in str(context.exception))

if __name__ == "__main__":
    unittest.main()