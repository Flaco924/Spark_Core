import unittest
from src.exo4.python_udf import spark, categorize_category
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

class TestPythonUDF(unittest.TestCase):
    def test_categorize_category(self):
        # Création du DataFrame d'entrée
        input_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("date", StringType(), True),
            StructField("category", IntegerType(), True),
            StructField("price", FloatType(), True)
        ])
        input_data = [
            Row(id=0, date='2019-02-17', category=6, price=40.0),
            Row(id=1, date='2015-10-01', category=4, price=69.0)
        ]
        input_df = spark.createDataFrame(input_data, input_schema)

        # Appel de la fonction à tester
        result_df = categorize_category(input_df, "category")

        # Vérification des résultats
        expected_data = [
            Row(id=0, date='2019-02-17', category=6, price=40.0, category_name='furniture'),
            Row(id=1, date='2015-10-01', category=4, price=69.0, category_name='food')
        ]
        expected_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("date", StringType(), True),
            StructField("category", IntegerType(), True),
            StructField("price", FloatType(), True),
            StructField("category_name", StringType(), True)
        ])
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        self.assertEqual(result_df.collect(), expected_df.collect())
        self.assertTrue(result_df.schema == expected_df.schema)

if __name__ == "__main__":
    unittest.main()
