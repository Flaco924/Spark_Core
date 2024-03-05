import unittest
from src.exo4.python_udf import spark, windows
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

class TestPythonUDF(unittest.TestCase):
    def test_windows(self):
        # Création du DataFrame d'entrée
        input_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("date", StringType(), True),
            StructField("category", IntegerType(), True),
            StructField("price", FloatType(), True),
            StructField("category_name", StringType(), True)
        ])
        input_data = [
            Row(id=0, date='2019-02-17', category=6, price=40.0, category_name='furniture'),
            Row(id=1, date='2015-10-01', category=4, price=69.0, category_name='food'),
            Row(id=2, date='2019-02-17', category=6, price=50.0, category_name='furniture'),
            Row(id=3, date='2015-10-01', category=4, price=60.0, category_name='food')
        ]
        input_df = spark.createDataFrame(input_data, input_schema)

        # Appel de la fonction à tester
        result_df = windows(input_df)

        # Schéma attendu du DataFrame de sortie
        expected_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("date", StringType(), True),
            StructField("category", IntegerType(), True),
            StructField("price", FloatType(), True),
            StructField("category_name", StringType(), True),
            StructField("total_price_per_category_per_day", FloatType(), True),
            StructField("total_price_per_category_per_day_last_30_days", FloatType(), True)
        ])

        # Données attendues
        expected_data = [
            Row(id=0, date='2019-02-17', category=6, price=40.0, category_name='furniture', total_price_per_category_per_day=90.0, total_price_per_category_per_day_last_30_days=90.0),
            Row(id=1, date='2015-10-01', category=4, price=69.0, category_name='food', total_price_per_category_per_day=129.0, total_price_per_category_per_day_last_30_days=129.0),
            Row(id=2, date='2019-02-17', category=6, price=50.0, category_name='furniture', total_price_per_category_per_day=90.0, total_price_per_category_per_day_last_30_days=90.0),
            Row(id=3, date='2015-10-01', category=4, price=60.0, category_name='food', total_price_per_category_per_day=129.0, total_price_per_category_per_day_last_30_days=129.0)
        ]
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Vérification des résultats
        self.assertEqual(result_df.collect(), expected_df.collect())
        self.assertTrue(result_df.schema == expected_schema)

if __name__ == "__main__":
    unittest.main()
