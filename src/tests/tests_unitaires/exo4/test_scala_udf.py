import unittest
from src.exo4.scala_udf import spark, addCategoryName
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

class TestScalaUDF(unittest.TestCase):
    def test_addCategoryName(self):
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

        # Création du DataFrame attendu
        expected_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("date", StringType(), True),
            StructField("category", IntegerType(), True),
            StructField("price", FloatType(), True),
            StructField("category_name", StringType(), True)
        ])
        expected_data = [
            Row(id=0, date='2019-02-17', category=6, price=40.0, category_name='furniture'),
            Row(id=1, date='2015-10-01', category=4, price=69.0, category_name='food')
        ]
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Appel de la fonction à tester
        result_df = addCategoryName(input_df, "category")

        # Vérification des résultats
        self.assertEqual(result_df.collect(), expected_df.collect())

        # Vérification du schéma
        expected_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("date", StringType(), True),
            StructField("category", IntegerType(), True),
            StructField("price", FloatType(), True),
            StructField("category_name", StringType(), True)
        ])

        self.assertTrue(result_df.schema == expected_schema)

if __name__ == "__main__":
    unittest.main()
