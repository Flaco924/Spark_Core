import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from src.exo1.word_count import calculate_word_count

class CalculateWordCountTest(unittest.TestCase):
    spark = SparkSession.builder.master("local[*]").appName("Test").getOrCreate()

    def test_calculate_word_count(self):
        # Given
        df = self.spark.createDataFrame([Row(text="This is a test"), Row(text="Another test")])

        # When
        actual = calculate_word_count(df)

        # Then
        expected = self.spark.createDataFrame([Row(word="This", count=1), Row(word="is", count=1),
                                               Row(word="a", count=1), Row(word="test", count=2),
                                               Row(word="Another", count=1)], ["word", "count"])
        self.assertTrue(actual.collect() == expected.collect())

if __name__ == "__main__":
    unittest.main()