import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from ..exo1.word_count import calculate_word_count

spark = SparkSession.builder.appName("IntegrationTestWC").master("local[*]").getOrCreate()

# Given
input_data = [("abc def abc",), ("def ghi",), ("ghi abc",)]
schema = StructType([StructField("text", StringType(), True)])
input_df = spark.createDataFrame(input_data, schema=schema)

# When
result_df = calculate_word_count(input_df)

# Then
assert "word" in result_df.columns
assert "count" in result_df.columns

expected_data = [("abc", 2), ("def", 2), ("ghi", 2)]
expected_df = spark.createDataFrame(expected_data, ["word", "count"])

assert result_df.orderBy("word").collect() == expected_df.orderBy("word").collect()
