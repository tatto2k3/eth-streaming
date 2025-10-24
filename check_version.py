import kafka
print(kafka.__file__)

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
print("Spark version:", spark.version)
