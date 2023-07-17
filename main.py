from pyspark.sql import SparkSession
from pyspark import SparkConf
import pyspark.sql.types as t
spark = (SparkSession.builder
         .master('local')
         .appName('my_assignment')
         .config(conf=SparkConf())
         .getOrCreate())


# create schema

schema = t.StructType([
    t.StructField("name", t.StringType(), nullable=True)
])

data = [("John",), ("Carl",)]

name_df = spark.createDataFrame(data, schema)

name_df.show()