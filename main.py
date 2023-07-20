from pyspark.sql import SparkSession
from pyspark import SparkConf
import pyspark.sql.types as t

import imdb_utils.config as imdb_conf

spark = (SparkSession.builder
         .master(imdb_conf.MASTER)
         .appName(imdb_conf.APP_NAME)
         .config(conf=SparkConf())
         .getOrCreate())

# Schemas

TITLE_BASICS_SCHEMA = t.StructType([
    t.StructField("tconst", t.StringType(), nullable=False),
    t.StructField("titleType", t.StringType()),
    t.StructField("primaryTitle", t.StringType()),
    t.StructField("originalTitle", t.StringType()),
    t.StructField("isAdult", t.BooleanType()),
    t.StructField("startYear", t.DateType()),
    t.StructField("endYear", t.DateType()),
    t.StructField("runtimeMinutes", t.StringType()),
    t.StructField("genres", t.StringType()) # Array type not supported in csv
])


# Read tsv file

title_basics_df = spark.read.csv(imdb_conf.TITLE_BASICS_DATASET_PATH,
                                 header=True, sep=imdb_conf.SEP,
                                 schema=TITLE_BASICS_SCHEMA)

# Print Schema and n-first elements

title_basics_df.printSchema()
title_basics_df.show(6)