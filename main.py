from pyspark.sql import SparkSession
from pyspark import SparkConf
import pyspark.sql.types as t

import imdb_utils.config as imdb_conf

spark = (SparkSession.builder
         .master(imdb_conf.MASTER)
         .appName(imdb_conf.APP_NAME)
         .config(conf=SparkConf())
         .getOrCreate())

import imdb_utils.columns as c

TITLE_BASICS_SCHEMA = t.StructType([
    t.StructField(c.TCONST, t.StringType(), nullable=False),
    t.StructField(c.TITLE_TYPE, t.StringType()),
    t.StructField(c.PRIMARY_TITLE, t.StringType()),
    t.StructField(c.ORIGINAL_TITLE, t.StringType()),
    t.StructField(c.IS_ADULT, t.BooleanType()),
    t.StructField(c.START_YEAR, t.DateType()),
    t.StructField(c.END_YEAR, t.DateType()),
    t.StructField(c.RUNTIME_MINUTES, t.StringType()),
    t.StructField(c.GENRES, t.StringType())  # Array
])


# Read tsv file

title_basics_df = spark.read.csv(imdb_conf.TITLE_BASICS_DATASET_PATH,
                                 header=True, sep=imdb_conf.SEP,
                                 schema=TITLE_BASICS_SCHEMA)

# Print Schema and n-first elements

title_basics_df.printSchema()
title_basics_df.show(6)