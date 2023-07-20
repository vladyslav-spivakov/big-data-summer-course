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

# Schemas

TITLE_AKAS_SCHEMA = t.StructType([
    t.StructField(c.TITLE_ID, t.StringType(), nullable=False),
    t.StructField(c.ORDERING, t.IntegerType()),
    t.StructField(c.TITLE, t.StringType()),
    t.StructField(c.REGION, t.StringType()),
    t.StructField(c.LANGUAGE, t.StringType()),
    t.StructField(c.TYPES, t.StringType()),  # Array
    t.StructField(c.ATTRIBUTES, t.StringType()),  # Array
    t.StructField(c.IS_ORIGINAL_TITLE, t.BooleanType())
])

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

TITLE_CREW_SCHEMA = t.StructType([
    t.StructField(c.TCONST, t.StringType(), nullable=False),
    t.StructField(c.DIRECTORS, t.StringType()),  # Array of nconsts (string)
    t.StructField(c.WRITERS, t.StringType())  # Array of nconsts (string)
])

TITLE_EPISODE_SCHEMA = t.StructType([
    t.StructField(c.TCONST, t.StringType(), nullable=False),
    t.StructField(c.PARENT_TCONST, t.StringType()),
    t.StructField(c.SEASON_NUMBER, t.IntegerType()),
    t.StructField(c.EPISODE_NUMBER, t.IntegerType())
])

TITLE_PRINCIPALS_SCHEMA = t.StructType([
    t.StructField(c.TCONST, t.StringType(), nullable=False),
    t.StructField(c.ORDERING, t.IntegerType()),
    t.StructField(c.NCONST, t.StringType()),
    t.StructField(c.CATEGORY, t.StringType()),
    t.StructField(c.JOB, t.StringType()),
    t.StructField(c.CHARACTERS, t.StringType())
])

TITLE_RATINGS_SCHEMA = t.StructType([
    t.StructField(c.TCONST, t.StringType(), nullable=False),
    t.StructField(c.AVERAGE_RATING, t.FloatType()),
    t.StructField(c.NUM_VOTES, t.IntegerType())
])

NAME_BASICS_SCHEMA = t.StructType([
    t.StructField(c.NCONST, t.StringType(), nullable=False),
    t.StructField(c.PRIMARY_NAME, t.StringType()),
    t.StructField(c.BIRTH_YEAR, t.StringType()),
    t.StructField(c.DEATH_YEAR, t.StringType()),
    t.StructField(c.PRIMARY_PROFESSION, t.StringType()),  # Array of strings
    t.StructField(c.KNOWN_FOR_TITLES, t.StringType())  # Array of tconsts (strings)
])




# Read tsv files

title_basics_df = spark.read.csv(imdb_conf.TITLE_BASICS_DATASET_PATH,
                                 header=True, sep=imdb_conf.SEP,
                                 schema=TITLE_BASICS_SCHEMA)

title_akas_df = spark.read.csv(imdb_conf.TITLE_AKAS_DATASET_PATH,
                                 header=True, sep=imdb_conf.SEP,
                                 schema=TITLE_AKAS_SCHEMA)

title_crew_df = spark.read.csv(imdb_conf.TITLE_CREW_DATASET_PATH,
                                 header=True, sep=imdb_conf.SEP,
                                 schema=TITLE_CREW_SCHEMA)

title_episode_df = spark.read.csv(imdb_conf.TITLE_EPISODE_DATASET_PATH,
                                 header=True, sep=imdb_conf.SEP,
                                 schema=TITLE_EPISODE_SCHEMA)

title_principals_df = spark.read.csv(imdb_conf.TITLE_PRINCIPALS_DATASET_PATH,
                                 header=True, sep=imdb_conf.SEP,
                                 schema=TITLE_PRINCIPALS_SCHEMA)

title_ratings_df = spark.read.csv(imdb_conf.TITLE_RATINGS_DATASET_PATH,
                                 header=True, sep=imdb_conf.SEP,
                                 schema=TITLE_RATINGS_SCHEMA)

name_basics_df = spark.read.csv(imdb_conf.NAME_BASICS_DATASET_PATH,
                                 header=True, sep=imdb_conf.SEP,
                                 schema=NAME_BASICS_SCHEMA)



# Print Schema and n-first elements

title_basics_df.printSchema()
title_basics_df.show(6)

title_akas_df.printSchema()
title_akas_df.show(6)

title_crew_df.printSchema()
title_crew_df.show(6)

title_episode_df.printSchema()
title_episode_df.show(6)

title_principals_df.printSchema()
title_principals_df.show(6)

title_ratings_df.printSchema()
title_ratings_df.show(6)

name_basics_df.printSchema()
name_basics_df.show(6)