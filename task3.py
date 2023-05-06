"""
 Get titles of all movies that last more than 2 hours.
Получить названия всех фильмов, которые длятся более 2 часов.
title.basics.tsv.gz
"""
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f
def task3():
    spark_session = (SparkSession.builder
        .master("local")
        .appName("task app")
        .config(conf=SparkConf())
        .getOrCreate())
    schema_title_basics = t.StructType([
        t.StructField("tconst", t.StringType(), nullable=True),
        t.StructField("titleType", t.StringType(), nullable=True),
        t.StructField("primaryTitle", t.StringType(), nullable=True),
        t.StructField("originalTitle", t.StringType(), nullable=True),
        t.StructField("isAdult", t.StringType(), nullable=True),
        t.StructField("startYear", t.IntegerType(), nullable=True),
        t.StructField("endYear", t.IntegerType(), nullable=True),
        t.StructField("runtimeMinutes", t.IntegerType(), nullable=True),
        t.StructField("genres", t.StringType(), nullable=True),
    ])
    file_read_basics = r'.\Data\input\title.basics.tsv.gz'
    from_csv_df = spark_session.read.csv(
        file_read_basics, header=True, nullValue='null', sep=r'\t', schema=schema_title_basics)
    from_csv_df_task3 = from_csv_df.select("titleType", "originalTitle", "runtimeMinutes")\
        .filter((f.col("runtimeMinutes") > 120) & (f.col("titleType").isin("movie", "tvMovie")))
    from_csv_df_task3.show(200, truncate=False)
    #file_write = r'.\Data\output\task03'
    #from_csv_df_task3.write.csv(file_write, header=True, mode="overwrite")
    return 0