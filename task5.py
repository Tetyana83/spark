"""
Get information about how many adult movies/series etc. there are per
region. Get the top 100 of them from the region with the biggest count to
the region with the smallest one.

Получите информацию о том, сколько фильмов/сериалов для взрослых и т. д. есть на
область, край. Получите 100 лучших из них из региона с наибольшим количеством
область с наименьшим из них.
title.basics.tsv.gz title.akas.tsv.gz
"""
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f
from pyspark.sql import Window
def task5():
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
    schema_title_akas = t.StructType([
            t.StructField("titleId", t.StringType(), nullable=False),
            t.StructField("ordering", t.StringType(), nullable=False),
            t.StructField("title", t.StringType(), nullable=False),
            t.StructField("region", t.StringType(), nullable=True),
            t.StructField("language", t.StringType(), nullable=True),
            t.StructField("types", t.StringType(), nullable=True),
            t.StructField("attributes", t.StringType(), nullable=True),
            t.StructField("isOriginalTitle", t.StringType(), nullable=True)
    ])
    schema_ratings_basics = t.StructType([
        t.StructField("tconst", t.StringType(), nullable=True),
        t.StructField("averageRating", t.DoubleType(), nullable=True),
        t.StructField("numVotes", t.IntegerType(), nullable=True)
    ])

    file_read_basics = r'.\Data\input\title.basics.tsv.gz'
    file_read_akas = r'.\Data\input\title.akas.tsv.gz'
    file_read_ratings = r'.\Data\input\title.ratings.tsv.gz'
    from_csv_df = spark_session.read.csv(
        file_read_basics, header=True, nullValue='null', sep=r'\t', schema=schema_title_basics)
    from_csv_df_akas = spark_session.read.csv(
        file_read_akas, header=True, nullValue='null', sep=r'\t', schema=schema_title_akas)
    from_csv_df_ratings = spark_session.read.csv(
        file_read_ratings, header=True, nullValue='null', sep=r'\t', schema=schema_ratings_basics)
    temp_df1 = from_csv_df.select("tconst", "isAdult").filter(f.col("isAdult") == 1)
    temp_df2 = from_csv_df_akas.select("region", "titleId", "title")\
        .filter((f.col("region").isNotNull()) & (f.col("region") != r"\N")).withColumnRenamed("titleId", "tconst")
    temp_df3 = temp_df1.join(temp_df2, "tconst")
    temp_df4 = temp_df3.join(from_csv_df_ratings.select("averageRating", "tconst"), "tconst")
    window = Window.partitionBy("region").orderBy("region")
    temp_df4 = temp_df4.withColumn("adult_per_region", f.count(f.col("region")).over(window))
    region_min = temp_df4.agg(f.min("adult_per_region")).collect()[0][0]
    region_max = temp_df4.agg(f.max("adult_per_region")).collect()[0][0]
    temp_dfmin = temp_df4.filter(f.col("adult_per_region") == region_min).orderBy(f.col("averageRating").desc()).limit(100)
    temp_dfmax = temp_df4.filter(f.col("adult_per_region") == region_max).orderBy(f.col("averageRating").desc()).limit(100)
    from_csv_df_task8 = temp_dfmin.union(temp_dfmax)
    from_csv_df_task8.show(200, truncate=False)
    #file_write = r'.\Data\output\task08'
    #from_csv_df_task8.write.csv(file_write, header=True, mode="overwrite")
    return 0
