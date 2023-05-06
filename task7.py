"""
 Get 10 titles of the most popular movies/series etc. by each decade.
Получите 10 наименований самых популярных фильмов/сериалов и т. д. за каждое десятилетие.
title.basics.tsv.gz title.ratings.tsv.gz
"""
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f
from pyspark.sql import Window
def task7():
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
    schema_ratings_basics = t.StructType([
        t.StructField("tconst", t.StringType(), nullable=True),
        t.StructField("averageRating", t.DoubleType(), nullable=True),
        t.StructField("numVotes", t.IntegerType(), nullable=True)
    ])
    file_read_basics = r'.\Data\input\title.basics.tsv.gz'
    file_read_ratings = r'.\Data\input\title.ratings.tsv.gz'
    from_csv_basics_df = spark_session.read.csv(file_read_basics, header=True, nullValue='null', sep=r'\t', schema=schema_title_basics)
    from_csv_ratings_df = spark_session.read.csv(file_read_ratings, header=True, nullValue='null', sep=r'\t', schema=schema_ratings_basics)
    temp_df1 = from_csv_basics_df.select("tconst", "originalTitle", "startYear").filter(f.col("startYear").isNotNull())
    temp_df2 = from_csv_ratings_df.select("tconst", "averageRating")
    temp_df3 = temp_df1.join(temp_df2, "tconst")
    temp_df3 = temp_df3.select("originalTitle", "startYear", "averageRating")\
        .withColumn("N_decade", f.floor(f.col("startYear") / 10).cast(t.IntegerType()))
    window = Window.partitionBy("N_decade").orderBy(f.desc("averageRating"))
    from_csv_df_task7 = temp_df3.withColumn("Rating_decade", f.row_number().over(window)).where(f.col("Rating_decade") <= 10)
    #from_csv_df_task7.show(100, truncate=False)
    file_write = r'.\Data\output\task07'
    from_csv_df_task7.write.csv(file_write, header=True, mode="overwrite")
    return 0
