"""
 Get information about how many episodes in each TV Series. Get the top
50 of them starting from the TV Series with the biggest quantity of
episodes.
Получите информацию о том, сколько эпизодов в каждом сериале. Получить вершину
50 из них, начиная с сериала с наибольшим количеством
эпизоды.
title.episode.tsv.gz title.basics.tsv.gz
"""
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f

def task6():
    spark_session = (SparkSession.builder
        .master("local")
        .appName("task app")
        .config(conf=SparkConf())
        .getOrCreate())
    schema_title_episode = t.StructType([
            t.StructField("tconst", t.StringType(), nullable=True),
            t.StructField("parentTconst", t.StringType(), nullable=True),
            t.StructField("seasonNumber", t.IntegerType(), nullable=True),
            t.StructField("episodeNumber", t.IntegerType(), nullable=True),
        ])
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
    file_read1 = r'.\Data\input\title.episode.tsv.gz'
    file_read2 = r'.\Data\input\title.basics.tsv.gz'

    from_csv_episode_df = spark_session.read.csv(
        file_read1, header=True, nullValue='null', sep=r'\t', schema=schema_title_episode)
    from_csv_title_df = spark_session.read.csv(
        file_read2, header=True, nullValue='null', sep='\t', schema=schema_title_basics)
    temp_df1 = from_csv_episode_df.groupBy("parentTconst").count().orderBy(f.desc("count")).limit(50)
    temp_df1 = temp_df1.withColumnRenamed("parentTconst", "tconst")
    temp_df2 = from_csv_title_df.select("tconst", "titleType", "originalTitle").filter(f.col("titleType").isin("tvSeries", "Series"))
    from_csv_df_task6 = temp_df1.join(temp_df2, "tconst").select("originalTitle", "titleType", "count").orderBy(f.desc("count"))
    from_csv_df_task6.show(100)
    #file_write = r'.\Data\output\task06'
    #from_csv_df_task6.write.csv(file_write, header=True, mode="overwrite")
    return 0
