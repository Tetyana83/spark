"""
 Get names of people, corresponding movies/series and characters they
played in those films.
Получите имена людей, соответствующие фильмы/сериалы и персонажей, которых они
играл в этих фильмах.
title.basics.tsv.gz title.principals.tsv.gz name.basics.tsv.gz
"""
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f
def task4():
    spark_session = (SparkSession.builder
        .master("local")
        .appName("task app")
        .config(conf=SparkConf())
        .getOrCreate())
    file_read1 = r'.\Data\input\title.basics.tsv.gz'
    file_read2 = r'.\Data\input\title.principals.tsv.gz'
    file_read3 = r'.\Data\input\name.basics.tsv.gz'
    schema_title_basics = t.StructType([
        t.StructField("tconst", t.StringType(), nullable=True),
        t.StructField("titleType", t.StringType(), nullable=True),
        t.StructField("primaryTitle", t.StringType(), nullable=True),
        t.StructField("originalTitle", t.StringType(), nullable=True),
        t.StructField("isAdult", t.StringType(), nullable=True),
        t.StructField("startYear", t.IntegerType(), nullable=True),
        t.StructField("endYear", t.IntegerType(), nullable=True),
        t.StructField("runtimeMinutes", t.IntegerType(), nullable=True),
        t.StructField("genres", t.StringType(), nullable=True)
        ])
    from_csv_title_df = spark_session.read.csv(
        file_read1,
        sep='\t',
        header=True,
        schema=schema_title_basics
    )
    schema_title_principals = t.StructType([
        t.StructField("tconst", t.StringType(), nullable=True),
        t.StructField("ordering", t.StringType(), nullable=True),
        t.StructField("nconst", t.StringType(), nullable=True),
        t.StructField("category", t.StringType(), nullable=True),
        t.StructField("job", t.StringType(), nullable=True),
        t.StructField("characters", t.StringType(), nullable=True)
        ])
    from_csv_principals_df = spark_session.read.csv(
        file_read2,
        sep='\t',
        header=True,
        schema=schema_title_principals
    )
    schema_title_name = t.StructType([
        t.StructField("nconst", t.StringType(), nullable=True),
        t.StructField("primaryName", t.StringType(), nullable=False),
        t.StructField("birthYear", t.IntegerType(), nullable=True),
        t.StructField("deathYear", t.IntegerType(), nullable=True),
        t.StructField("primaryProfession", t.StringType(), nullable=True),
        t.StructField("knownForTitles", t.StringType(), nullable=True)
        ])
    from_csv_name_df = spark_session.read.csv(
        file_read3,
        sep='\t',
        header=True,
        schema=schema_title_name
    )
    df_temp01 = from_csv_principals_df.select("tconst", "nconst", "characters").filter(f.col("characters") != r"\N")
    df_temp02 = from_csv_name_df.select("nconst", "primaryName")
    df_temp03 = df_temp01.join(df_temp02, "nconst")
    df_temp04 = from_csv_title_df.select("tconst", "primaryTitle")
    from_csv_df_task4 = df_temp03.join(df_temp04, "tconst")\
        .select("primaryName", "characters", "primaryTitle").orderBy("primaryName")
    #from_csv_df_task4.show(100, truncate=False)
    file_write = r'.\Data\output\task04'
    from_csv_df_task4.write.csv(file_write, header=True, mode="overwrite")
    return 0
