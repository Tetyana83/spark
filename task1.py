"""
Get all titles of series/movies etc. that are available in Ukrainian.
Получить все названия сериалов/фильмов и т.д., которые доступны на украинском языке.
title.akas.tsv.gz
"""
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f
def task1():
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("task app")
                     .config(conf=SparkConf())
                     .getOrCreate())
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
    file_read = r'.\Data\input\title.akas.tsv.gz'
    from_csv_df = spark_session.read.csv(file_read, header=True, nullValue='null', sep=r'\t', schema=schema_title_akas)
    from_csv_df_task1 = from_csv_df.select("title", "region").where(f.col("region") == "UA")
    from_csv_df_task1.show(100, truncate=False)
    #file_write = r'.\Data\output\task01'
    #from_csv_df_task1.write.csv(file_write, header=True, mode="overwrite")
    return 0
