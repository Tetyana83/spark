"""
Get the list of peopleʼs names, who were born in the 19th century.
Получить список имен людей, родившихся в 19 веке.
name.basics.tsv.gz
"""
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f


def task2():
    spark_session = (SparkSession.builder
        .master("local")
        .appName("task app")
        .config(conf=SparkConf())
        .getOrCreate())
    schema_name_basics = t.StructType([
        t.StructField("nconst", t.StringType(), nullable=True),
        t.StructField("primaryName", t.StringType(), nullable=False),
        t.StructField("birthYear", t.IntegerType(), nullable=True),
        t.StructField("deathYear", t.IntegerType(), nullable=True),
        t.StructField("primaryProfession", t.StringType(), nullable=True),
        t.StructField("knownForTitles", t.StringType(), nullable=True)
        ])
    file_read = r'.\Data\input\name.basics.tsv.gz'
    from_csv_df = spark_session.read.csv(file_read, header=True, nullValue='null', sep=r'\t', schema=schema_name_basics)
    from_csv_df_task2 = from_csv_df.select("primaryName", "birthYear")\
        .filter((f.col("birthYear") < 1900) & (f.col("birthYear") > 1799))
    from_csv_df_task2.show(100, truncate=False)
    #file_write = r'.\Data\output\task02'
    #from_csv_df_task2.write.csv(file_write, header=True, mode="overwrite")
    return 0
