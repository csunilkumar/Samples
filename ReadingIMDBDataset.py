# Databricks notebook source
dbutils.fs.ls("abfss://imdbdataset@adbsampleswestus2.dfs.core.windows.net")

# COMMAND ----------

spark.read.csv("abfss://imdbdataset@adbsampleswestus2.dfs.core.windows.net/title.basics.tsv.gz", header="true", inferSchema="true", sep="\t").show()

# COMMAND ----------

spark.read.csv("abfss://imdbdataset@adbsampleswestus2.dfs.core.windows.net/name.basics.tsv.gz", header="true", inferSchema="true", sep="\t").show()

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Read the data into a DataFrame
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("sep", "\t").load("abfss://imdbdataset@adbsampleswestus2.dfs.core.windows.net/title.basics.tsv.gz")

# Write the DataFrame to a table
df.write.format("delta").mode("overwrite").saveAsTable("imdbdataset.imdb.title")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS imdbdataset.imdb.volume
# MAGIC LOCATION 'abfss://imdbdataset@adbsampleswestus2.dfs.core.windows.net/volume';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE CATALOG imdbdataset;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into imdbdataset.imdb.volume select * from imdb.full
