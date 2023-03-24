# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest json data

# COMMAND ----------

constructor_schema = "constructorId INTEGER, constructorRef STRING, name STRING, nationality STRING,url STRING"

# COMMAND ----------

constructor_df = spark.read.schema(constructor_schema).json("dbfs:/FileStore/tables/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### drop unwanted columns

# COMMAND ----------

constructor_dropped_df = constructor_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ### rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import *
constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp())
                                             

# COMMAND ----------

# MAGIC %md
# MAGIC ### write data to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("dbfs:/FileStore/tables/processed/constructors")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "dbfs:/FileStore/tables/processed"

# COMMAND ----------


