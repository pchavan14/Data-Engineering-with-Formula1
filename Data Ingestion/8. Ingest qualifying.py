# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),


# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "dbfs:/FileStore/tables/"

# COMMAND ----------

paths = ["dbfs:/FileStore/tables/qualifying_split_1.json","dbfs:/FileStore/tables/qualifying_split_2.json"]
qualifying_df = spark.read.format("json").option("schema",qualifying_schema).option("multiLine", True).load(paths)

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

from pyspark.sql.functions import *
final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

final_df.write.mode("overwrite").parquet("dbfs:/FileStore/tables/processed/qualifying")

# COMMAND ----------


