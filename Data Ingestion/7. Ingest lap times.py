# Databricks notebook source
# MAGIC %fs
# MAGIC ls "dbfs:/FileStore/tables"

# COMMAND ----------

from pyspark.sql.types import *
lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

paths = ["dbfs:/FileStore/tables/lap_times_split_1.csv","dbfs:/FileStore/tables/lap_times_split_2.csv","dbfs:/FileStore/tables/lap_times_split_3.csv","dbfs:/FileStore/tables/lap_times_split_4.csv","dbfs:/FileStore/tables/lap_times_split_5.csv"]
lap_times_df = spark.read.format("csv").option("schema",lap_times_schema).load(paths)

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

from pyspark.sql.functions import *
lap_times_final_df = lap_times_df.withColumnRenamed("_c0", "raceId") \
                                             .withColumnRenamed("_c1", "driverId") \
                                             .withColumnRenamed("_c2", "lap") \
                                             .withColumnRenamed("_c3", "position") \
                                             .withColumnRenamed("_c4", "time") \
                                             .withColumnRenamed("_c5", "milliseconds") \
                                             .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

lap_times_final_df.count()

# COMMAND ----------

display(lap_times_final_df)

# COMMAND ----------

final_df = lap_times_final_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id")

# COMMAND ----------

final_df.write.mode("overwrite").parquet("dbfs:/FileStore/tables/processed/laptimes")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "dbfs:/FileStore/tables/processed"

# COMMAND ----------


