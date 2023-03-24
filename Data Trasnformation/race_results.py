# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "dbfs:/FileStore/tables/processed"

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers/").withColumnRenamed("number","driver_number").withColumnRenamed("name","driver_name").withColumnRenamed("nationality","driver_nationality")

# COMMAND ----------

display(drivers_df.count())

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors/").withColumnRenamed("name","team")

# COMMAND ----------

display(constructors_df.count())

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits/").withColumnRenamed("location","circuit_location")

# COMMAND ----------

display(circuits_df.count())

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races/").withColumnRenamed("name","race_name").withColumnRenamed("race_timestamp","race_date")

# COMMAND ----------

display(races_df.count())

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results/").withColumnRenamed("time","race_time")

# COMMAND ----------

display(results_df.count())

# COMMAND ----------

races_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id,"inner").select(races_df.race_id,races_df.race_year,races_df.race_name,races_df.race_date,circuits_df.circuit_location)

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

races_results_df  = results_df.join(races_circuits_df,races_circuits_df.race_id == results_df.race_id) \
                              .join(drivers_df,results_df.driver_id == drivers_df.driver_id) \
                              .join(constructors_df,constructors_df.constructor_id == results_df.constructor_id)

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

display(races_results_df)

# COMMAND ----------

from pyspark.sql.functions import *
final_df = races_results_df.select("race_year","race_name","race_date","circuit_location","driver_name","driver_number","driver_nationality","team","grid","fastest_lap","race_time","points","position").withColumn("created_date",current_timestamp())

# COMMAND ----------

display(final_df.filter("race_year = 2019 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/tables/presentation")

# COMMAND ----------

final_df.write.mode("overwrite").parquet("dbfs:/FileStore/tables/presentation/race_results")

# COMMAND ----------

# MAGIC %sql

# COMMAND ----------

# MAGIC %sql
# MAGIC create table race_results
# MAGIC as
# MAGIC select * from PARQUET.`dbfs:/FileStore/tables/presentation/race_results`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from race_results

# COMMAND ----------


