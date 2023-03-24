# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

## rename column names before join if same column names
circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits").withColumnRenamed("name","circuit_name")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019")

# COMMAND ----------

races_df.count()

# COMMAND ----------

circuits_df.count()

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id , "inner").select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

##left outer join
