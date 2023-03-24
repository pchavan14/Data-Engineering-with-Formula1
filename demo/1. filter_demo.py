# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

##filter the records using sql
races_filtered_df = races_df.filter("race_year = 2019 and round <=5")

# COMMAND ----------

##filter the records using python can use where or filter 
races_filtered_df = races_df.where((races_df["race_year"] == 2019) & (races_df["round"] <= 5))
