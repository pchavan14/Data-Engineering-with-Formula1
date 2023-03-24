# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

constructor_standings_df = race_results_df.groupBy("race_year","team") \
                                     .agg(sum("points").alias("total_points"),count(when(col("position") == 1 , True)).alias("wins"))

# COMMAND ----------

display(constructor_standings_df.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank

# COMMAND ----------

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points") , desc("wins"))
final_df = constructor_standings_df.withColumn("rank" , rank().over(constructor_rank_spec))

# COMMAND ----------

display(final_df.filter("race_year = 2020"))

# COMMAND ----------

final_df.write.parquet(f"{presentation_folder_path}/constructor_standings")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "dbfs:/FileStore/tables/presentation/race_results/"

# COMMAND ----------

# MAGIC %sql
# MAGIC create table constructor_standings
# MAGIC as
# MAGIC select * from PARQUET.`dbfs:/FileStore/tables/presentation/constructor_standings`

# COMMAND ----------


