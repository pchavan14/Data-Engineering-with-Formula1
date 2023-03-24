# Databricks notebook source
race_results_df = spark.read.parquet("dbfs:/FileStore/tables/presentation/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

demo_df = race_results_df.filter("race_year = 2020")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

demo_df.select(countDistinct("race_name")).show()

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum("points"),countDistinct("race_name")).show()

# COMMAND ----------

demo_df.groupBy("driver_name").agg(sum("points").alias("total_points"),countDistinct("race_name")).show()

# COMMAND ----------

demo_df = race_results_df.filter("race_year in (2019,2020)")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

demo_grouped_df = demo_df.groupBy("race_year","driver_name").agg(sum("points").alias("total_points"),countDistinct("race_name"))

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

##all window functions are available for the use
driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))
demo_grouped_df.withColumn("rank",rank().over(driverRankSpec)).show(100)

# COMMAND ----------


