# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls dbfs:/FileStore/tables

# COMMAND ----------

races_df = spark.read.option("header",True).csv("dbfs:/FileStore/tables/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### change the schema of the file

# COMMAND ----------

from pyspark.sql.types import *
races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True) 
])

# COMMAND ----------

races_df = spark.read.option("header",True).schema(races_schema).csv("dbfs:/FileStore/tables/races.csv")

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### add additional column into table with "withcolumn" function

# COMMAND ----------

from pyspark.sql.functions import *
races_with_timestamp_df = races_df.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')).withColumn("data_source",lit(v_data_source))


# COMMAND ----------

display(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select the columns we want with aliasing

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'), col('round'), col('circuitId').alias('circuit_id'),col('name'),col('race_timestamp'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write data to parquet file

# COMMAND ----------

races_selected_df.write.mode("overwrite").partitionBy("race_year").parquet("dbfs:/FileStore/tables/processed/races")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "dbfs:/FileStore/tables/processed/races"

# COMMAND ----------

display(spark.read.parquet("dbfs:/FileStore/tables/processed/races/race_year=2021/"))

# COMMAND ----------


