# Databricks notebook source
df = spark.read.format('csv') \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .load('dbfs:/FileStore/tables/circuits.csv')

display(df)

# COMMAND ----------

 
