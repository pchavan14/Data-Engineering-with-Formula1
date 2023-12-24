from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

races_df = spark.read.option("header",True).csv("Data/races.csv")

#we can use & (and) and || (or) conditions 
races_filtered_df = races_df.filter((col("year")== '2019') & (col("round") <= 5))
races_filtered_df.show(truncate=False)