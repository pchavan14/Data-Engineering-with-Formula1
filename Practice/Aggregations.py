from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

races_df = spark.read.option("header","true").option("inferSchema","true").csv("Data/races.csv")

#races_df.show(5,truncate=False)

filtered_races_df = races_df.filter(col("year") == 2020)

#Count and count distinct

selected_df = filtered_races_df.select(count('*').alias("all_count"), countDistinct('name'))

#count() , col() and sum() all these are functions 

#selected_df.show()

#group by
races_result_df = spark.read.parquet("Output/parquet")
filetered_races_result_df = races_result_df.filter("race_year in (2019,2020)")
##Group by is a grouped object on which we can apply different methods
new_df = filetered_races_result_df.groupBy("race_year","driver_name").agg(sum("points").alias("total_points")).orderBy(col("total_points").desc())


new_df.show(truncate=False)
## Window functions 