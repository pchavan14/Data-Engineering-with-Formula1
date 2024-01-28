from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

csv_data = spark.read.option("inferSchema","true").option("header","true").csv("Data/races.csv")


csv_data.filter(col("year") == '2019').explain(True)

#filtered_csv_data.show(5,truncate=False)


