from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

races_df = spark.read.option("header",True).csv("Data/races.csv").filter(col("year")==2019)


#we can use & (and) and || (or) conditions 
# races_filtered_df = races_df.filter((col("year")== '2019') & (col("round") <= 5))
# races_filtered_df.show(truncate=False)

#join transformations 
circuits_df = spark.read.option("header",True).option("inferSchema",True).csv("Data/circuits.csv")

circuits_df.printSchema()
races_df.printSchema()

races_circuits_df = races_df.join(circuits_df,races_df.circuitId == circuits_df.circuitId, "inner").select(races_df.circuitId, races_df.year,circuits_df.name)

races_circuits_df.show(5,truncate=False)



