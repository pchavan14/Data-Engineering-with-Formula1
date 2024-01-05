#creating the results dataframe

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

races_df = spark.read.option("header",True).option("inferSchema",True).csv("Data/races.csv").select(col("year").alias("race_year"),col("name").alias("race_name"),col("date").alias("race_date"),col("circuitId"),col("raceId"))

#races_df.show(5,truncate=False)

circuits_df = spark.read.option("header",True).option("inferSchema",True).csv("Data/circuits.csv").select(col("location").alias("circuit_location"),col("circuitId"))


#circuits_df.show(5,truncate=False)

#drivers - nested json

nested_json_schema = StructType(fields=[
                        StructField("code",StringType()),
                        StructField("dob",StringType()),
                        StructField("driverId",LongType()),
                        StructField("driverRef",StringType()),
                        StructField("name",StructType(fields=[(StructField("forename",StringType())),(StructField("surname",StringType()))])),
                        StructField("nationality",StringType()),
                        StructField("number",StringType()),
                        StructField("url",StringType())

])

nested_json = spark.read.option("schema",nested_json_schema).json("Data/drivers.json").select(col("name").alias("driver_name"),col("number").alias("driver_number"),col("nationality").alias("driver_nationality"),col("driverId"))
#nested_json.show(5,truncate=False)

constructors_df = spark.read.option("inferSchema",True).json("Data/constructors.json").select(col("name").alias("team"),col("constructorId"))

results_df = spark.read.option("inferSchema",True).json("Data/results.json").select(col("grid"),col("fastestLap"),col("time").alias("race_time"),col("points"),col("constructorId"),col("driverId"),col("raceId"))

races_cicrcuits_join_df = races_df \
.join(circuits_df,races_df.circuitId==circuits_df.circuitId,"inner")

results_nested_join_constrcutors = results_df \
.join(constructors_df,results_df.constructorId == constructors_df.constructorId,"inner") \
.join(nested_json, nested_json.driverId == results_df.driverId, "inner") \
.join(races_cicrcuits_join_df , races_cicrcuits_join_df.raceId == results_df.raceId,"inner") 


results_nested_join_constrcutors.printSchema()

final_results = results_nested_join_constrcutors.withColumn("created_date",current_timestamp()).select(results_nested_join_constrcutors.race_year,results_nested_join_constrcutors.race_name , results_nested_join_constrcutors.race_date,results_nested_join_constrcutors.circuit_location,results_nested_join_constrcutors.driver_name,results_nested_join_constrcutors.driver_number,results_nested_join_constrcutors.driver_nationality,results_nested_join_constrcutors.team,results_nested_join_constrcutors.grid,results_nested_join_constrcutors.fastestLap,results_nested_join_constrcutors.race_time,results_nested_join_constrcutors.points)

final_results.filter((col("race_year") == 2008) & (col("race_name") == 'British Grand Prix')).orderBy(col("points").desc())

final_results.write.partitionBy("race_year").parquet("Output/parquet")

