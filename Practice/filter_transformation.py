from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()




#we can use & (and) and || (or) conditions 
# races_filtered_df = races_df.filter((col("year")== '2019') & (col("round") <= 5))
# races_filtered_df.show(truncate=False)

#join transformations 

#read the data
races_df = spark.read.option("header",True).option("inferSchema",True).csv("Data/races.csv").filter(col("year")==2019)


races_df.show(5,truncate=False)

circuits_df = spark.read.option("header",True).option("inferSchema",True).csv("Data/circuits.csv").withColumnRenamed("name","circuit_name").filter(col("circuitId")<70)

races_df.printSchema()
circuits_df.printSchema()


races_circuits_join = races_df.join(circuits_df,races_df.circuitId == circuits_df.circuitId,"inner").select(races_df.circuitId, races_df.name,circuits_df.location , circuits_df.circuitId).show(truncate=False)


#circuits which do not have the races
#circuits_no_races_df = circuits_df.join(races_df,races_df.circuitId == circuits_df.circuitId,"left").select(races_df.circuitId, races_df.name,circuits_df.location , circuits_df.circuitId).show(truncate=False)

#races_no_circuits_df = circuits_df.join(races_df,races_df.circuitId == circuits_df.circuitId,"right").select(races_df.circuitId, races_df.name,circuits_df.location , circuits_df.circuitId).show(truncate=False)

races_no_circuits_df = circuits_df.join(races_df,races_df.circuitId == circuits_df.circuitId,"full").select(races_df.circuitId, races_df.name,circuits_df.location , circuits_df.circuitId).show(truncate=False)

#semi join - shows only records  of the left dataframe ( columns can be selected from only left dataframe)
#anti join - shows only the non joined records of left dataframe

#results - race_year , race_name , race_date , circuit_location , driver_name , number , nationality , team , grid , fastest lap , race time , points , created_date(current_timestamp)


