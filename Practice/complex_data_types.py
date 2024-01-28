#structs , arrays and maps

#Structs is dataframe within a dataframe
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, split, size, explode

spark = SparkSession.builder.getOrCreate()

circuits_df = spark.read.option("header",True).option("inferSchema",True).csv("Data/circuits.csv").filter(col("circuitId") < 15)

circuits_df_complex = circuits_df.select(struct(col("name"),col("location")).alias("complex"))

circuits_df_complex.show(5,truncate=False)

#circuits_df_complex.select(col("complex").getField("name"),col("complex").getField("location")).show(truncate=False)

#split() - we can convert our column values into a arrays

# circuits_df.select(split(col("name")," ").alias("complex_name"), size(split(col("name")," ")) \
#                    .alias("complex_name_size")) \
#                     .filter(col("complex_name_size")<5) \
#                     .show(truncate=False)

#explode - explode each value of array in seperate row
circuits_df.withColumn("complex_name",split(col("name")," ")) \
            .withColumn("exploded_name",explode(col("complex_name"))) \
            .select("complex_name", "exploded_name", col("location"),col("lat") ).show(truncate=False)