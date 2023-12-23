#read the file 

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

#dataframe reader API , without any options , inferSchema is ok in DEV or with small amount of data . But in production we want spark to use our schema and to fail if it does not  infer to schema 
csv_df = spark.read.csv("Data/f1db_csv/circuits.csv")


#define your own schema , structtype is row and structfield are columns of these rows
csv_schema = StructType(fields=[StructField("circuitId",IntegerType(),nullable=False),
                                StructField("circuitRef",StringType(),nullable=True),
                                StructField("name",StringType(),nullable=True),
                                StructField("location",StringType(),nullable=True),
                                StructField("country",StringType(),nullable=True),
                                StructField("lat",DoubleType(),nullable=True),
                                StructField("lng",DoubleType(),nullable=True),
                                StructField("alt",IntegerType(),nullable=True),
                                StructField("url",StringType(),nullable=True)
                                ])

csv_df = spark.read \
    .option("schema",csv_schema) \
    .option("header","true") \
    .csv("Data/f1db_csv/circuits.csv")

csv_df.show(5,truncate=False)

#get the schema
csv_df.printSchema()

#Select only required columns from data
csv_selected = csv_df.select(col("circuitId"),col("country").alias("race_country"),col("lat"), col("lng"))

csv_selected.show(truncate=False)

#renaming the column (can be done with .alias() function as well)
csv_renamed = csv_selected.withColumnRenamed("circuitId","circuit_id") \
                          .withColumnRenamed("lat","latitude")

csv_renamed.show(5,truncate=False)

#adding new column withColumn() function 
csv_final = csv_renamed.withColumn("ingestion_time",current_timestamp())

csv_final.show(5,truncate=False)

#to add a literal value as a new column 

csv_final_2 = csv_final.withColumn("env",lit("production"))

csv_final_2.show(5,truncate=False)





