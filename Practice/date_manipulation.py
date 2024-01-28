from pyspark.sql import SparkSession
from pyspark.sql.functions import date_add,col, date_sub ,current_date , current_timestamp, coalesce

spark = SparkSession.builder.getOrCreate()

date_df = spark.range(10).withColumn("current_date",current_date()).withColumn("current_time",current_timestamp())

date_df.printSchema()

#date addition and substraction
#date_df.select(date_add(col("current_date"),5).alias("added_date"),col("current_date")).show()

#datediff() , to_date() - convert string to date and months_between()

#Null managing
#coalesce() - resturns first non NULL value

circuits_df = spark.read.option("header",True).option("inferSchema",True).csv("Data/circuits.csv").filter(col("circuitId") < 15)

#drop columns if it has NULL
circuits_df.na.drop("all") #all columns are NULL are dropped
circuits_df.na.drop("any") #any column is NULL in a row

#Fill - fill the NULL values with a particular value for particular columns
circuits_df.na.fill("random",subset=["location","country"])

#or we can also provide a map
circuits_df.na.fill({'location':'random','country':'somewhere'})

