from pyspark.sql import Row , SparkSession
from pyspark.sql.functions import expr, col , lit
from pyspark.sql.types import StructField, StructType, StringType, LongType , IntegerType

spark = SparkSession.builder.getOrCreate()
myManualSchema = StructType([ StructField("some", StringType(), True), StructField("col", StringType(), True), StructField("names", LongType(), False) ])
myRow = [("Hello", None, 1) ,("Hello", None, 1), ("Bye", None , 2)]
myDf = spark.createDataFrame(data=myRow, schema=myManualSchema) 

#getting distinct value in single or multiple columns
myDf.select(col("some")).show()
myDf.select(col("some")).distinct().show()

#myDf.select(expr("some as tomb"), col("names")).show()

##selectExpr is more of a SQL way of doing things

# csv_df = spark.read.option("header",True).option("inferSchema",True).csv("Data/races.csv")

# csv_df.show(5,truncate=False)

new_csv_df = myDf.select(expr("names * 5 as new_names") , col("some"))

#new_csv_df.show(5)

#show all columns , like SQL 

#myDf.select(expr("*") , lit("1").alias("random")).show()

new_df = myDf.withColumn("This long column-name", col("names"))
#new_df.select(expr("`This long column-name`"), col("This long column-name")).show()

#Union of two dataframes
myRow = [("Hello", None, 1) ,("Bye", None , 2)]
myRow2 = [("Ciao", None, 3) ,("Halo", None, 4)]

schema = StructType(fields=[
    StructField("Salutation", StringType()),
    StructField("Value", StringType()),
    StructField("Count", StringType())
])

df1 = spark.createDataFrame(data=myRow, schema=schema)
df2 = spark.createDataFrame(data=myRow2, schema=schema)

df3 = df1.union(df2)

df3.sort(col("Salutation").asc_nulls_first()).show()
df3.orderBy(col("Salutation").asc_nulls_first(), col("count")).show()

df3.limit(2).show()