from pyspark.sql.functions import lit, ltrim, rtrim, expr, rpad, lpad, trim, locate, col, regexp_replace
from pyspark.sql import SparkSession


spark = SparkSession.builder.getOrCreate()

df = spark.read.option("header",True).option("inferSchema",True).csv("Data/circuits.csv").filter(col("circuitId") < 15)

# df.select(
# ltrim(lit(" HELLO ")).alias("ltrim"),
# rtrim(lit(" HELLO ")).alias("rtrim"),
# trim(lit(" HELLO ")).alias("trim"),
# lpad(lit("HELLO"), 7, '#').alias("lp"),
# rpad(lit("HELLO"), 7, '#').alias("rp")).show(2)

# df.select(regexp_replace(col("url"),"http","sftp").alias("updated_url"),col("url")).show(truncate=False)

#Python Function - Circuit name locator
# simpleNames = ["circuit","de"]
# def names_locator(column , name_string):
#     return locate (name_string.upper() , column) \
#             .cast("boolean") \
#             .alias("is_present")

# selectedRows = [names_locator(df.name,c) for c in simpleNames]

#selectedRows.append(expr("*"))

#df.select(*selectedRows,expr('*')).show(3, False)
x = [4,5,6,7]

df.select(col("name"),col("location"),*x).show()
