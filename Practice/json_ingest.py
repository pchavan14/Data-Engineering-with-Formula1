from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()




json_schema = StructType(fields=[
                StructField("constructorId",LongType(),nullable=True),
                StructField("constructorRef",StringType(),nullable=True),
                StructField("name",StringType(),nullable=True),
                StructField("nationality",StringType(),nullable=True),
                StructField("url",StringType(),nullable=True)
])

json_file = spark.read \
            .option("schema",json_schema) \
            .json("Data/constructors.json")

#json_file.show(5,truncate=False)

json_file.printSchema()

#drop the unwanted column or select only needed columns

dropped_json_file = json_file.drop(col("url"))

#drop duplicates and distinct on a dataframe to remove duplicate rows 
# dropped_json_file = json_file.dropDuplicates()
# dropped_json_file = json_file.distinct()

#dropped_json_file.show(5)

#rename column and add ingestion date
json_file_enhanced = dropped_json_file.withColumnRenamed("constructorId","constructor_id") \
                                      .withColumn("ingestion_date",current_timestamp())

#json_file_enhanced.show(5,truncate=False)

#nested JSON ingestion


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

nested_json = spark.read.option("schema",nested_json_schema).json("Data/drivers.json")

#nested_json.show(5,truncate=False)
#nested_json.printSchema()

#spark by default cannot read multi line JSON , we need to set an option for it
multiline_json = spark.read.option("multiLine",True).json("Data/pit_stops.json")

#multiline_json.show(5,truncate=False)

data = [
    ["Alice", 30,[2,3,4],{"height": "170 cm", "weight": "65 kg"}],
    ["Bob", 25,[3,4,5],{"height": "170 cm", "weight": "65 kg"}]
]

schema_data = StructType(fields=[
                StructField("name",StringType()),
                StructField("age",IntegerType()),
                StructField("salary",ArrayType(IntegerType())),
                StructField("body",StructType(fields=[(StructField("height",StringType())),(StructField("weight",StringType()))]))
])
#createDataFrame function when we have data we just need to process
data_df = spark.createDataFrame(data=data,schema=schema_data)

data_df.show()
