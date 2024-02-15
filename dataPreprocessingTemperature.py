#Entrypoint 2.x
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import os

spark = SparkSession.builder.appName("Spark SQL basic example").enableHiveSupport().getOrCreate()
sc = spark.sparkContext


from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, DateType
schema = StructType([
    StructField("properties", StructType([
        StructField("parameter", StructType([
            StructField("PS", MapType(StringType(),DoubleType()),True),
            StructField("PSC", MapType(StringType(),DoubleType()),True),
            StructField("T2MWET",  MapType(StringType(),DoubleType()),True),
            StructField("T2MDEW",  MapType(StringType(),DoubleType()),True),
            StructField("T2M",  MapType(StringType(),DoubleType()),True)
        ]), True)
    ]), True),
    StructField("geometry", StructType([
        StructField("coordinates", ArrayType(DoubleType()),True)
    ]), True)
])


city_df = spark.read.csv('file:///home/talentum/test-jupyter/Daily/program/Indian_Cities_Database.csv', header=True)
for city in city_df.collect():
    # print(city["City"])
    # Load the json file
    file_path = f'/home/talentum/test-jupyter/Hourly/dataset/{city["City"]}_*.json'

    data_df = spark.read.json(f'file://{file_path}', multiLine=True, schema=schema) \
    .select(F.col("properties.parameter.*"),F.col("geometry.coordinates"))

    # create dataframe for features "PS","PSC","T2M","T2MWET","T2MDEW" 
    from pyspark.sql.functions import explode
    PS =  data_df.select('coordinates',explode(data_df.PS).alias("Date", "PS"))
    PSC = data_df.select(explode(data_df.PSC).alias("Date", "PSC"))
    T2MDEW = data_df.select(explode(data_df.T2MDEW).alias("Date", "T2MDEW"))
    T2MWET = data_df.select(explode(data_df.T2MWET).alias("Date", "T2MWET"))
    T2M = data_df.select(explode(data_df.T2M).alias("Date", "T2M"))


    from pyspark.sql.functions import desc
    temp_df = PS.join(PSC, PS.key == PSC.key, 'inner') \
    .join(T2M , PS.key == T2M.key, 'inner') \
    .join(T2MWET , PS.key == T2MWET.key, 'inner') \
    .join(T2MDEW , PS.key == T2MDEW.key, 'inner') \
    .select(PS.key, "coordinates","PS","PSC","T2M","T2MWET","T2MDEW")

    
    temp_df.write.csv(f'file:///home/talentum/test-jupyter/Hourly/Output/{city["City"]}_temp', header=True)