#Entrypoint 2.x
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import os

spark = SparkSession.builder.appName("City Temperature Data Preprocessing").getOrCreate()
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


city_df = spark.read.csv('file:///home/hadoop/Documents/Indian_Cities_Database.csv', header=True)
for city in city_df.collect():
    # print(city["City"])
    # Load the json file
    file_path = f'file:///home/hadoop/Documents/newdata/temperature/{city["City"]}_{city["latitude"]}_{city["longitude"]}_*.json'

    data_df = spark.read.json(file_path, multiLine=True, schema=schema) \
    .select(F.col("properties.parameter.*"),F.col("geometry.coordinates"))

    # create dataframe for features "PS","PSC","T2M","T2MWET","T2MDEW" 
    from pyspark.sql.functions import explode
    PS =  data_df.select('coordinates',explode(data_df.PS).alias("Date", "PS"))
    PSC = data_df.select(explode(data_df.PSC).alias("Date", "PSC"))
    T2MDEW = data_df.select(explode(data_df.T2MDEW).alias("Date", "T2MDEW"))
    T2MWET = data_df.select(explode(data_df.T2MWET).alias("Date", "T2MWET"))
    T2M = data_df.select(explode(data_df.T2M).alias("Date", "T2M"))


    from pyspark.sql.functions import desc
    temp_df = PS.join(PSC, PS.Date == PSC.Date, 'inner') \
    .join(T2M , PS.Date == T2M.Date, 'inner') \
    .join(T2MWET , PS.Date == T2MWET.Date, 'inner') \
    .join(T2MDEW , PS.Date == T2MDEW.Date, 'inner') \
    .select(PS.Date,"PS","PSC","T2M","T2MWET","T2MDEW")
    
    data_df.unpersist()
    T2M.unpersist()
    T2MWET.unpersist()
    T2MDEW.unpersist()
    PS.unpersist()

    
    temp_df.coalesce(1).write.csv(f'file:///home/hadoop/Documents/newdata/temp/{city["City"]}_temp', header=True, mode="ignore")
