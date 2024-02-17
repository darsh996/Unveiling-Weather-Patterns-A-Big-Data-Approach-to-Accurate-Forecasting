#Entrypoint 2.x
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import os


spark = SparkSession.builder.appName("City Humid Data Preprocessing ").getOrCreate()
sc = spark.sparkContext

from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, DateType

# schema for dataframe
schema = StructType([
    StructField("properties", StructType([
        StructField("parameter", StructType([
            StructField("RH2M", MapType(StringType(),DoubleType()),True),
            StructField("QV2M", MapType(StringType(),DoubleType()),True),
            StructField("WS2M",  MapType(StringType(),DoubleType()),True),
            StructField("PRECTOTCORR",  MapType(StringType(),DoubleType()),True),
        ]), True)
    ]), True),
    StructField("geometry", StructType([
        StructField("coordinates", ArrayType(DoubleType()),True)
    ]), True)
])

city_df = spark.read.csv('file:///home/hadoop/Documents/Indian_Cities_Database.csv', header=True)

for city in city_df.collect():
    #print(city["City"])
    # Load the json file
    file_path = f'file:///home/hadoop/Documents/Dataset/humid/{city["City"]}_{city["latitude"]}_{city["longitude"]}_*.json'
    #file_path = f'file:///home/hadoop/Documents/Dataset/humid/{city["City"]}.json'
    data_df = spark.read.json(file_path, multiLine=True, schema=schema) \
    .select(F.col("properties.parameter.*"),F.col("geometry.coordinates"))
    
    # create dataframe for features #"WS2M","QV2M","RH2M","PRECTOTCORR" 
    from pyspark.sql.functions import explode
    WS2M =  data_df.select('coordinates',explode(data_df.WS2M).alias("Date", "WS2M"))
    RH2M = data_df.select(explode(data_df.RH2M).alias("Date", "RH2M"))
    PRECTOTCORR = data_df.select(explode(data_df.PRECTOTCORR).alias("Date", "PRECTOTCORR"))
    QV2M = data_df.select(explode(data_df.QV2M).alias("Date", "QV2M"))
    
    
    # combining all feature into single dataframe
    from pyspark.sql.functions import desc
    humid_df = WS2M.join(RH2M, WS2M.Date == RH2M.Date, 'inner') \
    .join(QV2M , WS2M.Date == QV2M.Date, 'inner') \
    .join(PRECTOTCORR, WS2M.Date == PRECTOTCORR.Date, 'inner') \
    .select(WS2M.Date,"WS2M","QV2M","RH2M","PRECTOTCORR")
    # coordinates column pending
    # to free up memory from data_df
    data_df.unpersist()
    WS2M.unpersist()
    QV2M.unpersist()
    RH2M.unpersist()
    PRECTOTCORR.unpersist()
    
    # final_df.show()
    # write dataframe to file in csv format
    humid_df.coalesce(1).write.csv(f'file:///home/hadoop/Documents/temp/{city["City"]}_humid', header=True, mode="ignore")
