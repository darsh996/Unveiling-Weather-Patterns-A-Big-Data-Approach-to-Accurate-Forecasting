#Entrypoint 2.x
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import os


spark = SparkSession.builder.appName("combine temperature wind and humidity data").getOrCreate()
sc = spark.sparkContext

from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, DateType

file_path = 'file:///home/hadoop/Documents'

city_df = spark.read.csv(f'{file_path}/Indian_Cities_Database.csv', header=True)

for city in city_df.collect():
    humid_df = spark.read.csv(f'{file_path}/newdata/temp/{city["City"]}_humid/*.csv', header=True)
    temp_df = spark.read.csv(f'{file_path}/newdata/temp/{city["City"]}_temp/*.csv', header=True)
    wind_df = spark.read.csv(f'{file_path}/newdata/temp/{city["City"]}_wind/*.csv', header=True)

    # joining humid_df, temp_df and wind_df to produce final dataframe
    final_df = humid_df.join(temp_df, humid_df.Date == temp_df.Date, 'outer') \
    .join(wind_df , humid_df.Date == wind_df.Date, 'outer') \
    .select(humid_df.Date,"PS","PSC","T2M","T2MWET","T2MDEW","WS2M","WD2M","WD10M","WS10M","QV2M","RH2M","PRECTOTCORR")

    # final_df.show()
    
    city_df = spark.read.csv(f'{file_path}/Dataset/city/{city["City"]}/*.csv', header=True)
    
    city_df = final_df.union(city_df)
    # write dataframe to file in csv format
    city_df.coalesce(1).write.csv(f'{file_path}/newdata/city/{city["City"]}', header=True, mode="overwrite")
    # TO DUMP INTO MONGO DATABASE
