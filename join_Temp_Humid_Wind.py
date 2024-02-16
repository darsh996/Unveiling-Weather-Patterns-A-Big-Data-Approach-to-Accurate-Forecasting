#Entrypoint 2.x
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import os


spark = SparkSession.builder.appName("Spark SQL basic example").enableHiveSupport().getOrCreate()
sc = spark.sparkContext

from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, DateType

city_df = spark.read.csv('file:///home/talentum/test-jupyter/Daily/program/Indian_Cities_Database.csv', header=True)

for city in city_df.collect():
    humid_df = spark.read.csv(f'file:///home/talentum/test-jupyter/Hourly/{city["City"]}_humid/*.csv', header=True)
    temp_df = spark.read.csv(f'file:///home/talentum/test-jupyter/Hourly/{city["City"]}_temp/*.csv', header=True)
    wind_df = spark.read.csv(f'file:///home/talentum/test-jupyter/Hourly/{city["City"]}_wind/*.csv', header=True)

    # joining humid_df, temp_df and wind_df to produce final dataframe
    final_df = humid_df.join(temp_df, humid_df.Date == temp_df.Date, 'inner') \
    .join(wind_df , humid_df.Date == wind_df.Date, 'inner') \
    .select(humid_df.Date, "PS","PSC","T2M","T2MWET","T2MDEW","WS2M","WD2M","WD10M","WS10M","QV2M","RH2M","PRECTOTCORR")

    # final_df.show()
    # write dataframe to file in csv format
    final_df.write.csv(f'file:///home/talentum/test-jupyter/Hourly/{city["City"]}',header=True)
    # TO DUMP INTO MONGO DATABASE