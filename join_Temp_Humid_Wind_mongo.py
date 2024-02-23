#Entrypoint 2.x
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import os


spark = SparkSession.builder.appName("combine temperature wind and humidity data") \
.config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/weather.city") \
.config("spark.mongodb.input.uri","mongodb://127.0.0.1:27017/weather.city") \
.getOrCreate()

# from pymongo import MongoClient
# client = MongoClient("mongodb://localhost:27017/")
# db = client.weather

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
    .select(humid_df.Date,"PS","PSC","T2M","T2MWET","T2MDEW","WS2M","WD2M","WD10M","WS10M","QV2M","RH2M","PRECTOTCORR") \
    .withColumn("City",F.lit(city["City"])) \
    .withColumn("_id", F.concat(F.col('Date'),F.lit("_"),F.col("City")))
    
    
    # read historical data first time only
    #city_records_df = spark.read.csv(f'{file_path}/Dataset/city/{city["City"]}/*.csv', header=True)
    #print("city_df")
    #city_records_df.printSchema()
    #print("final_df")
    #final_df.printSchema()
    #city_records_df = final_df.union(city_records_df).withColumn("City",F.lit(city["City"])) \
    #.withColumn("_id", F.concat(F.col('Date'),F.lit("_"),F.col("City")))
    
    final_df.coalesce(1).write.csv(f'{file_path}/newdata/city/{city["City"]}', header=True, mode="overwrite")
    
    # using python mongo write into mongodb

    # for row in city_records_df.collect():
    #     if db.city.find_one({'Date': row['Date'], 'City': row['City']}):
    #         print(f"Data with Date '{row['Date']}' City : {row['City']} already exists. Skipping...")
    #     else:
    #        db.city.insert_one(row.asDict())
    # else:
    # 	print(f"{city['City']} done")
    
    
    # write to mongodb 
    final_df.write \
    .format("com.mongodb.spark.sql.DefaultSource") \
    .mode("append") \
    .save()
    
   
