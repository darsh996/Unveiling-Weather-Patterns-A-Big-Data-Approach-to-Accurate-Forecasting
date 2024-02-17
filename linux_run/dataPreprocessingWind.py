

#Entrypoint 2.x
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, DateType

spark = SparkSession.builder.appName("City Humid Data Preprocessing ").getOrCreate()
sc = spark.sparkContext

schema = StructType([
    StructField("properties", StructType([
        StructField("parameter", StructType([
            StructField("WD10M", MapType(StringType(),DoubleType()),True),
            StructField("WS10M", MapType(StringType(),DoubleType()),True),
            StructField("WD2M",  MapType(StringType(),DoubleType()),True),
        ]), True)
    ]), True),
    StructField("geometry", StructType([
        StructField("coordinates", ArrayType(DoubleType()),True)
    ]), True)
])


city_df = spark.read.csv('file:///home/hadoop/Documents/Indian_Cities_Database.csv', header=True)

for city in city_df.collect():      
    # Load the json file
    file_path = f'file:///home/hadoop/Documents/Dataset/wind/{city["City"]}_{city["latitude"]}_{city["longitude"]}_*.json'

    data_df = spark.read.json(file_path, multiLine=True,schema=schema) \
    .select(F.col("properties.parameter.*"), F.col("geometry.coordinates"))


    # create dataframe for features "PS","PSC","T2M","T2MWET","T2MDEW" 
    from pyspark.sql.functions import explode
    WD2M =  data_df.select('coordinates',explode(data_df.WD2M).alias("Date", "WD2M"))
    WD10M = data_df.select(explode(data_df.WD10M).alias("Date", "WD10M"))
    WS10M = data_df.select(explode(data_df.WS10M).alias("Date", "WS10M"))
    #"WD10M",WS10M, WD2M

    from pyspark.sql.functions import desc
    wind_df = WD2M.join(WD10M, WD2M.Date == WD10M.Date, 'inner') \
    .join(WS10M , WD2M.Date == WS10M.Date, 'inner') \
    .select(WD2M.Date, "WD2M","WD10M","WS10M")

    # another approach is by renaming.
    # final_df.show()
    WD2M

    # write dataframe to file in csv format
    wind_df.coalesce(1).write.csv(f'file:///home/hadoop/Documents/temp/{city["City"]}_wind', header=True, mode="ignore")
