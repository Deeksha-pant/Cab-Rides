# Importing required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


#Initializing Spark session
spark=SparkSession   \
      .builder  \
      .appName("Kafka-to-HDFS") \
      .master("local")  \
      .getOrCreate()
spark

#Reading the .json files from hdfs
df=spark.read.json("/user/root/clickstream_dump/part-00000-cf7f4676-ae09-4e99-97df-2276f068cdf8-c000.json")

#Selecting the required columns from .json files 
df=df.select(get_json_object(df['value_str'],"$.customer_id").alias("customer_id"),
            get_json_object(df['value_str'],"$.app_version").alias("app_version"),
            get_json_object(df['value_str'],"$.OS_version").alias("OS_version"),
            get_json_object(df['value_str'],"$.lat").alias("lat"),
            get_json_object(df['value_str'],"$.lon").alias("lon"),
            get_json_object(df['value_str'],"$.page_id").alias("page_id"),
            get_json_object(df['value_str'],"$.button_id").alias("button_id"),
            get_json_object(df['value_str'],"$.is_button_click").alias("is_button_click"),
            get_json_object(df['value_str'],"$.is_page_view").alias("is_page_view"),
            get_json_object(df['value_str'],"$.is_scroll_up").alias("is_scroll_up"),
            get_json_object(df['value_str'],"$.is_scroll_down").alias("is_scroll_down"),
            get_json_object(df['value_str'],"$.timestamp").alias("timestamp")
             )

#validating schema
df.printSchema()

#Validating few records 
df.show(10)

#Writing the data to hdfs as .csv file
df.coalesce(1).write.format('csv').mode('overwrite').save('/user/root/clickstream',header='true')