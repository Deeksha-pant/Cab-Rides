#Importing required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# Initializing Spark session
spark = SparkSession \
       .builder \
       .appName("Kafka-to-local") \
       .getOrCreate()

#Reading clickstream data from kafka topic
df = spark.readStream \
       .format("kafka") \
       .option("kafka.bootstrap.servers", "18.211.252.152:9092") \
       .option("subscribe", "de-capstone5") \
       .option("startingOffsets", "earliest") \
       .load()

#Dropping not required columns and transforming columns
df= df \
      .withColumn('value_str',df['value'].cast('string').alias('key_str')).drop('value') \
      .drop('key','topic','partition','offset','timestamp','timestampType')

#Writing data from kakfa to HDFS
df.writeStream \
  .format("json") \
  .outputMode("append") \
  .option("path", "/user/root/clickstream_dump") \
  .option("checkpointLocation", "/user/root/clickstream_dump_checkpoint") \
  .start() \
  .awaitTermination()
