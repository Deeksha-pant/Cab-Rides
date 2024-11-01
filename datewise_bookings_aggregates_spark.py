# Importing required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# Iniliating Spark session
spark=SparkSession.builder.appName("datewise_bookings_aggregates_spark").master("local").getOrCreate()
spark

#Reading bookings data from HDFS
df=spark.read.csv("/user/root/bookings/part-m-00000")

#Validating the schema
df.printSchema()

# Validating few records
df.show(10)

#Validating the number of records 
df.count()

#List of new column names
new_col = ["booking_id","customer_id","driver_id","customer_app_version","customer_phone_os_version","pickup_lat","pickup_lon","drop_lat",
          "drop_lon","pickup_timestamp","drop_timestamp","trip_fare","tip_amount","currency_code","cab_color","cab_registration_no","customer_rating_by_driver",
          "rating_by_customer","passenger_count"]

#Renaming dataframe columns as per new list of column names
new_df = df.toDF(*new_col)

#Converting pickup_timestamp to date by extracting date from pickup_timestamp for aggregation
new_df=new_df.select("booking_id","customer_id","driver_id","customer_app_version","customer_phone_os_version","pickup_lat","pickup_lon","drop_lat",
          "drop_lon",to_date(col('pickup_timestamp')).alias('pickup_date').cast("date"),"drop_timestamp","trip_fare","tip_amount","currency_code","cab_color","cab_registration_no","customer_rating_by_driver",
          "rating_by_customer","passenger_count")


#Validating few records
new_df.show(5)

#Aggregating the data on pickup_date
agg_df=new_df.groupBy("pickup_date").count().orderBy("pickup_date")

#Validating few records
agg_df.show(5)

# writing the aggregated data to HDFS as .csv
agg_df.coalesce(1).write.format('csv').mode('overwrite').save('/user/root/datewise_bookings_agg',header='true')