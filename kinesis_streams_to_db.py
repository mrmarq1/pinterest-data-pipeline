# Databricks notebook source
dbutils.fs.ls('/FileStore/tables/authentication_credentials.csv')

# COMMAND ----------

from pyspark.sql.functions import *
import urllib
import pyspark.pandas as ps
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType

file_type = 'csv'
first_row_is_header = 'true'
delimiter = ','

aws_keys_df = spark.read.format(file_type)\
.option('header', first_row_is_header)\
.option('sep', delimiter)\
.load('/FileStore/tables/authentication_credentials.csv')

ACCESS_KEY = aws_keys_df.where(col('User name')=='databricks-user').select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.where(col('User name')=='databricks-user').select('Secret access key').collect()[0]['Secret access key']
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")


def get_data_by_topic(topic_ext):
    KINESIS_STREAM_NAME = f'streaming-0a2bc878981f-{topic_ext}'
    KINESIS_REGION = 'us-east-1'

    kinesis_df = spark.readStream.format('kinesis') \
                .option('streamName', KINESIS_STREAM_NAME) \
                .option('region', KINESIS_REGION) \
                .option('initialPosition', 'LATEST') \
                .option('format', 'json') \
                .option('awsAccessKey', ACCESS_KEY) \
                .option('awsSecretKey', SECRET_KEY) \
                .option('inferSchema', 'true') \
                .load()

    return kinesis_df

dfs = {topic_ext: get_data_by_topic(topic_ext) for topic_ext in ('pin', 'geo', 'user')}

# COMMAND ----------

schema = StructType([
            StructField('index', IntegerType()),
            StructField('unique_id', StringType()),
            StructField('title', StringType()),
            StructField('description', StringType()),
            StructField('poster_name', StringType()),
            StructField('follower_count', StringType()),
            StructField('tag_list', StringType()),
            StructField('is_image_or_video', StringType()),
            StructField('image_src', StringType()),
            StructField('downloaded', IntegerType()),
            StructField('save_location', StringType()),
            StructField('category', StringType())
        ])

pin_df = dfs['pin'] \
        .selectExpr('CAST(data AS STRING)') \
        .select(from_json('data', schema).alias('data')) \
        .select('data.*')

# COMMAND ----------

from pyspark.sql.functions import when

pin_df = pin_df.withColumn('description', 
    when(pin_df.description.contains('No description available'),regexp_replace(pin_df.description,'No description available','None')) \
   .when(pin_df.description.contains('No description available Story format'),regexp_replace(pin_df.description,'No description available Story format','None')) \
   .when(pin_df.description.contains('Untitled'),regexp_replace(pin_df.description,'Untitled','None'))\
   .otherwise(pin_df.description))

pin_df = pin_df.withColumn('follower_count', 
    when(pin_df.follower_count.contains('User Info Error'),regexp_replace(pin_df.follower_count,'User Info Error','None')) \
   .when(pin_df.follower_count.contains('k'),regexp_replace(pin_df.follower_count,'k','000')) \
   .otherwise(pin_df.follower_count))

pin_df = pin_df.withColumn('follower_count', pin_df.follower_count.cast('int'))

pin_df = pin_df.withColumn('image_src', 
    when(pin_df.image_src.contains('Image src error.'),regexp_replace(pin_df.image_src,'Image src error.', 'None')) \
    .otherwise(pin_df.image_src))

pin_df = pin_df.withColumnRenamed('index', 'ind')

pin_df = pin_df.withColumn('poster_name', 
    when(pin_df.poster_name.contains('User Info Error.'), regexp_replace(pin_df.poster_name,'User Info Error.', 'None')) \
    .otherwise(pin_df.poster_name))

pin_df = pin_df.withColumn('save_location', 
    when(pin_df.save_location.contains('Local save in'), regexp_replace(pin_df.save_location,'Local save in','')) \
    .otherwise(pin_df.poster_name))

pin_df = pin_df.withColumn('tag_list', 
    when(pin_df.tag_list.contains('N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e'), regexp_replace(pin_df.tag_list,'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e','None')) \
    .otherwise(pin_df.tag_list))

pin_df = pin_df.withColumn('title', 
    when(pin_df.title.contains('No Title Data Available'), regexp_replace(pin_df.title,'No Title Data Available','None')) \
    .otherwise(pin_df.title))

pin_df = pin_df.select('ind', 'unique_id', 'title', 'description', 'follower_count', 'poster_name', 'tag_list', 'is_image_or_video', 'image_src', 'save_location', 'category')


# COMMAND ----------

pin_df.writeStream.format('delta').outputMode("append").option("checkpointLocation", "/tmp/delta/_checkpoints/").start("/delta/pin_df")

# COMMAND ----------

schema = StructType([
            StructField('country', StringType()),
            StructField('ind', IntegerType()),
            StructField('latitude', StringType()),
            StructField('longitude', StringType()),
            StructField('timestamp', TimestampType())
        ])

geo_df = dfs['geo'] \
        .selectExpr('CAST(data AS STRING)') \
        .select(from_json('data', schema).alias('data')) \
        .select('data.*')

# COMMAND ----------

geo_df = geo_df.select('ind', 'country', concat_ws(',', geo_df.latitude, geo_df.longitude).alias('coordinates'), 'timestamp')
geo_df = geo_df.select('ind', 'country', split(col('coordinates'),',').alias('coordinates'), 'timestamp')

# COMMAND ----------

geo_df.writeStream.format('delta').outputMode("append").option("checkpointLocation", "/tmp/delta/_checkpoints/").start("/delta/geo_df")

# COMMAND ----------

schema = StructType([
            StructField('age', IntegerType()),
            StructField('date_joined', TimestampType()),
            StructField('first_name', StringType()),
            StructField('ind', IntegerType()),
            StructField('last_name', StringType())
        ])

user_df = dfs['user'] \
        .selectExpr('CAST(data AS STRING)') \
        .select(from_json('data', schema).alias('data')) \
        .select('data.*')

# COMMAND ----------

user_df = user_df.select('ind', concat_ws(' ', user_df.first_name, user_df.last_name).alias('user_name'), 'age', 'date_joined')


# COMMAND ----------

user_df.writeStream.format('delta').outputMode("append").option("checkpointLocation", "/tmp/delta/_checkpoints/").start("/delta/user_df")

# COMMAND ----------

delta_pin_df = spark.read.format("delta").load("/delta/pin_df")
delta_pin_df.show() 

# COMMAND ----------

delta_geo_df = spark.read.format("delta").load("/delta/geo_df")
delta_geo_df.show() 

# COMMAND ----------

delta_user_df = spark.read.format("delta").load("/delta/user_df")
delta_user_df.show() 
