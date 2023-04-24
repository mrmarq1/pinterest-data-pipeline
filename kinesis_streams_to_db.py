# Databricks notebook source
dbutils.fs.ls('/FileStore/tables/authentication_credentials.csv')

# COMMAND ----------

from pyspark.sql.functions import *
import urllib
import pyspark.pandas as ps
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

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

    pyspark_df = kinesis_data[topic_ext] \
                .selectExpr('CAST(data AS STRING)') \
                .select(from_json('data', schema).alias('data')) \
                .select('data.*')

    return pyspark_df


dfs = {topic_ext: get_data_by_topic(topic_ext) for topic_ext in ('pin', 'geo', 'user')}
display(dfs['pin'])

# COMMAND ----------

from pyspark.sql.functions import when

dfs['pin'] = dfs['pin'].withColumn('description', 
    when(dfs['pin'].description.contains('No description available'),regexp_replace(dfs['pin'].description,'No description available','None')) \
   .when(dfs['pin'].description.contains('No description available Story format'),regexp_replace(dfs['pin'].description,'No description available Story format','None')) \
   .when(dfs['pin'].description.contains('Untitled'),regexp_replace(dfs['pin'].description,'Untitled','None'))\
   .otherwise(dfs['pin'].description))

dfs['pin'] = dfs['pin'].withColumn('follower_count', 
    when(dfs['pin'].follower_count.contains('User Info Error'),regexp_replace(dfs['pin'].follower_count,'User Info Error','None')) \
   .when(dfs['pin'].follower_count.contains('k'),regexp_replace(dfs['pin'].follower_count,'k','000')) \
   .otherwise(dfs['pin'].follower_count))

dfs['pin'] = dfs['pin'].withColumn('follower_count', dfs['pin'].follower_count.cast('int'))

dfs['pin'] = dfs['pin'].withColumn('image_src', 
    when(dfs['pin'].image_src.contains('Image src error.'),regexp_replace(dfs['pin'].image_src,'Image src error.', 'None')) \
    .otherwise(dfs['pin'].image_src))

dfs['pin'] = dfs['pin'].withColumnRenamed('index', 'ind')

dfs['pin'] = dfs['pin'].withColumn('poster_name', 
    when(dfs['pin'].poster_name.contains('User Info Error.'), regexp_replace(dfs['pin'].poster_name,'User Info Error.', 'None')) \
    .otherwise(dfs['pin'].poster_name))

dfs['pin'] = dfs['pin'].withColumn('save_location', 
    when(dfs['pin'].save_location.contains('Local save in'), regexp_replace(dfs['pin'].save_location,'Local save in','')) \
    .otherwise(dfs['pin'].poster_name))

dfs['pin'] = dfs['pin'].withColumn('tag_list', 
    when(dfs['pin'].tag_list.contains('N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e'), regexp_replace(dfs['pin'].tag_list,'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e','None')) \
    .otherwise(dfs['pin'].tag_list))

dfs['pin'] = dfs['pin'].withColumn('title', 
    when(dfs['pin'].title.contains('No Title Data Available'), regexp_replace(dfs['pin'].title,'No Title Data Available','None')) \
    .otherwise(dfs['pin'].title))

pin_df = dfs['pin'].select('ind', 'unique_id', 'title', 'description', 'follower_count', 'poster_name', 'tag_list', 'is_image_or_video', 'image_src', 'save_location', 'category')


# COMMAND ----------

display(pin_df)

# COMMAND ----------


