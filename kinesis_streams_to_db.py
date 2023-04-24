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

# Convert all instances of missing 'description' data into 'None'

pin_df = dfs['pin'].withColumn('description', 
    when(dfs['pin'].description.contains('No description available'),regexp_replace(dfs['pin'].description,'No description available','None')) \
   .when(dfs['pin'].description.contains('No description available Story format'),regexp_replace(dfs['pin'].description,'No description available Story format','None')) \
   .when(dfs['pin'].description.contains('Untitled'),regexp_replace(dfs['pin'].description,'Untitled','None'))\
   .otherwise(dfs['pin'].description))


# COMMAND ----------

display(pin_df)

# COMMAND ----------


