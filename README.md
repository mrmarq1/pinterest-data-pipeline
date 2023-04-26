# Pinterest Data Pipeline

This project aims to create a Pinterest data pipeline using simulated user data. The project will leverage a cloud-based stack to process and manage both batch and streaming data. More specifically, using an AWS EC2 instance, the data will be processed by Kafka running on a MSK cluster before being transferred to a s3 bucket. Said bucket will be mounted to Databricks for subsequent transformation and querying purposes.      

## Processing simulated data into s3 bucket

Python script created to loop through data and send the related JSON objects to cloud storage based on three distinct Kafka topics.

## Mounting s3 bucket to Databricks

Pulled data into Databricks, cleaned it using the Pandas API and executed a range of SQL-based queries.

## Streaming data into Databricks

Pulled data into Databricks, cleaned the associated Spark Dataframes and saved transformed datasets in Delta Tables.
