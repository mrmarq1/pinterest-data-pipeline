# Pinterest Data Pipeline

This project aims to create a Pinterest data pipeline using simulated user data. The project will leverage a Cloud-based stack to process and manage both batch and streaming data. That is, the data will be processed by Kafka running on a MSK cluster in an AWS EC2 instance before being transferred to a S3 bucket. Said bucket will be mounted to Databricks and the related data flow will be orchestrated by Airflow. Within Databricks the batch and streamed data will be transformed and queried before being stored in Delta Tables.      

## Project Motivation

- Broadly, to replicate commercial data engineering workflows that increasingly rely on Cloud-based systems and, in turn, gain hands-on experience with related technologies. Moreover, from a data science perspective, to gain insight into the practical considerations of sourcing, processing and storing data when provisioning downstream teams.

- More specifically, Pinterest user data is a suitable fit for batch and streamed data processing. Furthermore, said data contains a mixture of data types and presents cleaning challenges for the transformation phase. Lastly, if extending the project at later point, the tabular outputs are congruent with various algorithms, including those for NLP tasks.  

## Learning Points

- How to set up and configure tools in an EC2 instance to allow for the transfer of data to a S3 bucket via an AWS Gateway API.
- What the use cases are for Spark's Pandas API.
- How Databricks works with AWS and the functionalities it provides.
- What Delta Tables are and their merits vis-a-vis traditional storage solutions.
- What some of the practical challenges are for Data Engineering and how they may relate to Data Science tasks. 

## Project Breakdown

### Processing simulated data into s3 bucket

- Function created with indefinite 'while' loop implemented to pull tabular data from source using pymysql and sqlalchemy. Stored outputs in 3 distinct data objects ('pin_data', 'geo_data' and 'user_data') in line with source tables. 

- Passed data objects to a function that serialized them as JSON objects and transferred them to a AWS S3 bucket via a HTTP protocol-enabled AWS Gateway API.

### Reviewing S3 status and orchestrating data flow

- Checked data objects successfully stored in S3 bucket under related 'pin', 'geo' and 'user' related topics.

### Batch processing in Databricks

- Mounted S3 bucket to Databricks.
- Defined function to extract data from s3 bucket and store it in Spark Dataframes.
- Transformed the batch data using the Pandas API and used SQL to query the resulting data.

### Automate data flow
- Set up and configured Airflow with associated Python scripting (DAG) to pass data from bucket to Databricks.

### Streaming data into Databricks

- Used AWS Kinesis to set up data streams for subsequent Databricks ingestion.
- Transformed the streamed data using PySpark (as Pandas API incompatible), queried it once again using SQL and stored resulting data in Delta Tables. 
