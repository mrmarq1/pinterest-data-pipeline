# Databricks notebook source
from pyspark import pandas as ps

# COMMAND ----------

# MAGIC %run ./db_data_handling

# COMMAND ----------

ps.sql('''
  SELECT 
      geo.country,
      pin.category,
      COUNT(category) AS category_count
  FROM {pin_df} pin INNER JOIN {geo_df} geo
  ON pin.ind = geo.ind
  GROUP BY country, category
  ORDER BY country ASC, category_count DESC
''')

# COMMAND ----------


