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

ps.sql('''
  WITH cte1 AS
  (SELECT 
      YEAR(geo.timestamp) AS post_year,
      pin.category
  FROM {geo_df} geo INNER JOIN {pin_df} pin
  ON geo.ind = pin.ind)
  
  SELECT 
      *,
      COUNT(category) AS category_count
  FROM cte1
  WHERE post_year BETWEEN 2018 AND 2022
  GROUP BY category, post_year
  ORDER BY category ASC, post_year ASC
''')

# COMMAND ----------


