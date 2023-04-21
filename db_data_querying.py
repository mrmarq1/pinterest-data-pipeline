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

follower_count_table = ps.sql('''
      WITH cte1 AS 
      (SELECT 
          country, 
          poster_name, 
          follower_count,
          RANK() OVER (PARTITION BY country ORDER BY follower_count DESC) as rank
      FROM {pin_df} pin INNER JOIN {geo_df} geo
      ON pin.ind = geo.ind
      GROUP BY poster_name, country, follower_count
      ORDER BY country),
      cte2 AS     
      (SELECT
          country, 
          poster_name, 
          follower_count
      FROM cte1
      WHERE rank = 1)
      
      SELECT 
          *
      FROM cte2
''')

display(follower_count_table)

# COMMAND ----------

ps.sql('''      
      SELECT 
          country,
          follower_count
      FROM {follower_count_table}
      WHERE follower_count = (SELECT MAX(follower_count) FROM {follower_count_table})
''')

# COMMAND ----------

ps.sql('''      
      WITH cte1 AS
      (SELECT 
          (CASE
            WHEN u.age BETWEEN 18 AND 24 THEN '18-24'
            WHEN u.age BETWEEN 25 AND 35 THEN '25-35'
            WHEN u.age BETWEEN 36 AND 50 THEN '36-50'
            WHEN u.age > 50 THEN '50+'
          END) AS age_group,
          p.category,
          COUNT(p.category) AS category_count      
      FROM {user_df} u INNER JOIN {pin_df} p
      ON u.ind = p.ind
      GROUP BY age_group, p.category
      ORDER BY age_group),
      cte2 AS 
      (SELECT
          *,
          RANK(category_count) OVER(PARTITION BY age_group ORDER BY category_count DESC) AS category_count_rank
      FROM cte1
      )

      SELECT
        age_group,
        category,
        category_count
      FROM cte2
      WHERE category_count_rank = 1
''')

# COMMAND ----------


