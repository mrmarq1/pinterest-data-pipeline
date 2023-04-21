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

ps.sql('''      
      WITH 
      cte1 AS
      (SELECT 
          (CASE
            WHEN u.age BETWEEN 18 AND 24 THEN '18-24'
            WHEN u.age BETWEEN 25 AND 35 THEN '25-35'
            WHEN u.age BETWEEN 36 AND 50 THEN '36-50'
            WHEN u.age > 50 THEN '50+'
          END) AS age_group,
          p.follower_count
      FROM {user_df} u INNER JOIN {pin_df} p
      ON u.ind = p.ind
      ORDER BY age_group),
      cte2 AS 
      (SELECT
          *,
          ROW_NUMBER() OVER(PARTITION BY age_group ORDER BY follower_count) AS row_no      
      FROM cte1
      ORDER BY age_group
      ),
      cte3 AS
      (SELECT
        *,
        MAX(row_no) OVER(PARTITION BY age_group) AS max_row_no
      FROM cte2)

      SELECT
        age_group, 
        follower_count AS median_follower_count
      FROM cte3
      WHERE row_no = ROUND(max_row_no/2)
      ORDER BY age_group
''')

# COMMAND ----------

ps.sql('''      
      SELECT 
          YEAR(g.timestamp) AS post_year,
          COUNT(YEAR(u.date_joined)) AS number_users_joined
      FROM {geo_df} g INNER JOIN {user_df} u
      ON g.ind = u.ind
      GROUP BY post_year
      HAVING post_year BETWEEN 2015 AND 2020
      ORDER BY post_year
''')

# COMMAND ----------

ps.sql('''      
      WITH 
      cte1 AS
      (SELECT 
          YEAR(g.timestamp) AS post_year,
          p.follower_count
      FROM {geo_df} g INNER JOIN {pin_df} p
      ON g.ind = p.ind
      WHERE YEAR(g.timestamp) BETWEEN 2015 AND 2020
      ORDER BY post_year),
      cte2 AS 
      (SELECT
          *,
          ROW_NUMBER() OVER(PARTITION BY post_year ORDER BY follower_count) AS row_no      
      FROM cte1
      ORDER BY post_year
      ),
      cte3 AS
      (SELECT
        *,
        MAX(row_no) OVER(PARTITION BY post_year) AS max_row_no
      FROM cte2)

      SELECT
        post_year, 
        follower_count AS median_follower_count
      FROM cte3
      WHERE row_no = ROUND(max_row_no/2)
      ORDER BY post_year
''')

# COMMAND ----------


