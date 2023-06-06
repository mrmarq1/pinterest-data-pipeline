# Databricks notebook source
# MAGIC %run ./db_data_handling

# COMMAND ----------

from pyspark import pandas as ps

# COMMAND ----------

# What's the most popular Pinterest category in each country? 

ps.sql('''
  WITH country_cats_counted AS
  (SELECT 
      geo.country,
      pin.category,
      COUNT(category) AS category_count
  FROM {pin_df} pin INNER JOIN {geo_df} geo
  ON pin.ind = geo.ind
  GROUP BY country, category
  ORDER BY country ASC, category_count DESC),
  country_cats_ranked AS 
  (SELECT 
      *, 
      RANK() OVER(PARTITION BY country ORDER BY category_count DESC) AS ranking 
  FROM country_cats_counted)

  SELECT
      country,
      category,
      category_count
  FROM country_cats_ranked
  WHERE ranking = 1
''')

# COMMAND ----------

# What's the most popular Pinterest category each year? 

ps.sql('''
  WITH year_cat AS
  (SELECT 
      YEAR(geo.timestamp) AS post_year,
      pin.category,
      COUNT(category) AS category_count
  FROM {geo_df} geo INNER JOIN {pin_df} pin
  ON geo.ind = pin.ind  
  WHERE YEAR(geo.timestamp) BETWEEN 2018 AND 2022
  GROUP BY category, post_year
  ORDER BY post_year, category_count DESC),
  yr_cats_ranked AS
  (SELECT 
      *, 
      RANK() OVER(PARTITION BY post_year ORDER BY category_count DESC) AS ranking 
  FROM year_cat)

  SELECT
      post_year,
      category,
      category_count 
  FROM yr_cats_ranked
  WHERE ranking = 1
''')

# COMMAND ----------

# What's the user with the most followers in each country? 

follower_count_table = ps.sql('''
      WITH country_posters_ranked AS 
      (SELECT 
          country, 
          poster_name, 
          follower_count,
          RANK() OVER (PARTITION BY country ORDER BY follower_count DESC) as ranking
      FROM {pin_df} pin INNER JOIN {geo_df} geo
      ON pin.ind = geo.ind
      GROUP BY poster_name, country, follower_count
      ORDER BY country)
      
      SELECT
          country, 
          poster_name, 
          follower_count
      FROM country_posters_ranked
      WHERE ranking = 1
''')

display(follower_count_table)

# COMMAND ----------

# What's the country with the user that has the most followers? 

ps.sql('''      
      SELECT 
          country,
          follower_count
      FROM {follower_count_table}
      WHERE follower_count = (SELECT MAX(follower_count) FROM {follower_count_table})
''')

# COMMAND ----------

# What's the most popular catgeory for different age groups? 

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

# What's the median follower count for different age groups? 

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

# How many users have joined each year?

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

# What's the median follower count for users that joined between 2015 and 2020?

ps.sql('''      
      WITH 
      year_follower_cnt AS
      (SELECT 
          YEAR(g.timestamp) AS post_year,
          p.follower_count
      FROM {geo_df} g INNER JOIN {pin_df} p
      ON g.ind = p.ind
      WHERE YEAR(g.timestamp) BETWEEN 2015 AND 2020
      ORDER BY post_year),
      year_follower_cnt_row_numbered AS 
      (SELECT
          *,
          ROW_NUMBER() OVER(PARTITION BY post_year ORDER BY follower_count) AS row_no      
      FROM year_follower_cnt
      ORDER BY post_year
      ),
      year_follower_cnt_rows_counted AS
      (SELECT
        *,
        MAX(row_no) OVER(PARTITION BY post_year) AS max_row_no
      FROM year_follower_cnt_row_numbered)

      SELECT
        post_year, 
        follower_count AS median_follower_count
      FROM year_follower_cnt_rows_counted
      WHERE row_no = ROUND(max_row_no/2)
      ORDER BY post_year
''')

# COMMAND ----------

# What's the median follower count for users based on their joining year and age group?

ps.sql('''      
      WITH 
      follower_cnt_age_post_yr AS
      (SELECT 
          (CASE
            WHEN u.age BETWEEN 18 AND 24 THEN '18-24'
            WHEN u.age BETWEEN 25 AND 35 THEN '25-35'
            WHEN u.age BETWEEN 36 AND 50 THEN '36-50'
            WHEN u.age > 50 THEN '50+'
          END) AS age_group,
          YEAR(g.timestamp) AS post_year,
          p.follower_count
      FROM {user_df} u INNER JOIN {geo_df} g
      ON u.ind = g.ind
      INNER JOIN {pin_df} p
      ON g.ind = p.ind
      WHERE YEAR(g.timestamp) BETWEEN 2015 AND 2020
      ORDER BY age_group, post_year),
      row_numbered AS 
      (SELECT
          *,
          ROW_NUMBER() OVER(PARTITION BY age_group, post_year ORDER BY follower_count) AS row_no      
      FROM follower_cnt_age_post_yr
      ORDER BY age_group, post_year),
      max_rows_by_age_yr AS
      (SELECT
        *,
        MAX(row_no) OVER(PARTITION BY age_group, post_year) AS max_row_no
      FROM row_numbered)

      SELECT
        age_group, 
        post_year, 
        follower_count AS median_follower_count
      FROM max_rows_by_age_yr
      WHERE row_no = ROUND(max_row_no/2)
      ORDER BY post_year
''')
