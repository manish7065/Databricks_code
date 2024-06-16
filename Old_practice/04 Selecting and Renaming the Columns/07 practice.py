# Databricks notebook source
# MAGIC %run "./02 Creating Spark Data Frame to Select and Rename Columns "

# COMMAND ----------

users_df['id']

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

col('id')

# COMMAND ----------

users_df.select('id','first_name','last_name').show()

# COMMAND ----------

cols = ['id','first_name','last_name']
users_df.select(*cols).show()

# COMMAND ----------

help(col)

# COMMAND ----------

user_id = col('id')

# COMMAND ----------

users_df.select(user_id).show()

# COMMAND ----------

users_df.select('id','customer_from').show()

# COMMAND ----------

users_df.select('id','customer_from').dtypes

# COMMAND ----------

from pyspark.sql.functions import date_format

# COMMAND ----------

users_df.select(
    col('id'),
    date_format('customer_from', 'yyyMMdd')).show()

# COMMAND ----------

users_df.select(
    col('id'),
    date_format('customer_from', 'yyyMMdd')).printSchema()

# COMMAND ----------

users_df.select(
    col('id'),
    date_format('customer_from', 'yyyMMdd').alias('customer_from')
    ).show()

# COMMAND ----------

users_df.select(
    col('id'),
    date_format('customer_from', 'yyyMMdd').cast('int').alias('customer_from')
    ).show()

# COMMAND ----------

users_df.select(
    col('id'),
    date_format('customer_from', 'yyyMMdd').cast('int').alias('customer_from')
    ).printSchema()

# COMMAND ----------

cols = [col('id'), date_format('customer_from','yyyyMMdd').cast('int').alias('customer_from')]
users_df.select(*cols).show()

# COMMAND ----------

