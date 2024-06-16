# Databricks notebook source
# MAGIC %run "./02 Creating Spark Data Frame to Select and Rename Columns "

# COMMAND ----------

from pyspark.sql.functions import lit,concat,col

# COMMAND ----------

help(concat)

# COMMAND ----------

full_name_col = concat(col('first_name'),lit(', '),col('last_name'))

# COMMAND ----------

full_name_alias=full_name_col.alias('full_name')

# COMMAND ----------

type(full_name_alias)

# COMMAND ----------

users_df.select('id',full_name_alias).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## . convert customer_from date data type to numeric type

# COMMAND ----------

from pyspark.sql.functions import date_format

# COMMAND ----------

users_df.select('id','customer_from').show()

# COMMAND ----------

from pyspark.sql.functions import date_format

# COMMAND ----------

date_format('customer_from','yyyMMdd')

# COMMAND ----------

date_format('customer_from','yyyyMMdd').cast('int')

# COMMAND ----------

customer_from_alias = date_format('customer_from','yyyyMMdd').cast('int').alias('customer_from')

# COMMAND ----------

users_df.select('id',customer_from_alias).show()

# COMMAND ----------

users_df.select('id',customer_from_alias).dtypes

# COMMAND ----------

