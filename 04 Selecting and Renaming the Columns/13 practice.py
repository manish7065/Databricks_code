# Databricks notebook source
# MAGIC %run "./02 Creating Spark Data Frame to Select and Rename Columns"

# COMMAND ----------

from pyspark.sql.functions import col,lit,concat

# COMMAND ----------

# Renaming the column using alias and col 
users_df.select(
    col('id').alias('user_id'),
    col('first_name').alias('user_first_name'),
    col('last_name').alias('user_last_name'),
).show()

# COMMAND ----------

# renaming the columns first then adding full_name colume usinf withColumn
users_df.select(
    col('id').alias('user_id'),
    col('first_name').alias('user_first_name'),
    col('last_name').alias('user_last_name'),
).withColumn('full_name',concat('user_first_name',lit(', '),'user_last_name')).show()

# COMMAND ----------

#  first adding full_name colume usinf withColumn then renaming the columns
users_df.withColumn('full_name',concat('first_name',lit(', '),'last_name')).select(
    col('id').alias('user_id'),
    col('first_name').alias('user_first_name'),
    col('last_name').alias('user_last_name'),
    'full_name'
).show()

# COMMAND ----------

#  first adding full_name colume usinf withColumn  then renaming the columns
users_df.withColumn('full_name',concat(users_df['first_name'],lit(', '),'last_name')).select(
    col('id').alias('user_id'),
    col('first_name').alias('user_first_name'),
    col('last_name').alias('user_last_name'),
    'full_name'
).show()

# COMMAND ----------

