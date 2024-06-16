# Databricks notebook source
# MAGIC %run "./02 Creating Spark Data Frame to Select and Rename Columns"

# COMMAND ----------

help(users_df.select)

# COMMAND ----------

users_df.select('*').show()

# COMMAND ----------

users_df.select('id','first_name','last_name').show()

# COMMAND ----------

users_df.select(['id','first_name','last_name']).show()


# COMMAND ----------

# Defining aliases to the Data Frame
users_df.alias('u').select('u.id','u.first_name','u.last_name').show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

users_df.select(col('id'),col('first_name'),'last_name').show()

# COMMAND ----------

from pyspark.sql.functions import col,concat,lit

# COMMAND ----------

# Concaniting first_name last_name to full_name

users_df.select('first_name','last_name',concat('first_name',lit(', '),'last_name').alias('full_name')).show()

# COMMAND ----------

