# Databricks notebook source
# MAGIC %run "./02 Creating Spark Data Frame to Select and Rename Columns"

# COMMAND ----------

users_df.select('id','first_name','last_name').show()

# COMMAND ----------

from pyspark.sql.functions import concat,lit,col

# COMMAND ----------

# Creating new column using alias
users_df.select(
    'id','first_name','last_name',
    concat('first_name','last_name').alias('full_name')
).show()

# COMMAND ----------

# Creaating new column with withColumn
users_df.select('id','first_name','last_name').withColumn('full_name',concat('first_name',lit(', '),'last_name')).show()


# COMMAND ----------

help(users_df.withColumn)

# COMMAND ----------

from pyspark.sql.functions import size

# COMMAND ----------

users_df.select('id','courses').withColumn('course_count',size('courses')).show()

# COMMAND ----------

