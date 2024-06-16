# Databricks notebook source
# MAGIC %run "./02 Creating Spark Data Frame to Select and Rename Columns"

# COMMAND ----------

data1


# COMMAND ----------

help(users_df.selectExpr)

# COMMAND ----------

users_df.selectExpr('*').show()

# COMMAND ----------

# Defining alias to the dataframe
users_df.alias('u').selectExpr('u.*').show()

# COMMAND ----------

users_df.selectExpr('id','first_name','last_name').show()

# COMMAND ----------

from pyspark.sql.functions import concat,col,lit

# COMMAND ----------

users_df.select('id','first_name','last_name',concat(col('first_name'),lit(', '),'last_name').alias('full_name')).show()

# COMMAND ----------

# Using selectExpr to use spark SQL functions
users_df.selectExpr('first_name','last_name',"concat(first_name,', ',last_name) AS full_name").show()

# COMMAND ----------

users_df.createOrReplaceTempView("users")

# COMMAND ----------

# using concatination 
spark.sql("""
    SELECT id,first_name,last_name,
    concat(first_name,', ',last_name) AS full_name
    FROM users
""").show()

# COMMAND ----------

spark.sql("SELECT first_name FROM users").show()

# COMMAND ----------

use