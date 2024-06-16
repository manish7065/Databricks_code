# Databricks notebook source
# MAGIC %run "./02 Creating Spark Data Frame to Select and Rename Columns "

# COMMAND ----------

users_df['id']

# COMMAND ----------

from pyspark.sql.functions import col


# COMMAND ----------

# Both users_df['id'] and col('id') giving the same type
col('id')

# COMMAND ----------

type(users_df['id'])

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

users_df.select('id', col('first_name'), 'last_name').show()

# COMMAND ----------

users_df.select(users_df['id'],col('first_name'),'last_name').show()

# COMMAND ----------

users_df.alias('u').select(u['id'], col('first_name'), 'last_name').show()

# COMMAND ----------

users_df.alias('u').select('u.id',col('first_name'),'last_name').show()

# COMMAND ----------

users_df.selectExpr(col('id'),'first_name','last_name').show()

# COMMAND ----------

from pyspark.sql.functions import concat,lit,col

# COMMAND ----------

users_df.\
    select(
        'id','first_name','last_name',
        concat(users_df['first_name'],lit(', '),col('last_name')).alias("full_name")
    ).show()

# COMMAND ----------

# Using selectExpr to use spark sql functions
users_df.alias('u').selectExpr('id','first_name','last_name',"concat(u.first_name,', ',u.last_name) AS full_name").show()

# COMMAND ----------

users_df.createOrReplaceTempView('users')

# COMMAND ----------

spark.sql("""
SELECT id,first_name,last_name,
concat(u.first_name,', ',u.last_name) as full_name
from users as u
""").show()

# COMMAND ----------

