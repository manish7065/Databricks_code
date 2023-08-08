# Databricks notebook source
# MAGIC %run "./02 Creating Spark Data Frame to Select and Rename Columns"

# COMMAND ----------

users_df.createOrReplaceTempView('users')


# COMMAND ----------

spark.sql("""
select id, amount_paid + 25 as amount_paid
from users 
""").show()

# COMMAND ----------

users_df.selectExpr('id', "amount_paid + 25 AS amount_paid").show()

# COMMAND ----------

# This will fail
users_df.select('id', 'amount_paid' + 25).show()

# COMMAND ----------

from pyspark.sql.functions import lit,col

# COMMAND ----------

users_df.select('amount_paid', 'amount_paid'.cast('int') + lit('25')

# COMMAND ----------

users_df.printSchema()

# COMMAND ----------

df=users_df.withColumn('amount_paid',col('amount_paid').cast('int'))
df.printSchema()

# COMMAND ----------

# Here lit is working with column object
df.select(col('amount_paid')+lit(25)).show()

# COMMAND ----------

df.select('amount_paid').dtypes

# COMMAND ----------

users_df.select('id', col('amount_paid') + lit(25.0)).show()


# COMMAND ----------

# lit returns column type
lit(25.0)

# COMMAND ----------

