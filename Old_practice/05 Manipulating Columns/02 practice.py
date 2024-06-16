# Databricks notebook source
# Reading data

orders = spark.read.csv('/public/retail_db/orders',schema='order_id INT, order_date STRING, order_customer_id INT, order_status STRING')

# COMMAND ----------

orders.show()

# COMMAND ----------

from pyspark.sql.functions import date_format

# COMMAND ----------

orders.printSchema()

# COMMAND ----------

# Function as part of projections

orders.select('*', date_format('order_date', 'yyyyMM').alias('order_month')).show()

# COMMAND ----------

orders.select('*', date_format('order_date', 'yyyyMM').alias('order_month')).printSchema()

# COMMAND ----------


# using withColumn
orders.withColumn('order_month',date_format('order_date','yyyyMM')).show()

# COMMAND ----------

orders.withColumn('order_month',date_format('order_date','yyyyMM')).printSchema()


# COMMAND ----------

orders.filter(date_format('order_date','yyyyMM')==201401).show()

# COMMAND ----------

orders.filter(date_format('order_date','yyyyMM')==201401).show()


# COMMAND ----------

# function as apart of groupby
# total order / month
orders.groupBy(date_format('order_date','yyyyMM').alias('order_month')).count().show()

# COMMAND ----------

