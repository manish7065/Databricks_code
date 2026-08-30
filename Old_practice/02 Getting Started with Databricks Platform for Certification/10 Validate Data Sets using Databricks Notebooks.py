# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %fs ls /databricks-datasets

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets
# MAGIC

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %cat databricks-datasets/SPARK_README.md

# COMMAND ----------

# MAGIC %fs ls /public/retail_db

# COMMAND ----------

# MAGIC %fs ls /public/retail_db/

# COMMAND ----------

# MAGIC %fs ls /public/retail_db/orders

# COMMAND ----------

# MAGIC %fs ls /public/retail_db_json

# COMMAND ----------

# MAGIC %fs ls /public/retail_db_json/orders

# COMMAND ----------

dbutils.fs.ls('/public/retail_db_json/orders')

# COMMAND ----------

for file_details in dbutils.fs.ls('/public/retail_db_json'):
  print(file_details)

# COMMAND ----------

for file_details in dbutils.fs.ls('/public/retail_db_json'):
  print(file_details.path)

# COMMAND ----------


