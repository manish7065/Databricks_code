# Databricks notebook source
users_list = [(1,"Scott"), (2, "Donald"), (3, "Mickey"), (4,"Elvis")]
df = spark.createDataFrame(users_list, 'id int, name string')

# COMMAND ----------

df.show()

# COMMAND ----------

df.collect()

# COMMAND ----------

type(df.collect())

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

help(Row)

# COMMAND ----------

row1 = Row("Alice", 11)
row1

# COMMAND ----------

row2 = Row(name="Alice", age=11)

# COMMAND ----------

row2.name

# COMMAND ----------

row2['name']

# COMMAND ----------

