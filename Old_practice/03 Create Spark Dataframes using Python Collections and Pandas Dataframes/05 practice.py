# Databricks notebook source
users_list = [[1,"Scott"],[2,"Donald"],[3,"Mickey"],[4,'Elvis']]

# COMMAND ----------

type(users_list)

# COMMAND ----------

type(users_list[1])


# COMMAND ----------

# Method1
spark.createDataFrame(users_list, 'id int, name string')

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

# Method 2 (List comprehension)
users_rows = [Row(*user) for user in users_list]

# COMMAND ----------

users_rows

# COMMAND ----------

spark.createDataFrame(users_rows)

# COMMAND ----------

spark.createDataFrame(users_rows, "id int, name string")

# COMMAND ----------

def dummy(*args):
    print(args)
    print(len(args))

# COMMAND ----------

dummy(1)

# COMMAND ----------

dummy(1,"Man")

# COMMAND ----------

user_datails = [1, 'scott']

# COMMAND ----------

dummy(user_datails)

# COMMAND ----------

dummy(*user_datails)

# COMMAND ----------

