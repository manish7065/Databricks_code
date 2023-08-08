# Databricks notebook source
users_list = [(1,'Scott'),(2,'Donalds'),(3,"Mickey"),(4,"Elice")]

# COMMAND ----------

type(users_list)

# COMMAND ----------

type(users_list[1])

# COMMAND ----------

spark.createDataFrame(users_list,"id int, name string")

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

def dummy(*args):
    print(args)
    print(len(args))

# COMMAND ----------

user_details = (1,"Scott")

# COMMAND ----------

dummy(user_details)

# COMMAND ----------

dummy(*user_details)

# COMMAND ----------

users_rows = [Row(*user) for user in users_list]

# COMMAND ----------

users_rows

# COMMAND ----------

spark.createDataFrame(users_rows)

# COMMAND ----------

spark.createDataFrame(users_list, "id int,name string")

# COMMAND ----------

spark.createDataFrame(users_list, )