# Databricks notebook source
ages_list = [(21,),(23,),(41,),(32,)]

# COMMAND ----------

type(ages_list)

# COMMAND ----------

type(ages_list[2])


# COMMAND ----------

spark.createDataFrame(ages_list)

# COMMAND ----------

spark.createDataFrame(ages_list,'age int')

# COMMAND ----------

users_list = [(1, 'Scott'),(2,'Donalds'),(3,'Micky'),(4,"Elvis")]

# COMMAND ----------

spark.createDataFrame(users_list)

# COMMAND ----------

spark.createDataFrame(users_list, 'id int, name string')

# COMMAND ----------

