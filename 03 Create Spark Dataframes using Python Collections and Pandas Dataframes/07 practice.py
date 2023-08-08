# Databricks notebook source
users_list = [
    {'user_id': 1, 'user_first_name': 'Scott'},
    {'user_id': 2, 'user_first_name': 'Donald'},
    {'user_id': 3, 'user_first_name': 'Mickey'},
    {'user_id': 4, 'user_first_name': 'Elvis'}
]

# COMMAND ----------

users_list

# COMMAND ----------

# Creating data frame from dictionary is depricated and Row method is more preferred
spark.createDataFrame(users_list)

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

help(Row)

# COMMAND ----------

user_details = users_list[1]

# COMMAND ----------

user_details

# COMMAND ----------

user_details.values()

# COMMAND ----------

print(len(user_details.values()))
print(len(user_details.values()))

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

user_row = [Row(user.values()) for user in users_list ]

# COMMAND ----------

user_row

# COMMAND ----------

user_row = [Row(*user.values()) for user in users_list ]
user_row

# COMMAND ----------

spark.createDataFrame(user_row)

# COMMAND ----------

spark.createDataFrame(user_row, 'id int, name string')

# COMMAND ----------

users_rows = [Row(**user) for user in users_list] 

# COMMAND ----------

spark.createDataFrame(users_rows)


# COMMAND ----------

def dummy(**kwargs):
    print(kwargs)
    print(len(kwargs))

# COMMAND ----------

user_details = {'user_id':1, 'user_first_name': "Scott"}

# COMMAND ----------

dummy(user_details)

# COMMAND ----------

dummy(user_details=user_details)

# COMMAND ----------

dummy(**user_details)

# COMMAND ----------

