# Databricks notebook source
# MAGIC %run "./02 Creating Spark Data Frame to Select and Rename Columns"

# COMMAND ----------

from pyspark.sql.functions import col,lit,concat


# COMMAND ----------

# required column name list from data
require_names = ['id','first_name','last_name','email','phone_numbers','courses']

# Renamed column list 
target_column_names = ['user.id','user_first_name','user_last_name','user_email','user_phone_number','enrolled_courses']

# COMMAND ----------

help(users_df.toDF)

# COMMAND ----------

users_df.select(require_names).show()

# COMMAND ----------


#giving error because of target_column_name is giving one object
users_df.select(require_names).toDF(target_column_names).show()

# COMMAND ----------

users_df.select(require_names).toDF(*target_column_names).show()


# COMMAND ----------

def my_df(*cols):
    print(type(cols))
    print(cols)



# COMMAND ----------

# Here output is in single tuple object
my_df(['f1','f2'])

# COMMAND ----------

# Now its distinguised tuple object
my_df(*['f1','f2'])


# COMMAND ----------

